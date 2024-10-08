#include "postgres.h"
#include "c.h"
#include "fmgr.h"


#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/stratnum.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "executor/tuptable.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"


#include "access/attnum.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/model.h"
#include "utils/rel.h"
#include "utils/syscache.h"
// #include "utils/varlena.h"



#define QUOTEMARK '"'


// сделать shmem
static Oid mlJsonWrapperOid = InvalidOid;

// static char*  quotation(char* in);


inline char* 
strip(char *p)
{
    char *p2 = p;

    while(*p2 == ' ' || *p2 == ',' )
        p2++;
    return p2;
}

// static char* 
// quotation(char* in)
// {
//     char * out, *p , *p2;
//     p = in;

//     elog(WARNING, "text='%s'", p);
//     p2 = out = palloc0(256);
    
//    while (p = strip(p))
//     {    
//         if (*p == 0)
//             break;
//         *(p2++) = QUOTEMARK;
//         while ( isalpha(*p) || isdigit(*p) )
//         {
//             *p2++ = *p++ ;
//         }
//         *(p2++) = QUOTEMARK;
//         *(p2++) = ',';
//     }
//     *(p2--) = '\0'; 
//     *(p2--) = '\0'; 
//     return out;
// }



/*
 * Get a tuple descriptor for CREATE MODEL
 */
TupleDesc
GetCreateModelResultDesc(void)
{
	TupleDesc   tupdesc;

	/* need a tuple descriptor representing three TEXT columns */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "accuracy",
					   TEXTOID, -1, 0);
	return tupdesc;
}



/*
 * Get a tuple descriptor for PREDICT MODEL
 */

TupleDesc GetPredictModelResultDesc(PredictModelStmt *node){

	TupleDesc   tupdesc;
	IndexScanDesc scan;
	TupleTableSlot* slot;
	Relation rel, idxrel;
	HeapTuple tup;
	ScanKeyData skey[1];
	Form_pg_class form;

	Oid reloid  = get_relname_relid((const char*)node->tablename,(Oid) PG_PUBLIC_NAMESPACE);

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(reloid));

	if (!HeapTupleIsValid(tup))
	    elog(ERROR, "cache lookup failed for relation %d", reloid);
	form = (Form_pg_class) GETSTRUCT(tup);

	ReleaseSysCache(tup);

	tupdesc = CreateTemplateTupleDesc(form->relnatts);

	rel = table_open(AttributeRelationId, RowExclusiveLock);
	idxrel = index_open(AttributeRelidNumIndexId, AccessShareLock);

	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1, 0);

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid, 
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(reloid));

	index_rescan(scan, skey, 1, NULL, 0 );

	slot = table_slot_create(rel, NULL);
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		Form_pg_attribute record;
		bool should_free;

		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		record = (Form_pg_attribute) GETSTRUCT(tup);
		if (record->attnum < 0) continue;
		TupleDescInitEntry(tupdesc, (AttrNumber) record->attnum,  NameStr(record->attname),
							record->atttypid, -1, 0);
	}
	
	index_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	index_close(idxrel, AccessShareLock);
	table_close(rel, RowExclusiveLock);

	return tupdesc;
}

/*
 * Model accuratly 
 */
void
ModelExecute(CreateModelStmt *stmt, DestReceiver *dest)
{
	TupOutputState *tstate;
	TupleDesc   tupdesc;
	int len;
	// char *p, *p2;
	// Datum options;
	ListCell  *lc;
	StringInfoData  buf;
	Datum res;
	float4 out;
	char *res_out;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');


	foreach(lc, stmt->options)
	{        
		ModelOptElement *opt;
		opt = (ModelOptElement *) lfirst(lc);
				

		elog(WARNING, "ModelOptElement parm=%d value=%s", opt->parm, opt->value);
		if (opt->parm == MODEL_PARAMETER_TARGET)
		{
			appendStringInfo(&buf, "\"target\":\"%s\"", (char*)opt->value);
		}
		else
		{
			if (opt->value){
				appendStringInfo(&buf, "\"ignored\":[\"%s\"]",opt->value);
			}
			else
			{
				ListCell  *lc2;
				StrModelElement *el;
				appendStringInfo(&buf, "\"ignored\":[");
				foreach( lc2, opt->elements)
				{
					el = (StrModelElement *) lfirst(lc2);
					appendStringInfo(&buf,"\"%s\",", el->value);
				}
				len = buf.len;
				*(buf.data + len - 1) = ']';
			}
		}
		appendStringInfo(&buf, ",");
	}

	len = buf.len;
	*(buf.data + len - 1) = '}';
	elog(WARNING, "***** options : %s", buf.data);

	resetStringInfo(&buf);	

	appendStringInfo(&buf, "{\"ignored\":[\"name\"], \"target\":\"res\"}");

	// buf.data[buf.len-2] = '}';

	if (mlJsonWrapperOid == InvalidOid)
	{
		mlJsonWrapperOid = GetProcOidByName("ml_learn");
	}
	elog(WARNING, "call func by oid %d", mlJsonWrapperOid);
	elog(WARNING, "options %s", buf.data);
	res = OidFunctionCall4(  mlJsonWrapperOid, 
					CStringGetTextDatum(stmt->modelname),
					DatumGetInt32(stmt->modelclass),
					CStringGetTextDatum(buf.data),
					CStringGetTextDatum(stmt->tablename));


	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "accuracy",
							  TEXTOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

	out = DatumGetFloat8(res);
	res_out = psprintf("%g", out);

	/* Send it */
	do_text_output_oneline(tstate, res_out);

	end_tup_output(tstate);
}

Oid
GetProcOidByName(const char* proname)
{
	Oid found_oid = -1;
	Relation rel, idxrel;
	NameData fname;
	IndexScanDesc scan;
	TupleTableSlot* slot;

	HeapTuple tup;
	ScanKeyData skey[1];
	Name name = &fname;

	namestrcpy(name, proname);

	rel = table_open(ProcedureRelationId, RowExclusiveLock);
	idxrel = index_open(ProcedureOidIndexId, AccessShareLock);


	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1 /* nkeys */, 0 /* norderbys */);

	ScanKeyInit(&skey[0],
				Anum_pg_proc_proname , 
				BTGreaterEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(name));



	index_rescan(scan, skey, 1, NULL /* orderbys */, 0 /* norderbys */);

	slot = table_slot_create(rel, NULL);
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		Form_pg_proc record;
		bool should_free;

		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		record = (Form_pg_proc) GETSTRUCT(tup);

		if(strcmp(record->proname.data, name->data) == 0)
		{
			found_oid = record->oid;
			if(should_free) heap_freetuple(tup);
			break;
		}

		if(should_free) heap_freetuple(tup);
	}

	index_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	index_close(idxrel, AccessShareLock);
	table_close(rel, RowExclusiveLock);

	if (found_oid == InvalidOid)
		elog(ERROR, "procedure %s not found", proname);

	// elog(WARNING, "oid=%d", found_oid);
	return found_oid;
}
