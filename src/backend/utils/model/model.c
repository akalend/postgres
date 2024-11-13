#include <sys/stat.h>
#include <errno.h>
#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/stratnum.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"
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


static Form_pg_class GetPredictTableFormByName(const char *tablename);
static char * read_whole_file(const char *filename, int *length);


// сделать shmem
static Oid mlJsonWrapperOid = InvalidOid;
// static Oid PredictTableOid = InvalidOid;
static Oid MetadataTableOid = InvalidOid;
static Oid MetadataTableIdxOid = InvalidOid;



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


/**
 * @TODO тут нужно распарсить tablename, если в нем указано tablespace, то использовать tablespace
 * и еще посмотреть в path_search.
 * */
static Form_pg_class
GetPredictTableFormByName(const char *tablename)
{
	HeapTuple tup;
	Form_pg_class form;
	Oid PredictTableOid;

	PredictTableOid = get_relname_relid(tablename,(Oid) PG_PUBLIC_NAMESPACE);

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(PredictTableOid));

	if (!HeapTupleIsValid(tup))
	    elog(ERROR, "cache lookup failed for relation %d", PredictTableOid);
	form = (Form_pg_class) GETSTRUCT(tup);

	ReleaseSysCache(tup);
	return form;
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
	Oid PredictTableOid;

	form = GetPredictTableFormByName((const char*)node->tablename);
	PredictTableOid = form->oid;

	tupdesc = CreateTemplateTupleDesc(form->relnatts +1);

	rel = table_open(AttributeRelationId, RowExclusiveLock);
	idxrel = index_open(AttributeRelidNumIndexId, AccessShareLock);

	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1, 0);

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(PredictTableOid));

	index_rescan(scan, skey, 1, NULL, 0 );

	slot = table_slot_create(rel, NULL);
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		Form_pg_attribute record;
		bool should_free;

		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		record = (Form_pg_attribute) GETSTRUCT(tup);
		if (record->attnum < 0) continue;
		TupleDescInitEntry(tupdesc, (AttrNumber) record->attnum, NameStr(record->attname),
							record->atttypid, -1, 0);
	}

	TupleDescInitEntry(tupdesc, (AttrNumber) (form->relnatts + 1), "class",
							INT4OID, -1, 0);

	index_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	index_close(idxrel, AccessShareLock);
	table_close(rel, RowExclusiveLock);

	return tupdesc;
}

void
PredictModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest)
{
	Relation rel;
	HeapTuple tup;
	TableScanDesc scan;
	SysScanDesc sscan;
	TupleDesc tupdesc;
	TupOutputState *tstate;
	Datum *values, *outvalues;
	bool *nulls, *outnulls;
	ScanKeyData skey[1];
	Form_pg_class form;
	MemoryContext resultcxt, oldcxt;
	Oid PredictTableOid;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;
	oldcxt = MemoryContextSwitchTo(resultcxt);

	form = GetPredictTableFormByName((const char*)stmt->tablename);
	tupdesc = CreateTemplateTupleDesc(form->relnatts + 1);

	PredictTableOid = form->oid;

	values = (Datum*)palloc0( sizeof(Datum) * form->relnatts);
	nulls = (bool *) palloc0(sizeof(bool) * form->relnatts);
	outvalues = (Datum*)palloc0( sizeof(Datum) * (form->relnatts + 1));
	outnulls = (bool *) palloc0(sizeof(bool) * (form->relnatts + 1));

	/* attribute table scanning */
	rel = table_open(AttributeRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(PredictTableOid));


	sscan = systable_beginscan(rel, AttributeRelidNumIndexId, true,
							   SnapshotSelf, 1, &skey[0]);

	while ((tup = systable_getnext(sscan)) != NULL)
	{
		Form_pg_attribute record;
		record = (Form_pg_attribute) GETSTRUCT(tup);
		if (record->attnum < 0) continue;

		TupleDescInitEntry(tupdesc, (AttrNumber) record->attnum, NameStr(record->attname),
							record->atttypid, -1, 0);
	}

	TupleDescInitEntry(tupdesc, (AttrNumber) form->relnatts+1, "class", INT4OID, -1, 0);

	systable_endscan(sscan);
	table_close(rel, RowExclusiveLock);

	/* end create tupledesc of out data*/


	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);


	rel = table_open(PredictTableOid, AccessShareLock);
	scan = table_beginscan(rel, GetLatestSnapshot(), 0, NULL);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		int i;
		CHECK_FOR_INTERRUPTS();
		if (!HeapTupleIsValid(tup))
		{
			elog(ERROR, " lookup failed for tuple");
		}

		/* Data row */
		heap_deform_tuple(tup, 	tupdesc, values, nulls);

		for (i=0; i < form->relnatts; i++)
		{
					outvalues[i] = values[i];
					outnulls[i] = nulls[i];
		}
		outvalues[form->relnatts] = 777;
		do_tup_output(tstate, outvalues, outnulls);
	}
	end_tup_output(tstate);

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Model accuratly 
 */
void
CreateModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest)
{
	TupOutputState *tstate;
	TupleDesc   tupdesc;
	HeapTuple tup;
	int len;
	ListCell  *lc;
	StringInfoData  buf;
	Datum res;
	float4 out;
	char *res_out;
	Relation rel, idxrel;
	IndexScanDesc scan;
	TupleTableSlot* slot;
	ScanKeyData skey[1];
	NameData	name_name;
	bool found = false;
	Datum  *values;
	bool   *nulls, *doReplace;
	struct stat st;
	int rc;
	const char* tmp_name = tempnam("/tmp/", "cbm_");

	namestrcpy(&name_name, stmt->modelname);
	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');

	foreach(lc, stmt->options)
	{
		ModelOptElement *opt;
		opt = (ModelOptElement *) lfirst(lc);
				
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

	if (mlJsonWrapperOid == InvalidOid)
	{
		mlJsonWrapperOid = GetProcOidByName(ML_MODEL_LEARN_FUNCTION);
	}
	res = OidFunctionCall5(  mlJsonWrapperOid, 
					CStringGetTextDatum(stmt->modelname),
					DatumGetInt32(stmt->modelclass),
					CStringGetTextDatum(buf.data),
					CStringGetTextDatum(stmt->tablename),
					CStringGetTextDatum(tmp_name));


	rc = stat(tmp_name, &st);
	if (rc)
	{
		const char * errmsg = strerror(errno);
		elog(ERROR, "Temporaly model file \"%s\" not found\n%s", tmp_name, errmsg);
	}


	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "accuracy",
							  TEXTOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

	out = DatumGetFloat8(res);
	res_out = psprintf("%g", out);


	/* save metadata  */

	if (MetadataTableOid == InvalidOid)
	{
			MetadataTableOid  = get_relname_relid(ML_MODEL_METADATA, PG_PUBLIC_NAMESPACE);
			MetadataTableIdxOid = get_relname_relid(ML_MODEL_METADATA_IDX, PG_PUBLIC_NAMESPACE);
	}


	tupdesc = CreateTemplateTupleDesc(Natts_model);

	TupleDescInitEntry(tupdesc, 1, "name", NAMEOID, -1, 0);
	TupleDescInitEntry(tupdesc, 2, "file", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 3, "model_type", BPCHAROID, -1, 0); // 1042
	TupleDescInitEntry(tupdesc, 4, "acc", FLOAT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, 5, "info", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 6, "args", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 7, "data", BYTEAOID, -1, 0);



	rel = table_open(MetadataTableOid, RowExclusiveLock);
	idxrel = index_open(MetadataTableIdxOid, AccessShareLock);

	values = (Datum*)palloc0( sizeof(Datum) * Natts_model);
	nulls = (bool *) palloc0(sizeof(bool) * Natts_model);
	doReplace = (bool *) palloc0(sizeof(bool) * Natts_model);

	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1 /* nkeys */, 0 /* norderbys */);

	ScanKeyInit(&skey[0],
				Anum_ml_name ,
				BTGreaterEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&name_name));

	index_rescan(scan, skey, 1, NULL /* orderbys */, 0 /* norderbys */);

	slot = table_slot_create(rel, NULL);
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		bool should_free;
		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		
		heap_deform_tuple(tup,  tupdesc, values, nulls);

		if(should_free) heap_freetuple(tup);
		elog(WARNING,"OK, FOUND should_free=%d", should_free);
		found = true;
	}

	if (!found)
	{
		elog(WARNING,"record NOT FOUND");
	}
	else
	{
		int file_length = 0;
		bytea *result;
		char *s;
		elog(WARNING, "name:%s type=%s acc=%g [%s]", DatumGetCString(values[0]), 
			DatumGetCString(values[2]), DatumGetFloat4(values[3]), buf.data);

		nulls[3] = false;
		values[3] = Float4GetDatum(out);
		doReplace[3]  = true;

		nulls[5] = false;
		values[5] = CStringGetTextDatum(buf.data);
		doReplace[5]  = true;
	
		s = read_whole_file(tmp_name, &file_length);
		result = (text *) palloc(file_length + VARHDRSZ);

		SET_VARSIZE(result, len + VARHDRSZ);
		memcpy(VARDATA(result), s, len);
		pfree(s);

		elog(WARNING, "readfile %d bytes", file_length);		
		nulls[6] = false;
		values[6] = PointerGetDatum(result);
		doReplace[6]  = true;

		tup = heap_modify_tuple(tup, tupdesc,values, nulls, doReplace);
		if (HeapTupleIsValid(tup))
		{
			CatalogTupleUpdate(rel, &tup->t_self, tup);
		}
	}

	index_endscan(scan);
	index_close(idxrel, AccessShareLock);
	table_close(rel, RowExclusiveLock);

	ExecDropSingleTupleTableSlot(slot);


	/* Send it */
	do_text_output_oneline(tstate, res_out);

	end_tup_output(tstate);

	if (rc == 0)
		remove(tmp_name);
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

	return found_oid;
}

static char *
read_whole_file(const char *filename, int *length)
{
	char	   *buf;
	FILE	   *file;
	size_t		bytes_to_read;
	struct stat fst;

	if (stat(filename, &fst) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", filename)));

	if (fst.st_size > (MaxAllocSize - 1))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("file \"%s\" is too large", filename)));
	bytes_to_read = (size_t) fst.st_size;

	if ((file = AllocateFile(filename, PG_BINARY_R)) == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						filename)));

	buf = (char *) palloc(bytes_to_read + 1);

	*length = fread(buf, 1, bytes_to_read, file);

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));

	FreeFile(file);

	buf[*length] = '\0';
	return buf;
}
