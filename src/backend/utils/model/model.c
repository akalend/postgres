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
#include "utils/c_api.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/model.h"
#include "utils/rel.h"
#include "utils/syscache.h"

// #include "utils/varlena.h"



#define QUOTEMARK '"'

static TupleDesc GetMlModelTableDesc(void);
static ModelCalcerHandle* GetMlModelByName(const char * name);
static char * GetFeaturesInfo(ModelCalcerHandle *modelHandle, int *resultLen);
static char* CreateJsonModelParameters(CreateModelStmt *stmt);
static Datum LoadFileToBuffer(const char * tmp_name,  int file_length,void **model_buffer);
static Datum GetFeaturesFieldInfo(void* model_buffer, int file_length);
static ml_Features_Count CreateTemplateTypesOfRecord(ModelCalcerHandle *modelHandle, TupleDesc tupdesc, int32** arrTypes);


static char * ArrayToStringList(char **featureName, int featureCount);
static char * IntArrayToStringList(size_t *featureData, int featureCount);

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


/*
 * Get a tuple descriptor for ml+nodel table
 */
static TupleDesc
GetMlModelTableDesc(void)
{

	TupleDesc   tupdesc = CreateTemplateTupleDesc(Natts_model);

	TupleDescInitEntry(tupdesc, 1, "name", NAMEOID, -1, 0);
	TupleDescInitEntry(tupdesc, 2, "fieldlist", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 3, "model_type", BPCHAROID, -1, 0); // 1042
	TupleDescInitEntry(tupdesc, 4, "acc", FLOAT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, 5, "info", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 6, "args", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, 7, "data", BYTEAOID, -1, 0);
	return tupdesc;
}

/* create in model options parameter*/
static char*
CreateJsonModelParameters(CreateModelStmt *stmt)
{
	int len;
	StringInfoData  buf;
	ListCell  *lc;
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
	return buf.data;
}


static Datum
LoadFileToBuffer(const char * tmp_name,  int file_length, void **model_buffer)
{
	bytea *result;
	int len;
	result = (text *) palloc(file_length + VARHDRSZ);
	*model_buffer = read_whole_file(tmp_name, &len);

	SET_VARSIZE(result, file_length + VARHDRSZ);
	memcpy(VARDATA(result), *model_buffer, file_length);
	
	return PointerGetDatum(result);
}


static Datum 
GetFeaturesFieldInfo(void* model_buffer, int file_length)
{
	char *modelInfo;
	int len;
	text *infoOutDatum;
	ModelCalcerHandle *modelHandle = ModelCalcerCreate();
	if (!LoadFullModelFromBuffer(modelHandle, model_buffer, file_length))
	{
		elog(ERROR, "LoadFullModelFromBuffer error message: %s\n", GetErrorString());
	}


	modelInfo = GetFeaturesInfo(modelHandle, &len);

	infoOutDatum = (text *) palloc(len + VARHDRSZ);
	SET_VARSIZE(infoOutDatum, len + VARHDRSZ);
	memcpy(VARDATA(infoOutDatum), modelInfo, len);

	ModelCalcerDelete(modelHandle);
	return PointerGetDatum(infoOutDatum);
}


static char *
GetFeaturesInfo(ModelCalcerHandle *modelHandle, int *resultLen)
{
	char *strbuf, *data;
	size_t *indices;
	size_t featureCount;
	char** featureName ; 

	StringInfo outInfoString = makeStringInfo();

	MemoryContext resultcxt, oldcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt =  AllocSetContextCreate(TopMemoryContext,
											"FeatureInfoContext",
											ALLOCSET_DEFAULT_SIZES);

	oldcxt = MemoryContextSwitchTo(resultcxt);


	featureName = palloc(sizeof(void*) * FIELDCOUNT);
	if (! GetModelUsedFeaturesNames(modelHandle, &featureName, &featureCount))
	{
		elog(ERROR,"get model feature names error: %s", GetErrorString());
	}

	if (featureCount > FIELDCOUNT)
		elog(ERROR, "count of field %ld is overflow.", featureCount);


	strbuf = ArrayToStringList(featureName, featureCount);

	appendStringInfo(outInfoString, "{ \"fieldList\":\"%s\",", strbuf);
	
	
	featureCount = GetCatFeaturesCount(modelHandle);
	indices = palloc0(sizeof(size_t) * featureCount);

	
	if (!GetCatFeatureIndices(modelHandle, &indices, &featureCount))
	{
		elog(ERROR,"CatBoost error: %s", GetErrorString());
	}


	strbuf = IntArrayToStringList(indices, featureCount);
	appendStringInfo(outInfoString, " \"CategoryFieldList\":\"%s\",", strbuf);
	// pfree(strbuf);
	// pfree(indices);



	featureCount = GetFloatFeaturesCount(modelHandle);
	indices = palloc0(sizeof(size_t) * featureCount);

	if (!GetFloatFeatureIndices(modelHandle, &indices, &featureCount))
	{
		elog(ERROR,"CatBoost error: %s", GetErrorString());
	}


	strbuf = IntArrayToStringList(indices, featureCount);
	appendStringInfo(outInfoString, " \"FloatFieldList\":\"%s\"}", strbuf);
	// pfree(strbuf);
	// pfree(indices);


	data = outInfoString->data;

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(resultcxt);
	
	// pfree(outInfoString);
	*resultLen = outInfoString->len;
	return data;
}


static char *
ArrayToStringList(char **featureName, int featureCount)
{
	int i = 0;
	char *p;
	char * strbuf = palloc( NAMEDATALEN * featureCount);
	
	p = strbuf;

	for(i = 0; i < featureCount; i++)
	{
		if (featureName[i] == NULL)
			elog(ERROR, "feature #%d is null", i);

		strcpy(p, featureName[i]);
		p += strlen(featureName[i]);
		*p = ',';
		p++;
	}
	
	*--p = '\0';
	return strbuf;
}


static char *
IntArrayToStringList(size_t *featureData, int featureCount)
{
	int i = 0;
	char * data;
	StringInfo buf = makeStringInfo();

	for(i = 0; i < featureCount; i++)
	{
		appendStringInfo(buf, "%ld,", featureData[i]);
	}

	*(buf->data + buf->len -1) = '\0';
	data = 	buf->data;
	pfree(buf);
	return data;
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

static ml_Features_Count
CreateTemplateTypesOfRecord(ModelCalcerHandle *modelHandle, TupleDesc tupdesc, int32** arrTypes)
{
	int32 i,j;
	size_t *  arrFloat;
	size_t *  arrCat;
	char **featureNames;
	size_t  cat_count2, float_count2, featureCount;
	int32 table_natts = tupdesc->natts;
	int32 *p = *arrTypes;
	ml_Features_Count features_count; 

	features_count.cat_cnt = GetCatFeaturesCount(modelHandle);
	features_count.float_cnt = GetFloatFeaturesCount(modelHandle);

	if (!GetModelUsedFeaturesNames(modelHandle, &featureNames, &featureCount))
	{
		elog(ERROR,"get model feature names: %s", GetErrorString());
	}

	if (!GetFloatFeatureIndices(modelHandle, &arrFloat , &float_count2))
	{
		elog(ERROR,"get model float feature indexes: %s", GetErrorString());
	}

	if (!GetCatFeatureIndices(modelHandle, &arrCat , &cat_count2))
	{
		elog(ERROR,"get model categorical feature indexes: %s", GetErrorString());
	}

	for (i = 0; i < table_natts; i++)
	{
		for (j=0; j < features_count.cat_cnt; j++)
		{
			if (strcmp(tupdesc->attrs[i].attname.data , featureNames[arrFloat[j]]) == 0)
			{
				p[i] = ML_FEATURE_FLOAT;
			}
		}
	}

	for (i = 0; i < table_natts; i++)
	{
		for (j=0; j < features_count.float_cnt; j++)
		{
			if (strcmp(tupdesc->attrs[i].attname.data, featureNames[arrCat[j]]) == 0)
			{
				p[i] = ML_FEATURE_CATEGORICAL;
			}
		}
	}

	free(arrFloat); // allocated in c_api GetFloatFeatureIndices
	free(arrCat);   // allocated in c_api GetCatFeatureIndices
	free(featureNames); // возможно стоит удалить  каждый элемент featureNames

	return features_count;
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
	ModelCalcerHandle *modelHandle;
	int32  i, table_natts;
	int32 *  arrTypes;
	ml_Features_Count features_count;
	char **arrCat;
	float *arrFloat;
	size_t model_dimension;
	double* result_pa;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;
	oldcxt = MemoryContextSwitchTo(resultcxt);

	form = GetPredictTableFormByName((const char*)stmt->tablename);
	table_natts = form->relnatts;

	arrTypes = palloc0(sizeof(int32) * table_natts);



	tupdesc = CreateTemplateTupleDesc(form->relnatts + 1);
	PredictTableOid = form->oid;

	values = (Datum*)palloc0( sizeof(Datum) * table_natts);
	nulls = (bool *) palloc0(sizeof(bool) * table_natts);
	outvalues = (Datum*)palloc0( sizeof(Datum) * (table_natts + 1));
	outnulls = (bool *) palloc0(sizeof(bool) * (table_natts + 1));

	/* attribute table scanning */
	rel = table_open(AttributeRelationId, RowExclusiveLock); // shareLock ??

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

	modelHandle = GetMlModelByName((const char*)stmt->modelname);
	model_dimension = (size_t)GetDimensionsCount(modelHandle);
	result_pa  = (double*) palloc( sizeof(double) * model_dimension);

	features_count = CreateTemplateTypesOfRecord(modelHandle, tupdesc, &arrTypes);

	arrCat   = palloc0(sizeof(char*) * features_count.cat_cnt);
	arrFloat = palloc0(sizeof(float) * features_count.float_cnt);


	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);


	rel = table_open(PredictTableOid, AccessShareLock);
	scan = table_beginscan(rel, GetLatestSnapshot(), 0, NULL);

	{
		int32 j=0, float_cnt =0, cat_cnt = 0;
		while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			int i;
			float_cnt =0;
			cat_cnt = 0;
			if (j++ > 3) break;

			CHECK_FOR_INTERRUPTS();
			if (!HeapTupleIsValid(tup))
			{
				elog(ERROR, " lookup failed for tuple");
			}

			/* Data row */
			heap_deform_tuple(tup, 	tupdesc, values, nulls);

			for (i=0; i < form->relnatts; i++)
			{
				if (arrTypes[i] == ML_FEATURE_FLOAT)
				{

					switch (tupdesc->attrs[i].atttypid)
					{
						case FLOAT4OID: 
							arrFloat[float_cnt] = (float)DatumGetFloat4(values[i]);
							break;
						case FLOAT8OID: 
							arrFloat[float_cnt] = (float)DatumGetFloat8(values[i]);
							break;
						case INT4OID: 
							arrFloat[float_cnt] = (float) DatumGetInt32(values[i]);
							break;
						case INT8OID: 
							arrFloat[float_cnt] = (float)DatumGetInt64(values[i]);
							break;
						case INT2OID: 
							arrFloat[float_cnt] = (float)DatumGetInt16(values[i]);
							break;
						case BOOLOID: 
							arrFloat[float_cnt] = (float)DatumGetBool(values[i]);
							break;
						default:
							elog(ERROR,"field %s type oid=%d undefined", NameStr(tupdesc->attrs[i].attname), tupdesc->attrs[i].atttypid);
					}

					elog(WARNING, "float field %s", NameStr(tupdesc->attrs[i].attname));
					float_cnt++;
				}

				if (arrTypes[i] == ML_FEATURE_CATEGORICAL)
				{
					switch (tupdesc->attrs[i].atttypid)
					{
						case TEXTOID: 
						case BPCHAROID:
							arrCat[cat_cnt] = DatumGetCString(values[i]);
							break;
						default:
							elog(ERROR,"field %s type oid=%d undefined", NameStr(tupdesc->attrs[i].attname), tupdesc->attrs[i].atttypid);
					}
					cat_cnt++;
				}

				outvalues[i] = values[i];
				outnulls[i] = nulls[i];
			}

			if ( !CalcModelPredictionSingle(
			        modelHandle,
        			arrFloat, features_count.float_cnt,
        			arrCat, features_count.cat_cnt,
        			result_pa, model_dimension)
				)
			{
				elog(ERROR, "prediction error in row %d: %s",j, GetErrorString());
			}

			outvalues[form->relnatts] = 777;
			do_tup_output(tstate, outvalues, outnulls);
		}
	}
	end_tup_output(tstate);

	pfree(arrTypes);
	table_close(rel, AccessShareLock);

	table_endscan(scan);

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
	HeapTuple tup = NULL;
	Datum res;
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
	int file_length;
	void* model_buffer;
	char * str_parameter;


	namestrcpy(&name_name, stmt->modelname);

	str_parameter = CreateJsonModelParameters(stmt);


	if (mlJsonWrapperOid == InvalidOid)
	{
		mlJsonWrapperOid = GetProcOidByName(ML_MODEL_LEARN_FUNCTION);
	}
	res = OidFunctionCall5(  mlJsonWrapperOid, 
					CStringGetTextDatum(stmt->modelname),
					DatumGetInt32(stmt->modelclass),
					CStringGetTextDatum(str_parameter),
					CStringGetTextDatum(stmt->tablename),
					CStringGetTextDatum(tmp_name));


	rc = stat(tmp_name, &st);
	if (rc)
	{
		const char * errmsg = strerror(errno);
		elog(ERROR, "Temporaly model file \"%s\" not found\n%s", tmp_name, errmsg);
	}

	file_length = st.st_size;

	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = GetCreateModelResultDesc();


	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);


	/* save metadata  */
	if (MetadataTableOid == InvalidOid)
	{
			MetadataTableOid  = get_relname_relid(ML_MODEL_METADATA, PG_PUBLIC_NAMESPACE);
			MetadataTableIdxOid = get_relname_relid(ML_MODEL_METADATA_IDX, PG_PUBLIC_NAMESPACE);
	}


	tupdesc = GetMlModelTableDesc();

	values = (Datum*)palloc0( sizeof(Datum) * Natts_model);
	nulls = (bool *) palloc(sizeof(bool) * Natts_model);
	doReplace = (bool *) palloc0(sizeof(bool) * Natts_model);

	memset(nulls, true, (sizeof(nulls)));

	/* Found by model name in ml_model */
 
	rel = table_open(MetadataTableOid, RowExclusiveLock);
	idxrel = index_open(MetadataTableIdxOid, AccessShareLock);

	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1 /* nkeys */, 0 /* norderbys */);

	ScanKeyInit(&skey[0],
				Anum_ml_name ,
				BTGreaterEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&name_name));

	index_rescan(scan, skey, 1, NULL /* orderbys */, 0 /* norderbys */);

	slot = table_slot_create(rel, NULL);
	found = false;
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		bool should_free;
		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		
		heap_deform_tuple(tup,  tupdesc, values, nulls);

		if(should_free) heap_freetuple(tup);
		found = true;
	}

	nulls[Anum_ml_model_acc-1] = false;
	values[Anum_ml_model_acc-1] = Float4GetDatum(DatumGetFloat8(res));
	doReplace[Anum_ml_model_acc-1]  = true;

	nulls[Anum_ml_model_args-1] = false;
	values[Anum_ml_model_args-1] = CStringGetTextDatum(str_parameter);
	doReplace[Anum_ml_model_args-1]  = true;


	nulls[Anum_ml_model_data-1] = false;
	values[Anum_ml_model_data-1] = LoadFileToBuffer(tmp_name, file_length, &model_buffer);
	doReplace[Anum_ml_model_data-1]  = true;

	nulls[Anum_ml_model_fieldlist-1] = false;
	values[Anum_ml_model_fieldlist-1] = GetFeaturesFieldInfo(model_buffer, file_length);
	doReplace[Anum_ml_model_fieldlist-1]  = true;


	if (found)
	{
		tup = heap_modify_tuple(tup, tupdesc,values, nulls, doReplace);
		CatalogTupleUpdate(rel, &tup->t_self, tup);
	}
	

	index_endscan(scan);
	index_close(idxrel, AccessShareLock);
	ExecDropSingleTupleTableSlot(slot);

	if (!found)
	{
		nulls[Anum_ml_name-1] = false;
		values[Anum_ml_name-1] = CStringGetDatum(stmt->modelname);
		
		tup = heap_form_tuple(tupdesc, values, nulls);

		CatalogTupleInsert(rel, tup);
		heap_freetuple(tup);
	}
	
	table_close(rel, RowExclusiveLock);

	/* Send it */
	res_out = psprintf("%g", DatumGetFloat8(res));
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

static ModelCalcerHandle*
GetMlModelByName(const char * name)
{
	Relation rel, idxrel;;
	ScanKeyData skey[1];
	IndexScanDesc scan;
	NameData name_data;
	TupleTableSlot* slot;
	Datum  *values;
	bool   *nulls;
	TupleDesc tupdesc;

	bool found = false;
	namestrcpy(&name_data, name);
	
	
	if (MetadataTableOid == InvalidOid)
	{
			MetadataTableOid  = get_relname_relid(ML_MODEL_METADATA, PG_PUBLIC_NAMESPACE);
			MetadataTableIdxOid = get_relname_relid(ML_MODEL_METADATA_IDX, PG_PUBLIC_NAMESPACE);
	}

	values = (Datum*)palloc0( sizeof(Datum) * Natts_model);
	nulls = (bool *) palloc0(sizeof(bool) * Natts_model);
	tupdesc = GetMlModelTableDesc();

	rel = table_open(MetadataTableOid, RowExclusiveLock);
	idxrel = index_open(MetadataTableIdxOid, AccessShareLock);

	scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 1 /* nkeys */, 0 /* norderbys */);

	ScanKeyInit(&skey[0],
				Anum_ml_name ,
				BTGreaterEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&name_data));

	index_rescan(scan, skey, 1, NULL /* orderbys */, 0 /* norderbys */);

	slot = table_slot_create(rel, NULL);
	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		HeapTuple tup;
		bool should_free;
	
		tup = ExecFetchSlotHeapTuple(slot, false, &should_free);
		
		heap_deform_tuple(tup,  tupdesc, values, nulls);
		if(should_free) heap_freetuple(tup);
		found = true;
	}

	index_endscan(scan);
	index_close(idxrel, AccessShareLock);
	table_close(rel, RowExclusiveLock);

	ExecDropSingleTupleTableSlot(slot);

	if (found)
	{
		ModelCalcerHandle *modelHandle = ModelCalcerCreate();
		bytea	   *bstr = DatumGetByteaPP(values[6]);
		int len = VARSIZE(bstr);
		const char* bufferData = VARDATA(bstr);
		
		elog(WARNING, "name:%s datalen=%d acc=%g", DatumGetCString(values[0]), len, DatumGetFloat4(values[3]));
		if (!LoadFullModelFromBuffer(modelHandle, bufferData, len))
		{
			elog(ERROR, "LoadFullModelFromBuffer error message: %s\n", GetErrorString());
		}
		
		return modelHandle;
	}

	elog(ERROR, "name:%s not found", name);

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
