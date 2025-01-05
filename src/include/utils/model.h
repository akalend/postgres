#define ML_MODEL_METADATA "ml_model"
#define ML_MODEL_LEARN_FUNCTION "ml_learn"
#define ML_MODEL_METADATA_IDX "ml_model_pkey"

void CreateModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest);
void PredictModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest);
TupleDesc GetCreateModelResultDesc(void);
Oid GetProcOidByName(const char* proname);
TupleDesc GetPredictModelResultDesc(PredictModelStmt *node);


#define FIELDCOUNT 256

typedef struct FormData_model
{
    NameData name;
    text* file;
    BpChar type;
    float4 acc;
    text* info;
    text* args;
    bytea data;
} FormData_model;


typedef FormData_model* Form_model;

typedef enum Anum_model
{
	Anum_ml_name = 1,
	Anum_ml_model_fieldlist,
	Anum_ml_model_type,
	Anum_ml_model_acc,
	Anum_ml_model_info,
	Anum_ml_model_args,
	Anum_ml_model_data,
	Anum_ml_model_classes,
	Anum_ml_model_loss_function,
	_Anum_ml_max,
} Anum_model;

#define Natts_model (_Anum_ml_max - 1)

typedef enum Anum_ml_name_idx
{
	Anum_ml_name_idx_name = 1,
	_Anum_ml_name_idx_max,
} Anum_ml_name_idx;

#define Natts_ml_name_idx (_Anum_ml_name_idx_max - 1)


enum ml_class_state_t {
    ML_STATE_NONE,
    ML_STATE_KEY,
    ML_STATE_BEG_ARRAY,
    ML_STATE_BND_ARRAY,
};

enum ml_feature_type
{
	ML_FEATURE_NONE = 0,
	ML_FEATURE_FLOAT ,
	ML_FEATURE_CATEGORICAL ,
	ML_FEATURE_TEXT ,
};
