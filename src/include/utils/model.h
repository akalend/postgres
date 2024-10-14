#define ML_MODEL_METADATA "ml_model"
#define ML_MODEL_LEARN_FUNCTION "ml_learn"
#define ML_MODEL_METADATA_IDX "ml_model_pkey"

void CreateModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest);
void PredictModelExecuteStmt(CreateModelStmt *stmt, DestReceiver *dest);
TupleDesc GetCreateModelResultDesc(void);
Oid GetProcOidByName(const char* proname);
TupleDesc GetPredictModelResultDesc(PredictModelStmt *node);

typedef struct FormData_model
{
    NameData name;
    text* file;
    char type;
    float acc;
    text* info;
    text* args;
} FormData_model;

typedef FormData_model* Form_model;

typedef enum Anum_model
{
    Anum_ml_name = 1,
    Anum_ml_model_file,
    Anum_ml_model_type,
    Anum_ml_model_acc,
    Anum_ml_model_info,
    Anum_ml_model_args,
    _Anum_ml_max,
} Anum_model;

#define Natts_model (_Anum_ml_max - 1)

typedef enum Anum_ml_name_idx
{
    Anum_ml_name_idx_name = 1,
    _Anum_ml_name_idx_max,
} Anum_ml_name_idx;

typedef struct FormData_test
{
    int32 id;
    text* name;
    BpChar acc;
    bool type;
} FormData_test;



#define Natts_ml_name_idx (_Anum_ml_name_idx_max - 1)
