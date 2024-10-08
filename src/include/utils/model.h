
void ModelExecute(CreateModelStmt *stmt, DestReceiver *dest);
TupleDesc GetCreateModelResultDesc(void);
Oid GetProcOidByName(const char* proname);
TupleDesc GetPredictModelResultDesc(PredictModelStmt *node);