#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dbHandler.h"
#include "expression.h"
#include "AllTables.h"

// Bookkeeping for scan
typedef struct RM_ScanHandle
{
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;

// BookKeeping for serialization and deserialization
typedef struct RM_TABLE_INFO
{
	Schema *s;
	int totalNumOfRec;
	RID *firstFreeRec;
	RID LastRecID;
}RM_TABLE_INFO;

// PageInfo for every page
typedef struct PAGE_INFO
{
	RID firstDeletedRecord;
	RID lastRecord;
}PAGE_INFO;

//Handling the conditions
typedef struct ExpressionHandler
{
	Expr* cond;
	RID ridToScan;
	RID lastRecID;
}ExpressionHandler;

// table and manager
extern RC initRManager(void *mgmtData);
extern RC shutdownRecordManager();
extern RC generate_table(char *name, Schema *schema);
extern RC openTable(RM_TableData *rel, char *name);
extern RC closeTable(RM_TableData *rel);
extern RC deleteTable(char *name);
extern int get_row_num(RM_TableData *rel);

// handling records in a table
extern RC insert_Record(RM_TableData *rel, Record *record);
extern RC delete_Record(RM_TableData *rel, RID id);
extern RC record_upgradation(RM_TableData *rel, Record *record);
extern RC get_Record(RM_TableData *rel, RID id, Record *record);

// scans
extern RC ScanStart(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next(RM_ScanHandle *scan, Record *record);
extern RC closeScan(RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize(Schema *schema);
extern Schema *Schema_Creation(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema(Schema *schema);

// dealing with records and attribute values
extern RC create_Record(Record **record, Schema *schema);
extern RC free_Record(Record *record);
extern RC AttrGetter(Record *record, Schema *schema, int attrNum, Value **value);
extern RC AttrSetter(Record *record, Schema *schema, int attrNum, Value *value);

//user defined methods
RC serialize_Information(char *name, RM_TABLE_INFO *r, char *res);
RC info_deserialize(char *page, RM_TABLE_INFO *infoPage);

#endif // RECORD_MGR_H