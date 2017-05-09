#include"record_manage.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include"manage_buff.h"
#include"manage_strg.h"
#include"dbHandler.h"


RC AttrSetter(Record *record, Schema *schema, int attributeNumber, value *valueToPointer)
 {
	RC returnVal = isAttrValid(attributeNumber);
    

	if (returnVal != RC_OK) 
	{
		return returnVal;
	}

	int reset = 0;
	
	unsigned char *attributeData = NULL;
	
	int length;
	
	int rotateAti;
	
	int j;

	if (attributeNumber != 0)
	{
		for (rotateAti = 1; rotateAti <= attributeNumber; rotateAti++)
		{
			switch (schema->dataTypes[attributeNumber - rotateAti])
			{
			case 0:
			 
			 reset += sizeof(int);	
			 
			 break;
			
			case 1:
			 
			  reset += schema->typeLength[attributeNumber - rotateAti]; 
			  
			  break;

			case 2:
			
			 reset += sizeof(float);
			 
			 	break;
			
			case 3:
			 
			 reset += sizeof(bool);	
			 
			 break;
			}
		}

	}

	if (valueToPointer->dt == DT_INT)
	 {
		//Int valueToPointer
		attributeData = (char *)malloc(sizeof(int));
	
		sprintf(attributeData, "%d", valueToPointer->vToPoint.intV);
	}
	else if (valueToPointer->dt == DT_STRING) {
		//String valueToPointer
		attributeData = (char *)malloc(schema->typeLength[attributeNumber]);
	
		memmove(attributeData, valueToPointer->vToPoint.stringV, schema->typeLength[attributeNumber]);
	}
	else if (valueToPointer->dt == DT_FLOAT) {
		//float valueToPointer
	
		attributeData = (char *)malloc(sizeof(float));
	
		sprintf(attributeData, "%f", valueToPointer->vToPoint.floatV);
	}
	else {
		// boolean valueToPointer
	
		attributeData = (char *)malloc(sizeof(int));
	
		_itoa(valueToPointer->vToPoint.boolV, attributeData, 10);
	}
	
	switch (schema->dataTypes[attributeNumber])
	{
	
	case 0: 
	
	length = sizeof(int);	
	
	break;
	
	case 1: 
	
	length = schema->typeLength[attributeNumber];
	
	 break;
	
	case 2:
	
	 length = sizeof(float);	
	 
	 break;
	
	case 3: 
	
	length = sizeof(bool);	
	
	break;
	}

	memmove(record->data + reset, attributeData, 4);

	return RC_OK;
}


RC shutdownRecordManager() {
	return RC_OK;
}

RC info_deserialize(char *page_n, RM_TABLE_INFO *page_Detail)
{
	char *tokenInformation;

	int rotateAti;
	
	char *temp1;

	rotateAti = strlen(page_n);
	
	temp1 = (char *)malloc(rotateAti + 1);
	
	strcpy(temp1, page_n);
	
	Schema *temporary = (Schema *)malloc(sizeof(Schema));

	rotateAti = 0;

	tokenInformation = strtok(temp1, ";");

	tokenInformation = strtok(NULL, ";");

	temporary->numAttr = atoi(tokenInformation);
	
	temporary->attrNames = (char **)malloc(sizeof(char*) * temporary->numAttr);

     
     while(rotateAti < temporary->numAttr)
	 {

		 tokenInformation = strtok(NULL, ";");
	
		temporary->attrNames[rotateAti] = tokenInformation;

          rotateAti++;
	 }


	temporary->dataTypes = (DataType *)malloc(sizeof(DataType) * temporary->numAttr);

	//retrives the type of each record

	rotateAti = 0;


	while(rotateAti < temporary->numAttr)
	{
		tokenInformation = strtok(NULL, ";");

		temporary->dataTypes[rotateAti] = atoi(tokenInformation);

       rotateAti++;
	}
	
	temporary->typeLength = (int *)malloc(sizeof(int) * temporary->numAttr);



    rotateAti = 0;


	while(rotateAti < temporary->numAttr)
	{
		tokenInformation = strtok(NULL, ";");

		temporary->typeLength[rotateAti] = atoi(tokenInformation);

       rotateAti++;
	}

	tokenInformation = strtok(NULL, ";");

	temporary->keySize = atoi(tokenInformation);

	temporary->keyAttrs = (int *)malloc(sizeof(int) * temporary->keySize);


    rotateAti = 0;


	while(rotateAti < temporary->keySize)
	{
		tokenInformation = strtok(NULL, ";");
		
		temporary->keyAttrs[rotateAti] = atoi(tokenInformation);
      
	    rotateAti++;
	}

	

	page_Detail->s = temporary;

	tokenInformation = strtok(NULL, ";");
	
	page_Detail->totalNumOfRec = atoi(tokenInformation);

	page_Detail->firstFreeRec = (RID *)malloc(sizeof(RID));

	tokenInformation = strtok(NULL, ",");

	page_Detail->firstFreeRec->page_n = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ";");

	page_Detail->firstFreeRec->slot = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ",");

	page_Detail->LastRecID.page_n = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ";");

	page_Detail->LastRecID.slot = atoi(tokenInformation);

	return RC_OK;
}



RC initRManager(void *mgmtData)
{
	return RC_OK;
}



int get_row_num(RM_TableData *relation) 
{

	BM_PageHandle *bPoolHandle; 

	BM_BufferPool *bufferManager; 
	
	RM_TABLE_INFO *rmTableInfo;
	
	bPoolHandle = MAKE_PAGE_HANDLE();

    bufferManager = (BM_BufferPool *)relation->mgmtData;
	
	rmTableInfo = NULL;

	if (page_pinned(bufferManager, bPoolHandle, 0) != RC_OK) {
		free(bPoolHandle);
		return RC_PIN_PAGE_FAILED;
	}
	info_deserialize(bPoolHandle, rmTableInfo);

	page_unpin(bufferManager, bPoolHandle);
	free(bPoolHandle);

	return rmTableInfo->totalNumOfRec;
}





Schema *Schema_Creation(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *newSchema = (Schema *)malloc(sizeof(Schema));

	newSchema->attrNames = attrNames;

	newSchema->dataTypes = dataTypes;

	newSchema->keyAttrs = keys;

	newSchema->keySize = keySize;

	newSchema->typeLength = typeLength;

	newSchema->numAttr = numAttr;

	return newSchema;
}



RC table_Info_Creation(RM_TABLE_INFO *DataOfTable)
{
	DataOfTable->firstFreeRec = (RID *)malloc(sizeof(RID));

	DataOfTable->firstFreeRec->page_n = -1;

	DataOfTable->firstFreeRec->slot = -1;

	DataOfTable->LastRecID.page_n = -1;

	DataOfTable->LastRecID.slot = -1;

	DataOfTable->totalNumOfRec = 0;

	DataOfTable->s = (Schema *)malloc(sizeof(Schema));

	return RC_OK;

}



#define PAGE_INFO_SIZE 50
#define TOMBSTONE_SIZE sizeof(bool)



RC generate_table(char *name, Schema *schema)
{
	char *resourse = (char *)malloc(PAGE_SIZE);

	if (newPageFileCreation(name) != RC_OK)
	{
		free(resourse);
		return RC_CANNOT_CREATE_FILE;
	}


	BM_BufferPool *bufferPool = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));

	BM_PageHandle *pageHandler = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	if (Buffer_Pool_init(bufferPool, name, 5, RS_FIFO, NULL) != RC_OK)
	{
		free(resourse);
		return RC_BM_BUFFERPOOL_NOT_INIT;
	}

	if (page_pinned(bufferPool, pageHandler, 0) != RC_OK)
	{
		free(resourse);
		return RC_PIN_PAGE_FAILED;
	}

	RM_TABLE_INFO *DataOfTable = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));

	createTableInfo(DataOfTable);

	DataOfTable->s = schema;

	serialize_Information(name, DataOfTable, resourse);

	memcpy(pageHandler->data, resourse, PAGE_SIZE);

	if (markBadPages(bufferPool, pageHandler) != RC_OK)
	{
		free(resourse);
		return RC_MAKE_DIRTY_FAILED;
	}

	page_unpin(bufferPool, pageHandler);

	force_buffer_Pool_Page(bufferPool, pageHandler);

	Buffer_Pool_ShutDown(bufferPool);

	free(bufferPool);

	free(pageHandler);

	free(DataOfTable);

	free(resourse);

	return RC_OK;
}

RC serialize_Information(char *name, RM_TABLE_INFO *tableInfo, char *resourse)
{
	int recLength;

	int rotateAti;

	int nameLen;

	int numAttrLen;

	char *temporary;

     recLength = 0;
	
	nameLen = strlen(name);
	
    numAttrLen = tableInfo->s->numAttr;
	
	temporary = (char *)malloc(PAGE_SIZE);

	memcpy(resourse, "", 1);

	
	if (strlen(name) > 0)
	{
		strcpy(resourse, name);
	}
	else
	{
		return RC_NAME_NOT_VALID;
	}

	recLength = strlen(resourse);

	if (tableInfo->s->numAttr > 0)
	{
		strcpy(resourse + recLength, ";");
	
		recLength = strlen(resourse);
	
		strcpy(temporary, " ");
	
		_itoa(tableInfo->s->numAttr, temporary, 10);						//converts integer to string
	
		strcat(resourse + recLength, temporary);
	}
	else
	{
		return RC_INVALID_NUM_ATTR;
	}

	recLength = strlen(resourse);


 //puts the names of all the attributes
   
    rotateAti = 0;

	while(rotateAti < tableInfo->s->numAttr)
	{

		if (strcmp(tableInfo->s->attrNames[rotateAti], ""))
		{
		
			strcat(resourse + recLength, ";");
		
			recLength = strlen(resourse);
		
			strcat(resourse + recLength, tableInfo->s->attrNames[rotateAti]);
		
			recLength = strlen(resourse);
		}
		else
		{
			return RC_INVALID_ATTR_NAMES;
		}

		rotateAti++;
	}



    
    rotateAti = 0;

	while(rotateAti < numAttrLen)
	{

		if (tableInfo->s->dataTypes[rotateAti] == DT_BOOL || tableInfo->s->dataTypes[rotateAti] == DT_FLOAT
			|| tableInfo->s->dataTypes[rotateAti] == DT_INT || tableInfo->s->dataTypes[rotateAti] == DT_STRING)
		{
		
			strcat(resourse + recLength, ";");
		
			recLength = strlen(resourse);
		
			_itoa((int *)tableInfo->s->dataTypes[rotateAti], temporary, 10);
		
			strcat(resourse + recLength, temporary);
		
			recLength = strlen(resourse);
		}
		else
		{
			return RC_INVALID_DATATYPE;
		}

		rotateAti++;
	}


	  rotateAti = 0;

	while( rotateAti < tableInfo->s->numAttr)
	{
		if (tableInfo->s->typeLength[rotateAti] != -1)
		{
			strcat(resourse + recLength, ";");
	
			recLength = strlen(resourse);
	
			strcpy(temporary, " ");
	
			_itoa(tableInfo->s->typeLength[rotateAti], temporary, 10);
	
			strcat(resourse + recLength, temporary);
	
			recLength = strlen(resourse);
		}
		else
		{
			return RC_INVALID_TYPE_LENGTH;
		}

		rotateAti++;
	}

	if (tableInfo->s->keySize > 0)
	{
	
		strcat(resourse + recLength, ";");
	
		recLength = strlen(resourse);
	
		strcpy(temporary, " ");
	
		_itoa(tableInfo->s->keySize, temporary, 10);
	
		strcat(resourse + recLength, temporary);
	
		recLength = strlen(resourse);

		//inserting the primary keys
		for (rotateAti = 0; rotateAti < tableInfo->s->keySize; rotateAti++)
		{
	
			strcat(resourse + recLength, ";");
	
			recLength = strlen(resourse);
	
			strcpy(temporary, " ");
	
			_itoa(tableInfo->s->keyAttrs[rotateAti], temporary, 10);
	
			strcat(resourse + recLength, temporary);
	
			recLength = strlen(resourse);
		}
	}
	else
	{
		return RC_INVALID_KEY_SIZE;
	}

	if (tableInfo->totalNumOfRec < 0)
	{
		tableInfo->totalNumOfRec = 0;
	}
	//inserts the total number of rec in the table

	strcat(resourse + recLength, ";");

	recLength = strlen(resourse);

	strcpy(temporary, " ");

	_itoa(tableInfo->totalNumOfRec, temporary, 10);

	strcat(resourse + recLength, temporary);

	recLength = strlen(resourse);

	//inserts the first avaiable free space of the record
	if (tableInfo->totalNumOfRec == NULL)
	{
	
		tableInfo->firstFreeRec->page_n = -1;
	
		tableInfo->firstFreeRec->slot = -1;
	
		tableInfo->LastRecID.page_n = -1;
	
		tableInfo->LastRecID.slot = -1;
	}

	strcat(resourse + recLength, ";");
	
	recLength = strlen(resourse);
	
	strcpy(temporary, " ");
	
	_itoa(tableInfo->firstFreeRec->page_n, temporary, 10);
	
	strcat(resourse + recLength, temporary);
	
	recLength = strlen(resourse);
	
	strcat(resourse + recLength, ",");
	
	recLength = strlen(resourse);
	
	strcpy(temporary, " ");
	
	_itoa(tableInfo->firstFreeRec->slot, temporary, 10);
	
	strcat(resourse + recLength, temporary);
	
	recLength = strlen(resourse);

	//for lastRec in the table
	strcat(resourse + recLength, ";");
	
	recLength = strlen(resourse);
	
	strcpy(temporary, " ");
	
	_itoa(tableInfo->LastRecID.page_n, temporary, 10);
	
	strcat(resourse + recLength, temporary);
	
	recLength = strlen(resourse);
	
	strcat(resourse + recLength, ",");
	
	recLength = strlen(resourse);
	
	strcpy(temporary, " ");
	
	_itoa(tableInfo->LastRecID.slot, temporary, 10);
	
	strcat(resourse + recLength, temporary);
	
	recLength = strlen(resourse);


	strcat(resourse + recLength, ";");

	recLength = strlen(resourse);

	if (recLength > PAGE_SIZE)
	{
		return RC_INFO_PAGE_TOO_BIG;
	}

	free(temporary);

	return RC_OK;
}



RC insert_Record(RM_TableData *relation, Record *record)
{
	if (relation->mgmtData == NULL)
	{
		return RC_BM_BUFFERPOOL_NOT_INIT;
	}

	BM_BufferPool *bufferPool;
	
    BM_PageHandle *pageHandler;
	
	
	pageHandler = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	bufferPool =  BM_BufferPool *)relation->mgmtData;


	if (page_pinned(bufferPool, pageHandler, 0) != RC_OK)
	{
		return RC_PIN_PAGE_FAILED;
	}


	RM_TABLE_INFO *newTable = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));

	Record *nextFreeRec = (Record *)malloc(sizeof(Record));

	PAGE_INFO *pageHeader = (PAGE_INFO *)malloc(PAGE_INFO_SIZE);

	BM_PageHandle *ph1 = MAKE_PAGE_HANDLE();

	RID prevLastRec;

	info_deserialize(pageHandler->data, newTable);

	prevLastRec = newTable->LastRecID;

	if (newTable->firstFreeRec->page_n < 0 || newTable->firstFreeRec->slot < 0)
	{
		if (newTable->totalNumOfRec == 0)
		{
			//this means the table was just created
			record->id.page_n = 1;

			record->id.slot = 1;

			newTable->totalNumOfRec++;

			newTable->LastRecID = record->id;
		}
		else
		{
			// find the RID of last record of the table

			record->id.page_n = newTable->LastRecID.page_n;

			record->id.slot = newTable->LastRecID.slot;

			//make sure that there is enough space in the current page_n for placing the forwed record
			getNextRec(record, bufferPool, relation);

			newTable->totalNumOfRec++;

			newTable->LastRecID = record->id;
		}
	}
	else
	{
		//else if there is a free space available 
		record->id = *(newTable->firstFreeRec);
	
		getRecord(relation, *(newTable->firstFreeRec), nextFreeRec);
	
	
		emptyRecordDe_Serialize(nextFreeRec->data, &nextFreeRec->id);
	
		//stores the id to the 
		newTable->firstFreeRec = &nextFreeRec->id;
	}

	if (page_pinned(bufferPool, ph1, record->id.page_n) != RC_OK)
	{
		return RC_PIN_PAGE_FAILED;
	}

	if (updateRecordWithTombStone(relation, record, FALSE, ph1->data) != RC_OK)
		return RC_INSERT_FAILED;

	if (strcmp(ph1->data, "") == 0)
	{
		pageHeader->firstDeletedRecord.page_n = -1;
	
		pageHeader->firstDeletedRecord.slot = -1;
	
		pageHeader->lastRecord.page_n = ph1->pageNum;
	
		pageHeader->lastRecord.slot = 1;
	}
	else
		HeaderExtractor(ph1->data, pageHeader);

	if (prevLastRec.page_n > record->id.page_n)
	{
		pageHeader->lastRecord = record->id;
		
		pageHeader->firstDeletedRecord.page_n = -1;
		
		pageHeader->firstDeletedRecord.slot = -1;
	}


	if ((prevLastRec.page_n == record->id.page_n && prevLastRec.slot < record->id.slot))
	{
		pageHeader->lastRecord = record->id;
		
		pageHeader->firstDeletedRecord.page_n = -1;
		
		pageHeader->firstDeletedRecord.slot = -1;
	}


	if (record->id.page_n == pageHeader->firstDeletedRecord.page_n)
	{
		if(record->id.slot == pageHeader->firstDeletedRecord.slot)
		 
		 {
		
		if (nextFreeRec->id.page_n > pageHeader->firstDeletedRecord.page_n)
		{
			pageHeader->firstDeletedRecord.page_n = -1;
			pageHeader->firstDeletedRecord.slot = -1;
		}
		else
		{
			// the page_n will remain the same only the slot number changes
			pageHeader->firstDeletedRecord.slot = nextFreeRec->id.slot;
		}

		 }
	}

	update_Page_Header(ph1->data, pageHeader);

	markBadPages(bufferPool, ph1);

	page_unpin(bufferPool, ph1);

	free(ph1);

	serialize_Information(relation->name, newTable, pageHandler->data);

	pageHandler->pageNum = 0;

	markBadPages(bufferPool, pageHandler);

	page_unpin(bufferPool, pageHandler);

	free(newTable);

	free(pageHeader);

	free(pageHandler);

	return RC_OK;
}


RC delete_Record(RM_TableData *relation, RID id)
{

	int rotateAti;

	PageNumber pageNumber;
	
	RID prevFreeRID, nextFreeRID;
	
	int recLen;
	
	Record *prevRec;

   int slotNumber; 
	
	prevRec->data = (char *)malloc(sizeof(recLen));

	char *recordToDelete;
	
	RC status; 

    BM_PageHandle *pageHandler;

	BM_BufferPool *bufferPool;
  
   int tsi;


	if (relation->mgmtData == NULL)
       {
		return RC_BM_BUFFERPOOL_NOT_INIT;
	   }

	pageNumber = id.page_n;
	
	recLen = getRecordSize(relation->schema);
	
	prevRec = (Record *)malloc(sizeof(Record));
	
	prevRec->data = (char *)malloc(sizeof(recLen));
	
	slotNumber = id.slot;
	
	pageHandler = MAKE_PAGE_HANDLE();

	bufferPool = (BM_BufferPool *)relation->mgmtData;

	if (page_pinned(bufferPool, pageHandler, pageNumber) != RC_OK)
	{
		free(pageHandler);
	
		free(prevRec->data);
	
		free(prevRec);
	
		return RC_PIN_PAGE_FAILED;
	}

	recordToDelete = pageHandler->data;
	
	recordToDelete += PAGE_INFO_SIZE + (recLen + TOMBSTONE_SIZE) * (slotNumber - 1);

	char *ts = (char *)malloc(TOMBSTONE_SIZE);
	
	memmove(ts, recordToDelete, TOMBSTONE_SIZE);
	tsi = atoi(ts);

	if (tsi == 1)
	{
	
		free(pageHandler);
	
		free(prevRec->data);
	
		free(prevRec);
	
		return RC_OK;
	}
	page_unpin(bufferPool, pageHandler);

	prevAndNextDel_Record(relation, id, prevRec, &nextFreeRID);
	
	free(pageHandler);

	pageHandler = MAKE_PAGE_HANDLE();

	if (prevRec->id.page_n == -1 || prevRec->id.slot == -1)
	{
		if (page_pinned(bufferPool, pageHandler, 0) != RC_OK)
		{
			free(pageHandler);
	
			free(prevRec->data);
	
			free(prevRec);
	
			return RC_PIN_PAGE_FAILED;
		}

		RM_TABLE_INFO *DataOfTable = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));
	
		info_deserialize(pageHandler->data, DataOfTable);

		DataOfTable->firstFreeRec = &id;

		serialize_Information(relation->name, DataOfTable, pageHandler->data);

		markBadPages(bufferPool, pageHandler);
		page_unpin(bufferPool, pageHandler);
		free(DataOfTable);
	}
	else
	{
		//serializing the previous empty rec data with the RID of newly freed empty rec
		serializaeEmptyRec(prevRec->data, id);

		BM_PageHandle *ph1 = MAKE_PAGE_HANDLE();
		if (page_pinned(bufferPool, ph1, prevRec->id.page_n) != RC_OK)
		{
			free(pageHandler);
			free(ph1);
			free(prevRec->data);
			free(prevRec);
			return RC_PIN_PAGE_FAILED;
		}
		updateRecordWithTombStone(relation, prevRec, TRUE, ph1->data);
		markBadPages(bufferPool, ph1);
		page_unpin(bufferPool, ph1);
		free(ph1);
	}

	//free(prevRec->data);
	free(prevRec);

	BM_PageHandle *ph2 = MAKE_PAGE_HANDLE();
	if (page_pinned(bufferPool, ph2, id.page_n) != RC_OK)
	{
		free(pageHandler);
		free(ph2);
		free(prevRec->data);
		free(prevRec);
		return RC_PIN_PAGE_FAILED;
	}

	prevRec = (Record *)malloc(sizeof(Record));
	prevRec->data = (char *)malloc(getRecordSize(relation->schema));

	//pushing the RID of the forwed free space available
	serializaeEmptyRec(prevRec->data, nextFreeRID);

	prevRec->id = id;

	updateRecordWithTombStone(relation, prevRec, TRUE, ph2->data);



	// this is to update the header of the page_n if the deleted rec was the first record of the page_n
	PAGE_INFO *pageHeader = (PAGE_INFO *)malloc(sizeof(PAGE_INFO));

	HeaderExtractor(ph2->data, pageHeader);

	if ((pageHeader->firstDeletedRecord.page_n == -1
		&& pageHeader->firstDeletedRecord.slot == -1) ||
		(pageHeader->firstDeletedRecord.page_n == id.page_n
			&& pageHeader->firstDeletedRecord.slot > id.slot))
	{
		pageHeader->firstDeletedRecord.page_n = id.page_n;
		pageHeader->firstDeletedRecord.slot = id.slot;

		//once the firstDeletedRecord is formed the header has to be updated
		if ((status = update_Page_Header(ph2->data, pageHeader)) != RC_OK)
		{
			free(prevRec->data);
			free(prevRec);
			free(pageHandler);
			free(ph2);
			return status;
		}
	}

	markBadPages(bufferPool, ph2);
	page_unpin(bufferPool, ph2);

	free(prevRec->data);
	free(prevRec);
	free(pageHandler);
	free(ph2);
	return RC_OK;
}


RC update_Page_Header(char *pageHandler, PAGE_INFO *pageHeader)
{
	char *temporary = (char *)malloc(PAGE_INFO_SIZE);

	//this will serialize the info that is to be put in the header
	serializePageHeader(pageHeader, temporary);

	if (strlen(temporary) > PAGE_INFO_SIZE)
		return RC_PAGE_HEADER_SIZE_OVERFLOW;

	memmove(pageHandler, temporary, PAGE_INFO_SIZE);

	if (strlen(pageHandler) > PAGE_SIZE)
		return RC_UPDATE_HEADER_FAILED;

	free(temporary);
	return RC_OK;
}


RC serializePageHeader(PAGE_INFO *pageHeader, char *temporary)
{
	char *intermediate = (char *)malloc(10);

	_itoa(pageHeader->firstDeletedRecord.page_n, intermediate, 10);
	strcpy(temporary, intermediate);
	strcat(temporary, ",");

	_itoa(pageHeader->firstDeletedRecord.slot, intermediate, 10);
	strcat(temporary, intermediate);
	strcat(temporary, ";");

	_itoa(pageHeader->lastRecord.page_n, intermediate, 10);
	strcat(temporary, intermediate);
	strcat(temporary, ",");

	_itoa(pageHeader->lastRecord.slot, intermediate, 10);
	strcat(temporary, intermediate);
	strcat(temporary, ";");

	free(intermediate);
	return RC_OK;
}


RC HeaderExtractor(char *pageHandler, PAGE_INFO *pageHeader)
{
	char *tokenInformation;

	char *copyData;
	
	copyData = (char *)malloc(PAGE_INFO_SIZE);

	memmove(copyData, pageHandler, PAGE_INFO_SIZE);


	tokenInformation = strtok(copyData, ",");

	pageHeader->firstDeletedRecord.page_n = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ";");

	pageHeader->firstDeletedRecord.slot = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ",");

	pageHeader->lastRecord.page_n = atoi(tokenInformation);

	tokenInformation = strtok(NULL, ";");

	pageHeader->lastRecord.slot = atoi(tokenInformation);

	free(copyData);

	return RC_OK;
}


RC emptyRecordDe_Serialize(char *data, RID *rid)
{
	char *tokenInformation;
	
	tokenInformation = (char *)malloc(sizeof(int));

	memmove(tokenInformation, data, sizeof(int));

	rid->page_n = atoi(tokenInformation);

	memmove(tokenInformation, data + sizeof(int) + 1, sizeof(int));

	rid->slot = atoi(tokenInformation);

	free(tokenInformation);

	return RC_OK;
}


RC closeTable(RM_TableData *relation)
{
	BM_BufferPool *bufferManager = (BM_BufferPool *)relation->mgmtData;
	//free the schema

	free(relation->schema->dataTypes);

  //free schema length

	free(relation->schema->typeLength);

   //free attribute Name

	free(relation->schema->attrNames);

   //free key attributes

	free(relation->schema->keyAttrs);

	//free(relation->name);

	free(relation->schema);

	//shutdown the BufferPool associated with the table

	shutdownBufferPool(bufferManager);

	free(bufferManager);

	return RC_OK;
}


RC deleteTable(char *name)
{
	destructFiles(name); // Deleting the page_n File
	return RC_OK;
}


RC EmptySerialization(char *data, RID rid)
{
	char *tokenInformation;

	int reset;
	

	reset = 0;

	tokenInformation = (char *)malloc(sizeof(int));

	_itoa(rid.page_n, tokenInformation, 10);

	memmove(data + reset, tokenInformation, sizeof(int));

	reset  =  reset + sizeof(int);

	memmove(data + reset, ",", 1);

	reset = reset +  1;

	free(tokenInformation);

	tokenInformation = (char *)malloc(sizeof(int));

	_itoa(rid.slot, tokenInformation, 10);

	memmove(data + reset, tokenInformation, sizeof(int));

	reset += sizeof(int);

	memmove(data + reset, ";", 1);

	return RC_OK;
}

RC getNextRec(Record *rec, BM_BufferPool *bufferManager, RM_TableData *relation)
{
	BM_PageHandle *temporaryPage; 

	PAGE_INFO *temporaryPageHeader;

	int numOfRecInPage, recLen;

    temporaryPage = MAKE_PAGE_HANDLE();

	temporaryPageHeader = (PAGE_INFO *)malloc(PAGE_INFO_SIZE);

	numOfRecInPage, recLen = getRecordSize(relation->schema) + TOMBSTONE_SIZE;


	if (page_pinned(bufferManager, temporaryPage, rec->id.page_n) != RC_OK)
	{
		return RC_PIN_PAGE_FAILED;
	}

	page_unpin(bufferManager, temporaryPage);

	HeaderExtractor(temporaryPage->data, temporaryPageHeader);

	numOfRecInPage = (int *)((PAGE_SIZE - PAGE_INFO_SIZE) / recLen);	
	
	
	if (temporaryPageHeader->lastRecord.slot + 1 > numOfRecInPage)
	{
		rec->id.page_n++;
		rec->id.slot = 1;
	}
	else
	{
		rec->id.slot++;
	}

	
	free(temporaryPage);
	
	free(temporaryPageHeader);
	
	return RC_OK;
}


RC close_Table(RM_TableData *relation)
{
	BM_BufferPool *bufferManager = (BM_BufferPool *)relation->mgmtData;
	//free the schema
	free(relation->schema->dataTypes);
	free(relation->schema->typeLength);
	free(relation->schema->attrNames);
	free(relation->schema->keyAttrs);
	//free(relation->name);
	free(relation->schema);
	//shutdown the BufferPool associated with the table
	Buffer_Pool_ShutDown(bufferManager);
	free(bufferManager);
	return RC_OK;
}






RC freeSchema(Schema *schema)
{
	free(schema);
	return RC_OK;
}


RC create_Record(Record **record, Schema *schema)
{
	int record_Size = 0;
	//Calculating size of the record
	record_Size = getRecordSize(schema);

	//Allocating memory to record
	*record = (Record *)malloc(sizeof(Record));
	(*record)->data = (char *)malloc(record_Size);
	return RC_OK;
}



int getRecordSize(Schema *schema)
{
	int rotateAti;
	int sizeofRecord = 0;
	DataType *dt = schema->dataTypes;
	int *tpl = schema->typeLength;
	for (rotateAti = 0; rotateAti<schema->numAttr; rotateAti++) {
		switch (dt[rotateAti]) {
		case DT_INT:
			sizeofRecord += sizeof(int);
			break;
		case DT_FLOAT:
			sizeofRecord += sizeof(float);
			break;
		case DT_BOOL:
			sizeofRecord += sizeof(bool);
			break;
		case DT_STRING:
			sizeofRecord += tpl[rotateAti];
			break;
		}
	}
	return sizeofRecord;
}



RC free_Record(Record *record)
{
	free(record->data);
	free(record);
	return RC_OK;
}



RC record_upgradation(RM_TableData *relation, Record *record)
{

	PageNumber pageNumber;

	int recordLength;
	
	RID id;

	int slotNumber;

	M_PageHandle *pageHandler;

	BM_BufferPool *bufferPool;
	
	
	
	id  = record->id;
	
	pageNumber = id.page_n;

    slotNumber = id.slot;
	
	recordLength = 0;

    pageHandler = MAKE_PAGE_HANDLE();
	


	if ((BM_BufferPool *)relation->mgmtData != NULL)
	{
		bufferPool = (BM_BufferPool *)relation->mgmtData;
	}

	else 
	{
		return RC_BM_BUFFERPOOL_NOT_INIT;
	}

	recordLength = getRecordSize(relation->schema);

	if (page_pinned(bufferPool, pageHandler, pageNumber) == RC_OK)
	{
		
	}
	else if(page_pinned(bufferPool, pageHandler, pageNumber) != RC_OK)
	{
        free(pageHandler);
		
		return RC_PIN_PAGE_FAILED;
	}

    memmove(pageHandler->data + (recordLength + TOMBSTONE_SIZE) * (slotNumber - 1) + TOMBSTONE_SIZE + PAGE_INFO_SIZE, record->data, recordLength);

	if (markBadPages(bufferPool, pageHandler) != RC_OK)
	{
		free(pageHandler);
		return RC_MAKE_DIRTY_FAILED;
	}

	page_unpin(bufferPool, pageHandler);


	free(pageHandler);

	return RC_OK;

}



RC updateRecordWithTombStone(RM_TableData *relation, Record *record, bool tombStone, char *pageData)
{
	int recordLength = 0;
	RID id = record->id;
	PageNumber pageNumber = id.page_n;
	int slotNumber = id.slot;


	recordLength = getRecordSize(relation->schema);

	char *rotateAti = (char *)malloc(sizeof(TOMBSTONE_SIZE));
	
	_itoa(tombStone, rotateAti, 10);

	memmove(pageData + (recordLength + TOMBSTONE_SIZE) * (slotNumber - 1) + PAGE_INFO_SIZE, rotateAti, TOMBSTONE_SIZE);

	memmove(pageData + (recordLength + TOMBSTONE_SIZE) * (slotNumber - 1) + PAGE_INFO_SIZE + TOMBSTONE_SIZE, record->data, recordLength);

      return RC_OK;
}


RC closeScan(RM_ScanHandle *scan)
{
	free(scan->mgmtData);
	return RC_OK;
}


RC assignDataToDataType(Schema *schema, int attributeNumber, value **valueToPointer, char **attributeData) {

	if ((*valueToPointer)->dt == DT_INT) {
		
		//attribute data reserve memory

		//of size int

		attributeData[0] = (char *)malloc(sizeof(int));

		sprintf(attributeData, "%d", (*valueToPointer)->vToPoint.intV);

	}
	else if ((*valueToPointer)->dt == DT_STRING) 
	{
      //attributeData at o index reserved memeory 


		attributeData[0];
		
	    attributeData[0] = (char *)malloc(schema->typeLength[attributeNumber]);

		attributeData[0] = (*valueToPointer)->vToPoint.stringV;
	}
	else if ((*valueToPointer)->dt == DT_FLOAT)
	 {

		 //attributeData at o index reserved memeory 
		 
		 //for float

		attributeData[0] = (char *)malloc(sizeof(float));

		sprintf(attributeData, "%f", (*valueToPointer)->vToPoint.floatV);

	}
	else {
		// boolean valueToPointer 

		//reserving size of int 4 byte in memory

		 attributeData[0];
		 
		 attributeData[0] = (char *)malloc(sizeof(int));
		

		//checking valueTo Pointer if true

		if ((*valueToPointer)->vToPoint.boolV) 
		{
			//attributeData valueTo Pointer if true will assign true

			attributeData[0] = "true";
		}

		else 
		{
			//attributeData valueTo Pointer if false  will assign false

			attributeData[0] = "false";
		}

	}

	(*valueToPointer)->dt = schema->dataTypes[attributeNumber];
	
	return RC_OK;
}

RC AttrGetter(Record *record, Schema *schema, int attributeNumber, value **valueToPointer)
 {
	 int reset;
	
	int length;
	
	int  rotateAti;
	
	int j;
	
	unsigned char *attributeData;

	RC returnVal = isAttrValid(attributeNumber);
	
	if (returnVal != RC_OK) 
	{
		return returnVal;
	}

      reset = 0;
	
      attributeData = NULL;

	if (attributeNumber != 0)

		for (rotateAti = 1; rotateAti <= attributeNumber; rotateAti++)
		{
			switch (schema->dataTypes[attributeNumber - rotateAti])
			{
			case 0: 
			
			reset += sizeof(int);
			
			break;
		
			case 1:
			
			 reset += schema->typeLength[attributeNumber - rotateAti]; 
			 
			 break;
			
			case 2: 
			
			reset += sizeof(float);	
			
			break;
			
			case 3: 
			
			reset += sizeof(bool);	
			
			break;
			}
		}

	switch (schema->dataTypes[attributeNumber])
	{

	case 0:
	
	 length = sizeof(int);
	 
	 	break;
	
	case 1:
	
	 length = schema->typeLength[attributeNumber];
	 
	  break;
	
	
	case 2:
	
	 length = sizeof(float);	
	 
	 break;
	
	case 3: 
	
	length = sizeof(bool);
	
	break;
	
	}
	
	attributeData = (char *)malloc(length);

	memmove(attributeData, record->data + reset, length);

	if (schema->dataTypes[attributeNumber] == DT_INT)
	 {
	
		MAKE_VALUE(*valueToPointer, schema->dataTypes[attributeNumber], atof(attributeData));
	
	}
	else if (schema->dataTypes[attributeNumber] == DT_STRING) {
		
		value *vToPoint;
		
	    
		vToPoint	= (valueToPointer *)malloc(sizeof(valueToPointer));
		
		(*valueToPointer) = (valueToPointer *)malloc(sizeof(valueToPointer));
		
		vToPoint->dt = DT_STRING;
		
		(*valueToPointer)->dt = (DataType)malloc(1);
		
		vToPoint->vToPoint.stringV = (char *)malloc(schema->typeLength[attributeNumber]);
		
		memmove(vToPoint->vToPoint.stringV, attributeData, schema->typeLength[attributeNumber]);
		
		memset(vToPoint->vToPoint.stringV + schema->typeLength[attributeNumber], '\0', schema->typeLength[attributeNumber]);
		
		size_t length = 1 + strlen(vToPoint->vToPoint.stringV);
		
	    (*valueToPointer)->vToPoint.stringV = (char *)malloc(length);
	
	 	strcpy((*valueToPointer)->vToPoint.stringV, vToPoint->vToPoint.stringV);
	
		(*valueToPointer)->dt = DT_STRING;
	}
	else if (schema->dataTypes[attributeNumber] == DT_STRING) {

		MAKE_VALUE(*valueToPointer, schema->dataTypes[attributeNumber], atof(attributeData));
	
	}
	else {
		// boolean valueToPointer
		MAKE_VALUE(*valueToPointer, schema->dataTypes[attributeNumber], atoi(attributeData));
		
	}


	free(attributeData);
	
	return RC_OK;
}




RC isAttrValid(int attributeNumber) {

	if (0 > attributeNumber) 
	{
		return RC_INVALID_ATTRIBUTE_NUM;
	}

	return RC_OK;
}


RC dataTypeValue(Schema *schema, int attributeNumber, value **valueToPointer, char *attributeData) {

   switch(schema->dataTypes[attributeNumber])
   {
	   case DT_INT:

	   	//Int valueToPointer
		(*valueToPointer)->vToPoint.intV = atoi(attributeData);

		break;

       case DT_STRING:

		//String valueToPointer
		
		(*valueToPointer)->vToPoint.stringV = attributeData;

		break;
	  
	  case DT_FLOAT: 

		//float valueToPointer

		(*valueToPointer)->vToPoint.floatV = strtof(attributeData, NULL);
	
	   break;

	   default:

		// boolean valueToPointer
		if (attributeData == "true") {
		
			(*valueToPointer)->vToPoint.boolV = true;
		}
		else
		 {
			(*valueToPointer)->vToPoint.boolV = false;
		}
	}
	(*valueToPointer)->dt = schema->dataTypes[attributeNumber];
	
	return RC_OK;
}




RC openTable(RM_TableData *relation, char *name) {

	
	RM_TableData *DataOfTable;
	
	Schema *schema;
	
	BM_BufferPool *bufferManager;
	
	bufferManager = MAKE_POOL();

	DataOfTable = NULL;
	
	Schema *schema;

	
	Buffer_Pool_init(bufferManager, name, 5, RS_FIFO, NULL);

	
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();
	
	RM_TABLE_INFO *rmTableInfo = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));


	if (page_pinned(bufferManager, bm_ph, 0) != RC_OK) {
	
		free(bm_ph);
	
		return RC_PIN_PAGE_FAILED;
	}
	
	info_deserialize(bm_ph->data, rmTableInfo);

	relation->schema = rmTableInfo->s;

	relation->name = name;

	relation->mgmtData = bufferManager;

	page_unpin(bufferManager, bm_ph);

	//free(bufferManager);
	free(bm_ph);

	return RC_OK;
}





RC get_Record(RM_TableData *relation, RID id, Record *record) {

	int recordLength;


	PageNumber pageNumber;
	
	int slotNumber;

	int reset;

	BM_PageHandle *pageHandler;


	BM_BufferPool *bufferPool;

    recordLength = 0;
	
	pageNumber = id.page_n;
	
	slotNumber = id.slot;

	char *tombstoneData;
	
	reset = 0;

	pageHandler = MAKE_PAGE_HANDLE();
	
	bufferPool = (BM_BufferPool *)relation->mgmtData;
	
	record->id = id;
	
	recordLength = getRecordSize(relation->schema);


	if (page_pinned(bufferPool, pageHandler, pageNumber) != RC_OK) {
		
		free(pageHandler);
		
		return RC_PIN_PAGE_FAILED;
	}


	reset = (recordLength + TOMBSTONE_SIZE) * (slotNumber - 1);
	
	tombstoneData = (char *)malloc(TOMBSTONE_SIZE);
	
	memmove(tombstoneData, pageHandler->data + reset + PAGE_INFO_SIZE, TOMBSTONE_SIZE);
	
	if (memcmp(tombstoneData, "1", TOMBSTONE_SIZE) == 0) 
	{
		record->data = "";
	}
	else 
	{
		memmove(record->data, pageHandler->data + reset + TOMBSTONE_SIZE + PAGE_INFO_SIZE, recordLength);
	}

	page_unpin(bufferPool, pageHandler);

	return RC_OK;
}


RC getRecordWithEmptyRecData(RM_TableData *relation, RID id, Record *record)

 {

	int recordLength;

	PageNumber pageNumber;
	
	int slotNumber;
	
	int reset;

	char *recordToUpdate;

	BM_PageHandle *pageHandler ;

	BM_BufferPool *bufferPool;

	record->id = id;
	
	recordLength = getRecordSize(relation->schema);

    recordLength = 0;
	
	pageNumber = id.page_n;
   
    slotNumber = id.slot;
	
	reset = 0;
	
	recordToUpdate = NULL;

	pageHandler = MAKE_PAGE_HANDLE();
	
	bufferPool = (BM_BufferPool *)relation->mgmtData;

	if (page_pinned(bufferPool, pageHandler, pageNumber) != RC_OK) {
	
		free(pageHandler);
	
		return RC_PIN_PAGE_FAILED;
	}
	
	reset = (recordLength + TOMBSTONE_SIZE) * (slotNumber - 1);
	
	memmove(record->data, pageHandler->data + reset + TOMBSTONE_SIZE + PAGE_INFO_SIZE, recordLength);

	return RC_OK;
}


RC prevAndNextDel_Record(RM_TableData *relation, RID id, Record *prevRec, RID *nextResultRid) {
	
	BM_PageHandle *bPoolHandle;
	
	BM_BufferPool *bufferManager;
	
	int flag ;

	int reset;
	
	int recordLength;

	RID currRid;

	RID prevRid;

   //initialize


    bPoolHandle = MAKE_PAGE_HANDLE();
	
     bufferManager = (BM_BufferPool *)relation->mgmtData;
	
	flag = 0;
	
	prevRec->id.page_n = -1;
	
	prevRec->id.slot = -1;
	
	prevRec->data = "";
	
	nextResultRid->page_n = -1;
	
	nextResultRid->slot = -1;
	
	 reset = 0;
	
	 recordLength = 0;


	recordLength = getRecordSize(relation->schema);


	RM_TABLE_INFO *rmTableInfo = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));

	rmTableInfo->s = (Schema *)malloc(sizeof(Schema));

	if (page_pinned(bufferManager, bPoolHandle, 0) != RC_OK) 
	{
	
		free(bPoolHandle);
	
		return RC_PIN_PAGE_FAILED;
	}

	
	info_deserialize(bPoolHandle->data, rmTableInfo);
	
	page_unpin(bufferManager, bPoolHandle);

	if (rmTableInfo->firstFreeRec->page_n == -1 || rmTableInfo->firstFreeRec->slot == -1)
	{
	
		free(bPoolHandle);
	
		free(rmTableInfo);
	
		return RC_OK;
	}

 
   if (rmTableInfo->firstFreeRec->page_n == id.page_n && rmTableInfo->firstFreeRec->slot > id.slot)) 
   
   {
		nextResultRid->page_n = rmTableInfo->firstFreeRec->page_n;
	
		nextResultRid->slot = rmTableInfo->firstFreeRec->slot;
	
		free(bPoolHandle);
	
		free(rmTableInfo);
	
		return RC_OK;
	}



	if (rmTableInfo->firstFreeRec->page_n > id.page_n)
	
	 {
	
		nextResultRid->page_n = rmTableInfo->firstFreeRec->page_n;
	
		nextResultRid->slot = rmTableInfo->firstFreeRec->slot;
	
		free(bPoolHandle);
	
		free(rmTableInfo);
	
		return RC_OK;
	}


	currRid.page_n = rmTableInfo->firstFreeRec->page_n;

	currRid.slot = rmTableInfo->firstFreeRec->slot;

	prevRid = currRid;

	while (id.page_n >= currRid.page_n && flag == 0)
	 {
		//For each page_n
		if (page_pinned(bufferManager, bPoolHandle, currRid.page_n) != RC_OK) {
	
			free(bPoolHandle);
	
			return RC_PIN_PAGE_FAILED;
		}
	
		if (id.page_n == currRid.page_n)
		 {
			//When record exist 

			// in same page


			while (currRid.page_n == prevRid.page_n && currRid.slot < id.slot)
	       {
	      		char *recordData;
				  
				recordData = (char *)malloc(recordLength);
				
				reset = (recordLength + TOMBSTONE_SIZE) * (currRid.slot - 1);
				
				memcpy(recordData, bPoolHandle->data + reset + TOMBSTONE_SIZE + PAGE_INFO_SIZE, recordLength);
				
				prevRid = currRid;
				
				emptyRecordDe_Serialize(recordData, &currRid);
				
				prevRec->id = prevRid;
				
				prevRec->data = recordData;
				
				nextResultRid->page_n = currRid.page_n;
				
				nextResultRid->slot = currRid.slot;

				if (currRid.page_n == -1)
				{
					if( currRid.slot == -1)
				    {
				
					free(bPoolHandle);
					
					page_unpin(bufferManager, bPoolHandle);
					
					return RC_OK;
					
					}

				}
			}

			flag = 1;
		}
		else {
		
			PAGE_INFO *pageInfo;
			
			pageInfo= NULL;
		
			HeaderExtractor(bPoolHandle, pageInfo);
		
			int lastRecordSlotInPage;
			
			lastRecordSlotInPage = pageInfo->lastRecord.slot;
		
		    
		     while((currRid.page_n == prevRid.page_n && currRid.slot <= lastRecordSlotInPage))  
			
			  {
				//If the slots are in the same page_n
				
				char *recordData = NULL;
				
				reset = (recordLength + TOMBSTONE_SIZE) * (currRid.slot - 1);
				
				memcpy(recordData, bPoolHandle->data + reset + TOMBSTONE_SIZE + PAGE_INFO_SIZE, recordLength);
				
				prevRid = currRid;
				
				emptyRecordDe_Serialize(recordData, &currRid);
				
				prevRec->data = recordData;
				
				prevRec->id = prevRid;
				
				nextResultRid->page_n = currRid.page_n;
				
				nextResultRid->slot = currRid.slot;
				
				if (currRid.page_n == -1)
				{
                  if(currRid.slot == -1)
				  {

					free(bPoolHandle);
					
					page_unpin(bufferManager, bPoolHandle);
					
					return RC_OK;

				  }
				}
			} 

			if (currRid.page_n > id.page_n) 
			{
				flag = 1;
			}
			else {
				prevRid = currRid;
			}

          if (currRid.page_n == id.page_n && currRid.slot > id.slot)
		    {
				flag = 1;
			}

			else 
			{
				prevRid = currRid;
			}

		}

		page_unpin(bufferManager, bPoolHandle);
	}

	free(bPoolHandle);
	
	return RC_OK;
}


RC startScan(RM_TableData *relation, RM_ScanHandle *scan, Expr *condidate) {

	expHandle* expHandle;
	
	BM_PageHandle *bPoolHandle;
	 
	BM_BufferPool *bufferManager;
	
	RM_TABLE_INFO *rmTableInfo;
	
	rmTableInfo->s = (Schema *)malloc(sizeof(Schema));

	scan->relation = relation;

	
	expHandle->condidate = (Expr *)condidate;
	
	expHandle->ridToScan.page_n = 1;
	
	expHandle->ridToScan.slot = 1;

    expHandle = (expHandle*)malloc(sizeof(expHandle));
	
	bPoolHandle = MAKE_PAGE_HANDLE();
	
	bufferManager = (BM_BufferPool *)relation->mgmtData;
	
	rmTableInfo = (RM_TABLE_INFO *)malloc(sizeof(RM_TABLE_INFO));
	
	
	//Pin page_n 0 to get the last rec Id.
	if (page_pinned(bufferManager, bPoolHandle, 0) != RC_OK) 
	{
		free(bPoolHandle);
	
		return RC_PIN_PAGE_FAILED;
	}
	info_deserialize(bPoolHandle->data, rmTableInfo);
	
	expHandle->lastRecID = rmTableInfo->LastRecID;
	
	page_unpin(bufferManager, bPoolHandle);
	
	free(bPoolHandle);

	scan->mgmtData = (expHandle*)expHandle;

	return RC_OK;
}

RC forwed(RM_ScanHandle *scan, Record *record) {

	RM_TableData *relation;
	
	value *valueToPointer;
	
	int flag;
	
	BM_PageHandle *bPoolHandle;
	
	char *bmPh1 = (char *)malloc(PAGE_SIZE);
	
	BM_BufferPool *bufferManager;
	 
   expHandle* expHandle;
   
   Expr *condidate;

	RID id;
    
	relation = scan->relation;
	
     flag = 0;

	 int recordLength;
	
	bPoolHandle = MAKE_PAGE_HANDLE();
	
	bmPh1 = (char *)malloc(PAGE_SIZE);
	
	bufferManager = (BM_BufferPool *)relation->mgmtData;
	
	expHandle = (expHandle*)scan->mgmtData;
	
    condidate = (Expr *)expHandle->condidate;
	

	if (expHandle->ridToScan.slot > expHandle->lastRecID.slot 
			|| expHandle->ridToScan.page_n > expHandle->lastRecID.page_n) {
		page_unpin(bufferManager, bPoolHandle);
		free(bPoolHandle);
		return RC_RM_NO_MORE_TUPLES;
	}

	recordLength = getRecordSize(relation->schema);
	
	
	 int rotateAti, j;

   rotateAti = expHandle->ridToScan.page_n;

   while(rotateAti <= expHandle->lastRecID.page_n)
   {
	if (page_pinned(bufferManager, bPoolHandle, rotateAti) != RC_OK) {
	
			free(bPoolHandle);
	
			return RC_PIN_PAGE_FAILED;
	
		}
	
		memmove(bmPh1, bPoolHandle->data, PAGE_SIZE);
	
		for (j = (expHandle->ridToScan.slot); j <= expHandle->lastRecID.slot; j++)
		{
			id.page_n = rotateAti;
	
			id.slot = j;
	
			int reset = 0;
	
			reset = (recordLength + TOMBSTONE_SIZE) * (j - 1);
	
	
			char *tombstoneData = (char *)malloc(TOMBSTONE_SIZE);
	
			memmove(tombstoneData, bmPh1 + reset + PAGE_INFO_SIZE, TOMBSTONE_SIZE);
	
			if (memcmp(tombstoneData, "1", TOMBSTONE_SIZE) == 0) {
	
				record->data = "";
	
			}
	
			else {
	
				memmove(record->data, bmPh1 + reset + TOMBSTONE_SIZE + PAGE_INFO_SIZE, recordLength);
	
			}

	
			evalExpr(record, relation->schema, condidate, &valueToPointer);
	
			if (record->data == "" ||  !valueToPointer->vToPoint.boolV)
			{
				continue;
			}
			else {
				flag = 1;
				break;
			}
		}
		free(bmPh1);
		if (flag == 1) {
			break;
		}
		
		page_unpin(bufferManager, bPoolHandle);

		rotateAti++;
	}
	expHandle->ridToScan.page_n = id.page_n;

	expHandle->ridToScan.slot = id.slot + 1;

	expHandle->condidate = condidate;

	scan->mgmtData = expHandle;

	if (flag == 1)
		return RC_OK;
	else
		return RC_RM_NO_MORE_TUPLES;
}


int isTombEncount(RM_TableData *relation, RID id) {

	BM_PageHandle *bPoolHandle;
	
	BM_BufferPool *bufferManager;
	
	int recordLength;
	
	int reset;

	bPoolHandle = MAKE_PAGE_HANDLE();

	bufferManager = (BM_BufferPool *)relation->mgmtData;

	recordLength = getRecordSize(relation->schema);

	reset = 0;

	if (page_pinned(bufferManager, bPoolHandle, id.page_n) != RC_OK)
	 {
	
		free(bPoolHandle);
	
		return RC_PIN_PAGE_FAILED;
	}
	
	char *tombStoneData = (char *)malloc(sizeof(10));;
	
	reset = (recordLength + TOMBSTONE_SIZE) * (id.slot - 1);
	
	memcpy(tombStoneData, bPoolHandle->data + reset + PAGE_INFO_SIZE, sizeof(TOMBSTONE_SIZE));
	

	if (memcmp(tombStoneData, "1", TOMBSTONE_SIZE) == 0)
	 {
	
		return 1;
	}
	
	page_unpin(bufferManager, bPoolHandle);
	
	return 0;
}
