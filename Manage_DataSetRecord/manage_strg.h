#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dbHandler.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void storageManagerInitilization();
extern RC newPageFileCreation(char * f_name);
extern RC  Open_Buffer_Page_File(char *f_name, SM_FileHandle *fHandle);
extern RC close_Buffer_Page_File(SM_FileHandle *fileHandler);
extern RC destructFiles(char *fName);

/* reading blocks from disc */
extern RC Block_reader(int pageCount, SM_FileHandle *fileHandler, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC block_writer(int page_num, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC check_Cap(int countPages, SM_FileHandle *fHandle);

/* Extra functions which is a enhanced version of the write blocks*/
extern RC writeFromCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage, int countOfMemPage);
extern RC writeFromBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, int countOfMemPage);
extern void initializeFileHandlerDefault( SM_FileHandle * createFile );

#endif
