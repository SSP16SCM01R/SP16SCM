#ifndef BUFFER_MGR_STAT_H
#define BUFFER_MGR_STAT_H

#include "manage_buff.h""

// debug functions
void printPoolContent(BM_BufferPool *const bm);
void printPageContent(BM_PageHandle *const page);
char *sprintPoolContent(BM_BufferPool *const bm);
char *sprintPageContent(BM_PageHandle *const page);

#endif
