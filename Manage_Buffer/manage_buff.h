#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include"dbHandler.h"

// Include bool DT
#include"define.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

/*
* Stores details of a pageframe
*/
typedef struct pageFrameDetails
{
	int order;
	bool isDirtyPage;
	int fixedCount;
	int pageNumberOnDisk;
	char data[PAGE_SIZE];
	struct pageFrameDetails *next_node;
}pageFrameDetails;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  void *mgmtData; // use this one to store the bookkeeping info your buffer 
                  // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))


// Buffer Manager Interface Pool Handling
RC Buffer_Pool_init(BM_BufferPool *const bfool, const char *const pageFName,const int countPages, ReplacementStrategy stategy,	void *initData);
RC Buffer_Pool_ShutDown(BM_BufferPool *const bfool);
RC Force_Flush_Buffer_Pool( BM_BufferPool * const  bfool);



// Buffer Manager Interface Access Pages


RC markBadPages(BM_BufferPool *const bfool, BM_PageHandle *const pageDetail);
RC page_unpin(BM_BufferPool *const bP, BM_PageHandle *const pageDetail);
RC force_buffer_Pool_Page(BM_BufferPool *const bm, BM_PageHandle *const page);
RC page_pinned(BM_BufferPool *const bfool, BM_PageHandle *const pageDetail,const PageNumber page_num);


// Statistics Interface
PageNumber *fetch_frame_Contents(BM_BufferPool *const bm);
bool *get_bad_flags(BM_BufferPool *const bm);
int *get_Counts(BM_BufferPool *const bPool);
int getreadIO(BM_BufferPool *const bmPool);
int getWriteIO(BM_BufferPool *const bf);

#endif
	