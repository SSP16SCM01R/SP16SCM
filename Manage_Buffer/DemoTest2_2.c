#include"manage_buff.h"
#include"dbHandler.h"
#include"manage_strg.h"
#include "help_test.h"
#include "buf_manager_statistic.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;


#define ASSERT_EQUALS_POOL(expected,bm,message)			        \
  do {									\
    char *real;								\
    char *_exp = (char *) (expected);                                   \
    real = sprintPoolContent(bm);					\
    if (strcmp((_exp),real) != 0)					\
		      {									\
	printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _exp, real, message); \
	free(real);							\
	exit(1);							\
		      }									\
    printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _exp, real, message); \
    free(real);								\
      } while(0)

// test and helper methods
static void testBufferMgrMethodsForNullValues(void);
static void testIfUnipinPageNotInBufferPool(void);
static void testFunctionsWhenPoolIsEmpty(void);
static void testShutDownBufferPoolWhenPageIsDirty(void);


// main method
int main(int argc, char *argv[])
{
	storageManagerInitilization();

	testBufferMgrMethodsForNullValues();
	testIfUnipinPageNotInBufferPool();
	testFunctionsWhenPoolIsEmpty();
	testShutDownBufferPoolWhenPageIsDirty();
}

/* Test methods for null values
*	void
*/
void testBufferMgrMethodsForNullValues(void)
{
	printf(">> testBufferMgrMethodsForNullValues() ");

	ASSERT_TRUE((Force_Flush_Buffer_Pool(NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((force_buffer_Pool_Page(NULL, NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((get_bad_flags(NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((LRUCacheInsert(NULL, NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((LRUCacheInsert(NULL, NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((Buffer_Pool_ShutDown(NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((page_unpin(NULL, NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((fetch_frame_Contents(NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((page_pinned(NULL, NULL, 1) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");
	ASSERT_TRUE((markBadPages(NULL, NULL) == RC_BM_BUFFERPOOL_NOT_INIT), "Buffer Pool not initiated.");

	printf("<< testBufferMgrMethodsForNullValues() ");
	TEST_DONE();
}

/* Test methods If we are Unipining Page which is not in buffer pool
*	void
*/
void testIfUnipinPageNotInBufferPool(void)
{
	printf(">> testIfUnipinPageNotInBufferPool() ");

	BM_BufferPool *const bm = MAKE_POOL();
	int i;
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	BM_PageHandle *invalidPage = MAKE_PAGE_HANDLE();
	invalidPage->pageNum = 999999999999999;//Assuming this pagenum is invalid
	CHECK(newPageFileCreation("testbuffer9999.bin"));
	CHECK(Buffer_Pool_init(bm, "testbuffer9999.bin", 3, RS_FIFO, NULL));
	for (i = 0; i < 5; i++)
	{
		CHECK(page_pinned(bm, h, i));
	}
	ASSERT_TRUE((page_unpin(bm, invalidPage) == RC_INVALID_PAGE_NUM), "Cannot unpin page for invalid page number.");

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer9999.bin"));
	free(bm);
	free(h);
	free(invalidPage);
	printf("<< testIfUnipinPageNotInBufferPool() ");
	TEST_DONE();
}

/* Test specific functions When Pool Is Empty
*	void
*/
void testFunctionsWhenPoolIsEmpty(void)
{
	printf(">> testFunctionsWhenPoolIsEmpty() ");

	BM_BufferPool *const bm = MAKE_POOL();
	int i;
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	CHECK(newPageFileCreation("testbuffer99999.bin"));
	CHECK(Buffer_Pool_init(bm, "testbuffer99999.bin", 3, RS_FIFO, NULL));

	ASSERT_TRUE((Force_Flush_Buffer_Pool(bm) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((force_buffer_Pool_Page(bm, h) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((get_bad_flags(bm) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((LRUCacheInsert(NULL, bm) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((page_unpin(bm, h) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((fetch_frame_Contents(bm) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");
	ASSERT_TRUE((markBadPages(bm, NULL) == RC_BUFFER_POOL_EMPTY), "Buffer Pool is empty.");

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer99999.bin"));
	free(bm);
	free(h);
	printf("<< testFunctionsWhenPoolIsEmpty() ");
	TEST_DONE();
}

/* test ShutDownBufferPool When Page Is Dirty
*	void
*/
void testShutDownBufferPoolWhenPageIsDirty(void)
{
	printf(">> testShutDownBufferPoolWhenPageIsDirty() ");

	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	testName = "Reading a page";

	CHECK(newPageFileCreation("testbuffer.bin"));
	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 3, RS_FIFO, NULL));
	CHECK(page_pinned(bm, h, 0));
	CHECK(markBadPages(bm, h));
	ASSERT_TRUE((Buffer_Pool_ShutDown(bm) == RC_OK), "Successfully shutdown buffer pool after force page.");
	CHECK(destructFiles("testbuffer.bin"));
	free(bm);
	free(h);

	printf("<< testShutDownBufferPoolWhenPageIsDirty() ");
	TEST_DONE();
}
