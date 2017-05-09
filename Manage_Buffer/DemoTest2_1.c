
#include "buf_manager_statistic.h"
#include "manage_buff.h"
#include "dbHandler.h"
#include"manage_strg.h"
#include "help_test.h"
#include "buf_manager_statistic.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;

// check whether two the content of a buffer pool is the same as an expected content 
// (given in the format produced by sprintPoolContent)
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
static void testCreatingAndReadingDummyPages(void);
static void createDummyPages(BM_BufferPool *bm, int num);
static void checkDummyPages(BM_BufferPool *bm, int num);

static void testReadPage(void);

static void testFIFO(void);
static void testLRU(void);

// main method
int main(int argc, char *argv[])
{
	
	storageManagerInitilization();
	testName = "";

	testCreatingAndReadingDummyPages();

	testReadPage();

	testFIFO();

	testLRU();
}

// create n pages with content "Page X" and read them back to check whether the content is right
void testCreatingAndReadingDummyPages(void)
{
	BM_BufferPool *bm = MAKE_POOL();
	testName = "Creating and Reading Back Dummy Pages";

	CHECK(newPageFileCreation("testbuffer.bin"));

	createDummyPages(bm, 22);
	checkDummyPages(bm, 20);

	createDummyPages(bm, 10000);
	checkDummyPages(bm, 10000);

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer.bin"));

	free(bm);
	TEST_DONE();
}


void createDummyPages(BM_BufferPool *bm, int num)
{
	int i;
	BM_PageHandle *h = MAKE_PAGE_HANDLE();

	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

	for (i = 0; i < num; i++)
	{
		CHECK(page_pinned(bm, h, i));
		
		sprintf(h->data, "%s-%i", "Page", h->pageNum);
		
		CHECK(markBadPages(bm, h));
		
		CHECK(page_unpin(bm, h));
	}

	CHECK(Buffer_Pool_ShutDown(bm));

	free(h);
}

void checkDummyPages(BM_BufferPool *bm, int num)
{
	int i;
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	char *expected = malloc(sizeof(char) * 512);

	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

	for (i = 0; i < num; i++)
	{
		CHECK(page_pinned(bm, h, i));

		sprintf(expected, "%s-%i", "Page", h->pageNum);
		ASSERT_EQUALS_STRING(expected, h->data, "reading back dummy page content");

		CHECK(page_unpin(bm, h));
	}

	CHECK(Buffer_Pool_ShutDown(bm));

	free(expected);
	free(h);
}

void testReadPage()
{
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	testName = "Reading a page";

	CHECK(newPageFileCreation("testbuffer.bin"));
	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

	CHECK(page_pinned(bm, h, 0));
	CHECK(page_pinned(bm, h, 0));

	CHECK(markBadPages(bm, h));

	CHECK(page_unpin(bm, h));
	CHECK(page_unpin(bm, h));

	CHECK(force_buffer_Pool_Page(bm, h));

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer.bin"));

	free(bm);
	free(h);

	TEST_DONE();
}

void
testFIFO()
{
	// expected results
	const char *poolContents[] = {
		"[0 0],[-1 0],[-1 0]",
		"[0 0],[1 0],[-1 0]",
		"[0 0],[1 0],[2 0]",
		"[3 0],[1 0],[2 0]",
		"[3 0],[4 0],[2 0]",
		"[3 0],[4 1],[2 0]",
		"[3 0],[4 1],[5x0]",
		"[6x0],[4 1],[5x0]",
		"[6x0],[4 1],[0x0]",
		"[6x0],[4 0],[0x0]",
		"[6 0],[4 0],[0 0]"
	};
	const int requests[] = { 0, 1, 2, 3, 4, 4, 5, 6, 0 };
	const int numLinRequests = 5;
	const int numChangeRequests = 3;

	int i;
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	testName = "Testing FIFO page replacement";

	CHECK(newPageFileCreation("testbuffer.bin"));

	createDummyPages(bm, 100);

	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

	// reading some pages linearly with direct unpin and no modifications
	for (i = 0; i < numLinRequests; i++)
	{
		page_pinned(bm, h, requests[i]);
		page_unpin(bm, h);
		ASSERT_EQUALS_POOL(poolContents[i], bm, "check pool content");
	}

	// pin one page and test remainder
	i = numLinRequests;
	page_pinned(bm, h, requests[i]);
	ASSERT_EQUALS_POOL(poolContents[i], bm, "pool content after pin page");

	// read pages and mark them as dirty
	for (i = numLinRequests + 1; i < numLinRequests + numChangeRequests + 1; i++)
	{
		page_pinned(bm, h, requests[i]);
		markBadPages(bm, h);
		page_unpin(bm, h);
		ASSERT_EQUALS_POOL(poolContents[i], bm, "check pool content");
	}

	// flush buffer pool to disk
	i = numLinRequests + numChangeRequests + 1;
	h->pageNum = 4;
	page_unpin(bm, h);
	ASSERT_EQUALS_POOL(poolContents[i], bm, "unpin last page");

	i++;
	Force_Flush_Buffer_Pool(bm);
	ASSERT_EQUALS_POOL(poolContents[i], bm, "pool content after flush");

	// check number of write IOs
	ASSERT_EQUALS_INT(3,  getWriteIO(bm), "check number of write I/Os");
	ASSERT_EQUALS_INT(8, readIO(bm), "check number of read I/Os");

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer.bin"));

	free(bm);
	free(h);
	TEST_DONE();
}

// test the LRU page replacement strategy
void testLRU(void)
{
	// expected results
	const char *poolContents[] = {
		// read first five pages and directly unpin them
		"[0 0],[-1 0],[-1 0],[-1 0],[-1 0]",
		"[0 0],[1 0],[-1 0],[-1 0],[-1 0]",
		"[0 0],[1 0],[2 0],[-1 0],[-1 0]",
		"[0 0],[1 0],[2 0],[3 0],[-1 0]",
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		// use some of the page to create a fixed LRU order without changing pool content
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		"[0 0],[1 0],[2 0],[3 0],[4 0]",
		// check that pages get evicted in LRU order
		"[0 0],[1 0],[2 0],[5 0],[4 0]",
		"[0 0],[1 0],[2 0],[5 0],[6 0]",
		"[7 0],[1 0],[2 0],[5 0],[6 0]",
		"[7 0],[1 0],[8 0],[5 0],[6 0]",
		"[7 0],[9 0],[8 0],[5 0],[6 0]"
	};
	
	const int orderRequests[] = { 3, 4, 0, 2, 1 };
	
	const int numLRUOrderChange = 5;

	int i;
	
	int snapshot = 0;
	
	BM_BufferPool *bm = MAKE_POOL();
	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	
	testName = "Testing LRU page replacement";

	CHECK(newPageFileCreation("testbuffer.bin"));
	
	createDummyPages(bm, 100);
	
	CHECK(Buffer_Pool_init(bm, "testbuffer.bin", 5, RS_LRU, NULL));

	// reading first five pages linearly with direct unpin and no modifications
	for (i = 0; i < 5; i++)
	{
		page_pinned(bm, h, i);
		page_unpin(bm, h);
		ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content reading in pages");
	}

	// read pages to change LRU order
	for (i = 0; i < numLRUOrderChange; i++)
	{
		page_pinned(bm, h, orderRequests[i]);
		page_unpin(bm, h);
		ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content using pages");
	}

	// replace pages and check that it happens in LRU order
	for (i = 0; i < 5; i++)
	{
		page_pinned(bm, h, 5 + i);
		page_unpin(bm, h);
		ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content using pages");
	}

	// check number of write IOs
	ASSERT_EQUALS_INT(0,  getWriteIO(bm), "check number of write I/Os");
	ASSERT_EQUALS_INT(10, readIO(bm), "check number of read I/Os");

	CHECK(Buffer_Pool_ShutDown(bm));
	CHECK(destructFiles("testbuffer.bin"));

	free(bm);
	free(h);
	TEST_DONE();
}
