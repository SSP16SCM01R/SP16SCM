#include<string.h>
#include<math.h>
#include<stdlib.h>
#include"define.h"
#include"manage_strg.h"
#include"dbHandler.h"
#include"manage_buff.h"


pageFrameDetails *headNode;

int IOCountWriter;
int IOCountReader;
int count_FIFO;

//pointers to SM_FileHandle and SM_PageHandle

SM_FileHandle * fileHandler;
SM_PageHandle * pageHandler;






RC Buffer_Pool_ShutDown(BM_BufferPool *const bfool)
{


    RC valueToReturn;

	 valueToReturn = isBufferPoolValid(bfool);

       
if(isBufferPoolValid(bfool) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bfool);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

printf("Calling Buffer_Pool_ShutDown() : bfool->pageFile:%s",bfool->pageFile);

Force_Flush_Buffer_Pool(bfool);

close_Buffer_Page_File(fileHandler);

printf("Calling  Buffer_Pool_ShutDown() : bfool->pageFile:%s", bfool->pageFile);

return RC_OK;



}

// fix all dirty pageDetail with it's fix_count set to zero.

RC Force_Flush_Buffer_Pool( BM_BufferPool * const  bfool)

{

printf("You are in  force_Flush_Buffer_Pool() ");

RC valueToReturn;

valueToReturn = isBufferPoolValid(bfool);


if(isBufferPoolValid(bfool) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bfool);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

pageFrameDetails * pageFrame = headNode;


while(((pageFrameDetails *)  pageFrame) != NULL )
{
	BM_PageHandle *tmp;

	tmp = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
 
    tmp->pageNum =  ((pageFrameDetails *)pageFrame)->pageNumberOnDisk;
    
	tmp->data = ((pageFrameDetails *)pageFrame)->data;

    force_buffer_Pool_Page(bfool, tmp);

	 free(tmp);
	
	 pageFrame = ((pageFrameDetails *)  pageFrame)->next_node;



}

printf(" << Force_Flush_Buffer_Pool() end");
 
 return RC_OK;
} 




#define FOR_EACH(iterate, pagesize) \
    for (iterate = 0; iterate < pagesize; iterate++)



RC Buffer_Pool_init(BM_BufferPool *const bfool, const char *const pageFName,const int countPages, ReplacementStrategy stategy,	void *initData)

	{

		printf("Your are in initialization_Buffer_Pool() ");

		//setter

            IOCountWriter = IOCountReader = 0;
			
		    bfool->pageFile = pageFName;
			
			bfool->numPages = countPages;
			
			bfool->strategy = stategy;
			
			bfool->mgmtData = NULL;
			
			fileHandler = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));

			initializeFileHandlerDefault(fileHandler);
			
			count_FIFO = 0;

			pageHandler = (SM_PageHandle *)malloc(PAGE_SIZE);

			headNode = NULL;

			
			return Open_Buffer_Page_File(bfool->pageFile, fileHandler);



	}



RC page_pinned(BM_BufferPool *const bfool, BM_PageHandle *const pageDetail,const PageNumber page_num)
{

RC valueToReturn;

valueToReturn = isBufferPoolValid(bfool);
 

if(isBufferPoolValid(bfool) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bfool);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

printf("page_pinned() : bfool->pageFile %s ", bfool->pageFile);

int BufferPoolPageCounter;

BufferPoolPageCounter = 0;

pageFrameDetails * visitNode;

visitNode = (pageFrameDetails *)malloc(sizeof(pageFrameDetails));

visitNode = headNode;

if(visitNode != NULL)
{
BufferPoolPageCounter= 1;
}

char tmp[PAGE_SIZE];


while( visitNode == NULL || visitNode->pageNumberOnDisk != page_num)
{

//pageDetail not present in buffer pool so fetch from memory

if(visitNode == NULL || visitNode->next_node == NULL)
{
	//capacity issues
	if(RC_OK != check_Cap(page_num , fileHandler))
	{

		 return  RC_ENSURE_CAPACITY_FAILED;
	}
   //if readbblocked due to permission
    if (RC_OK != Block_reader(page_num, fileHandler, pageHandler))
			{
				return RC_READ_FAILED;
			}

         strcpy(tmp, pageHandler);
			
	      //setters assigning data

		  pageDetail->data = tmp;

		  pageDetail->pageNum = page_num;

		  IOCountReader += 1;
          
		 pageFrameDetails *new_Node;
		 
		 new_Node = (pageFrameDetails *)malloc(sizeof(pageFrameDetails));
		
		new_Node->pageNumberOnDisk = page_num;

        strcpy(new_Node->data, (pageDetail->data));

    	new_Node->fixedCount = 1;
		
		new_Node->isDirtyPage = FALSE;
		
		//setting the next node to null 
		new_Node->next_node = NULL;

		//size of buffer pool is not full

		
		if(bfool->numPages > BufferPoolPageCounter)
		{
		if(RS_LRU != bfool->strategy)
         {
			 if(visitNode == NULL)
			 {
				 //in case the visit node reach the null or there
				 //is no node
				 headNode = new_Node;
			 }
			 else
			 {
				 //if head node is present 
				 //new node will be attach next.
				 visitNode->next_node = new_Node;
			 }

			 return RC_OK;

		 }

		 else
		 {
			 if (bfool->strategy == RS_FIFO) {
				FirstInFirstOut(new_Node, bfool);
					return RC_OK;
				}
				else if (bfool->strategy == RS_LRU){
				 LRUCacheInsert(new_Node, bfool);
					return RC_OK;
				}

		 }

		}//end of if(bfool->numPages ...
       

		}//end of if visit node == null
    
	  //forwed the node to visit next one.
      
	  visitNode = visitNode->next_node;
		
	  BufferPoolPageCounter++;

}//end of while


//buffer pool contain the pageDetail just need reorder

visitNode->fixedCount ++;

pageDetail->pageNum = visitNode->pageNumberOnDisk;

pageDetail->data = visitNode->data;

if(bfool->strategy == RS_LRU)
{
  LRUCacheReorder(visitNode);

}

printf("<< pinPage() : bfool->pageFile:%s", bfool->pageFile);

return RC_OK;



}




RC FirstInFirstOut(pageFrameDetails *pageDetail , BM_BufferPool *const bf)
{

RC valueToReturn;

RC valueToReturn1;

valueToReturn = isBufferPoolValid(bf);


if(isBufferPoolValid(bf) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bf);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

valueToReturn1 = isBufferPoolEmpty();

if(valueToReturn1 != RC_OK)
{
	return valueToReturn1;
}

pageFrameDetails *forwed;

pageFrameDetails  *back;

int iterate;

for(iterate = 0; iterate < count_FIFO ; iterate++)
{
	
	back = forwed;

	back = back->next_node;
}

int totalCount = count_FIFO;


while(count_FIFO != totalCount)
{

if (((pageFrameDetails *)forwed)->fixedCount == 0)
		{
			if (((pageFrameDetails *)forwed)->isDirtyPage == TRUE)
			{
				BM_PageHandle *tmp;
				
				tmp = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

				tmp->data = forwed->data;

				tmp->pageNum = ((pageFrameDetails *)forwed)->pageNumberOnDisk;

			    RC forwedent_status;
				
				forwedent_status = force_buffer_Pool_Page(bf, tmp);

				free(tmp);

				if (RC_OK != forwedent_status)
				{
					return forwedent_status;
				}
			}
			count_FIFO = ++count_FIFO % bf->numPages;
			break;
		}
		else
		{
			count_FIFO = ++count_FIFO % bf->numPages;
			if (count_FIFO == 0)
			{
				back = NULL;
				forwed = headNode;
			}
			else
			{
				back = forwed;

				forwed = forwed->next_node;
			}
		}

}

}



RC isBufferPoolEmpty()
{
	if(headNode == NULL)
	{
     // no node inserted so far
	 return RC_BUFFER_POOL_EMPTY;

	}
    else if(headNode != NULL)
	{
      return RC_OK;
	}


}


RC page_unpin(BM_BufferPool *const bPool, BM_PageHandle *const pageDetail)
 {
	
RC valueToReturn;

RC valueToReturn1;

valueToReturn = isBufferPoolValid(bPool);

       
if(isBufferPoolValid(bPool) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bPool);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

valueToReturn1 = isBufferPoolEmpty();

if (valueToReturn1 != RC_OK) 
{
	
  return valueToReturn1;
		
}



printf(">> unpin_Page() : bPool->pageFile:%s", bPool->pageFile);

pageFrameDetails *tmp_node = headNode;

while (tmp_node->pageNumberOnDisk != pageDetail->pageNum) {
	   // loop till next_nede is not empty
		if (tmp_node->next_node == NULL)
	    {
           //if next node empty that will return invalid

			return RC_INVALID_PAGE_NUM;
		}

		tmp_node = tmp_node->next_node;
	}

	tmp_node->fixedCount--;

	printf(" bPool->pageFile:%s", bPool->pageFile);

	return RC_OK;
}





RC force_buffer_Pool_Page( BM_BufferPool *const bfool , BM_PageHandle *const pageDetail)
{

 RC valueToReturn;
 
valueToReturn = isBufferPoolValid(bfool);



if(isBufferPoolValid(bfool) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bfool);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}


pageFrameDetails *pageFrame = headNode;


while(((pageFrameDetails *)  pageFrame) != NULL )
{
		if (((pageFrameDetails *)pageFrame)->pageNumberOnDisk == pageDetail->pageNum)
		{
			
			if (((pageFrameDetails *)pageFrame)->fixedCount == 0)
			{
			
				if(((pageFrameDetails *)pageFrame)->isDirtyPage == TRUE)
				{
					
					if (block_writer(pageDetail->pageNum , fileHandler , ((pageFrameDetails *)pageFrame)->data) != RC_OK)
					{
					
						return RC_WRITE_FAILED;
					
					}
					
					IOCountWriter++;
					
					pageFrame->isDirtyPage = FALSE;
				}
				return RC_OK;
			}
			else
			{
				return RC_BUFFER_PAGE_IN_USE;
			}
		}

		pageFrame = ((pageFrameDetails *)pageFrame)->next_node;
	}
    
	printf("<< forcePage() ");
	
	return RC_BUFFER_PAGE_NOT_FOUND;

}


RC isBufferPoolValid(BM_BufferPool *const  bfool)
{
   if(bfool == NULL)
   {
	   //checking if bfool initiated

	   if(bfool->pageFile == NULL || bfool->strategy < 0 || bfool->numPages < 0)

           {
              return RC_BM_BUFFERPOOL_NOT_INIT;           
		   }
		   else
		   {
			   return RC_OK;
		   }
	           	   
    }



}


RC LRUCacheInsert(pageFrameDetails *pageDetail, BM_BufferPool *const bP)
{

 RC valueToReturn;
       
if(isBufferPoolValid(bP) == RC_OK)
{
    valueToReturn = isBufferPoolValid(bP);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}

	
bool isGotHit;

isGotHit = FALSE;

pageFrameDetails *forwed;

pageFrameDetails *back;

forwed = headNode;

//nothing present on back side

back = NULL;

int countElements;

countElements = 0;

	if (forwed == NULL)
	{
		pageDetail->order = 1;

		pageDetail->next_node = NULL;

        //make the pageDetail node the head node
		headNode = pageDetail;

		return RC_OK;
	}

	while (forwed != NULL)
	{
		back  = forwed;

		if (forwed->pageNumberOnDisk == pageDetail->pageNumberOnDisk)
		{
			isGotHit = TRUE;
		}
		//increment the counter for node counting

		countElements++;
 

        // forwed the node

		forwed = forwed->next_node;

	}


	if (isGotHit == TRUE)
	{
		if (RC_OK != LRUCacheReorder(pageDetail))
		{
			return RC_LRU_FAIL;
		}
	}
	else
	{
		if (countElements < bP->numPages)
		{
			//make the temp nod at head

			// then visit all nodes and count the elements
			pageFrameDetails *tmp1 = headNode;

			while (tmp1 != NULL)
			{
				tmp1->order++;
				
				tmp1 = tmp1->next_node;
			
			}
			
			pageDetail->order = 1;
			
			back->next_node = pageDetail;
		}
		else
		{
			
			forwed = headNode;

		    back = NULL;
			
			int iterate;
			
			iterate = bP->numPages;

			while (forwed != NULL && iterate > 0)
			{
				
				if (forwed->order == iterate)
				{
					if (forwed->fixedCount != 0)
					{
						iterate--;
						forwed = headNode;
						continue;
					}
					else
					{
						int assignOrder;

						pageFrameDetails *tmp;
						
						assignOrder = forwed->order;
						
						tmp = headNode;
						
						while (tmp != NULL)
						{
							if (tmp->order < assignOrder)
							{
								tmp->order++;
							}
							
							tmp = tmp->next_node;
						}

						if (back == NULL)
						{
							pageDetail->order = 1;
					
							pageDetail->next_node = forwed->next_node;
					
							free(forwed);
					
							headNode =pageDetail;
						}
						else
						{
							back->next_node = pageDetail;
						
							pageDetail->order = 1;
						
							pageDetail->next_node = forwed->next_node;
						
							free(forwed);
						}
						return RC_OK;
					}
				}
				
				back = forwed;
               
			    // move to next node

				forwed = forwed->next_node;
			}
		}
	}

    forwed = NULL;

	back = NULL;
	
	free(forwed);
	
	free(back);
 
 printf("LRUCacheInsert()");

	return RC_OK;
 

}

PageNumber *fetch_frame_Contents(BM_BufferPool *const bm)
 {
		RC valueToReturn;

		RC valueToReturn1;

		valueToReturn = isBufferPoolValid(bm);

		if(isBufferPoolValid(bm) == RC_OK)
		{
			valueToReturn = isBufferPoolValid(bm);

		}
		else if( valueToReturn !=  RC_OK)
		{
			return valueToReturn;
		}
    
		valueToReturn1 = isBufferPoolEmpty();

		if (valueToReturn1 != RC_OK) 
		{
			
		return valueToReturn1;
				
		}

    	printf(">> fetch_frame_Contents() : bm->pageFile:%s", bm->pageFile);
	
	   int iterator = 0;
    
		int *arrayOfPageNumbers = (int *)malloc(sizeof(int) * bm->numPages);
    
		pageFrameDetails* tempNode = headNode;
	   

	   for(iterator = 0; iterator < bm->numPages; iterator++)
	   {
       

		if (tempNode == NULL)
		{
			arrayOfPageNumbers[iterator] = NO_PAGE;
		}
		else
		{
			arrayOfPageNumbers[iterator] = tempNode->pageNumberOnDisk;
			tempNode = tempNode->next_node;
		}

	   }
	 
	printf("frame content fetched fetch_frame_Contents() : bm->pageFile:%s", bm->pageFile);

		return arrayOfPageNumbers;
}


int *get_Counts(BM_BufferPool *const bPool)
{
     
	 printf(">> Called getFixCounts() ");

        int iterate;
       
       int *fixCount;
	
    	RC valueToReturn;

		RC valueToReturn1;

		valueToReturn = isBufferPoolValid(bPool);

		if(isBufferPoolValid(bPool) == RC_OK)
		{
			valueToReturn = isBufferPoolValid(bPool);

		}
		else if( valueToReturn !=  RC_OK)
		{
			return valueToReturn;
		}
    
		valueToReturn1 = isBufferPoolEmpty();

		if (valueToReturn1 != RC_OK) 
		{
			
		return valueToReturn1;
				
		}

	  fixCount = (int *)malloc(sizeof(int) * bPool->numPages);
	
	pageFrameDetails *forwed_node = headNode;

	for (iterate = 0; iterate < bPool->numPages; iterate++)
	{
		if (forwed_node != NULL)
		{
			
			fixCount[iterate] = forwed_node->fixedCount;
			
			forwed_node  = forwed_node->next_node;
		}
		else
		{
			fixCount[iterate] = 0;
		}
	}
    
	printf("getting getFixCounts() ");
	
	return fixCount;
}

bool *get_bad_flags(BM_BufferPool *const bm)
{
   
   printf(">> getDirtyFlags() ");
	
        int iterate;
       
        bool *dirtyFlagStatus;
	   
	   pageFrameDetails *forwed;

    	RC valueToReturn;

		RC valueToReturn1;

		valueToReturn = isBufferPoolValid(bm);

		if(isBufferPoolValid(bm) == RC_OK)
		{
			valueToReturn = isBufferPoolValid(bm);

		}
		else if( valueToReturn !=  RC_OK)
		{
			return valueToReturn;
		}
    
		valueToReturn1 = isBufferPoolEmpty();

		if (valueToReturn1 != RC_OK) 
		{
			
		return valueToReturn1;
				
		}

	forwed = headNode;
	
	dirtyFlagStatus = (bool*) malloc(sizeof(bool) * bm->numPages);

    
	for(iterate = 0;iterate < bm->numPages;iterate++)
    {
    
	  if (((pageFrameDetails *)forwed) != NULL)
		{
			dirtyFlagStatus[iterate] = ((pageFrameDetails *)forwed )->isDirtyPage;
			
			forwed = ((pageFrameDetails *)forwed)->next_node;
		}
		else
		{
			dirtyFlagStatus[iterate] = FALSE;
		}

	}

    
	printf("<< getDirtyFlags() ");
	
	return dirtyFlagStatus;

}


RC LRUCacheReorder(pageFrameDetails *pageDetail)
{


	pageFrameDetails *forwed;

	int order;
	
	forwed = headNode;

	
	
	order = pageDetail->order;
	
	while (forwed != NULL)
	{
		if (forwed->order < order)
		{
			forwed->order++;
		}
		else if (forwed->order == order)
		{
			forwed->order = 1;
		}
		forwed = forwed->next_node;
	}

	return RC_OK;



}


int readIO(BM_BufferPool *const bmPool)
{

	RC returnVal;
	
	returnVal = isBufferPoolValid(bmPool);
   
   if(isBufferPoolValid(bmPool) == RC_OK)
   {
      	returnVal = isBufferPoolValid(bmPool);

   }   
   else if (returnVal != RC_OK)
	{
		return 0;
	}

    printf(" readIO()");

	return IOCountReader;
}


int getWriteIO(BM_BufferPool *const bf)
{
  RC ValueToReturn;
	
	 ValueToReturn = isBufferPoolValid(bf);
   
   if(isBufferPoolValid(bf) == RC_OK)
   {
      	 ValueToReturn= isBufferPoolValid(bf);

   }   
   else if ( ValueToReturn != RC_OK)
	{
		return 0;
	}
  
    printf(" readIO() ");
	
	return IOCountWriter;
}

//Function to mark given page as bad Page in the Buffer Pool

RC markBadPages(BM_BufferPool *const bfool, BM_PageHandle *const pageDetail)
{
 
  RC ValueToReturn;

  RC ValueToReturn1; 

   printf("Will mark the bad pages ");

   ValueToReturn = isBufferPoolValid(bfool);

   if(isBufferPoolValid(bfool) == RC_OK)
   {
      	ValueToReturn = isBufferPoolValid(bfool);

   }   
   else if (ValueToReturn != RC_OK)
	{
		return 0;
	}

	ValueToReturn1 = isBufferPoolEmpty();
	
	if (ValueToReturn1 != RC_OK) {
	
		return ValueToReturn1;
	}

	pageFrameDetails *forwed;
	
	forwed = headNode;

	while (forwed != NULL && forwed->pageNumberOnDisk != pageDetail->pageNum)
	{
		forwed = forwed->next_node;
	}

	if (forwed == NULL)
	{
		return RC_BUFFER_PAGE_NOT_FOUND;
	}
	else
	{
		forwed->isDirtyPage = true;
	
		strcpy(forwed->data, pageDetail->data);

        printf(" markBadPages() ");
		
		return RC_OK;
	}
}