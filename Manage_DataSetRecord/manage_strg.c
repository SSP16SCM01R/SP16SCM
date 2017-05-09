#include <sys/stat.h>
#include <errno.h>
#include "manage_strg.h"
#include "math.h"
#include "stdlib.h"
#include <string.h>
#include <fcntl.h>
#include "dbHandler.h"
#include <stdio.h>


#pragma warning (disable : 4996)

//initialize file handler to default values

void initializeFileHandlerDefault( SM_FileHandle * createFile )
{
   createFile->fileName = NULL;

   createFile->curPagePos = 0;

   createFile->totalNumPages = 0;

   createFile->mgmtInfo = NULL;


}


//this function check for the validation of file name

RC isFileNameValid(char * f_name)
{
  if(f_name == NULL)
  {
       
       return RC_INVALID_FILE_NAME;

  }

  return RC_OK;

}


//this function check if file handle is initiated

RC isFileHandleInit(SM_FileHandle *fileHandler)
 {
     // any conditio among them true will not initiate the file handler
	if (fileHandler == NULL ||		fileHandler->fileName == NULL|| fileHandler->mgmtInfo == NULL||		fileHandler->totalNumPages < 0 ||	fileHandler->curPagePos < 0) {
		
        return RC_FILE_HANDLE_NOT_INIT;
	}

	return RC_OK;
}



#define foreach(iteration , pageSize) \
             for(iteration= 0; iteration < pageSize; iteration++);


//initialize all values required for storage manager

void storageManagerInitilization()
{

}



//create page file having at least one page as default

RC newPageFileCreation(char * f_name)
{

//call isFileNameValid function if correct or not


    RC valueToReturn;
       
if(isFileNameValid(f_name) == RC_OK)
{
    valueToReturn = isFileNameValid(f_name);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}


printf(" newPageFileCreation()  : Name of file %s ", f_name);

FILE *new_file_p = fopen(f_name, "w+");

if(new_file_p == NULL)
{
    return RC_FILE_NOT_FOUND;
}

int i;

foreach( i , PAGE_SIZE )
{
    fprintf(new_file_p , "%c" , '\0' );
}

fclose(new_file_p);

printf("File creation of new newPageFileCreation() : Name of file %s ", f_name);

return RC_OK;

}

//destory page file 

RC destructFiles(char *fName)
{
   

    RC valueToReturn;

    valueToReturn = isFileNameValid(fName);
   

if(isFileNameValid(fName) == RC_OK)
{
    valueToReturn = isFileNameValid(fName);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}
    

printf("file to destory by function destructFile() : Name of file %s ", fName);

if( remove(fName) == 0)
{
    return RC_OK;

}
else
{
    printf("file to destory not destroy by function destructFile() : Name of file %s ", fName);
 
    return RC_FILE_NOT_FOUND;

}



}

// checks if the give page number is valid
	
RC isPageNumberValid(int page_num) {
	
    //it means no page present

    if (page_num < 0) {

		return RC_INVALID_PAGE_NUM;
	}
   else if(page_num >= 0)
   {
	return RC_OK;
   }
}


RC Block_reader(int pageCount, SM_FileHandle *fileHandler, SM_PageHandle memPage)
{

	RC isNumberValid;
    
    RC isfileInit;
      
     isNumberValid = isPageNumberValid(pageCount);
 
     if(isPageNumberValid(pageCount)== RC_OK)
     {
           
             isNumberValid =  pageCount;

     }
	else if (isPageNumberValid != RC_OK)
	{
		   return isNumberValid;
	}

	isfileInit = isFileHandleInit(fileHandler);
	
    if (isfileInit != RC_OK)
	{
		return isfileInit;
	}


	if (pageCount > fileHandler->totalNumPages) 
    {
		return RC_READ_NON_EXISTING_PAGE;
	
    }

   // seek functio move the curser to the position of user desired

	fseek(fileHandler->mgmtInfo, PAGE_SIZE*(pageCount), SEEK_SET);

  //will start reading the contend

    fread(memPage, PAGE_SIZE, 1, fileHandler->mgmtInfo);
	

    //current position when read

    fileHandler->curPagePos = pageCount;

	printf(" reader_Block finished ");

	return RC_OK;
}


RC close_Buffer_Page_File(SM_FileHandle *fileHandler)
{
	RC isFileInit;
    
    isFileInit = isFileHandleInit(fileHandler);

	if (isFileInit != RC_OK)
	{
		return  isFileInit;
	}

	printf("close page file fileName:%s", fileHandler->fileName);

	fclose(fileHandler->mgmtInfo);

      //close file
	fileHandler->curPagePos = 0;	
	
    fileHandler->fileName = NULL;

	fileHandler->mgmtInfo = NULL;

	fileHandler->totalNumPages = 0;

	free(fileHandler);

	printf("close_Buffer_Page_File()");
	
    return RC_OK;
}


RC  Open_Buffer_Page_File(char *f_name, SM_FileHandle *fHandle)
{
	    RC valueToReturn;

    valueToReturn = isFileNameValid(f_name);
   

if(isFileNameValid(f_name) == RC_OK)
{
    valueToReturn = isFileNameValid(f_name);

}
else if( valueToReturn !=  RC_OK)
{
	return valueToReturn;
}
	
if ( fHandle != NULL)
{
    if(fHandle->mgmtInfo != NULL)
    {
		//file already exist
        
        return RC_FILE_ALREADY_IN_USE;

    }
}

	FILE *new_file = fopen(f_name, "r+");
	
    if (!new_file)
	{
		// this mean already exist file
        
        	return RC_FILE_NOT_FOUND;
	}
	//to find the total number of pages in the file
	int isSeeked = fseek(new_file, 0L, SEEK_END);  /* fseek returns non-zero on error. */

	if (isSeeked != 0)
	{
		fclose(new_file);
		return RC_RM_NO_MORE_TUPLES;
	}


	long fileSize = ftell(new_file);   
	
    rewind(new_file); 

	if (fileSize == 0)	
    {
		fHandle->totalNumPages = 0;
	
    	fclose(new_file);
	
    	return RC_IM_NO_MORE_ENTRIES;
	}

	int totalPageSize = 0;
    
    totalPageSize = (int *)(fileSize / PAGE_SIZE);

	fHandle->fileName = f_name;
	
    fHandle->curPagePos = 0;

	if ((fileSize % PAGE_SIZE) == 0)
	{
		fHandle->totalNumPages = totalPageSize;
	}
	else
	{
		fHandle->totalNumPages = ++totalPageSize;
	}

	fHandle->mgmtInfo = new_file;
	
    
	return RC_OK;
}



RC check_Cap(int countPages, SM_FileHandle *fHandle)
{
	RC isNumberValid;
    
    RC isfileInit;

    isNumberValid = isPageNumberValid(countPages);

     if(isPageNumberValid(countPages)== RC_OK)
     {
           
             isNumberValid =  countPages;

     }
	else if (isPageNumberValid != RC_OK)
	{
		   return isNumberValid;
	}
    
    isfileInit = isFileHandleInit(fHandle);

	if (isfileInit!= RC_OK)
	{
		return isfileInit;
	}

	printf("capcity ensure by ensureCapacity() : for numberOfPages:%d", countPages);

	if (fHandle->totalNumPages < countPages)
	{
		while (fHandle->totalNumPages != countPages)
		{
			append_empty_block(fHandle);
		}
	}

	return RC_OK;
}

RC append_empty_block(SM_FileHandle *fHandle)
{
    
    RC isFileInit;
    
    isFileInit = isFileHandleInit(fHandle);

	if (isFileInit != RC_OK)
	{
		return  isFileInit;
	}


	printf("file handle init : fileName:%s", fHandle->fileName);
	
    int count;
	
    FILE *f_pointer;
	
    
    f_pointer = fHandle->mgmtInfo;
	
    for (count = 0; count < PAGE_SIZE; count++) {
	
    fprintf(f_pointer, "%c", '\0');

	}

	fHandle->curPagePos += 1;
	
    fHandle->totalNumPages = fHandle->totalNumPages + 1;
	
    printf("<< appendEmptyBlock() ");
	
    return RC_OK;
}


RC block_writer(int page_num, SM_FileHandle *fHandle, SM_PageHandle memPage)
 {
	
    RC isFileInit;

    RC isNumberValid;
    
    isNumberValid = isPageNumberValid(page_num);

     if(isPageNumberValid(page_num)== RC_OK)
     {
           
             isNumberValid =  page_num;

     }
	else if (isPageNumberValid != RC_OK)
	{
		   return isNumberValid;
	}


    
    isFileInit = isFileHandleInit(fHandle);

	if (isFileInit != RC_OK)
	{
		return  isFileInit;
	}

	printf(" To write blockblockWriter() : To page_num:%d", page_num);

	if (fHandle->totalNumPages == 0) {

		return RC_EMPTY_FILE;

	}

	if (sizeof(memPage) / PAGE_SIZE > 1)
     {
             // no limit for writing

		return RC_CANNOT_WRITE_MORE_THAN_ONE_BLOCK;
	}
	
    int totalPages = fHandle->totalNumPages > page_num ? fHandle->totalNumPages : page_num;
	
    check_Cap(totalPages, fHandle);

	int isSeeked = fseek(fHandle->mgmtInfo, PAGE_SIZE*(page_num), SEEK_SET);  /* fseek returns non-zero on error. */
	
    if (isSeeked != 0) 
    {
		
        return RC_RM_NO_MORE_TUPLES;
	}
	
    fwrite(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
	
    fHandle->curPagePos = page_num;
	
    printf("blockWriter() ");
	
    return RC_OK;
}

