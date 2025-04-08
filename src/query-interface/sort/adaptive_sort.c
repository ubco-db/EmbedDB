/******************************************************************************/
/**
@file		adaptive_sort.c
@author		Ramon Lawrence
@brief		Adaptive sort combining no output buffer sort and MinSort that 
            dynamically determines best sorting algorithm based on input 
            distribution. Uses replacement selection.
@copyright	Copyright 2020
			The University of British Columbia,
			IonDB Project Contributors (see AUTHORS.md)
@par Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
	this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following  disclaimer in the documentation
	and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
	may be used to endorse or promote products derived from this software without
	specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	POSSIBILITY OF SUCH DAMAGE.
*/
/******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <time.h>
#include <math.h>

#include "adaptive_sort.h"
#include "flash_minsort.h"
#include "flash_minsort_sublist.h"
#include "in_memory_sort.h"
#include "no_output_heap.h"

// #define     DEBUG         1
// #define     DEBUG_OUTPUT  1
// #define     DEBUG_READ    1
// #define     DEBUG_HEAP    0

/**
 * Prints the contents of the heap. Used for debugging.
 */
void print_heap(char* buffer,  int32_t heap_start_offset, int heap_size, int list_size, external_sort_t *es)
{    
    // Prints the heap
    int32_t aa;
    char *addr;
    int j;
    for (aa = 0; aa < 1; aa++) {
        addr = buffer + heap_start_offset;            
        printf("heap: ");
        for (j = 0; j < heap_size; j++)
            printf(" %li", *(int32_t *) (addr - j*es->record_size));
        printf("| ");
    }
    printf("   ");
    
    // Prints the list
    for (aa = 0; aa < 1; aa++) {
        addr = buffer + es->page_size;            
        printf("list: ");
        for (j = 0; j < list_size; j++)
            printf(" %li", *(int32_t *) (addr + j*es->record_size));
        printf("| ");
    }
    printf("\n");             
}

/**
@brief      Adaptive sort combining no output buffer sort and MinSort that dynamically determines best sorting
                algorithm based on input distribution. Uses replacement selection.
@param      iterator
                Row iterator for reading input rows
@param      iteratorState
                Structure stores state of iterator (file info etc.)
@param      tupleBuffer
                Pre-allocated space to store one tuple (row) of input being sorted
@param      outputFile
                Already opened file to store sorting output (and in-progress temporary results)
@param      buffer
                Pre-allocated space used by algorithm during sorting
@param      bufferSizeInBlocks
                Size of buffer in blocks
@param      es
                Sorting state info (block size, record size, etc.)
@param      resultFilePtr
                Offset within output file of first output record
@param      metric
                Tracks algorithm metrics (I/Os, comparisons, memory swaps)
@param      compareFn
                Record comparison function for record ordering
@param      runGenOnly
                True if generate sorted runs but not whole merge process
@param      writeToReadRatio
                Write time divided by read time multiplied by 10. If ratio is 2.5 
                (writes over twice as expensive) then value is 25.
*/
int adaptive_sort(
    uint8_t     (*iterator)(void *state, void* buffer),
    void    *iteratorState,
	void    *tupleBuffer,
    void    *outputFile,		
	char    *buffer,        
	int     bufferSizeInBlocks,
	external_sort_t *es,
	long    *resultFilePtr,
	metrics_t *metric,
    int8_t  (*compareFn)(void *a, void *b),
    int8_t  runGenOnly,
    int8_t  writeToReadRatio,
    void *sortData
)
{
   
    int16_t     tuplesPerPage = (es->page_size - es->headerSize) / es->record_size;	
    es->compare_fcn = compareFn;
	long        lastWritePos = 0;	
	int16_t     i, status;
	int32_t     numSublist=0;
	void        *addr;
    int32_t     numShiftOutOutput = 0, numShiftIntoOutput = 0, numShiftOtherBlock = 0;     

    /* Distribution estimation variables */
    int16_t avgDistinct = 0;                /* Average # of distinct values per run. Multiplied by 10 so can do integer rather than float operations. */
                                            /* Note: Could be int8_t as larger than 255 is above cutoff for using MinSort. */
    uint8_t  numDistinctInRun = 0;          /* Number of distinct values in current run */

 
    int optimistic = true;
    if (optimistic)
    {   
        // Do FLASH MinSort init first
        #ifdef DEBUG
        printf("*Optimistic*\n");    
        #endif
  
        MinSortState ms;
        ms.buffer = buffer;
        ms.iteratorState = iteratorState;
        ms.memoryAvailable = bufferSizeInBlocks*es->page_size;
        ms.num_records = ((file_iterator_state_t*) iteratorState)->totalRecords;
    
        init_MinSort(&ms, es, metric, compareFn);
        avgDistinct = 16;        

        int16_t numPasses = (int) ceil(log(es->num_pages/bufferSizeInBlocks)/log(bufferSizeInBlocks));
        int32_t nobSortCost = numPasses *(10 + writeToReadRatio)/10;    
        
        #ifdef DEBUG
        printf("Adaptive calculation.\n");
        printf("NOB sort cost. # runs: %d", numSublist);
        printf(" # passes: %d cost: %d\n", numPasses, nobSortCost);
        printf("MinSort cost. Num sublists: %d ", numSublist);
        printf(" Avg. distinct/sublist: %d\n", avgDistinct/10);
        #endif
        

        if (avgDistinct < nobSortCost)
        // if (true)
        {
            #ifdef DEBUG
            printf("Performing MinSort Optimistic\n");            
            #endif
            
            int16_t count = 0;  
            int32_t blockIndex = 0;
            int16_t values_per_page = (es->page_size - es->headerSize) / es->record_size;
            char* outputBuffer = buffer+es->page_size;    

            // Main sorting loop for min sort: fetches and writes sorted records in blocks
            while (next_MinSort(&ms, es, (char*) (outputBuffer+count*es->record_size+es->headerSize), metric, compareFn) != NULL)
            {         
                // Store record in block (already done during call to next)        
                // buf = (void *)(outputBuffer+count*es->record_size+es->headerSize);        
                count++;

                // When a block is full, write it to the output file
                if (count == values_per_page)
                {   
                    *((int32_t *) outputBuffer) = blockIndex;                       /* Block index */
                    *((int16_t *) (outputBuffer + BLOCK_COUNT_OFFSET)) = count;     /* Block record count */
                    
                    // Write block to the ouput file
                    if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                        return 9; // Return error code if writing to the output file fails
                    }

                    count=0;            /* Reset count for the next block */
                    blockIndex++;       /* Update to next block id */           


                    #ifdef DEBUG_OUTPUT
                        printf("Wrote output block. Block index: %d\n", blockIndex);
                        for (int k = 0; k < values_per_page; k++)
                        {
                            printf("%3d: 1 Output Record: %d\n", k, outputBuffer + es->headerSize + k * es->record_size + es->key_offset);
                        }
                    #endif
                }       
            }

            // Write last block if there are remaining records
            if (count > 0)
            {  
                *((int32_t *) outputBuffer) = blockIndex;                             /* Block index */
                *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count;            /* Block record count */
                
                if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                    return 9; // Return error code if writing to the output file fails
                }

                count=0;            /* Reset count for the next block */
                blockIndex++;       /* Update to next block id */ 
                
                #ifdef DEBUG_OUTPUT
                    printf("Wrote output block. Block index: %d\n", blockIndex);
                    for (int k = 0; k < values_per_page; k++)
                    {
                        printf("%3d: 2 Output Record: %d\n", k, *(uint32_t *)(outputBuffer + es->headerSize + k * es->record_size + es->key_offset));
                    }
                #endif
            }
             
            close_MinSort(&ms, es);  

            *resultFilePtr = 0;
            return 0;
        }
        else
        {
            optimistic = 0;
        }        
    }
    
    if (!optimistic)
    {       

        /*                                 */	
        /* -----Replacement Selection----- */	
        /*                                 */	

        // Replacement selection variables
        int32_t recordsRead     = 0;    
        int32_t heapSize        = 0;
        int32_t heapStartOffset = bufferSizeInBlocks*es->page_size - es->record_size;
        int32_t listSize        = 0; 
        
        void *lastOutputKey = malloc(es->record_size);                            /* Pointer to memory storing value of last key output */
        int8_t haveOutputKey       = 0;
        int32_t sublistSize        = 0;                 /* size in blocks */
        int32_t outputCount        = 0;                 /* number of values in output block */
        int32_t recordsLeft        = 0;       /* number of records in buffer */
        void *heapVal, *inputVal;


        // Fill all blocks other than the first with tuples
        addr = buffer+es->page_size;
        for (i = 0; i < (bufferSizeInBlocks-1)*tuplesPerPage; i++)
        {
            status=!iterator(sortData, addr);
            if (status == 0)
                break;
            recordsRead++;
            addr += es->record_size;
        }
        
        recordsLeft = recordsRead;

        // Update metrics
        metric->num_reads += bufferSizeInBlocks-1;
        metric->num_runs++;

        // Build heap from tuples in filled blocks
        for(i = 0; i < recordsRead; i++)
        {        
            addr -= es->record_size;
            memcpy(tupleBuffer, addr, es->record_size);
            metric->num_memcpys++;
            shiftUp_rev(buffer + heapStartOffset, tupleBuffer, heapSize, es, metric);
            heapSize++;
        }

        // Read each block and sort
        while (recordsLeft != 0)
        {
            recordsRead = 0;

            // Read in page
            addr = buffer+es->headerSize;
            for(i = 0; i < tuplesPerPage; i++)
            {
                status=!iterator(sortData, addr);
                if (status == 0)
                    break;
                recordsRead++;
                addr += es->record_size;
            }
            recordsLeft += recordsRead;

            #ifdef DEBUG_HEAP 
                print_heap(buffer, heapStartOffset, heapSize, listSize, es); 
            #endif

            
            if (recordsRead > 1) {
                // Sort page using in memory quick sort
                metric->num_reads += 1;        
                in_memory_quick_sort(buffer + es->headerSize, (uint32_t)recordsRead, es->record_size, es->key_offset, es->compare_fcn);
            } else if (heapSize < tuplesPerPage) {   
                // May have enough records currently in heap to continue last block. TODO: Does this make sense? It will add to last run before starting new one.
                
                // Move everything in list to the heap
                for (listSize = listSize; listSize > 0; listSize--) {
                    shiftUp_rev(buffer + heapStartOffset, buffer + es->page_size + (listSize-1)*es->record_size, heapSize, es, metric);
                    heapSize++;
                }

                // If first value in heap is smaller than lastOutputValue then start new sublist, otherwise continue with previous one.
                heapVal = buffer+heapStartOffset;
                if ( lastOutputKey == NULL || es->compare_fcn(heapVal, lastOutputKey) < 0 ) {   
                    // Start new sublist
                    numSublist++;

                    // Track number of distinct values per sublist            
                    avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct/10)*10/numSublist;
                    #ifdef DEBUG
                        printf("Number of distinct values in sublist: %d Running average: %d\n",  numDistinctInRun, avgDistinct/10);
                    #endif
                    numDistinctInRun = 1;

                    // Restart building the sublist
                    outputCount = 0;
                    haveOutputKey = 0;
                    sublistSize = 0;                
                    metric->num_runs++;
                }
            }
            
            // Swap output records into output buffer from heap if smaller than records currently there. (I/O block is id zero)
            for(i = 0; i < tuplesPerPage; i++) {
                
                // Check if we've read all records from the current page
                if (recordsRead == 0) {                   
                    // Check if there are any records left
                    if (recordsLeft <= 0)
                        break;
                    
                    // Just copy over from heap
                    memcpy(buffer + es->headerSize + i*es->record_size, buffer+heapStartOffset, es->record_size);   /* Heap into input/output block */                
                    outputCount++;
                    recordsLeft--;

                    // Restore heap
                    heapSize--;
                    if(heapSize > 0)
                        heapify_rev(buffer+heapStartOffset, buffer + heapStartOffset - heapSize*es->record_size, heapSize, es, metric);
                    continue;
                }            

                heapVal = buffer+heapStartOffset;
                inputVal = buffer+es->headerSize + i*es->record_size;          
          
                // Check if both heap top and current input value are smaller than the last output key
                // This indicates we need to start a new sorted sublist
                if (haveOutputKey && (es->compare_fcn(heapVal+es->key_offset, lastOutputKey+es->key_offset) < 0 || heapSize <= 0) && es->compare_fcn(inputVal+es->key_offset, lastOutputKey+es->key_offset) < 0)
                {
                    // Start a new sublist (as cannot use heap value or input value)
                    numSublist++;

                    // Track number of distinct values per sublist               
                    avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct/10)*10/numSublist;
                    #ifdef DEBUG
                        printf("Number of distinct values in sublist: %d Running average: %d\n",  numDistinctInRun, avgDistinct/10);
                    #endif
                    numDistinctInRun = 1;

                    // Convert unsorted list into heap
                    for (listSize = listSize; listSize > 0; listSize--)
                    {
                        shiftUp_rev(buffer + heapStartOffset, buffer + es->page_size + (listSize-1)*es->record_size, heapSize, es, metric);
                        heapSize++;
                    }

                    // Restart building the sublist
                    outputCount = 0;
                    haveOutputKey = 0;
                    sublistSize = 0;
                    recordsLeft += i;
                    i=-1; 
                    metric->num_runs++;
                    continue;
                }

                /* 
                * Decide whether to use the heap value or input value for the current output position.
                * Use the heap value if:
                *   1. Heap value is less than input value AND
                *   2. Either:
                *      a. We haven't output any values yet, OR
                *      b. Heap value is greater than or equal to last output key (maintains sort order)
                *   OR
                *   3. The input value would break sort order (is smaller than last output key)
                */
                if ((es->compare_fcn(heapVal+es->key_offset, inputVal+es->key_offset) < 0 
                    && (haveOutputKey==0 || es->compare_fcn(heapVal+es->key_offset, lastOutputKey+es->key_offset) >= 0))
                    || (haveOutputKey && es->compare_fcn(inputVal+es->key_offset, lastOutputKey+es->key_offset) < 0)
                    ){
                    
                    // Use the heap value
                    memcpy(tupleBuffer, buffer + es->headerSize + i*es->record_size, es->record_size);              /* Input tuple into buffer */
                    memcpy(buffer + es->headerSize + i*es->record_size, buffer+heapStartOffset, es->record_size);   /* Heap into input/output block */                
                    
                    // Determine if the value is different than the last one to estimate the number of distinct values
                    if (numDistinctInRun < 255 && haveOutputKey){   
                        // Value is different
                        metric->num_compar++;
                        if (es->compare_fcn(lastOutputKey+es->key_offset, inputVal+es->key_offset) < 0)
                            numDistinctInRun++;
                    }
                    // lastOutputKey = inputVal;
                    memcpy(lastOutputKey, inputVal, es->record_size);             

                    // Find somewhere to put the input value
                    if(es->compare_fcn(tupleBuffer+es->key_offset, lastOutputKey+es->key_offset) < 0){
                        // Restore heap
                        heapSize--;
                        if(heapSize > 0)
                            heapify_rev(buffer+heapStartOffset, buffer + heapStartOffset - heapSize*es->record_size, heapSize, es, metric);

                        // Put value into the unsorted list
                        memcpy(buffer+es->page_size + listSize*es->record_size, tupleBuffer, es->record_size);
                        listSize++;
                    } else {
                        // Put value into the heap
                        heapify_rev(buffer + heapStartOffset, tupleBuffer, heapSize, es, metric);
                    }
                } else {
                    // Use the input value since it's smaller or equal to heap value and maintains sort order
                    // Track if this is a new distinct value for statistics
                    metric->num_compar++;
                    if (numDistinctInRun < 255 && haveOutputKey) {   
                        // Value is different
                        metric->num_compar++;
                        if (es->compare_fcn(lastOutputKey+es->key_offset, inputVal+es->key_offset) < 0)
                            numDistinctInRun++;
                    }              
                    // Use the newly read value. don't move it, it's in proper place.            
                    // lastOutputKey = inputVal;       // Update the last key output      
                    memcpy(lastOutputKey, inputVal, es->record_size);             
            
                }
                haveOutputKey = 1;
                outputCount++;
                recordsLeft--;
                #ifdef DEBUG_HEAP 
                    print_heap(buffer, heapStartOffset, heapSize, listSize, es); 
                #endif
                if(recordsLeft == 0) 
                    break;
            }

            // Add Page Headers
            *((int32_t *) buffer) = sublistSize;
            *((int16_t *) (buffer + BLOCK_COUNT_OFFSET)) = (int8_t)outputCount;        
            memcpy(tupleBuffer, buffer+(outputCount-1)*es->record_size+es->headerSize, es->key_size);
            // lastOutputKey = tupleBuffer;
            memcpy(lastOutputKey, tupleBuffer, es->record_size);             


            // Store the last key output temporarily in tuple buffer as once write out then read new block it would be gone
            // Write the output block
            ((file_iterator_state_t *) iteratorState)->fileInterface->writeRel(buffer, PAGE_SIZE, 1, outputFile);
            if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                // File write error
                free(lastOutputKey);
                return 9;
            } 

            #ifdef DEBUG_OUTPUT
            printf("Wrote block. Sublist: %d ", numSublist);
            printf(" Idx: %d\n", sublistSize);
            //printf("Offset: %lu\n",  ftell(outputFile)-es->page_size);
            for (int k=0; k < tuplesPerPage; k++)
            {
                printf("%3d: 3 Output Record: %d\n", k, *(uint32_t *)(buffer+es->headerSize+k*es->record_size+es->key_offset));
            }
            #endif         
            
            metric->num_writes +=1;
            sublistSize++;
            outputCount = 0;       
        } /* while records left */
    
        // free(lastOutputKey);
        numSublist = metric->num_runs;
        printf("Gen time: %d\n", metric->genTime);

        // Track number of distinct values per sublist    
        avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct/10)*10/numSublist;  
        printf("Final number of distinct values in sublist: %d Average: %d\n",  numDistinctInRun, avgDistinct);
        numDistinctInRun = 0;
    } /* end pessmistic */

    // No merge phase necessary
    if (numSublist == 1) {
        ((file_iterator_state_t *) iteratorState)->fileInterface->flush(outputFile);
	*resultFilePtr = 0;
	return 0;
    }

    // Run generation phase only (DEBUG)
    if (runGenOnly)
        return 0;

    // lastWritePos = ftell(outputFile);
    lastWritePos = ((file_iterator_state_t *) iteratorState)->fileInterface->tell(outputFile);
    
    // if (avgDistinct/10 < nobSortCost)    
    int bufferSizeBytes = (bufferSizeInBlocks-1) * es->page_size;   /* One of the buffers is used for a read buffer */
    int8_t sublistVersionPossible =  (numSublist <= bufferSizeBytes / (SORT_KEY_SIZE+4));  /* +4 is size of file offset pointer. Each record has a key and file offset. */

    if (sublistVersionPossible && avgDistinct > tuplesPerPage)
        avgDistinct  = tuplesPerPage * 10;

    printf("Adaptive calculation.\n");
    int16_t numPasses = (int) ceil(log(numSublist)/log(bufferSizeInBlocks));
    int32_t nobSortCost = numPasses *(10 + writeToReadRatio)/10;    
    printf("NOB sort cost. # runs: %d", numSublist);
    printf(" # passes: %d cost: %d\n", numPasses, nobSortCost);
    printf("MinSort cost. Num sublists: %d ", numSublist);
    printf(" Avg. distinct/sublist: %d\n", avgDistinct/10);

    // Make decision to use either no output buffer sort or MinSort
    if (avgDistinct/10 < nobSortCost) {   
        /*               */
        /*    MinSort    */
        /*               */
        
        // If can buffer smallest value per sublist, can use a better performing version
        if (sublistVersionPossible) {
            // Use better performing version of minsort
            printf("Performing MinSort with sorted sublists\n");
            ((file_iterator_state_t*) iteratorState)->file = outputFile;
            *resultFilePtr = 0;
            flash_minsort_sublist(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn, numSublist);
            *resultFilePtr = lastWritePos;
        } else {   
            // Use normal version of minsort. Do not have enough space to index a value per sublist. Assumes data is not sorted in each region
            printf("Performing MinSort\n");
            ((file_iterator_state_t*) iteratorState)->file = outputFile;
            flash_minsort(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn);
            *resultFilePtr = 0;
        }
    } else {
        /*                                   */
        /*    No Output Buffer Sort Merge    */ 
        /*                                   */     

        /* ----- Merge phase: recursively combine M sublists ----- */
        long	mergeSOW;                                                                           /* start of write */
        int32_t currentBlockId          = 0;
        long	lastMergeStart		    = 0;	                                                    /* start of read */
        long	lastMergeEnd            = lastWritePos;

        long	*sublsFilePtr		    = (long*) malloc(sizeof(long) * bufferSizeInBlocks);        /* location of current record in file */
        int32_t *sublsBlkPos		    = (int32_t*) malloc(sizeof(int32_t) * bufferSizeInBlocks);  /* current block of sublist being read */
        int32_t *blocksInSublist		= (int32_t*) malloc(sizeof(int32_t) * bufferSizeInBlocks);

        int32_t *record1				= (int32_t*) malloc(sizeof(int32_t) * bufferSizeInBlocks);  /* current record of each buffered block. (byte offset from start of buffer) */
        int32_t *record2				= (int32_t*) malloc(sizeof(int32_t) * bufferSizeInBlocks);  /* current output block record stored in each buffered block (byte offset from start of buffer) */
        /* Output block uses record2 to store position of last to-output record inserted */    
        int16_t run                     = 0;
        int8_t  passNumber              = 1;
        int32_t numRuns;
        int32_t resultRecOffset         = -1;   /* Number of records from start of buffer to the next record to output */
        int32_t resultBlock	            = -1;   /* Block containing next record to output */
        char	isRecord2			    = 0;    /* 1 if result record is from the output block but stored in a non outputblock */
        int32_t offset                  = 0;    /* Offset of current record being compared with current smallest record */
        int16_t heapSizeRecords;                /* Number of records in heap */      
        char    outputIsEmpty           = 0;    /* Flag indicating if there are still more input records in sublist in output block */    
        int16_t numTransferThisPass;
        int32_t blk                     = -1;
        int16_t space                   = 0;
        int16_t outputCursor;
        int8_t  destBlk;
        int32_t other = 0;

        // Verify all memory has been allocated successfully
        if (record2 == NULL) {
            free(record1); free(sublsBlkPos); free(sublsFilePtr); free(blocksInSublist);
            return 9;
        }

        while (numSublist > 1) 
        {         
            // Check if can finish using minsort with sorted sublists
            if (numSublist >= 32 && numSublist <= 64)// && avgDistinct/10 < 32)
            {   
                // Switch to MinSort to finish off
                printf("Finishing sort with MinSort with sorted sublists\n");
                ((file_iterator_state_t*) iteratorState)->file = outputFile;    
                // *resultFilePtr = lastMergeStart;   
                // fflush(outputFile);
                ((file_iterator_state_t *) iteratorState)->fileInterface->flush(outputFile);

                *resultFilePtr = lastMergeStart;           
                flash_minsort_sublist(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn, numSublist);
                lastMergeStart = lastMergeEnd;
                *resultFilePtr = lastMergeStart;               
                break; // Sort done
            }

            // Wrap-around in memory space/file after every 3rd pass
            if (passNumber % 3 == 0)
            { 
                lastWritePos = 0;
            }

            printf("Pass number: %u  Comparisons: %lu  MemCopies: %lu  TransferIn: %lu  TransferOut: %lu TransferOther: %lu Other: %lu\n", passNumber, metric->num_compar, metric->num_memcpys, numShiftIntoOutput, numShiftOutOutput, numShiftOtherBlock, other);
            passNumber++;

            // perform a merge
            mergeSOW = lastWritePos;

            numRuns	= (numSublist + bufferSizeInBlocks -1)/bufferSizeInBlocks; /* Equivalent to CEIL(numSublist/bufferSizeInBlocks) */

            // perform runs
            long ptrLastBlock = lastMergeEnd;
            for (run = 0; run < numRuns; run++) 
            {            
                // Setup the run
                int32_t sublistsInRun = bufferSizeInBlocks;
                if (numSublist < bufferSizeInBlocks) 
                    sublistsInRun = numSublist;
                numSublist -= sublistsInRun;        

                currentBlockId = 0;
                /* 
                Note: Reading from file to find block offsets of each sublist is ONLY required as not storing these offsets in memory.
                This code also makes sure the "smallest" sublist is in output block (0) as this results in fewest swaps (especially for sorted input).
                Since sublists are scanned from back of previous run, it alternates on each pass what sublist read will be smaller. 
                On first pass, the last sublist read will be smaller. On second pass, first sublist read will be smaller.
                Considered doing for loop like this instead: for (i = sublists_in_run-1; i >=0 ; i--) 
                However due to alternating nature of when smallest sublist will be, stuck with current implementation and checked every sublist read.
                Note that check is not perfect. It is actually comparing first record in last block of each sublist as that is the block that is read
                when determining the starting point of the sublist. The first block is not read at this point. That happens later in the code.
                
                Consider checking last record instead as they may be better for the random case when sublists are not the same size in blocks.
                */  

                // Find fist block of each run          
                for (i = 0; i < sublistsInRun; i++) 
                {
                    /* Read last block of sublist into buffer */
                    // fseek(outputFile, ptrLastBlock - es->page_size, SEEK_SET); 
                    // if (0 == fread(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile)) 
                    // {   /* File read error */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 10;
                    // }

                    ((file_iterator_state_t *) iteratorState)->fileInterface->seek(ptrLastBlock - es->page_size, outputFile);
                    ((file_iterator_state_t *) iteratorState)->fileInterface->readRel(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile);
                    if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                        /* File read error */
                        free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                        return 10;
                    }

                    metric->num_reads += 1;
                    ptrLastBlock = ptrLastBlock - (*(int32_t*) &buffer[i * es->page_size])*es->page_size - es->page_size;
                    blocksInSublist[i] = *(int32_t*) &buffer[i * es->page_size] + 1;       /* Retrieve block id (indexed from 0 - hence +1) to compute count of blocks in sublist */

                    // Validate vlock offset
                    if (ptrLastBlock < lastMergeStart) {   
                        // Invalid block offset
                        sublsFilePtr[i] = -1;
                        sublsBlkPos[i] = -1;
                    } else {
                        // Valid block offset
                        sublsFilePtr[i] = ptrLastBlock;
                        sublsBlkPos[i] = 0;

                        // Move smallest entry to index 0
                        if (i != 0) {
                 
                            metric->num_compar++;

                            // Check entry at index i is less than 0
                            if (es->compare_fcn(buffer + es->headerSize+es->key_offset, buffer + i * es->page_size + es->headerSize+es->key_offset) > 0)
                            {                            
                                #ifdef DEBUG
                                void *buffer0Rec = (void*) buffer + es->headerSize;
                                void *currentRec = (void*) buffer + i * es->page_size + es->headerSize;
                                printf("Swapping in buffer 0. Current key: %d  New key: %d\n", *(uint32_t *)(buffer0Rec + es->key_offset), *(uint32_t *)(currentRec + es->key_offset));
                                #endif
                                // Perform swap
                                sublsBlkPos[i] = sublsFilePtr[0];           /* Note: Using subls_blk_pos[i] as a temp variable during swap */ // TODO: Update swap to not be variable length
                                sublsFilePtr[0] = sublsFilePtr[i];
                                sublsFilePtr[i] = sublsBlkPos[i];
                                sublsBlkPos[i] = blocksInSublist[i];
                                blocksInSublist[i] = blocksInSublist[0];
                                blocksInSublist[0] = sublsBlkPos[i];
                                sublsBlkPos[i] = 0;                         /* Reset variable back to 0 */                                                   
                            }
                        }
                    }
                }

                // Load in first blocks into buffer
                for (i = 0; i < sublistsInRun; i++) 
                {
                    // fseek(outputFile, sublsFilePtr[i], SEEK_SET);
                    // if (0 == fread(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile)) 
                    // {   /* Read error */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 10;
                    // }

                    // Read first block into buffer
                    ((file_iterator_state_t *) iteratorState)->fileInterface->seek(sublsFilePtr[i], outputFile);
                    ((file_iterator_state_t *) iteratorState)->fileInterface->readRel(&buffer[i * es->page_size], PAGE_SIZE, 1, outputFile);
                    if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                        // File read error
                        free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                        return 10;
                    }

                    metric->num_reads += 1;                

                    #ifdef DEBUG_READ
                    void *firstRec = (void*) buffer + i * es->page_size + es->headerSize;
                    void *lastRec = (void*) buffer + i * es->page_size + es->headerSize + (*((int16_t *) (buffer + i * es->page_size + BLOCK_COUNT_OFFSET))-1) * es->record_size;               
                    printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", i, (int32_t) *(buffer + i * es->page_size), 
                                    *((int16_t *) (buffer + i * es->page_size + BLOCK_COUNT_OFFSET)), *(uint32_t *)(firstRec + es->key_offset), *(uint32_t *)(lastRec + es->key_offset)); 
                    #endif
                    // Initialize record1 to start of each block and record2 to empty
                    record1[i] = i * es->page_size + es->headerSize;
                    record2[i] = -1;
                }          

                // Perform the run
                while (1) 
                {
                    // Find next smallest tuple                
                    resultBlock	                    = -1;
                    isRecord2			            = 0;                  
                
                    // Find first sublist with valid data record
                    i = 0;
                    while (record1[i] == -1 && i < sublistsInRun) 
                        i++;

                    
                    // Find a sublist with a valid data record
                    if (i < sublistsInRun) {   
                        // record found
                        resultRecOffset = record1[i];
                        resultBlock = i;
                        i++;
                    }

                    // Go through rest of sublists looking for a smaller record
                    for ( ; i < sublistsInRun; i++) 
                    {
                        if (record1[i] == -1) 
                            continue;                       /* Sublist has no more records */

                        offset = record1[i];

                        metric->num_compar++;
                        if (0 < es->compare_fcn(buffer + resultRecOffset+es->key_offset, buffer + offset+es->key_offset))
                        {   /* Record is smaller than current smallest record */
                            resultRecOffset = offset;
                            resultBlock = i;
                        }
                    }

                    // Find smallest value of last block, it might be scattered amongst other blocks 
                    // Note: For loop code is assuming OUTPUT_BLOCK_ID is 0. Otherwise, i should start at 0 not 1 and must check if i == OUTPUT_BLOCK_ID.
                    for (i = 1; i < sublistsInRun; i++) 
                    {                
                        if (record2[i] == -1)
                            continue;       /* This block has no records from the output block */

                        /* Current value is at start of block in list 2 */
                        offset = i * es->page_size + es->headerSize;

                        if (resultBlock != -1)
                            metric->num_compar++;

                        if ((resultBlock == -1) || 0 < es->compare_fcn(buffer + resultRecOffset+es->key_offset, buffer + offset+es->key_offset))
                        {   /* Record is smaller than current smallest record */
                            resultRecOffset     = offset;
                            resultBlock	        = i;
                            isRecord2			= 1;
                        }
                    }

                    // Check if a record has been found
                    if (resultBlock == -1) break;
                    
                    // Record has been found
                    // Increment record2 to next position of output block. record2 is where the next record to output will be placed 
                    if (record2[OUTPUT_BLOCK_ID] == -1) 
                        record2[OUTPUT_BLOCK_ID] = BUFFER_OUTPUT_BLOCK_START_RECORD_OFFSET;                
                    else 
                        record2[OUTPUT_BLOCK_ID] += es->record_size;                
                                                            
                    #ifdef DEBUG
                    void *buf = (void*) buffer + resultRecOffset;
                    printf("Smallest Record: %d  From list: %d\n", *(uint32_t *)(buf + es->key_offset), resultBlock);             
                    printf("List status: 0: (%d, %d) 1: (%d, %d) 2: (%d, %d) ResultList: %d\n", record1[0],record2[0],
                                                    record1[1],record2[1],record1[2],record2[2], resultBlock);

                    if (*(uint32_t *)(buf + es->key_offset) == 27391)
                    {
                        /* Output all block contents */
                        for (int l=0; l < 2; l++)
                        {   
                            printf("Current  block: %d  # records: %d\n", l, tuplesPerPage);
                            for (int k=0; k < tuplesPerPage; k++)
                            {
                                void *buf = (void*) (buffer+es->headerSize+k*es->record_size+l*es->page_size);
                                printf("%d: Record: %d  Address: %p\n", k, buf + es->key_size, buf);             
                            }
                        }
                        printf("HERE\n");
                    }
                    #endif
                    
                    /* Add smallest tuple to output position in buffer (may already be in output buffer) */
                    if (resultBlock != OUTPUT_BLOCK_ID) 
                    {
                        if ((record1[OUTPUT_BLOCK_ID] == record2[OUTPUT_BLOCK_ID]) && (record1[OUTPUT_BLOCK_ID] != -1))
                        {   /* Output block does not have space for the result record */
                            /* Optimization (removed):  
                            Determine if space in block holding smallest record to store output block.
                            If so, can directly insert into the heap in that block rather than using a temporary tuple.
                            Note: Can extend this to check if space in other blocks not just the one with smallest record.
                            This would be more comparisons but would save record copies.
                            Savings on memory copies between 1 and 2% was determined not to be worth extra calculations.
                            This is for records of 16 bytes. May be different for larger records.                           
                            */                       

                            /* Move output block's record into temporary buffer */
                            metric->num_memcpys++;
                            memcpy(tupleBuffer, buffer + record1[OUTPUT_BLOCK_ID], (size_t)es->record_size);
                            numShiftOutOutput++;                                                                                                                                           
                            #ifdef DEBUG
                            void *buf = (void*) (buffer + record1[OUTPUT_BLOCK_ID]);
                            printf("Output record moved to list %d Key: %d\n", resultBlock, *(uint32_t *)(buf + es->key_size));
                            #endif                                
                            /* Move result record into output block (record1[output_block]==record2[output_block]) */
                            metric->num_memcpys++;
                            memcpy(buffer + record2[OUTPUT_BLOCK_ID], buffer + resultRecOffset, (size_t)es->record_size);
                                                    
                            /* Move displaced output block record out of the temp buffer and into the output list (list2) of the result record's block */
                            if (isRecord2 == 0) 
                            {   /* Smallest record is not originally from output block */
                                /* Result is from record1 list. Insert into heap of output records for block. */                                                      
                                if (record2[resultBlock] == -1)
                                    record2[resultBlock] = resultBlock * es->page_size + es->headerSize;						
                                else 
                                    record2[resultBlock] += es->record_size;							
                                heapSizeRecords = (record2[resultBlock]+es->record_size-resultBlock*es->page_size)/es->record_size;                            
                                /* Buffered output record is in tuple_buffer */
                                shiftUp(buffer + resultBlock*es->page_size + es->headerSize, tupleBuffer, heapSizeRecords -1, es, metric);                            
                            }
                            else 
                            {
                                /* Result is from record2 list. Insert the displaced output value into record2 list */
                                heapSizeRecords = (record2[resultBlock]+es->record_size-resultBlock*es->page_size)/es->record_size;

                                /* Output record to be inserted is already stored in the tuple_buffer */
                                heapify(buffer + resultBlock*es->page_size + es->headerSize, tupleBuffer, heapSizeRecords, es, metric);
                            }                      
                            
                            /* Displaced the output block's current record. Increment to next output block record. */
                            record1[OUTPUT_BLOCK_ID] += es->record_size;
                            if (record1[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + (*((int16_t *) (buffer + OUTPUT_BLOCK_ID * es->page_size + BLOCK_COUNT_OFFSET))) * es->record_size + es->headerSize) 
                            // if (record1[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + tuplesPerPage*es->record_size + es->headerSize)                        
                                record1[OUTPUT_BLOCK_ID] = -1;      						
                        }
                        else 
                        {   /* Output block already has an empty slot for the result value. Only need to move result value into result list of output block. */
                            /* Move result record into output block */
                            metric->num_memcpys++;
                            memcpy(buffer + record2[OUTPUT_BLOCK_ID], buffer + resultRecOffset, (size_t)es->record_size);

                            if (isRecord2 == 1) 
                            {
                                /* is_record2: result value came from list2 of result block */
                                record2[resultBlock] -= es->record_size;

                                if (record2[resultBlock] < resultBlock * es->page_size + es->headerSize) 
                                    record2[resultBlock] = -1;                            
                                else
                                {
                                    /* Move last value to front of heap */
                                    heapSizeRecords = (record2[resultBlock] + es->record_size - resultBlock * es->page_size) / es->record_size;
                                    heapify(buffer + resultBlock*es->page_size + es->headerSize, buffer + record2[resultBlock]+es->record_size, heapSizeRecords, es, metric);
                                }                           
                            }
                        }

                        /* increment to next position of block that smallest value was read from */
                        if (isRecord2 == 0) 
                            record1[resultBlock] += es->record_size;                    
                    } /* end if smallestblock != output block */
                    else /* The smallest value is already in output block, move it from record1 to record2 */
                    {
                        
                        if (record2[resultBlock] != record1[resultBlock]) 
                        {
                            metric->num_memcpys++;
                            memcpy(buffer + record2[resultBlock], buffer + record1[resultBlock], (size_t)es->record_size);
                        }

                        record1[resultBlock] += es->record_size;
                    }	/* end of adding smallest tuple to appropriate block */

                    /* Determine if block with smallest value has any more records in it */                
                    if (record1[resultBlock] >= resultBlock * es->page_size + (*((int16_t *) (buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET))) * es->record_size + es->headerSize) 
                        record1[resultBlock] = -1;

                    /* Output block is full, write it out */
                    if (record2[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + tuplesPerPage*es->record_size - es->record_size) 
                    {                
                        // fseek(outputFile, lastWritePos, SEEK_SET);
                        // if (0 == fwrite(buffer + OUTPUT_BLOCK_ID * es->page_size, (size_t)es->page_size, 1, outputFile))
                        // {   /* File write error - Arduino prints 1st value nmemb times if nmemb != 1  */
                        //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                        //     return 9;
                        // }  

                        // Setup block header
                        *((int32_t *) buffer) = currentBlockId++;
                        *((int16_t *) (buffer + BLOCK_COUNT_OFFSET)) = (int16_t)tuplesPerPage;
                             
                        ((file_iterator_state_t *) iteratorState)->fileInterface->seek(lastWritePos, outputFile);
                        ((file_iterator_state_t *) iteratorState)->fileInterface->writeRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                        if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                            // File read error
                            free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                            return 10;
                        }                     


                        lastWritePos		        = ((file_iterator_state_t *) iteratorState)->fileInterface->tell(outputFile);
                        record2[OUTPUT_BLOCK_ID]	= -1;
                        metric->num_writes++;
                        #ifdef DEBUG_OUTPUT
                        printf("Wrote output block: %d  # records: %d\n", *((int32_t *) buffer), tuplesPerPage);
                        for (int k=0; k < tuplesPerPage; k++)
                        {
                            void *buf = (void*) (buffer+es->headerSize+k*es->record_size);
                            printf("%3d: 4 Output Record: %d  Address: %p\n", k, *(uint32_t *)(buf + es->key_offset), buf);        
                        }
                        #endif                    
                    }                

                    /* Read in the next block of a sublist if buffered block is depleted (non-output block) */
                    if ((record1[resultBlock] == -1) && (sublsBlkPos[resultBlock] != -1) && (resultBlock != OUTPUT_BLOCK_ID))
                    {
                        /* check if we are finished with that sublist */
                        if (sublsBlkPos[resultBlock] >= blocksInSublist[resultBlock] - 1) 
                        {
                            sublsBlkPos[resultBlock]	 = -1;	/* sublist is spent */
                            record1[resultBlock]	     = -1;
                        }
                        else 
                        {
                            /* not finished with sublist read in next block of sublist */
                            sublsBlkPos[resultBlock]++;
                            sublsFilePtr[resultBlock] += es->page_size;

                            /* put any output records in this block into other blocks */
                            int32_t originPtr	= resultBlock * es->page_size + es->headerSize;
                            int32_t destBlk	= OUTPUT_BLOCK_ID;
                            int16_t numTransfer = (record2[resultBlock]-originPtr) / es->record_size + 1;

                            /* while there are still records left to move */
                            while (record2[resultBlock] != -1 && originPtr <= record2[resultBlock])
                            {
                                /* Find a block with space to store the record */
                                blk         = -1;
                                space		= 0;
                                while (blk == -1 && space == 0)
                                {                                                               
                                    if (record1[destBlk] != -1)
                                        space += record1[destBlk] - (destBlk * es->page_size + es->headerSize);                       
                                    else 
                                        space += es->page_size - es->headerSize;                                

                                    if (record2[destBlk] != -1)
                                        space -= (record2[destBlk] - destBlk * es->page_size + es->record_size - es->headerSize);                                

                                    space = space / es->record_size;

                                    if (space >= 1)
                                        blk = destBlk;
                                    else 
                                        destBlk++;

                                    if (resultBlock == destBlk) 
                                        destBlk++;                     /* Go to next destination block if currently at the original block that had smallest value */

                                    if (destBlk > bufferSizeInBlocks)
                                    {
                                        printf("Incorrect destination block. List 1: (%d, %d) List 2: (%d, %d) List 3: (%d, %d) ResultList: %d\n", record1[0],record2[0],
                                                record1[1],record2[1],record1[2],record2[2], resultBlock);

                                        /* Output all block contents */
                                        for (int l=0; l < 3; l++)
                                        {   
                                            printf("Current  block: %d  # records: %d\n", l, tuplesPerPage);
                                            for (int k=0; k < tuplesPerPage; k++)
                                            {
                                                void *buf = (void*) (buffer+es->headerSize+k*es->record_size+l*es->page_size);
                                                printf("%d: Record: %d  Address: %p\n", k, buf + es->key_offset, buf);             
                                            }
                                        }
                                    }
                                }

                                numTransferThisPass = space;
                                if (space > numTransfer)
                                    numTransferThisPass = numTransfer;
                                numTransfer -= numTransferThisPass;

                                if (destBlk == OUTPUT_BLOCK_ID) 
                                {   /* Returning tuples back to output block */                         
                                    /* Position record1 input pointer at first space for record to be inserted */                               
                                    if (record1[destBlk] == -1)
                                    {   /* There are no input records in sublist 0 currently in the block */
                                        record1[destBlk] = destBlk * es->page_size + (tuplesPerPage - numTransferThisPass) * es->record_size + es->headerSize;
                                        offset = record1[destBlk];                                  /* Remember first insert location */
                                        for (i=0; i <  numTransferThisPass; i++)
                                        {
                                            #ifdef DEBUG
                                            void *buf = (void*) (buffer + originPtr);
                                            printf("Empty output block case. Moved output record back from list %d Key: %d\n", resultBlock, *(uint32_t *)(buf + es->key_offset));
                                            #endif
                                            numShiftIntoOutput++;
                                            /* Get top value from heap */
                                            metric->num_memcpys++;
                                            memcpy(buffer + record1[destBlk], buffer + originPtr, (size_t)es->record_size);

                                            /* Fix heap */
                                            heapSizeRecords = (record2[resultBlock]+es->record_size-resultBlock*es->page_size)/es->record_size;
                                            heapSizeRecords--;              /* Subtract 1 as going to use last record in heap as insert record */
                                
                                            heapify(buffer + resultBlock*es->page_size + es->headerSize, (void*) (buffer+record2[resultBlock]), heapSizeRecords, es, metric);                                    
                                            record1[destBlk] += es->record_size;
                                            record2[resultBlock] -= es->record_size;
                                        }   
                                        record1[destBlk] = offset;         /* Set pointer to first insert location */                                                                                                     
                                    }
                                    else
                                    {
                                        for (i=0; i <  numTransferThisPass; i++)
                                        {
                                            record1[destBlk] = record1[destBlk] - es->record_size;
                                            #ifdef DEBUG
                                            void *buf = (void*) (buffer + originPtr);
                                            printf("Moved output record back from list %d Key: %d\n", resultBlock, buf + es->key_offset);
                                            #endif
                                            numShiftIntoOutput++;

                                            /* insertion sort type insert */
                                            int32_t insert_ptr = record1[destBlk];
                                            while (insert_ptr < destBlk * es->page_size + (tuplesPerPage-1)*es->record_size) 
                                            {
                                                metric->num_compar++;
                                                #ifdef DEBUG
                                                void *buf = (void*) (buffer + insert_ptr + es->record_size);
                                                printf("Compare with list %d Key: %d\n", resultBlock, buf + es->key_offset);
                                                #endif
                                                if ( 0 < es->compare_fcn(buffer + originPtr+es->key_offset, buffer + insert_ptr + es->record_size+es->key_offset))
                                                {
                                                    /* shift next_val down */
                                                    metric->num_memcpys++;
                                                    memcpy(buffer + insert_ptr, buffer + insert_ptr + es->record_size, (size_t)es->record_size);
                                                }
                                                else 
                                                    break;                                    

                                                insert_ptr += es->record_size;
                                            }

                                            metric->num_memcpys++;
                                            memcpy(buffer + insert_ptr, buffer + originPtr, (size_t)es->record_size); 
                                            originPtr += es->record_size;  
                                        }                            
                                    }
                                }
                                else 
                                {
                                    for (i=0; i <  numTransferThisPass; i++)
                                    {
                                        /* insert into a non output block, put into the record2 list of the block */
                                        if (record2[destBlk] == -1) 
                                            record2[destBlk] = destBlk * es->page_size + es->headerSize;	/* no other record2 values */
                                        else 
                                            record2[destBlk] += es->record_size;	                        /* other record2 values */
                                    
                                        #ifdef DEBUG
                                        void *buf = (void*) (buffer + originPtr);
                                        printf("Moved output record to list %d Key: %d\n", destBlk, buf + es->key_offset);                        
                                        #endif
                                        numShiftOtherBlock++;
                                    
                                        /* Insert at end of heap */
                                        int32_t heapSizeRecords = (record2[destBlk]+es->record_size - es->page_size*destBlk)/es->record_size; 
                                        shiftUp(buffer + destBlk*es->page_size + es->headerSize, buffer + originPtr, heapSizeRecords -1, es, metric);    

                                        originPtr += es->record_size;                              
                                    }
                                }                          
                            }

                            /* read in next block */
                            // fseek(outputFile, sublsFilePtr[resultBlock], SEEK_SET);
                            // if (0 == fread(buffer + resultBlock * es->page_size, (size_t)es->page_size, 1, outputFile)) 
                            // {   /* Read error */
                            //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                            //     return 10;
                            // }

                            // Read in next block
                            ((file_iterator_state_t *) iteratorState)->fileInterface->seek(sublsFilePtr[resultBlock], outputFile);
                            ((file_iterator_state_t *) iteratorState)->fileInterface->readRel(buffer + resultBlock * es->page_size, PAGE_SIZE, 1, outputFile);
                            if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                                // File read error
                                free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                                return 10;
                            }                     
    
                            
                            metric->num_reads		+= 1;                                             
                            record2[resultBlock]	= -1;
                            record1[resultBlock]	= resultBlock * es->page_size + es->headerSize;
                            #ifdef DEBUG_READ
                            printf("Read block sublist: %d\n", resultBlock);
                            void *firstRec = (void*) buffer + resultBlock * es->page_size + es->headerSize;
                            void *lastRec = (void*) buffer + resultBlock * es->page_size + es->headerSize + (*((int16_t *) (buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET))-1) * es->record_size;               
                            printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", resultBlock, (int32_t) *(buffer + resultBlock * es->page_size), 
                                    *((int16_t *) (buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET)), firstRec + es->key_offset, lastRec + es->key_offset);
                            #endif
                        }
                    }	/* end if is the non output block empty */

                    /* Determine if there are no records from the output block left */
                    outputIsEmpty = 1;
                    if (record1[OUTPUT_BLOCK_ID] != -1)
                    {
                        outputIsEmpty = 0;
                    }
                    else 
                    {
                        for (i = 0; i < sublistsInRun; i++) 
                        {
                            if (i == OUTPUT_BLOCK_ID) 
                                continue;                        

                            if (record2[i] != -1)
                            {
                                outputIsEmpty = 0;
                                break;
                            }
                        }
                    }

                    // read in next block of sublist (output block)
                    if (outputIsEmpty && (-1 != sublsBlkPos[OUTPUT_BLOCK_ID])) 
                    {
                        /* check if we are finished with output blocks associated sublist */
                        if (sublsBlkPos[OUTPUT_BLOCK_ID] >= blocksInSublist[OUTPUT_BLOCK_ID] - 1) 
                        {
                            sublsBlkPos[OUTPUT_BLOCK_ID]        = -1;	/* sublist is spent */
                            record1[OUTPUT_BLOCK_ID]		    = -1;                         
                        }
                        else 
                        {                        
                            /* sublist isn't empty read in next block of sublist */
                            sublsBlkPos[OUTPUT_BLOCK_ID]++;
                            sublsFilePtr[OUTPUT_BLOCK_ID] += es->page_size;

                            /* if the output block contains results they have to be temporarily stored in other blocks. */
                            if (record2[OUTPUT_BLOCK_ID] != -1)
                            {
                                outputCursor	= OUTPUT_BLOCK_ID * es->page_size + es->headerSize;
                                destBlk		    = 1;

                                /* While there are still output tuples to move */
                                while (outputCursor <= record2[OUTPUT_BLOCK_ID]) 
                                {
                                    /* find next block with space to store a tuple. Start at block 1 continue to block N where N>1 */
                                    blk = -1;
                                    space = 0;
                                    while (-1 == blk) 
                                    {                                    
                                        if (record1[destBlk] != -1) 
                                            space += record1[destBlk] - (destBlk * es->page_size + es->headerSize);                                    
                                        else 
                                            space += es->page_size - es->headerSize;

                                        if (record2[destBlk] != -1)
                                            space -= (record2[destBlk] - destBlk * es->page_size + es->record_size - es->headerSize);                                    

                                        space = space / es->record_size;

                                        if (space >= 1) 
                                            blk = destBlk;                                    
                                        else 
                                            destBlk++;                                                                   
                                    }

                                    if (record2[destBlk] == -1)
                                        record2[destBlk] = destBlk * es->page_size + es->headerSize;                                
                                    else 
                                        record2[destBlk] += es->record_size;                                

                                    /* move the record */
                                    #ifdef DEBUG
                                    void *buf = (void*) (buffer + outputCursor);
                                    printf("Output list empty so moved record in output to list %d Key: %d\n", destBlk, *(uint32_t *)(buf+es->key_offset));
                                    #endif
                                    numShiftOutOutput++;
                                    metric->num_memcpys++;
                                    memcpy(buffer + record2[destBlk], buffer + outputCursor, (size_t)es->record_size);
                                    outputCursor += es->record_size;
                                }
                            }

                            /* Perform the the read into the now empty output block */
                            // fseek(outputFile, sublsFilePtr[OUTPUT_BLOCK_ID], SEEK_SET);

                            // if (0 == fread(buffer + OUTPUT_BLOCK_ID * es->page_size, (size_t)es->page_size, 1, outputFile)) 
                            // {   // Read error
                            //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                            //     return 10;
                            // }

                            ((file_iterator_state_t *) iteratorState)->fileInterface->seek(sublsFilePtr[OUTPUT_BLOCK_ID], outputFile);
                            ((file_iterator_state_t *) iteratorState)->fileInterface->readRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                            if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                                // File read error
                                free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                                return 10;
                            }   
                                                    
                            
                            int16_t numRecords = *((int16_t*) (buffer + BLOCK_COUNT_OFFSET));
                            #ifdef DEBUG_READ
                            printf("Read block sublist: 0\n");
                            void *firstRec = (void*) buffer + es->headerSize;
                            void *lastRec = (void*) buffer + es->headerSize + (*((int16_t *) (buffer +  BLOCK_COUNT_OFFSET))-1) * es->record_size;               
                            printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", 0, (int32_t) *(buffer + 0 * es->page_size), 
                                    *((int16_t *) (buffer + BLOCK_COUNT_OFFSET)), firstRec + es->key_offset, lastRec + es->key_offset);
                            #endif

                            metric->num_reads	+= 1;
                            record1[OUTPUT_BLOCK_ID]	= OUTPUT_BLOCK_ID * es->page_size + es->headerSize;

                            /* put the results back into the output block, re-add them in reverse order from when we removed them (blocks N to 1)
                            * This will keep the blocks in sorted order.  */
                            if (record2[OUTPUT_BLOCK_ID] != -1)
                            {
                                outputCursor = OUTPUT_BLOCK_ID * es->page_size + es->headerSize;						

                                for (blk = 0; blk < sublistsInRun; blk++) 
                                {
                                    if (record2[blk] == -1)
                                        continue;								

                                    if (blk == OUTPUT_BLOCK_ID) 
                                        continue;								

                                    int16_t blkCursor = blk * es->page_size + es->headerSize;
                                    int16_t limit = record2[blk];

                                    /* Output block read may not be full of input records. Only swap the input records. */
                                    i = 0;
                                    while (blkCursor <= limit && i < numRecords)
                                    {
                                        i++;
                                        metric->num_memcpys += 3;
                                        /* swap record */
                                        memcpy(tupleBuffer, buffer + blkCursor, (size_t)es->record_size);                                  
                                        memcpy(buffer + blkCursor, buffer + outputCursor, (size_t)es->record_size);                                    
                                        memcpy(buffer + outputCursor, tupleBuffer, (size_t)es->record_size);                                    
                                        outputCursor	+= es->record_size;
                                        blkCursor		+= es->record_size;
                                        numShiftIntoOutput++;
                                    }
                                    /* Copy back to output block all remaining records into the free space in the output block */
                                    while (blkCursor <= limit)
                                    {           
                                        metric->num_memcpys += 1;                         
                                        memcpy(buffer + outputCursor, buffer + blkCursor, (size_t)es->record_size);                                                                     
                                        outputCursor	+= es->record_size;
                                        blkCursor		+= es->record_size;
                                        numShiftIntoOutput++;
                                        record2[blk] -= es->record_size;
                                    }
                                }

                                record1[OUTPUT_BLOCK_ID] = record2[OUTPUT_BLOCK_ID] + es->record_size;

                                if (record1[OUTPUT_BLOCK_ID] >=  OUTPUT_BLOCK_ID * es->page_size + es->headerSize + numRecords*es->record_size) 
                                    record1[OUTPUT_BLOCK_ID] = -1;					
                            }
                        }                       
                    } /*end of reading in next output block */
                }	/* end of run */

                if (record2[0] > 0)
                {   /* Tuples in output block to write out */
                    // fseek(outputFile, lastWritePos, SEEK_SET);
                    // if (0 == fwrite(buffer + OUTPUT_BLOCK_ID * es->page_size, (size_t)es->page_size, 1, outputFile))
                    // {   /* File write error - arduino prints 1st value nmemb times if nmemb != 1 */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 9;
                    // }


                    // setup header
                    *((int32_t *) buffer) = currentBlockId;
                    *((int16_t *) (buffer + BLOCK_COUNT_OFFSET)) = (int16_t) (record2[0]-es->headerSize)/es->record_size + 1; 
                    currentBlockId++;

                            
                    ((file_iterator_state_t *) iteratorState)->fileInterface->seek(lastWritePos, outputFile);
                    ((file_iterator_state_t *) iteratorState)->fileInterface->writeRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                    if (((file_iterator_state_t *) iteratorState)->fileInterface->error(outputFile)){
                        // File write error
                        free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                        return 10;
                    }  
                                        


                    lastWritePos		        = ((file_iterator_state_t *) iteratorState)->fileInterface->tell(outputFile);
                    record2[OUTPUT_BLOCK_ID]	= -1;
                    metric->num_writes          += 1;

                    #ifdef DEBUG_OUTPUT
                    printf("Wrote output block here.\n");
                    for (int k=0; k < tuplesPerPage; k++)
                    {
                        void *buf = (void*) (buffer+es->headerSize+k*es->record_size);
                        printf("%3d: 5 Output Record: %d  Address: %p\n", k, *(uint32_t *)(buf + es->key_offset), buf); // TODO: Update to no use test_record_t
                    }
                    #endif
                }

            }	/* end of runs */

            numSublist                  = numRuns;      /* each run produces 1 sublist */
            lastMergeStart			    = mergeSOW;     /* next merge reads where this one started writing */
            lastMergeEnd                = lastWritePos;            
        }	/* end of merge */
        *resultFilePtr = lastMergeStart;
  
        printf("Complete. Comparisons: %u  MemCopies: %u  TransferIn: %u  TransferOut: %u TransferOther: %u Other: %d\n", metric->num_compar, metric->num_memcpys, numShiftIntoOutput, numShiftOutOutput, numShiftOtherBlock, other);
    
        /* cleanup */
        free(sublsFilePtr);
        free(sublsBlkPos);
        free(blocksInSublist);
        free(record1);
        free(record2);
    }

	return 0;
}
