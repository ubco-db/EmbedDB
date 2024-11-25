/******************************************************************************/
/**
@file		flash_minsort.c
@author		Ramon Lawrence
@brief		Flash MinSort (Cossentine/Lawrence 2010) for flash sorting with no writes.
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

/*
This is no output sort with block headers and iterator input. Heap used when moving tuples in other blocks.
*/

#include "flash_minsort.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "in_memory_sort.h"

// #define DEBUG 1
// #define DEBUG_OUTPUT 1
// #define DEBUG_READ 1

/**
 * Reads a page from the source file into memory.
 * @param ms Pointer to the MinSortState structure holding sorting state.
 * @param pageNum The page number to read.
 * @param es Sorting configuration, including page and record sizes.
 * @param metric Metrics tracking structure for performance analysis.
 */
void readPageMinSort(MinSortState *ms, int pageNum, external_sort_t *es, metrics_t *metric) {
    file_iterator_state_t *is = (file_iterator_state_t *)ms->iteratorState;
    void *fp = is->file;

    // Read page into into the buffer
    if (0 == is->fileInterface->read(ms->buffer, pageNum, es->page_size, fp)) {
        printf("Failed to read block.\n");
    }

    metric->num_reads++;
    ms->blocksRead++;
    ms->lastBlockIdx = pageNum;

#ifdef DEBUG_READ
    printf("Reading block: %d\n", pageNum);
    for (int k = 0; k < 31; k++) {
        test_record_t *buf = (void *)(ms->buffer + es->headerSize + k * es->record_size);
        printf("%d: Record: %d\n", k, buf->key);
    }
#endif
}

/**
 * Returns a pointer to the value of a specific record within the buffer.
 * @param ms Pointer to the MinSortState structure holding sorting state.
 * @param recordNum The record number within the block to access.
 * @param es Sorting configuration, including offsets and sizes for keys.
 * @return Pointer to the key of the specified record.
 */
void *getValuePtr(MinSortState *ms, int recordNum, external_sort_t *es) {
    return ms->buffer + es->headerSize + recordNum * es->record_size + es->key_offset;
}

/**
 * Returns a pointer to the minimum key for a specific region.
 * @param ms Pointer to the MinSortState structure holding sorting state.
 * @param regionIdx The region index to access.
 * @param es Sorting configuration, including key size.
 * @return Pointer to the minimum key value for the region.
 */
void *getMinRegionPtr(MinSortState *ms, int regionIdx, external_sort_t *es) {
    return ms->min + regionIdx * es->key_size;
}

/**
 * Initializes the MinSort state, including memory allocations, regions, and metrics.
 * @param ms Pointer to the MinSortState structure to initialize.
 * @param es Sorting configuration.
 * @param metric Metrics tracking structure for performance analysis.
 * @param compareFn Comparison function pointer.
 */
void init_MinSort(MinSortState *ms, external_sort_t *es, metrics_t *metric, int8_t (*compareFn)(void *a, void *b)) {
    uint32_t i = 0, j = 0, regionIdx;
    void *val;

    /* Initialize statistics and tracking metrics */
    metric->num_reads = 0;
    metric->num_compar = 0;
    metric->num_writes = 0;
    metric->num_memcpys = 0;

    /* Set up MinSort state fields */
    ms->blocksRead = 0;
    ms->tuplesRead = 0;
    ms->tuplesOut = 0;
    ms->bytesRead = 0;

    ms->record_size = es->record_size;
    ms->numBlocks = es->num_pages;
    ms->records_per_block = (es->page_size - es->headerSize) / es->record_size;
    j = (ms->memoryAvailable - 2 * es->page_size - 2 * es->key_size - INT_SIZE) / (es->key_size + sizeof(uint8_t));
    ms->blocks_per_region = (uint32_t)ceil((float)ms->numBlocks / j);
    ms->numRegions = (uint32_t)ceil((float)ms->numBlocks / ms->blocks_per_region);

    /* Memory allocation for min values per region */
    // Allocate minimum index after block 2 (block 0 is input buffer, block 1 is output buffer)
    ms->min = (int8_t *)(ms->buffer + es->page_size * 2);
    ms->min_initialized = (int8_t *)(ms->min + es->key_size * ms->numRegions);

#ifdef DEBUG
    printf("Memory overhead: %d  Max regions: %d\r\n", 2 * SORT_KEY_SIZE + INT_SIZE, j);
    printf("Page size: %d, Memory size: %d Record size: %d, Number of records: %lu, Number of blocks: %d, Blocks per region: %d  Regions: %d\r\n",
           es->page_size, ms->memoryAvailable, ms->record_size, ms->num_records, ms->numBlocks, ms->blocks_per_region, ms->numRegions);
#endif

    /* Initialize each region’s minimum value */
    for (i = 0; i < ms->numRegions; i++) {
        ms->min_initialized[i] = false;
    }

    /* Populate each region’s minimum key by scanning blocks */
    for (i = 0; i < ms->numBlocks; i++) {
        readPageMinSort(ms, i, es, metric); // Load block i into buffer
        regionIdx = i / ms->blocks_per_region;


        // Set inital value to first read.
        // ms->min[regionIdx] = getValuePtr(ms, 0, es);
        memcpy(getMinRegionPtr(ms, regionIdx, es), getValuePtr(ms, 0, es), es->key_size);
        ms->min_initialized[regionIdx] = true;

       /* Process remaining records in the block */
        for (j = 1; j < ms->records_per_block; j++) {
            if (((i * ms->records_per_block) + j) < ms->num_records) {
                val = getValuePtr(ms, j, es);
                metric->num_compar++;
                
                /* Update region’s minimum if current record is smaller */
                if (compareFn(val, getMinRegionPtr(ms, regionIdx, es)) == -1) {
                    memcpy(getMinRegionPtr(ms, regionIdx, es), val, es->key_size);
                    ms->min_initialized[regionIdx] = true;
                }
            } else
                break;
        }
    }

#ifdef DEBUG
    for (i = 0; i < ms->numRegions; i++)
        printf("Region: %d  Min: %d\r\n", i, ms->min[i]);
#endif

    /* Allocate memory for current and next keys */
    ms->current = malloc(es->key_size);
    ms->next = malloc(es->key_size);
    ms->lastBlockIdx = INT_MAX;

    ms->nextIdx = 0;
    ms->current_initialized = false;
    ms->next_initialized = false;
}

/**
 * This function returns the next tuple in the sorted sequence during the MinSort process.
 * It searches through the blocks of data, finds the smallest value (based on a comparison function), 
 * and updates the state to reflect the progress in the sorting process.
 * 
 * @param ms Pointer to the MinSortState structure that maintains the current state of the sorting.
 * @param es Pointer to the external_sort_t structure that defines the external sorting configuration.
 * @param tupleBuffer A buffer where the next tuple will be copied when found.
 * @param metric Pointer to the metrics_t structure that tracks statistics such as comparisons and memory copies.
 * @param compareFn A comparison function used to compare two data values.
 * @return A pointer to the next tuple in the sorted sequence, or NULL if no more tuples are available.
 */
char *next_MinSort(MinSortState *ms, external_sort_t *es, void *tupleBuffer, metrics_t *metric, int8_t (*compareFn)(void *a, void *b)) {
    uint32_t i, curBlk, startBlk;
    uint64_t startIndex, k;
    void *dataVal;

    // Find the block with the minimum tuple value - otherwise continue on with last block
    if (ms->nextIdx == 0) {  
        // Find new block as do not know location of next minimum tuple
        
        ms->current_initialized = false;
        ms->regionIdx_initialized = false;
        ms->next_initialized = false;
        ms->regionIdx = INT_MAX; // Reset the region index to indicate no region has been selected yet

    
        for (i = 0; i < ms->numRegions; i++) {
            metric->num_compar++;

            // If the current region has a valid minimum, and it's less than the current tuple, update the minimum
            if (ms->min_initialized[i] && (!ms->current_initialized || compareFn(getMinRegionPtr(ms, i, es), ms->current) == -1)) {
                memcpy(ms->current, getMinRegionPtr(ms, i, es), es->key_size); // ms->current = ms->min[i];
                ms->current_initialized = true;
                ms->regionIdx = i; // Update the region index to the one containing the new minimum
            }
        }

        // If no valid minimum was found, return NULL indicating no more tuples are available
        if (ms->regionIdx == INT_MAX)
            return NULL; 
    }

    // Search current region for tuple with current minimum value
    startIndex = ms->nextIdx;
    startBlk = ms->regionIdx * ms->blocks_per_region;

    // Iterate through records in the block
    for (k = startIndex / ms->records_per_block; k < ms->blocks_per_region; k++) {
        curBlk = startBlk + k;
        
        // Read the current block into the buffer if it's not already loaded
        if (curBlk != ms->lastBlockIdx) {
            readPageMinSort(ms, curBlk, es, metric);
        }

        for (i = startIndex % ms->records_per_block; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records){
                break; // Stop if we've reached the end of records in the block
            }
            
            dataVal = getValuePtr(ms, i, es); // Pointer to the current record's value
            metric->num_compar++;

            // If the current record matches the minimum, copy it into the ouput buffer

            if (compareFn(dataVal, ms->current) == 0) {
                memcpy(tupleBuffer, &(ms->buffer[ms->record_size * i + es->headerSize]), ms->record_size);
                metric->num_memcpys++;
#ifdef DEBUG
                test_record_t *buf = (test_record_t *)(ms->buffer + es->headerSize + i * es->record_size);
                buf = (test_record_t *)tupleBuffer;
                printf("Returning tuple: %d\n", buf->key);
#endif
                i++; // Move to the next record
                ms->tuplesOut++;
                goto done;  // Exit the loop since we found the record we were looking for
            }
            metric->num_compar++;
            
            // If the current record is greater than the current minimum and is smaller than the next, update the next minimum
            if (compareFn(dataVal, ms->current) == 1 && (!ms->next_initialized || compareFn(dataVal, ms->next) == -1)) {
                memcpy(ms->next, dataVal, es->key_size); // ms->next = dataVal;
                ms->next_initialized = true;
                ms->nextIdx = 0;
            }
        }
    }

done:
#ifdef DEBUG
    printf("Updating minimum in region\r\n");
#endif
    
    // After processing the current block, scan the rest of the region to find a smaller record if possible
    ms->nextIdx = 0;

    // Continue searching the remaining blocks in the region for a smaller tuple
    for (; k < ms->blocks_per_region; k++) {
        curBlk = startBlk + k;
        
        // If the block is not already loaded, read it into the buffer
        if (curBlk != ms->lastBlockIdx) {
            readPageMinSort(ms, curBlk, es, metric);
            i = 0;
        }

        // Search through the records in the block
        for (; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records) {
                break; // Stop if we've reached the end of records in the block
            }
            dataVal = getValuePtr(ms, i, es);
            metric->num_compar++;

            // If the current record matches the minimum, update the index
            if (compareFn(dataVal, ms->current) == 0) {
                ms->nextIdx = k * ms->records_per_block + i;
#ifdef DEBUG
                printf("Next tuple at: %d  k: %d  i: %d\r\n", ms->nextIdx, k, i);
#endif
                goto done2;
            }
            metric->num_compar++;

            // If the current record is greater than the current minimum, update the next tuple if needed
            if (compareFn(dataVal, ms->current) == 1 && (!ms->next_initialized || compareFn(dataVal, ms->next) == -1)) {
                memcpy(ms->next, dataVal, es->key_size); // Update the next tuple
                ms->next_initialized = true;
                ms->nextIdx = 0;
            }
        }
    }

done2:

    // After finding the next minimum, update the minimum value for the region
    if (ms->nextIdx == 0) {

        if (!ms->next_initialized) {
            ms->min_initialized[ms->regionIdx] = false;
        } else {
            memcpy(getMinRegionPtr(ms, ms->regionIdx, es), ms->next, es->key_size); // Update the region's minimum
            ms->next_initialized = false;
            ms->min_initialized[ms->regionIdx] = true;
        }

#ifdef DEBUG
        printf("Updated minimum in block to: %d\r\n", ms->min[ms->regionIdx]);
#endif
    }



    return tupleBuffer; // Update the region's minimum
}

void close_MinSort(MinSortState *ms, external_sort_t *es) {
    /*
    printf("Tuples out:  %lu\r\n", ms->op.tuples_out);
    printf("Blocks read: %lu\r\n", ms->op.blocks_read);
    printf("Tuples read: %lu\r\n", ms->op.tuples_read);
    printf("Bytes read:  %lu\r\n", ms->op.bytes_read);
    */

    if (ms->current) {
        free(ms->current);
        ms->current = NULL;
    }
    if (ms->next) {
        free(ms->next);
        ms->next = NULL;
    }
}

/**
@brief      Flash Minsort implemented with full tuple reads.
@param      iteratorState
                Structure stores state of iterator (file info etc.)
@param      tupleBuffer
                Pre-allocated space to store one tuple (row) of input being sorted
@param      outputFile
                Already opened file to store sorting output (and in-progress temporary results)
@param      buffer
                Pre-allocated space used by algorithm during sorting
@param      bufferSizeInByes
                Size of buffer in byes
@param      es
                Sorting state info (block size, record size, etc.)
@param      resultFilePtr
                Offset within output file of first output record
@param      metric
                Tracks algorithm metrics (I/Os, comparisons, memory swaps)
@param      compareFn
                Record comparison function for record ordering
*/
int flash_minsort(
    void *iteratorState,
    void *tupleBuffer,
    void *outputFile,
    char *buffer,
    int bufferSizeInBytes,
    external_sort_t *es,
    long *resultFilePtr,
    metrics_t *metric,
    int8_t (*compareFn)(void *a, void *b)
) {
#ifdef DEBUG
    printf("*Flash Minsort*\n");
#endif
    clock_t start = clock();

    MinSortState ms;
    ms.buffer = buffer;
    ms.iteratorState = iteratorState;
    ms.memoryAvailable = bufferSizeInBytes;
    ms.num_records = ((file_iterator_state_t *)iteratorState)->totalRecords;

    init_MinSort(&ms, es, metric, compareFn);
    int16_t count = 0;
    int32_t blockIndex = 0;
    int16_t values_per_page = (es->page_size - es->headerSize) / es->record_size;
    uint8_t *outputBuffer = buffer + es->page_size;
    // test_record_t *buf;

    // Main sorting loop: fetches and writes sorted records in blocks
    while (next_MinSort(&ms, es, (char *)(outputBuffer + count * es->record_size + es->headerSize), metric, compareFn) != NULL) {
        // Store the current record in the buffer
        count++;

        // When a block is full, write it to the output file
        if (count == values_per_page) {                                // Write block
            *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
            *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */
            count = 0;  // Reset count for the next block
            
            // Write the block to the output file using the file interface's write method
            if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                return 9; // Return error code if writing to the output file fails
            }
                
#ifdef DEBUG
            printf("Wrote output block. Block index: %d\n", blockIndex);
            for (int k = 0; k < values_per_page; k++) {
                test_record_t *buf = (void *)(outputBuffer + es->headerSize + k * es->record_size);
                printf("%d: Output Record: %d\n", k, buf->key);
            }
#endif
            blockIndex++;
        }
    }

    // Write the last block if there are remaining records
    if (count > 0) {
        *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
        *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

        if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
            return 9; // Return error code if writing to the output file fails
        }

        blockIndex++;
        count = 0;
    }

#ifdef DEBUG
    printf("Number of sorted records: %d", ms.num_records);
#endif

    ((file_iterator_state_t *)iteratorState)->fileInterface->flush(outputFile);

    close_MinSort(&ms, es);

    clock_t end = clock();

    *resultFilePtr = 0;

#ifdef DEBUG 
    printf("Complete. Comparisons: %d  MemCopies: %d\n", metric->num_compar, metric->num_memcpys);
#endif

    return 0; // Successful completion
}
