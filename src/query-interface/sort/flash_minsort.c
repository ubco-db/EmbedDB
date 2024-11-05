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
#include "no_output_heap.h"

// #define DEBUG 1
// #define DEBUG_OUTPUT 1
// #define DEBUG_READ 1

void readPageMinSort(MinSortState *ms, int pageNum, external_sort_t *es, metrics_t *metric) {
    file_iterator_state_t *is = (file_iterator_state_t *)ms->iteratorState;
    void *fp = is->file;

    /* Read page into start of buffer */
    if (0 == is->fileInterface->read(ms->buffer, pageNum, es->page_size, fp)) {
        printf("Failed to read block. HERE.\n");
    }

    // fseek(fp, pageNum*es->page_size, SEEK_SET);
    //  if (0 ==  fread(ms->buffer, es->page_size, 1, fp))
    //  {	// printf("Failed to read block. HERE.\n");
    //  }

    metric->num_reads++;

    ms->blocksRead++;
    ms->lastBlockIdx = pageNum;
#ifdef DEBUG_READ
    printf("Reading block: %d\r\n", pageNum);
    for (int k = 0; k < 31; k++) {
        test_record_t *buf = (void *)(ms->buffer + es->headerSize + k * es->record_size);
        printf("%d: Record: %d\n", k, buf->key);
    }
#endif
}

/* Returns a value of a tuple given a record number in a block (that has been previously buffered) */
void *getValuePtr(MinSortState *ms, int recordNum, external_sort_t *es) {
    return ms->buffer + es->headerSize + recordNum * es->record_size + es->key_offset;
}

void *getMinRegionPtr(MinSortState *ms, int regionIdx, external_sort_t *es) {
    return ms->min + regionIdx * es->key_size;
}

void init_MinSort(MinSortState *ms, external_sort_t *es, metrics_t *metric, int8_t (*compareFn)(void *a, void *b)) {
    uint32_t i = 0, j = 0, regionIdx;
    void *val;

    /* Operator statistics */
    metric->num_reads = 0;
    metric->num_compar = 0;
    metric->num_writes = 0;
    metric->num_memcpys = 0;

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

    // Memory allocation
    // Allocate minimum index after block 2 (block 0 is input buffer, block 1 is output buffer)
    ms->min = (int8_t *)(ms->buffer + es->page_size * 2);
    ms->min_initialized = (int8_t *)(ms->min + es->key_size * ms->numRegions);

#ifdef DEBUG
    printf("Memory overhead: %d  Max regions: %d\r\n", 2 * SORT_KEY_SIZE + INT_SIZE, j);
    printf("Page size: %d, Memory size: %d Record size: %d, Number of records: %lu, Number of blocks: %d, Blocks per region: %d  Regions: %d\r\n",
           es->page_size, ms->memoryAvailable, ms->record_size, ms->num_records, ms->numBlocks, ms->blocks_per_region, ms->numRegions);
#endif

    for (i = 0; i < ms->numRegions; i++) {
        // ms->min[i] = INT_MAX;
        ms->min_initialized[i] = true;
    }

    /* Scan data to populate the minimum in each region */
    for (i = 0; i < ms->numBlocks; i++) {
        readPageMinSort(ms, i, es, metric);
        regionIdx = i / ms->blocks_per_region;


        // Set inital value to first read.
        // ms->min[regionIdx] = getValuePtr(ms, 0, es);
        memcpy(getMinRegionPtr(ms, regionIdx, es), getValuePtr(ms, 0, es), es->key_size);


        /* Process first record in block */
        for (j = 1; j < ms->records_per_block; j++) {
            if (((i * ms->records_per_block) + j) < ms->num_records) {
                val = getValuePtr(ms, j, es);
                metric->num_compar++;

                if (compareFn(val, getMinRegionPtr(ms, regionIdx, es)) == -1)
                    memcpy(getMinRegionPtr(ms, regionIdx, es), val, es->key_size);
            } else
                break;
        }
    }

#ifdef DEBUG
    for (i = 0; i < ms->numRegions; i++)
        printf("Region: %d  Min: %d\r\n", i, ms->min[i]);
#endif


    ms->current = malloc(es->key_size);
    ms->next = malloc(es->key_size);
    ms->lastBlockIdx = INT_MAX;

    ms->nextIdx = 0;
    ms->current_initialized = false;
    ms->next_initialized = false;
}

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
          
        // ms->current = INT_MAX;
        // ms->next = INT_MAX;
        ms->regionIdx = INT_MAX;

    
        for (i = 0; i < ms->numRegions; i++) {
            metric->num_compar++;

            // If min is not initalized we have exhasted all values from that region. Continue
            // If current is not found update current
            // If current < min[regionidex] update current
            if (ms->min_initialized[i] && (!ms->current_initialized || compareFn(getMinRegionPtr(ms, i, es), ms->current) == -1)) {
                ms->current_initialized = true;
                memcpy(ms->current, getMinRegionPtr(ms, i, es), es->key_size); // ms->current = ms->min[i];
                ms->regionIdx = i;
            }
        }
        if (ms->regionIdx == INT_MAX)
            return NULL;  // Join complete - no more tuples
    }

    // Search current region for tuple with current minimum value
    startIndex = ms->nextIdx;
    startBlk = ms->regionIdx * ms->blocks_per_region;

    for (k = startIndex / ms->records_per_block; k < ms->blocks_per_region; k++) {
        curBlk = startBlk + k;
        if (curBlk != ms->lastBlockIdx) {  // Read block into buffer
            readPageMinSort(ms, curBlk, es, metric);
        }

        for (i = startIndex % ms->records_per_block; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records)
                break;
            dataVal = getValuePtr(ms, i, es);
            metric->num_compar++;

            if (compareFn(dataVal, ms->current) == 0) {
                memcpy(tupleBuffer, &(ms->buffer[ms->record_size * i + es->headerSize]), ms->record_size);
                metric->num_memcpys++;
#ifdef DEBUG
                test_record_t *buf = (test_record_t *)(ms->buffer + es->headerSize + i * es->record_size);
                buf = (test_record_t *)tupleBuffer;
                printf("Returning tuple: %d\n", buf->key);
#endif
                i++;
                ms->tuplesOut++;
                goto done;  // Found the record we are looking for
            }
            metric->num_compar++;
            if (compareFn(dataVal, ms->current) == 1 && (!ms->next_initialized || compareFn(&dataVal, &ms->next) == -1)) {
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
    // Now update minimum in block - Scan rest of region after what we found to see if can find a smaller record
    ms->nextIdx = 0;

    for (; k < ms->blocks_per_region; k++) {
        curBlk = startBlk + k;
        if (curBlk != ms->lastBlockIdx) {  // Read block into buffer
            readPageMinSort(ms, curBlk, es, metric);
            i = 0;
        }

        for (; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records)
                break;
            dataVal = getValuePtr(ms, i, es);
            metric->num_compar++;

            if (compareFn(dataVal, ms->current) == 0) {
                ms->nextIdx = k * ms->records_per_block + i;
#ifdef DEBUG
                printf("Next tuple at: %d  k: %d  i: %d\r\n", ms->nextIdx, k, i);
#endif
                goto done2;
            }
            metric->num_compar++;

            if (compareFn(dataVal, ms->current) == 1 && (!ms->next_initialized || compareFn(&dataVal, &ms->next) == -1)) {
                memcpy(ms->next, dataVal, es->key_size); // ms->next = dataVal;
                ms->next_initialized = true;
                ms->nextIdx = 0;
            }
        }
    }

done2:

    if (ms->nextIdx == 0) {
        // Update minimum currently in block
        // ms->min[ms->regionIdx] = ms->next;
        if (!ms->next_initialized)
            ms->min_initialized[ms->regionIdx] = false;
        else 
            memcpy(getMinRegionPtr(ms, ms->regionIdx, es), ms->next, es->key_size);
#ifdef DEBUG
        printf("Updated minimum in block to: %d\r\n", ms->min[ms->regionIdx]);
#endif
    }
    return tupleBuffer;
}

void close_MinSort(MinSortState *ms, external_sort_t *es) {
    /*
    printf("Tuples out:  %lu\r\n", ms->op.tuples_out);
    printf("Blocks read: %lu\r\n", ms->op.blocks_read);
    printf("Tuples read: %lu\r\n", ms->op.tuples_read);
    printf("Bytes read:  %lu\r\n", ms->op.bytes_read);
    */
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

    // Write
    while (next_MinSort(&ms, es, (char *)(outputBuffer + count * es->record_size + es->headerSize), metric, compareFn) != NULL) {
        // Store record in block (already done during call to next)
        // buf = (void *)(outputBuffer+count*es->record_size+es->headerSize);
        count++;

        if (count == values_per_page) {                                // Write block
            *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
            *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */
            count = 0;
            if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile))
                return 9;
                // if (0 == fwrite(outputBuffer, es->page_size, 1, outputFile))
                // return 9;

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

    if (count > 0) {
        // Write last block
        *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
        *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

        if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile))
            return 9;
        // if (0 == fwrite(buffer, es->page_size, 1, outputFile))
        // return 9;
        blockIndex++;
        count = 0;
    }

    ((file_iterator_state_t *)iteratorState)->fileInterface->flush(outputFile);

    close_MinSort(&ms, es);

    clock_t end = clock();

    *resultFilePtr = 0;

    //    printf("Complete. Comparisons: %d  MemCopies: %d  TransferIn: %d  TransferOut: %d TransferOther: %d\n", metric->num_compar, metric->num_memcpys, numShiftIntoOutput, numShiftOutOutput, numShiftOtherBlock);

    /* cleanup */

    return 0;
}
