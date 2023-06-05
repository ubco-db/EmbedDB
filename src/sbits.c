/******************************************************************************/
/**
 * @file		sbits.c
 * @author		Ramon Lawrence
 * @brief		This file is for sequential bitmap indexing for time series
 * (SBITS).
 * @copyright	Copyright 2021
 * 						The University of British Columbia,
 * 						Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 * 		modification, are permitted provided that the following conditions are
 * met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 		this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 		this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 		AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 		ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 		CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 		SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 		INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 		CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 		ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sbits.h"
#include "spline/radixspline.h"
#include "spline/spline.h"

/**
 * 0 = Value-based search
 * 1 = Binary serach
 * 2 = Modified linear search (Spline)
 */
#define SEARCH_METHOD 2

/**
 * Number of bits to be indexed by the Radix Search structure
 * Note: The Radix search structure is only used with Spline (SEARCH_METHOD ==
 * 2) To use a pure Spline index without a Radix table, set RADIX_BITS to 0
 */
#define RADIX_BITS 0

/*
 * Defines if the radix table should be use, or just the spline structure. It is
 * only applicable for search method 2.
 */
#define USE_RADIX 0

void printBitmap(char *bm) {
    for (int8_t i = 0; i <= 7; i++) {
        printf(" " BYTE_TO_BINARY_PATTERN "", BYTE_TO_BINARY(*(bm + i)));
    }
    printf("\n");
}

int8_t bitmapOverlap(uint8_t *bm1, uint8_t *bm2, int8_t size) {
    for (int8_t i = 0; i < size; i++)
        if ((*((uint8_t *)(bm1 + i)) & *((uint8_t *)(bm2 + i))) >= 1)
            return 1;

    return 0;
}

void initBufferPage(sbitsState *state, int pageNum) {
    /* Initialize page */
    uint16_t i = 0;
    void *buf = (char *)state->buffer + pageNum * state->pageSize;

    for (i = 0; i < state->pageSize; i++) {
        ((int8_t *)buf)[i] = 0;
    }

    if (pageNum != SBITS_VAR_WRITE_BUFFER(state->parameters)) {
        /* Initialize header key min. Max and sum is already set to zero by the
         * for-loop above */
        void *min = SBITS_GET_MIN_KEY(buf);
        /* Initialize min to all 1s */
        for (i = 0; i < state->keySize; i++) {
            ((int8_t *)min)[i] = 1;
        }

        /* Initialize data min. */
        min = SBITS_GET_MIN_DATA(buf, state);
        /* Initialize min to all 1s */
        for (i = 0; i < state->dataSize; i++) {
            ((int8_t *)min)[i] = 1;
        }
    }
}

/**
 * @brief   Initializes the Radix Spline data structure and assigns it to state
 * @param   state       SBITS state structure
 * @param   size        Size data to load into data structure
 * @param   radixSize   number bits to be indexed by radix
 * @return  void
 */
void initRadixSpline(sbitsState *state, uint64_t size, size_t radixSize) {
    spline *spl = (spline *)malloc(sizeof(spline));
    state->spl = spl;

    splineInit(state->spl, size, state->indexMaxError, state->keySize);

    radixspline *rsidx = (radixspline *)malloc(sizeof(radixspline));
    state->rdix = rsidx;
    radixsplineInit(state->rdix, state->spl, radixSize, state->keySize);
}

/**
 * @brief   Return the smallest key in the node
 * @param   state   SBITS algorithm state structure
 * @param   buffer  In memory page buffer with node data
 */
void *sbitsGetMinKey(sbitsState *state, void *buffer) {
    return (void *)((int8_t *)buffer + state->headerSize);
}

/**
 * @brief   Return the largest key in the node
 * @param   state   SBITS algorithm state structure
 * @param   buffer  In memory page buffer with node data
 */
void *sbitsGetMaxKey(sbitsState *state, void *buffer) {
    int16_t count = SBITS_GET_COUNT(buffer);
    return (void *)((int8_t *)buffer + state->headerSize + (count - 1) * state->recordSize);
}

/**
 * @brief   Initialize SBITS structure.
 * @param   state           SBITS algorithm state structure
 * @param   indexMaxError   max error of indexing structure (spline or PGM)
 * @return  Return 0 if success. Non-zero value if error.
 */
int8_t sbitsInit(sbitsState *state, size_t indexMaxError) {
    if (state->keySize > 8) {
        printf("ERROR: Key size is too large. Max key size is 8 bytes.\n");
        return -1;
    }

    state->recordSize = state->keySize + state->dataSize;
    if (SBITS_USING_VDATA(state->parameters)) {
        state->recordSize += 4;
    }

    printf("Initializing SBITS.\n");
    printf("Buffer size: %d  Page size: %d\n", state->bufferSizeInBlocks, state->pageSize);
    printf("Key size: %d Data size: %d %sRecord size: %d\n", state->keySize, state->dataSize, SBITS_USING_VDATA(state->parameters) ? "Variable data pointer size: 4 " : "", state->recordSize);
    printf("Use index: %d  Max/min: %d Sum: %d Bmap: %d\n", SBITS_USING_INDEX(state->parameters), SBITS_USING_MAX_MIN(state->parameters), SBITS_USING_SUM(state->parameters), SBITS_USING_BMAP(state->parameters));

    state->file = NULL;
    state->indexFile = NULL;
    state->varFile = NULL;
    state->nextPageId = 0;
    state->nextPageWriteId = 0;
    state->wrappedMemory = 0;
    state->indexMaxError = indexMaxError;

    /* Calculate block header size */

    /* Header size depends on bitmap size: 6 + X bytes: 4 byte id, 2 for record count, X for bitmap. */
    state->headerSize = 6 + state->bitmapSize;
    if (SBITS_USING_MAX_MIN(state->parameters))
        state->headerSize += state->keySize * 2 + state->dataSize * 2;

    state->minKey = 0;
    state->bufferedPageId = -1;
    state->bufferedIndexPageId = -1;

    /* Calculate number of records per page */
    state->maxRecordsPerPage = (state->pageSize - state->headerSize) / state->recordSize;
    printf("Header size: %d  Records per page: %d\n", state->headerSize, state->maxRecordsPerPage);

    /* Initialize max error to maximum records per page */
    state->maxError = state->maxRecordsPerPage;

    /* Allocate first page of buffer as output page */
    initBufferPage(state, 0);

    resetStats(state);

    id_t numPages = (state->endAddress - state->startAddress) / state->pageSize;

    if (numPages < (SBITS_USING_INDEX(state->parameters) * 2 + 2) *
                       state->eraseSizeInPages) {
        printf("ERROR: Number of pages allocated must be at least twice erase block size for SBITS and four times when using indexing. Memory pages: %d\n", numPages);
        return -1;
    }

    state->startDataPage = 0;
    state->endDataPage = state->endAddress / state->pageSize;
    state->firstDataPage = 0;
    state->firstDataPageId = 0;
    state->erasedEndPage = 0;
    state->avgKeyDiff = 1;

    /* Setup data file. */
    if (state->storageType == FILE_STORAGE) {
        state->file = fopen("datafile.bin", "w+b");
        if (state->file == NULL) {
            printf("Error: Can't open file!\n");
            return -1;
        }
    } else { /* Setup data flash memory */
    }

    if (SBITS_USING_INDEX(state->parameters)) { /* Allocate file and buffer for index */
        if (state->bufferSizeInBlocks < 4) {
            printf("ERROR: SBITS using index requires at least 4 page buffers. Defaulting to without index.\n");
            state->parameters -= SBITS_USE_INDEX;
        } else {
            if (state->storageType == FILE_STORAGE) {
                /* Setup index file. */
                state->indexFile = fopen("indexfile.bin", "w+b");
                if (state->indexFile == NULL) {
                    printf("Error: Can't open index file!\n");
                    return -1;
                }
            }

            /* 4 for id, 2 for count, 2 unused, 4 for minKey (pageId), 4 for maxKey (pageId) */
            state->maxIdxRecordsPerPage = (state->pageSize - 16) / state->bitmapSize;

            /* Allocate third page of buffer as index output page */
            initBufferPage(state, SBITS_INDEX_WRITE_BUFFER);

            /* Add page id to minimum value spot in page */
            void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_INDEX_WRITE_BUFFER);
            id_t *ptr = ((id_t *)((int8_t *)buf + 8));
            *ptr = state->nextPageId;

            state->nextIdxPageId = 0;
            state->nextIdxPageWriteId = 0;

            count_t numIdxPages = numPages / 100; /* Index overhead is about 1% of data size */
            if (numIdxPages < state->eraseSizeInPages * 2) {
                /* Minimum index space is two erase blocks */
                numIdxPages = state->eraseSizeInPages * 2;
            } else {
                /* Ensure index space is a multiple of erase block size */
                numIdxPages = ((numIdxPages / state->eraseSizeInPages) + 1) * state->eraseSizeInPages;
            }

            /* Index pages are at the end of the memory space */
            state->endIdxPage = state->endDataPage;
            state->endDataPage -= numIdxPages;
            state->startIdxPage = state->endDataPage + 1;
            state->firstIdxPage = 0; /* TODO: Decide how to handle if share memory space. For now,
                                                            having logical pages start from 0 rather than the physical
                                                            page id after the data block */
            state->erasedEndIdxPage = 0;
            state->wrappedIdxMemory = 0;
        }
    }

    if (SBITS_USING_VDATA(state->parameters)) {
        if (state->bufferSizeInBlocks < 4 + (SBITS_USING_INDEX(state->parameters) ? 2 : 0)) {
            printf("ERROR: SBITS using variable records requires at least 4 page buffers if there is no index and 6 if there is. Defaulting to no variable data.\n");
            state->parameters -= SBITS_USE_VDATA;
        } else {
            if (state->storageType == FILE_STORAGE) {
                // SETUP FILE
                state->varFile = fopen("varFile.bin", "w+b");
                if (state->varFile == NULL) {
                    printf("Error: Can't open variable data file!\n");
                    return -1;
                }
            }

            // Initialize variable data outpt buffer
            initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

            state->currentVarLoc = state->keySize;
            state->minVarRecordId = 0;
            state->wrappedVariableMemory = 0;
            state->numAvailVarPages = (state->varAddressEnd - state->varAddressStart) / state->pageSize;
            state->numVarPages = state->numAvailVarPages;
            state->nextVarPageId = 0;

            printf("Variable data pages: %d\n", state->numVarPages);
        }
    }

    if (SEARCH_METHOD == 2) {
        if (USE_RADIX) {
            initRadixSpline(state, 1000, RADIX_BITS);
        } else {
            state->spl = malloc(sizeof(spline));
            splineInit(state->spl, 1000, indexMaxError, state->keySize);
        }
    }

    return 0;
}

/**
 * @brief	Given a state, uses the first and last keys to estimate a slope of keys
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @return	Returns slope estimate float
 */
float sbitsCalculateSlope(sbitsState *state, void *buffer) {
    // simplistic slope calculation where the first two entries are used, should be improved

    uint32_t slopeX1, slopeX2;
    slopeX1 = 0;
    slopeX2 = SBITS_GET_COUNT(buffer) - 1;

    if (state->keySize <= 4) {
        uint32_t slopeY1 = 0, slopeY2 = 0;

        // check if both points are the same
        if (slopeX1 == slopeX2) {
            return 1;
        }

        // convert to keys
        memcpy(&slopeY1, ((int8_t *)buffer + state->headerSize + state->recordSize * slopeX1), state->keySize);
        memcpy(&slopeY2, ((int8_t *)buffer + state->headerSize + state->recordSize * slopeX2), state->keySize);

        // return slope of keys
        return (float)(slopeY2 - slopeY1) / (float)(slopeX2 - slopeX1);
    } else {
        uint64_t slopeY1 = 0, slopeY2 = 0;

        // check if both points are the same
        if (slopeX1 == slopeX2) {
            return 1;
        }

        // convert to keys
        memcpy(&slopeY1, ((int8_t *)buffer + state->headerSize + state->recordSize * slopeX1), state->keySize);
        memcpy(&slopeY2, ((int8_t *)buffer + state->headerSize + state->recordSize * slopeX2), state->keySize);

        // return slope of keys
        return (float)(slopeY2 - slopeY1) / (float)(slopeX2 - slopeX1);
    }
}

/**
 * @brief	Returns the maximum error for current page.
 * @param	state	SBITS algorithm state structure
 * @return	Returns max error integer.
 */
int32_t getMaxError(sbitsState *state, void *buffer) {
    if (state->keySize <= 4) {
        int32_t maxError = 0, currentError;
        uint32_t minKey = 0, currentKey = 0;
        memcpy(&minKey, sbitsGetMinKey(state, buffer), state->keySize);

        // get slope of keys within page
        float slope = sbitsCalculateSlope(state, buffer);

        for (int i = 0; i < state->maxRecordsPerPage; i++) {
            // loop all keys in page
            memcpy(&currentKey, ((int8_t *)buffer + state->headerSize + state->recordSize * i), state->keySize);

            // make currentKey value relative to current page
            currentKey = currentKey - minKey;

            // Guard against integer underflow
            if ((currentKey / slope) >= i) {
                currentError = (currentKey / slope) - i;
            } else {
                currentError = i - (currentKey / slope);
            }
            if (currentError > maxError) {
                maxError = currentError;
            }
        }

        if (maxError > state->maxRecordsPerPage) {
            return state->maxRecordsPerPage;
        }

        return maxError;
    } else {
        int32_t maxError = 0, currentError;
        uint64_t currentKey = 0, minKey = 0;
        memcpy(&minKey, sbitsGetMinKey(state, buffer), state->keySize);

        // get slope of keys within page
        float slope = sbitsCalculateSlope(state, state->buffer); // this is incorrect, should be buffer. TODO: fix

        for (int i = 0; i < state->maxRecordsPerPage; i++) {
            // loop all keys in page
            memcpy(&currentKey, ((int8_t *)buffer + state->headerSize + state->recordSize * i), state->keySize);

            // make currentKey value relative to current page
            currentKey = currentKey - minKey;

            // Guard against integer underflow
            if ((currentKey / slope) >= i) {
                currentError = (currentKey / slope) - i;
            } else {
                currentError = i - (currentKey / slope);
            }
            if (currentError > maxError) {
                maxError = currentError;
            }
        }

        if (maxError > state->maxRecordsPerPage) {
            return state->maxRecordsPerPage;
        }

        return maxError;
    }
}

/**
 * @brief	Adds an entry for the current page into the search structure
 * @param	state	SBITS algorithm state structure
 */
void indexPage(sbitsState *state) {
    if (SEARCH_METHOD == 2) {
        if (USE_RADIX) {
            radixsplineAddPoint(state->rdix, sbitsGetMinKey(state, state->buffer));
        } else {
            splineAdd(state->spl, sbitsGetMinKey(state, state->buffer));
        }
    }
}

/**
 * @brief	Puts a given key, data pair into structure.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsPut(sbitsState *state, void *key, void *data) {
    /* Copy record into block */
    count_t count = SBITS_GET_COUNT(state->buffer);

    /* Write current page if full */
    if (count >= state->maxRecordsPerPage) {
        // As the first buffer is the data write buffer, no manipulation is required
        id_t pageNum = writePage(state, state->buffer);

        indexPage(state);

        /* Save record in index file */
        if (state->indexFile != NULL) {
            void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_INDEX_WRITE_BUFFER);
            count_t idxcount = SBITS_GET_COUNT(buf);
            if (idxcount >= state->maxIdxRecordsPerPage) {
                /* Save index page */
                writeIndexPage(state, buf);

                idxcount = 0;
                initBufferPage(state, SBITS_INDEX_WRITE_BUFFER);

                /* Add page id to minimum value spot in page */
                id_t *ptr = (id_t *)((int8_t *)buf + 8);
                *ptr = pageNum;
            }

            SBITS_INC_COUNT(buf);

            /* Copy record onto index page */
            void *bm = SBITS_GET_BITMAP(state->buffer);
            memcpy((void *)((int8_t *)buf + SBITS_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);
        }

        /* Update estimate of average key difference. */
        int32_t numBlocks = state->nextPageWriteId - 1;
        if (state->nextPageWriteId < state->firstDataPage) {
            /* Wrapped around in memory and first data page is after the next page that will write */
            numBlocks = state->endDataPage - state->firstDataPage + 1 + state->nextIdxPageWriteId;
        }

        if (numBlocks == 0)
            numBlocks = 1;

        if (state->keySize <= 4) {
            uint32_t maxKey = 0;
            memcpy(&maxKey, sbitsGetMaxKey(state, state->buffer), state->keySize);
            state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
        } else {
            uint64_t maxKey = 0;
            memcpy(&maxKey, sbitsGetMaxKey(state, state->buffer), state->keySize);
            state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
        }

        // Calculate error within the page
        int32_t maxError = getMaxError(state, state->buffer);
        if (state->maxError < maxError) {
            state->maxError = maxError;
        }

        count = 0;
        initBufferPage(state, 0);
    }

    /* Copy record onto page */
    memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize, key, state->keySize);
    memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize + state->keySize, data, state->dataSize);

    /* Copy variable data offset if using variable data*/
    if (SBITS_USING_VDATA(state->parameters)) {
        uint32_t dataLocation;
        if (state->recordHasVarData) {
            dataLocation = state->currentVarLoc % (state->numVarPages * state->pageSize);
        } else {
            dataLocation = SBITS_NO_VAR_DATA;
        }
        memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize + state->keySize + state->dataSize, &dataLocation, sizeof(uint32_t));
    }

    /* Update count */
    SBITS_INC_COUNT(state->buffer);

    /* Set minimum key for first record insert */
    if (state->minKey == 0)
        memcpy(&state->minKey, key, state->keySize);

    if (SBITS_USING_MAX_MIN(state->parameters)) {
        /* Update MIN/MAX */
        void *ptr;
        if (count != 0) {
            /* Since keys are inserted in ascending order, every insert will
             * update max. Min will never change after first record. */
            ptr = SBITS_GET_MAX_KEY(state->buffer, state);
            memcpy(ptr, key, state->keySize);

            ptr = SBITS_GET_MIN_DATA(state->buffer, state);
            if (state->compareData(data, ptr) < 0)
                memcpy(ptr, data, state->dataSize);
            ptr = SBITS_GET_MAX_DATA(state->buffer, state);
            if (state->compareData(data, ptr) > 0)
                memcpy(ptr, data, state->dataSize);
        } else {
            /* First record inserted */
            ptr = SBITS_GET_MIN_KEY(state->buffer);
            memcpy(ptr, key, state->keySize);
            ptr = SBITS_GET_MAX_KEY(state->buffer, state);
            memcpy(ptr, key, state->keySize);

            ptr = SBITS_GET_MIN_DATA(state->buffer, state);
            memcpy(ptr, data, state->dataSize);
            ptr = SBITS_GET_MAX_DATA(state->buffer, state);
            memcpy(ptr, data, state->dataSize);
        }
    }

    if (SBITS_USING_BMAP(state->parameters)) {
        /* Update bitmap */
        char *bm = (char *)SBITS_GET_BITMAP(state->buffer);
        state->updateBitmap(data, bm);
    }

    return 0;
}

/**
 * @brief	Puts the given key, data, and variable length data into the structure.
 * @param	state			SBITS algorithm state structure
 * @param	key				Key for record
 * @param	data			Data for record
 * @param	variableData	Variable length data for record
 * @param	length			Length of the variable length data in bytes
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsPutVar(sbitsState *state, void *key, void *data, void *variableData, uint32_t length) {
    if (SBITS_USING_VDATA(state->parameters)) {
        // Insert their data
        if (variableData != NULL) {
            // Check that there is enough space remaining in this page to start the insert of the variable data here
            void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_VAR_WRITE_BUFFER(state->parameters));
            if (state->currentVarLoc % state->pageSize > state->pageSize - 4) {
                writeVariablePage(state, buf);
                initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));
                // Move data writing location to the beginning of the next page, leaving the first `keySize` bytes for a header
                state->currentVarLoc += state->pageSize - state->currentVarLoc % state->pageSize + state->keySize;
            }

            // Perform the regular insert
            state->recordHasVarData = 1;
            int8_t r;
            if ((r = sbitsPut(state, key, data)) != 0) {
                return r;
            }

            // Update the header to include the maximum key value stored on this page
            memcpy(buf, key, state->keySize);

            // Write the length of the data item into the buffer
            memcpy((uint8_t *)buf + state->currentVarLoc % state->pageSize, &length, sizeof(uint32_t));
            state->currentVarLoc += 4;

            // Check if we need to write after doing that
            if (state->currentVarLoc % state->pageSize == 0) {
                writeVariablePage(state, buf);
                initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

                // Update the header to include the maximum key value stored on this page
                memcpy(buf, key, state->keySize);
                state->currentVarLoc += state->keySize;
            }

            int amtWritten = 0;
            while (length > 0) {
                // Copy data into the buffer. Write the min of the space left in this page and the remaining length of the data
                uint16_t amtToWrite = min(state->pageSize - state->currentVarLoc % state->pageSize, length);
                memcpy((uint8_t *)buf + (state->currentVarLoc % state->pageSize), (uint8_t *)variableData + amtWritten, amtToWrite);
                length -= amtToWrite;
                amtWritten += amtToWrite;
                state->currentVarLoc += amtToWrite;

                // If we need to write the buffer to file
                if (state->currentVarLoc % state->pageSize == 0) {
                    writeVariablePage(state, buf);
                    initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

                    // Update the header to include the maximum key value stored on this page
                    memcpy(buf, key, state->keySize);
                    state->currentVarLoc += state->keySize;
                }
            }
        } else {
            // Var data enabled, but not provided
            state->recordHasVarData = 0;
            return sbitsPut(state, key, data);
        }
    } else {
        printf("Error: Can't insert variable data because it is not enabled\n");
        return -1;
    }
    return 0;
}

/**
 * @brief	Given a key, estimates the location of the key within the node.
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @param	key		Key for record
 */
int16_t sbitsEstimateKeyLocation(sbitsState *state, void *buffer, void *key) {
    // get slope to use for linear estimation of key location
    // return estimated location of the key
    float slope = sbitsCalculateSlope(state, buffer);

    uint64_t minKey = 0, thisKey = 0;
    memcpy(&minKey, sbitsGetMinKey(state, buffer), state->keySize);
    memcpy(&thisKey, key, state->keySize);

    return (thisKey - minKey) / slope;
}

/**
 * @brief	Given a key, searches the node for the key. If interior node, returns child record number containing next page id to follow. If leaf node, returns if of first record with that key or (<= key). Returns -1 if key is not found.
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @param	key		Key for record
 * @param	pageId	Page id for page being searched
 * @param	range	1 if range query so return pointer to first record <= key, 0 if exact query so much return first exact match record
 */
id_t sbitsSearchNode(sbitsState *state, void *buffer, void *key, id_t pageId, int8_t range) {
    int16_t first, last, middle, count;
    int8_t compare;
    void *mkey;

    count = SBITS_GET_COUNT(buffer);
    middle = sbitsEstimateKeyLocation(state, buffer, key);

    // check that maxError was calculated and middle is valid (searches full node otherwise)
    if (state->maxError == -1 || middle >= count || middle <= 0) {
        first = 0;
        last = count - 1;
        middle = (first + last) / 2;
    } else {
        first = 0;
        last = count - 1;
    }

    if (middle > last) {
        middle = last;
    }

    while (first <= last) {
        mkey =
            (int8_t *)buffer + state->headerSize + (state->recordSize * middle);
        compare = state->compareKey(mkey, key);
        if (compare < 0)
            first = middle + 1;
        else if (compare == 0) {
            return middle;
        } else
            last = middle - 1;

        middle = (first + last) / 2;
    }
    if (range)
        return middle;
    return -1;
}

/**
 * @brief	Linear search function to be used with an approximate range of pages.
 * 			If the desired key is found, the page containing that record is loaded
 * 			into the passed buffer pointer.
 * @param	state		SBITS algorithm state structure
 * @param 	numReads	Tracks total number of reads for statistics
 * @param	buf			buffer to store page with desired record
 * @param	key			Key for the record to search for
 * @param	pageId		Page id to start search from
 * @param 	low			Lower bound for the page the record could be found on
 * @param 	high		Uper bound for the page the record could be found on
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t linearSearch(sbitsState *state, int16_t *numReads, void *buf, void *key, int32_t pageId, int32_t low, int32_t high) {
    int32_t pageError = 0;
    int32_t physPageId;
    while (1) {
        /* Move logical page number to physical page id based on location of first data page */
        physPageId = pageId + state->firstDataPage;
        if (physPageId >= state->endDataPage)
            physPageId = physPageId - state->endDataPage;

        if (pageId > high || pageId < low || low > high) {
            return -1;
        }

        /* Read page into buffer. If 0 not returned, there was an error */
        if (readPage(state, physPageId) != 0) {
            return -1;
        }
        (*numReads)++;

        if (state->compareKey(key, sbitsGetMinKey(state, buf)) < 0) { /* Key is less than smallest record in block. */
            high = --pageId;
            pageError++;
        } else if (state->compareKey(key, sbitsGetMaxKey(state, buf)) > 0) { /* Key is larger than largest record in block. */
            low = ++pageId;
            pageError++;
        } else {
            /* Found correct block */
            return 0;
        }
    }
}

/**
 * @brief	Given a key, returns data associated with key.
 * 			Note: Space for data must be already allocated.
 * 			Data is copied from database into data buffer.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsGet(sbitsState *state, void *key, void *data) {
    int32_t pageId, physPageId;
    int32_t first = 0, last;
    void *buf;
    int16_t numReads = 0;

    uint64_t thisKey = 0;
    memcpy(&thisKey, key, state->keySize);

    // For spline
    id_t lowbound;
    id_t highbound;

    /* Determine last page */
    buf = (int8_t *)state->buffer + state->pageSize;
    if (state->nextPageWriteId < state->firstDataPage) { /* Wrapped around in memory and first data page is after the next page that will write */
        last = state->endDataPage - state->firstDataPage + 1 + state->nextIdxPageWriteId;
    } else {
        last = state->nextPageWriteId - 1;
    }

#if SEARCH_METHOD == 0
    /* Perform a modified binary search that uses info on key location sequence for first placement. */
    if (state->compareKey(key, (void *)&(state->minKey)) < 0)
        pageId = 0;
    else {

        pageId = (thisKey - state->minKey) / (state->maxRecordsPerPage * state->avgKeyDiff);

        if (pageId > state->endDataPage || (state->wrappedMemory == 0 && pageId >= state->nextPageWriteId))
            pageId = state->nextPageWriteId - 1; /* Logical page would be beyond maximum. Set to last page. */
    }
    int32_t offset = 0;

    while (1) {
        /* Move logical page number to physical page id based on location of first data page */
        physPageId = pageId + state->firstDataPage;
        if (physPageId >= state->endDataPage)
            physPageId = physPageId - state->endDataPage;

        // printf("Page id: %d  Offset: %d\n", pageId, offset);
        /* Read page into buffer */
        if (readPage(state, physPageId) != 0)
            return -1;
        numReads++;

        if (first >= last)
            break;

        if (state->compareKey(key, sbitsGetMinKey(state, buf)) < 0) { /* Key is less than smallest record in block. */
            last = pageId - 1;
            int64_t minKey = 0;
            memcpy(&minKey, sbitsGetMinKey(state, buf), state->keySize);
            offset = (thisKey - minKey) / state->maxRecordsPerPage / ((int32_t)state->avgKeyDiff) - 1;
            if (pageId + offset < first)
                offset = first - pageId;
            pageId += offset;

        } else if (state->compareKey(key, sbitsGetMaxKey(state, buf)) > 0) { /* Key is larger than largest record in block. */
            first = pageId + 1;
            int64_t maxKey = 0;
            memcpy(&maxKey, sbitsGetMaxKey(state, buf), state->keySize);
            offset = (thisKey - maxKey) / (state->maxRecordsPerPage * state->avgKeyDiff) + 1;
            if (pageId + offset > last)
                offset = last - pageId;
            pageId += offset;
        } else { /* Found correct block */
            break;
        }
    }
#elif SEARCH_METHOD == 1
    /* Regular binary search */
    pageId = (first + last) / 2;
    while (1) {
        /* Move logical page number to physical page id based on location of
         * first data page */
        physPageId = pageId + state->firstDataPage;
        if (physPageId >= state->endDataPage)
            physPageId = physPageId - state->endDataPage;

        // printf("Page id: %d  \n", pageId);
        /* Read page into buffer */
        if (readPage(state, physPageId) != 0)
            return -1;
        numReads++;

        if (first >= last)
            break;

        if (state->compareKey(key, sbitsGetMinKey(state, buf)) < 0) { /* Key is less than smallest record in block. */
            last = pageId - 1;
            pageId = (first + last) / 2;
        } else if (state->compareKey(key, sbitsGetMaxKey(state, buf)) > 0) { /* Key is larger than largest record in block. */
            first = pageId + 1;
            pageId = (first + last) / 2;
        } else { /* Found correct block */
            break;
        }
    }
#elif SEARCH_METHOD == 2
    /* Modified linear search */
    // NEXT STEP: Make it so that a point is only added when a new spline point is added, not for every point
    id_t location;
    if (USE_RADIX) {
        radixsplineFind(state->rdix, key, state->compareKey, &location, &lowbound, &highbound);
    } else {
        splineFind(state->spl, key, state->compareKey, &location, &lowbound, &highbound);
    }

    pageId = location;

    if (linearSearch(state, &numReads, buf, key, location, lowbound, highbound) == -1) {
        return -1;
    }

#endif
    id_t nextId = sbitsSearchNode(state, buf, key, nextId, 0);

    if (nextId != -1) {
        /* Key found */
        memcpy(data, (void *)((int8_t *)buf + state->headerSize + state->recordSize * nextId + state->keySize), state->dataSize);
        return 0;
    }

    // Key not found
    return -1;
}

/**
 * @brief	Given a key, returns data associated with key.
 * 			Note: Space for data must be already allocated.
 * 			Data is copied from database into data buffer.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return 0 if success. Non-zero value if error.
 * 			-1 : Error reading file
 * 			1  : Variable data was deleted to make room for newer data
 */
int8_t sbitsGetVar(sbitsState *state, void *key, void *data, void **varData, uint32_t *length) {
    // Get the fixed data
    int8_t r = sbitsGet(state, key, data);
    if (r != 0) {
        return r;
    }

    // Now the input buffer contains the record, so we can use that to find the variable data
    void *buf = (int8_t *)state->buffer + state->pageSize;
    id_t recordNum = sbitsSearchNode(state, buf, key, 0, 0);

    // Get the location of the variable data from the record
    uint32_t varDataOffset;
    memcpy(&varDataOffset, (int8_t *)buf + state->headerSize + state->recordSize * recordNum + state->keySize + state->dataSize, sizeof(uint32_t));
    if (varDataOffset == SBITS_NO_VAR_DATA) {
        *varData = NULL;
        return 0;
    }

    // Check if the variable data associated with this key has been overwritten due to file wrap around
    if (state->compareKey(key, &state->minVarRecordId) < 0) {
        *varData = NULL;
        return 1;
    }

    /* Get the data */

    // Read the page into the buffer for variable data
    void *ptr = (int8_t *)state->buffer + SBITS_VAR_READ_BUFFER(state->parameters) * state->pageSize;
    id_t pageNum = (varDataOffset / state->pageSize) % state->numVarPages;
    if (readVariablePage(state, pageNum) != 0) {
        return -1;
    }

    // Get pointer to the beginning of the data
    uint16_t bufPos = varDataOffset % state->pageSize;
    // Get length of data and move to the data portion of the record
    uint32_t dataLength;
    memcpy(&dataLength, (int8_t *)ptr + bufPos, sizeof(uint32_t));
    *length = dataLength;
    bufPos += 4;
    // If the length was the last thing in the page, then we need to read the next page for the data
    if (bufPos >= state->pageSize) {
        pageNum = (pageNum + 1) % state->numVarPages;
        if (readVariablePage(state, pageNum) != 0) {
            return -1;
        }
        // Skip past the header
        bufPos = state->keySize;
    }

    // Allocate memory in the return pointer **TODO: Implement returning an iterator instead**
    *varData = malloc(dataLength);
    if (*varData == NULL) {
        printf("Malloc failed while reading in var data\n");
        exit(1);
    }

    uint32_t amtRead = 0;
    while (amtRead < dataLength) {
        // Read either the rest of the data or the rest of the page
        uint16_t amtToRead = min(dataLength - amtRead, state->pageSize - bufPos);
        memcpy((int8_t *)*varData + amtRead, (int8_t *)ptr + bufPos, amtToRead);
        amtRead += amtToRead;

        // If we need to keep reading, read the next page
        if (amtRead != dataLength) {
            pageNum = (pageNum + 1) % state->numVarPages;
            if (readVariablePage(state, pageNum) != 0) {
                return -1;
            }
            // Skip past the header
            bufPos = state->keySize;
        }
    }

    return 0;
}

/**
 * @brief	Initialize iterator on sbits structure.
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 */
void sbitsInitIterator(sbitsState *state, sbitsIterator *it) {
    /* Build query bitmap (if used) */
    it->queryBitmap = NULL;
    it->lastIdxIterRec = 20000; /* Flag to indicate that not using index */
    if (SBITS_USING_BMAP(state->parameters)) {
        /* Verify that bitmap index is useful (must have set either min or max data value) */
        if (it->minData != NULL || it->maxData != NULL) {
            // uint16_t *bm = malloc(sizeof(uint16_t));
            //  *bm = 0;
            //  buildBitmapInt16FromRange(state, it->minData, it->maxData, bm);
            uint64_t *bm = (uint64_t *)malloc(sizeof(uint64_t));
            *bm = 0;
            buildBitmapInt64FromRange(state, it->minData, it->maxData, bm);

            // printBitmap((char*) bm);
            it->queryBitmap = bm;

            /* Setup for reading index file */
            if (state->indexFile != NULL) {
                it->lastIdxIterPage = state->firstIdxPage;
                it->lastIdxIterRec = 10000; /* Force to read next index page */
                it->wrappedIdxMemory = 0;
            }
        }
    }

    /* Read first page into memory */
    it->lastIterPage = state->firstDataPage - 1;
    it->lastIterRec = 10000; /* Force to read next page */
    it->wrappedMemory = 0;
}

/**
 * @brief	Flushes output buffer.
 * @param	state	algorithm state structure
 */
int8_t sbitsFlush(sbitsState *state) {
    // As the first buffer is the data write buffer, no address change is required
    id_t pageNum = writePage(state, state->buffer);

    indexPage(state);

    if (state->indexFile != NULL) {
        void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_INDEX_WRITE_BUFFER);
        count_t idxcount = SBITS_GET_COUNT(buf);
        SBITS_INC_COUNT(buf);

        /* Copy record onto index page */
        void *bm = SBITS_GET_BITMAP(state->buffer);
        memcpy((void *)((int8_t *)buf + SBITS_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);

        writeIndexPage(state, buf);

        /* Reinitialize buffer */
        initBufferPage(state, SBITS_INDEX_WRITE_BUFFER);
    }

    /* Reinitialize buffer */
    initBufferPage(state, 0);

    // Flush var data page
    if (SBITS_USING_VDATA(state->parameters)) {
        writeVariablePage(state, (int8_t *)state->buffer + SBITS_VAR_WRITE_BUFFER(state->parameters) * state->pageSize);
    }
    return 0;
}

/**
 * @brief	Return next key, data pair for iterator.
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 * @param	key		Key for record
 * @param	data	Data for record
 */
int8_t sbitsNext(sbitsState *state, sbitsIterator *it, void **key, void **data) {
    void *buf = (int8_t *)state->buffer + state->pageSize;
    /* Iterate until find a record that matches search criteria */
    while (1) {
        if (it->lastIterRec >= SBITS_GET_COUNT(buf) || it->lastIterRec == 10000) { /* Read next page */
            it->lastIterRec = 0;

            while (1) {
                id_t readPageId = 0;

                if (it->lastIdxIterRec == 20000) { /* No index. Scan next data page by iterator. */
                    it->lastIterPage++;
                    if (it->lastIterPage >= state->endDataPage) {
                        it->lastIterPage = 0; /* Wrap around to start of memory */
                        it->wrappedMemory = 1;
                    }

                    if (state->wrappedMemory == 0 || it->wrappedMemory == 1) {
                        if (it->lastIterPage >= state->nextPageWriteId)
                            return 0; /* No more pages to read */
                    }
                    readPageId = it->lastIterPage;
                } else { /* Using index file. */
                    void *idxbuf = (int8_t *)state->buffer + state->pageSize * SBITS_INDEX_READ_BUFFER;
                    count_t cnt = SBITS_GET_COUNT(idxbuf);
                    if (it->lastIdxIterRec == 10000 || it->lastIdxIterRec >= cnt) { /* Read next index block. Special case for
                                              first block as will not be read into
                                              buffer (so count not accurate). */
                        if (it->lastIdxIterPage >= (state->endIdxPage - state->startIdxPage + 1)) {
                            it->wrappedIdxMemory = 1;
                            it->lastIdxIterPage = 0; /* Wrapped around */
                        }
                        if (state->wrappedIdxMemory == 0 || it->wrappedIdxMemory == 1) {
                            if (it->lastIdxIterPage >= state->nextIdxPageWriteId)
                                return 0; /* No more pages to read */
                        }
                        if (readIndexPage(state, it->lastIdxIterPage) != 0)
                            return 0;

                        it->lastIdxIterPage++;
                        it->lastIdxIterRec = 0;
                        cnt = SBITS_GET_COUNT(idxbuf);
                        // id_t* id = SBITS_GET_MIN_KEY(idxbuf, state);
                        id_t *id = ((id_t *)((int8_t *)idxbuf + 8));

                        /* Index page may have entries that are earlier than
                         * first active data page. Advance iterator beyond them.
                         */
                        it->lastIterPage = *id;
                        if (state->firstDataPageId > *id)
                            it->lastIdxIterRec += (state->firstDataPageId - *id);
                        if (it->lastIdxIterRec >= cnt) {
                            /* Jump ahead pages in the index */
                            /* TODO: Could improve this so do not read first page if know it will not be useful */
                            it->lastIdxIterPage += it->lastIdxIterRec / state->maxIdxRecordsPerPage - 1; // -1 as already performed increment
                            printf("Jumping ahead pages to: %d\n", it->lastIdxIterPage);
                        }
                    }

                    /* Check bitmaps in current index page until find a match */
                    while (it->lastIdxIterRec < cnt) {
                        void *bm = (int8_t *)idxbuf + SBITS_IDX_HEADER_SIZE + it->lastIdxIterRec * state->bitmapSize;
                        if (bitmapOverlap((uint8_t *)it->queryBitmap, (uint8_t *)bm, (int8_t)state->bitmapSize) >= 1) {
                            readPageId = (it->lastIterPage + it->lastIdxIterRec) % (state->endDataPage - state->startDataPage);
                            it->lastIdxIterRec++;
                            goto readPage;
                        }
                        it->lastIdxIterRec++;
                    }
                    /* Read next index block */
                    continue;
                }
            readPage:
                // printf("read page: %d\n", readPageId);
                if (readPage(state, readPageId) != 0)
                    return 0;

                /* Check bitmap overlap if present */
                if (it->queryBitmap == NULL || !SBITS_USING_BMAP(state->parameters))
                    break;

                /* Check bitmap */
                void *bm = SBITS_GET_BITMAP(buf);
                if (bitmapOverlap((uint8_t *)it->queryBitmap, (uint8_t *)bm, (int8_t)state->bitmapSize) >= 1) {
                    /* Overlap in bitmap - will process this page */
                    break;
                }
            }
        }

        /* Get record */
        *key = (int8_t *)buf + state->headerSize + it->lastIterRec * state->recordSize;
        *data = (int8_t *)buf + state->headerSize + it->lastIterRec * state->recordSize + state->keySize;
        it->lastIterRec++;

        /* Check that record meets filter constraints */
        if (it->minKey != NULL && state->compareKey(*key, it->minKey) < 0)
            continue;
        if (it->maxKey != NULL && state->compareKey(*key, it->maxKey) > 0)
            return 0;
        if (it->minData != NULL && state->compareData(*data, it->minData) < 0)
            continue;
        if (it->maxData != NULL && state->compareData(*data, it->maxData) > 0)
            continue;
        return 1;
    }
}

/**
 * @brief	Prints statistics.
 * @param	state	SBITS state structure
 */
void printStats(sbitsState *state) {
    printf("Num reads: %d\n", state->numReads);
    printf("Buffer hits: %d\n", state->bufferHits);
    printf("Num writes: %d\n", state->numWrites);
    printf("Num index reads: %d\n", state->numIdxReads);
    printf("Num index writes: %d\n", state->numIdxWrites);
    printf("Max Error: %d\n", state->maxError);

    if (SEARCH_METHOD == 2) {
        if (USE_RADIX) {
            splinePrint(state->rdix->spl);
            radixsplinePrint(state->rdix);
        } else {
            splinePrint(state->spl);
        }
    }
}

/**
 * @brief	Writes page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Buffer for writing out page
 * @return	Return page number if success, -1 if error.
 */
id_t writePage(sbitsState *state, void *buffer) {
    if (state->storageType == FILE_STORAGE && state->file == NULL)
        return -1;

    /* Always writes to next page number. Returned to user. */
    id_t pageNum = state->nextPageId++;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->nextPageWriteId >= state->erasedEndPage && state->nextPageWriteId + state->eraseSizeInPages < state->endDataPage) {
        if (state->erasedEndPage != 0) {
            state->erasedEndPage += state->eraseSizeInPages;
        } else {
            /* Special case for start of file and page 0 */
            state->erasedEndPage += state->eraseSizeInPages - 1;
        }

        if (state->wrappedMemory != 0) {
            /* Have went through memory at least once. Whatever is erased is actual data that is no longer available. */
            state->firstDataPage = state->erasedEndPage + 1;
            state->firstDataPageId += state->eraseSizeInPages;
            /* Estimate the smallest key now. Could determine exactly by reading this page */
            state->minKey += state->eraseSizeInPages * state->avgKeyDiff * state->maxRecordsPerPage;
        }
    }

    if (state->nextPageWriteId >= state->endDataPage) {
        /* Data storage is full. Reclaim space. */
        /* Perform erase */

        /* Update the start of the data */

        state->firstDataPageId += state->eraseSizeInPages;
        state->erasedEndPage = state->startDataPage + state->eraseSizeInPages - 1;
        /* First active physical data page is just after what was erased */
        state->firstDataPage = state->erasedEndPage + 1;
        state->wrappedMemory = 1;

        /* Wrap to start of memory space */
        state->nextPageWriteId = state->startDataPage;

        /* Estimate the smallest key now. Could determine exactly by reading this page */
        state->minKey += state->eraseSizeInPages * state->avgKeyDiff * state->maxRecordsPerPage;
    }

    if (state->storageType == FILE_STORAGE) {
        /* Seek to page location in file */
        fseek(state->file, state->nextPageWriteId * state->pageSize, SEEK_SET);
        int32_t val = fwrite(buffer, state->pageSize, 1, state->file);
        if (val == 0) {
            printf("Failed to write data page: %lu\n", pageNum);
            return -1;
        }
    } else { /* Data flash storage */
        dfwrite(state->nextPageWriteId, buffer, state->pageSize);
    }

    state->nextPageWriteId++;
    state->numWrites++;

    return pageNum;
}

/**
 * @brief	Writes index page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Buffer to use for writing index page
 * @return	Return page number if success, -1 if error.
 */
id_t writeIndexPage(sbitsState *state, void *buffer) {
    if (state->storageType == FILE_STORAGE && state->indexFile == NULL)
        return -1;

    /* Always writes to next page number. Returned to user. */
    id_t pageNum = state->nextIdxPageId++;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->nextIdxPageWriteId >= state->erasedEndIdxPage &&
        state->nextIdxPageWriteId + state->eraseSizeInPages < state->endIdxPage - state->startIdxPage + 1) {
        if (state->erasedEndIdxPage != 0) {
            state->erasedEndIdxPage += state->eraseSizeInPages;
        } else {
            /* Special case for start of file and page 0 */
            state->erasedEndIdxPage += state->eraseSizeInPages - 1;
        }

        if (state->wrappedIdxMemory != 0) // pageNum > state->nextPageWriteId)
        {                                 /* Have went through memory at least once. Whatever is erased is
                                                 actual data that is no longer available. */
            state->firstIdxPage = state->erasedEndIdxPage + 1;
        }
    }

    if (state->nextIdxPageWriteId >= state->endIdxPage - state->startIdxPage + 1) {
        printf("Exhausted index pages: %d.\n", state->nextIdxPageWriteId);

        /* Index storage is full. Reclaim space. */

        /* Perform erase */

        /* Update the start of the data */
        state->erasedEndIdxPage = 0 + state->eraseSizeInPages - 1;
        state->firstIdxPage = state->erasedEndIdxPage + 1; /* First active physical data page is just after what was erased */
        state->wrappedIdxMemory = 1;

        /* Wrap to start of memory space */
        state->nextIdxPageWriteId = 0;
    }

    if (state->storageType == FILE_STORAGE) {
        /* Seek to page location in file */
        fseek(state->indexFile, state->nextIdxPageWriteId * state->pageSize, SEEK_SET);
        int32_t val = fwrite(buffer, state->pageSize, 1, state->indexFile);
        if (val == 0) {
            printf("Failed to write index page: %lu\n", pageNum);
            return -1;
        }
    } else {
        dfwrite(state->startIdxPage + state->nextIdxPageWriteId, buffer, state->pageSize);
    }

    state->nextIdxPageWriteId++;
    state->numIdxWrites++;

    return pageNum;
}

/**
 * @brief	Writes variable data page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Buffer to use to write page to storage
 * @return	Return page number if success, -1 if error.
 */
id_t writeVariablePage(sbitsState *state, void *buffer) {
    if (state->storageType == FILE_STORAGE && state->varFile == NULL) {
        return -1;
    }

    // Make sure the address being witten to wraps around
    state->nextVarPageId %= state->numVarPages;

    // Erase data if needed
    if (state->numAvailVarPages <= 0) {
        state->numAvailVarPages += state->eraseSizeInPages;
        // Last page that is deleted
        id_t pageNum = (state->nextVarPageId + state->eraseSizeInPages - 1) % state->numVarPages;

        // Read in that page so we can update which records we still have the data for
        if (readVariablePage(state, pageNum) != 0) {
            return -1;
        }
        void *buf = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
        memcpy(&state->minVarRecordId, buf, state->keySize);
        state->minVarRecordId += 1; // Add one because the result from the last line is a record that is erased
    }

    if (state->storageType == FILE_STORAGE) {
        // Write to file
        fseek(state->varFile, state->pageSize * state->nextVarPageId, SEEK_SET);
        fwrite(buffer, state->pageSize, 1, state->varFile);
    } else {
        dfwrite(state->varAddressStart / state->pageSize + state->nextVarPageId, buffer, state->pageSize);
    }

    state->nextVarPageId++;
    state->numAvailVarPages--;
    state->numWrites++;

    return state->nextIdxPageId - 1;
}

/**
 * @brief	Reads given page from storage.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readPage(sbitsState *state, id_t pageNum) {
    /* Check if page is currently in buffer */
    if (pageNum == state->bufferedPageId) {
        state->bufferHits++;
        return 0;
    }

    void *buf = (int8_t *)state->buffer + state->pageSize;

    if (state->storageType == FILE_STORAGE) {
        /* Page is not in buffer. Read from storage. */
        SD_FILE *fp = state->file;

        /* Seek to page location in file */
        fseek(fp, pageNum * state->pageSize, SEEK_SET);

        /* Read page into start of buffer 1 */
        if (0 == fread(buf, state->pageSize, 1, fp))
            return -1;

    } else {
        dfread(pageNum, buf, state->pageSize);
    }

    state->numReads++;
    state->bufferedPageId = pageNum;
    return 0;
}

/**
 * @brief	Reads given index page from storage.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readIndexPage(sbitsState *state, id_t pageNum) {
    /* Check if page is currently in buffer */
    if (pageNum == state->bufferedIndexPageId) {
        state->bufferHits++;
        return 0;
    }

    void *buf = (int8_t *)state->buffer + state->pageSize * SBITS_INDEX_READ_BUFFER;
    if (state->storageType == FILE_STORAGE) {
        /* Page is not in buffer. Read from storage. */
        SD_FILE *fp = state->indexFile;

        /* Seek to page location in file */
        fseek(fp, pageNum * state->pageSize, SEEK_SET);

        /* Read page into start of buffer */
        if (0 == fread(buf, state->pageSize, 1, fp))
            return -1;

    } else {
        dfread(state->startIdxPage + pageNum, buf, state->pageSize);
    }

    state->numIdxReads++;
    state->bufferedIndexPageId = pageNum;
    return 0;
}

/**
 * @brief	Reads given variable data page from storage
 * @param 	state 	SBITS algorithm state structure
 * @param 	pageNum Page number to read
 * @return 	Return 0 if success, -1 if error
 */
int8_t readVariablePage(sbitsState *state, id_t pageNum) {
    // Get buffer to read into
    void *buf = (int8_t *)state->buffer + SBITS_VAR_READ_BUFFER(state->parameters) * state->pageSize;

    if (state->storageType == FILE_STORAGE) {
        // Go to page to read
        fseek(state->varFile, pageNum * state->pageSize, SEEK_SET);

        // Read in one page worth of data
        if (fread(buf, state->pageSize, 1, state->varFile) == 0) {
            return -1;
        }
    } else {
        dfread(state->varAddressStart / state->pageSize + pageNum, buf, state->pageSize);
    }

    // Track stats
    state->numReads++;
    return 0;
}

/**
 * @brief	Resets statistics.
 * @param	state	SBITS state structure
 */
void resetStats(sbitsState *state) {
    state->numReads = 0;
    state->numWrites = 0;
    state->bufferHits = 0;
    state->numIdxReads = 0;
    state->numIdxWrites = 0;
}

/**
 * @brief	Closes structure and frees any dynamic space.
 * @param	state	SBITS state structure
 */
void sbitsClose(sbitsState *state) {
    if (state->storageType == FILE_STORAGE) {
        if (state->file != NULL) {
            fclose(state->file);
        }
        if (state->indexFile != NULL) {
            fclose(state->indexFile);
        }
        if (state->varFile != NULL) {
            fclose(state->varFile);
        }
    }
    if (SEARCH_METHOD == 2) { // Spline
        if (USE_RADIX) {
            radixsplineClose(state->rdix);
            free(state->rdix);
        } else {
            splineFree(state->spl);
        }
        free(state->spl);
    }
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	SBITS state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(sbitsState *state, void *min, void *max, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = 65535; /* Everything */
        return;
    }

    int8_t i = 0;
    uint16_t val = 32768;
    if (min != NULL) {
        /* Set bits based on min value */
        state->updateBitmap(min, bm);

        /* Assume here that bits are set in increasing order based on smallest
         * value */
        /* Find first set bit */
        while ((val & *bmval) == 0 && i < 16) {
            i++;
            val = val / 2;
        }
        val = val / 2;
        i++;
    }
    if (max != NULL) {
        /* Set bits based on min value */
        uint16_t prev = *bmval;
        state->updateBitmap(max, bm);
        if (*bmval == prev)
            return; /* Min and max bit vector are the same */

        while ((val & *bmval) == 0 && i < 16) {
            i++;
            *bmval = *bmval + val;
            val = val / 2;
        }
    } else {
        while (i < 16) {
            i++;
            *bmval = *bmval + val;
            val = val / 2;
        }
    }
}

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	SBITS state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(sbitsState *state, void *min, void *max, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = UINT64_MAX; /* Everything */
        return;
    }

    int8_t i = 0;
    uint64_t val = (uint64_t)(INT64_MAX) + 1;
    if (min != NULL) {
        /* Set bits based on min value */
        state->updateBitmap(min, bm);

        /* Assume here that bits are set in increasing order based on smallest
         * value */
        /* Find first set bit */
        while ((val & *bmval) == 0 && i < 64) {
            i++;
            val = val / 2;
        }
        val = val / 2;
        i++;
    }
    if (max != NULL) {
        /* Set bits based on min value */
        uint64_t prev = *bmval;
        state->updateBitmap(max, bm);

        /* Min and max bit vector are the same */
        if (*bmval == prev)
            return;

        while ((val & *bmval) == 0 && i < 64) {
            i++;
            *bmval = *bmval + val;
            val = val / 2;
        }
    } else {
        while (i < 64) {
            i++;
            *bmval = *bmval + val;
            val = val / 2;
        }
    }
}
