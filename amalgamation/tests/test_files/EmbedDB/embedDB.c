/******************************************************************************/
/**
 * @file        embedDB.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code for EmbedDB.
 * @copyright   Copyright 2024
 *              EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *  may be used to endorse or promote products derived from this software without
 *  specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include "embedDB.h"

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../spline/radixspline.h"
#include "../spline/spline.h"

/**
 * 0 = Modified binary search
 * 1 = Binary serach
 * 2 = Modified linear search (Spline)
 */
#define SEARCH_METHOD 2

/**
 * Number of bits to be indexed by the Radix Search structure
 * Note: The Radix search structure is only used with Spline (SEARCH_METHOD == 2) To use a pure Spline index without a Radix table, set RADIX_BITS to 0
 */
#define RADIX_BITS 0

/* Helper Functions */
int8_t embedDBInitData(embedDBState *state);
int8_t embedDBInitDataFromFile(embedDBState *state);
int8_t embedDBInitIndex(embedDBState *state);
int8_t embedDBInitIndexFromFile(embedDBState *state);
int8_t embedDBInitVarData(embedDBState *state);
int8_t embedDBInitVarDataFromFile(embedDBState *state);
void updateAverageKeyDifference(embedDBState *state, void *buffer);
void embedDBInitSplineFromFile(embedDBState *state);
int32_t getMaxError(embedDBState *state, void *buffer);
void updateMaxiumError(embedDBState *state, void *buffer);
int8_t embedDBSetupVarDataStream(embedDBState *state, void *key, embedDBVarDataStream **varData, id_t recordNumber);
uint32_t cleanSpline(embedDBState *state, void *key);
void readToWriteBuf(embedDBState *state);
void readToWriteBufVar(embedDBState *state);
void embedDBFlushVar(embedDBState *state);

void printBitmap(char *bm) {
    for (int8_t i = 0; i <= 7; i++) {
        printf(" " BYTE_TO_BINARY_PATTERN "", BYTE_TO_BINARY(*(bm + i)));
    }
    printf("\n");
}

/**
 * @brief	Determine if two bitmaps have any overlapping bits
 * @return	1 if there is any overlap, else 0
 */
int8_t bitmapOverlap(uint8_t *bm1, uint8_t *bm2, int8_t size) {
    for (int8_t i = 0; i < size; i++)
        if ((*((uint8_t *)(bm1 + i)) & *((uint8_t *)(bm2 + i))) >= 1)
            return 1;

    return 0;
}

void initBufferPage(embedDBState *state, int pageNum) {
    /* Initialize page */
    uint16_t i = 0;
    void *buf = (char *)state->buffer + pageNum * state->pageSize;

    for (i = 0; i < state->pageSize; i++) {
        ((int8_t *)buf)[i] = 0;
    }

    if (pageNum != EMBEDDB_VAR_WRITE_BUFFER(state->parameters)) {
        /* Initialize header key min. Max and sum is already set to zero by the
         * for-loop above */
        void *min = EMBEDDB_GET_MIN_KEY(buf);
        /* Initialize min to all 1s */
        for (i = 0; i < state->keySize; i++) {
            ((int8_t *)min)[i] = 1;
        }

        /* Initialize data min. */
        min = EMBEDDB_GET_MIN_DATA(buf, state);
        /* Initialize min to all 1s */
        for (i = 0; i < state->dataSize; i++) {
            ((int8_t *)min)[i] = 1;
        }
    }
}

/**
 * @brief   Initializes the Radix Spline data structure and assigns it to state
 * @param   state       embedDB state structure
 * @param   size        Size data to load into data structure
 * @param   radixSize   number bits to be indexed by radix
 * @return  void
 */
int8_t initRadixSpline(embedDBState *state, size_t radixSize) {
    spline *spl = (spline *)malloc(sizeof(spline));
    state->spl = spl;

    int8_t initResult = splineInit(state->spl, state->numSplinePoints, state->indexMaxError, state->keySize);
    if (initResult == -1) {
        return -1;
    }

    radixspline *rsidx = (radixspline *)malloc(sizeof(radixspline));
    state->rdix = rsidx;
    radixsplineInit(state->rdix, state->spl, radixSize, state->keySize);
    return 0;
}

/**
 * @brief   Return the smallest key in the node
 * @param   state   embedDB algorithm state structure
 * @param   buffer  In memory page buffer with node data
 */
void *embedDBGetMinKey(embedDBState *state, void *buffer) {
    return (void *)((int8_t *)buffer + state->headerSize);
}

/**
 * @brief   Return the largest key in the node
 * @param   state   embedDB algorithm state structure
 * @param   buffer  In memory page buffer with node data
 */
void *embedDBGetMaxKey(embedDBState *state, void *buffer) {
    int16_t count = EMBEDDB_GET_COUNT(buffer);
    return (void *)((int8_t *)buffer + state->headerSize + (count - 1) * state->recordSize);
}

/**
 * @brief   Initialize embedDB structure.
 * @param   state           embedDB algorithm state structure
 * @param   indexMaxError   max error of indexing structure (spline)
 * @return  Return 0 if success. Non-zero value if error.
 */
int8_t embedDBInit(embedDBState *state, size_t indexMaxError) {
    if (state->keySize > 8) {
#ifdef PRINT_ERRORS
        printf("ERROR: Key size is too large. Max key size is 8 bytes.\n");
#endif
        return -1;
    }

    state->recordSize = state->keySize + state->dataSize;
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        state->recordSize += 4;
    }

    state->indexMaxError = indexMaxError;

    /* Calculate block header size */

    /* Header size depends on bitmap size: 6 + X bytes: 4 byte id, 2 for record count, X for bitmap. */
    state->headerSize = 6;
    if (EMBEDDB_USING_INDEX(state->parameters))
        state->headerSize += state->bitmapSize;

    if (EMBEDDB_USING_MAX_MIN(state->parameters))
        state->headerSize += state->keySize * 2 + state->dataSize * 2;

    /* Flags to show that these values have not been initalized with actual data yet */
    state->minKey = UINT32_MAX;
    state->bufferedPageId = -1;
    state->bufferedIndexPageId = -1;
    state->bufferedVarPage = -1;

    /* Calculate number of records per page */
    state->maxRecordsPerPage = (state->pageSize - state->headerSize) / state->recordSize;

    /* Initialize max error to maximum records per page */
    state->maxError = state->maxRecordsPerPage;

    /* Allocate first page of buffer as output page */
    initBufferPage(state, 0);

    if (state->numDataPages < (EMBEDDB_USING_INDEX(state->parameters) * 2 + 2) * state->eraseSizeInPages) {
#ifdef PRINT_ERRORS
        printf("ERROR: Number of pages allocated must be at least twice erase block size for embedDB and four times when using indexing. Memory pages: %d\n", state->numDataPages);
#endif
        return -1;
    }

    /* Initalize the spline or radix spline structure if either are to be used */
    if (SEARCH_METHOD == 2) {
        state->cleanSpline = 1;
        int8_t splineInitResult = 0;
        if (RADIX_BITS > 0) {
            splineInitResult = initRadixSpline(state, RADIX_BITS);

        } else {
            state->spl = malloc(sizeof(spline));
            splineInitResult = splineInit(state->spl, state->numSplinePoints, indexMaxError, state->keySize);
        }
        if (splineInitResult == -1) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to initialize spline.");
#endif
            return -1;
        }
    }

    /* Allocate file for data*/
    int8_t dataInitResult = 0;
    dataInitResult = embedDBInitData(state);

    if (dataInitResult != 0) {
        return dataInitResult;
    }

    /* Allocate file and buffer for index */
    int8_t indexInitResult = 0;
    if (EMBEDDB_USING_INDEX(state->parameters)) {
        if (state->bufferSizeInBlocks < 4) {
#ifdef PRINT_ERRORS
            printf("ERROR: embedDB using index requires at least 4 page buffers.\n");
#endif
            return -1;
        } else {
            indexInitResult = embedDBInitIndex(state);
        }
    } else {
        state->indexFile = NULL;
        state->numIndexPages = 0;
    }

    if (indexInitResult != 0) {
        return indexInitResult;
    }

    /* Allocate file and buffer for variable data */
    int8_t varDataInitResult = 0;
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        if (state->bufferSizeInBlocks < 4 + (EMBEDDB_USING_INDEX(state->parameters) ? 2 : 0)) {
#ifdef PRINT_ERRORS
            printf("ERROR: embedDB using variable records requires at least 4 page buffers if there is no index and 6 if there is.\n");
#endif
            return -1;
        } else {
            varDataInitResult = embedDBInitVarData(state);
        }
        return varDataInitResult;
    } else {
        state->varFile = NULL;
        state->numVarPages = 0;
    }

    embedDBResetStats(state);
    return 0;
}

int8_t embedDBInitData(embedDBState *state) {
    state->nextDataPageId = 0;
    state->avgKeyDiff = 1;
    state->nextDataPageId = 0;
    state->numAvailDataPages = state->numDataPages;
    state->minDataPageId = 0;

    if (state->dataFile == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: No data file provided!\n");
#endif
        return -1;
    }

    /* Setup data file. */
    if (!EMBEDDB_RESETING_DATA(state->parameters)) {
        int8_t openStatus = state->fileInterface->open(state->dataFile, EMBEDDB_FILE_MODE_R_PLUS_B);
        if (openStatus) {
            return embedDBInitDataFromFile(state);
        }
    }

    int8_t openStatus = state->fileInterface->open(state->dataFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    if (!openStatus) {
#ifdef PRINT_ERRORS
        printf("Error: Can't open data file!\n");
#endif
        return -1;
    }

    return 0;
}

int8_t embedDBInitDataFromFile(embedDBState *state) {
    id_t logicalPageId = 0;
    id_t maxLogicalPageId = 0;
    id_t physicalPageId = 0;

    /* This will become zero if there is no more to read */
    int8_t moreToRead = !(readPage(state, physicalPageId));

    bool haveWrappedInMemory = false;
    int count = 0;
    void *buffer = (int8_t *)state->buffer + state->pageSize;
    while (moreToRead && count < state->numDataPages) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        if (count == 0 || logicalPageId == maxLogicalPageId + 1) {
            maxLogicalPageId = logicalPageId;
            physicalPageId++;
            updateMaxiumError(state, buffer);
            moreToRead = !(readPage(state, physicalPageId));
            count++;
        } else {
            haveWrappedInMemory = logicalPageId == (maxLogicalPageId - state->numDataPages + 1);
            break;
        }
    }

    if (count == 0)
        return 0;

    state->nextDataPageId = maxLogicalPageId + 1;
    state->minDataPageId = 0;
    id_t physicalPageIDOfSmallestData = 0;
    if (haveWrappedInMemory) {
        physicalPageIDOfSmallestData = logicalPageId % state->numDataPages;
    }
    readPage(state, physicalPageIDOfSmallestData);
    memcpy(&(state->minDataPageId), buffer, sizeof(id_t));
    state->numAvailDataPages = state->numDataPages + state->minDataPageId - maxLogicalPageId - 1;
    if (state->keySize <= 4) {
        uint32_t minKey = 0;
        memcpy(&minKey, embedDBGetMinKey(state, buffer), state->keySize);
        state->minKey = minKey;
    } else {
        uint64_t minKey = 0;
        memcpy(&minKey, embedDBGetMinKey(state, buffer), state->keySize);
        state->minKey = minKey;
    }

    /* Put largest key back into the buffer */
    readPage(state, state->nextDataPageId - 1);

    updateAverageKeyDifference(state, buffer);
    if (SEARCH_METHOD == 2) {
        embedDBInitSplineFromFile(state);
    }

    return 0;
}

void embedDBInitSplineFromFile(embedDBState *state) {
    id_t pageNumberToRead = state->minDataPageId;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;
    id_t pagesRead = 0;
    id_t numberOfPagesToRead = state->nextDataPageId - state->minDataPageId;
    while (pagesRead < numberOfPagesToRead) {
        readPage(state, pageNumberToRead % state->numDataPages);
        if (RADIX_BITS > 0) {
            radixsplineAddPoint(state->rdix, embedDBGetMinKey(state, buffer), pageNumberToRead++);
        } else {
            splineAdd(state->spl, embedDBGetMinKey(state, buffer), pageNumberToRead++);
        }
        pagesRead++;
    }
}

int8_t embedDBInitIndex(embedDBState *state) {
    /* Setup index file. */

    /* 4 for id, 2 for count, 2 unused, 4 for minKey (pageId), 4 for maxKey (pageId) */
    state->maxIdxRecordsPerPage = (state->pageSize - 16) / state->bitmapSize;

    /* Allocate third page of buffer as index output page */
    initBufferPage(state, EMBEDDB_INDEX_WRITE_BUFFER);

    /* Add page id to minimum value spot in page */
    void *buf = (int8_t *)state->buffer + state->pageSize * (EMBEDDB_INDEX_WRITE_BUFFER);
    id_t *ptr = ((id_t *)((int8_t *)buf + 8));
    *ptr = state->nextDataPageId;

    state->nextIdxPageId = 0;
    state->numAvailIndexPages = state->numIndexPages;
    state->minIndexPageId = 0;

    if (state->numIndexPages < state->eraseSizeInPages * 2) {
#ifdef PRINT_ERRORS
        printf("ERROR: Minimum index space is two erase blocks\n");
#endif
        return -1;
    }

    if (state->numIndexPages % state->eraseSizeInPages != 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: Ensure index space is a multiple of erase block size\n");
#endif
        return -1;
    }

    if (state->indexFile == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: No index file provided!\n");
#endif
        return -1;
    }

    if (!EMBEDDB_RESETING_DATA(state->parameters)) {
        int8_t openStatus = state->fileInterface->open(state->indexFile, EMBEDDB_FILE_MODE_R_PLUS_B);
        if (openStatus) {
            return embedDBInitIndexFromFile(state);
        }
    }

    int8_t openStatus = state->fileInterface->open(state->indexFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    if (!openStatus) {
#ifdef PRINT_ERRORS
        printf("Error: Can't open index file!\n");
#endif
        return -1;
    }

    return 0;
}

int8_t embedDBInitIndexFromFile(embedDBState *state) {
    id_t logicalIndexPageId = 0;
    id_t maxLogicaIndexPageId = 0;
    id_t physicalIndexPageId = 0;

    /* This will become zero if there is no more to read */
    int8_t moreToRead = !(readIndexPage(state, physicalIndexPageId));

    bool haveWrappedInMemory = false;
    int count = 0;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_READ_BUFFER;

    while (moreToRead && count < state->numIndexPages) {
        memcpy(&logicalIndexPageId, buffer, sizeof(id_t));
        if (count == 0 || logicalIndexPageId == maxLogicaIndexPageId + 1) {
            maxLogicaIndexPageId = logicalIndexPageId;
            physicalIndexPageId++;
            moreToRead = !(readIndexPage(state, physicalIndexPageId));
            count++;
        } else {
            haveWrappedInMemory = logicalIndexPageId == maxLogicaIndexPageId - state->numIndexPages + 1;
            break;
        }
    }

    if (count == 0)
        return 0;

    state->nextIdxPageId = maxLogicaIndexPageId + 1;
    id_t physicalPageIDOfSmallestData = 0;
    if (haveWrappedInMemory) {
        physicalPageIDOfSmallestData = logicalIndexPageId % state->numIndexPages;
    }
    readIndexPage(state, physicalPageIDOfSmallestData);
    memcpy(&(state->minIndexPageId), buffer, sizeof(id_t));
    state->numAvailIndexPages = state->numIndexPages + state->minIndexPageId - maxLogicaIndexPageId - 1;

    return 0;
}

int8_t embedDBInitVarData(embedDBState *state) {
    // Initialize variable data outpt buffer
    initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));

    state->variableDataHeaderSize = state->keySize + sizeof(id_t);
    state->currentVarLoc = state->variableDataHeaderSize;
    state->minVarRecordId = 0;
    state->numAvailVarPages = state->numVarPages;
    state->nextVarPageId = 0;

    if (!EMBEDDB_RESETING_DATA(state->parameters)) {
        int8_t openResult = state->fileInterface->open(state->varFile, EMBEDDB_FILE_MODE_R_PLUS_B);
        if (openResult) {
            return embedDBInitVarDataFromFile(state);
        }
    }

    int8_t openResult = state->fileInterface->open(state->varFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    if (!openResult) {
#ifdef PRINT_ERRORS
        printf("Error: Can't open variable data file!\n");
#endif
        return -1;
    }

    return 0;
}

int8_t embedDBInitVarDataFromFile(embedDBState *state) {
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);
    id_t logicalVariablePageId = 0;
    id_t maxLogicalVariablePageId = 0;
    id_t physicalVariablePageId = 0;
    int8_t moreToRead = !(readVariablePage(state, physicalVariablePageId));
    uint32_t count = 0;
    bool haveWrappedInMemory = false;
    while (moreToRead && count < state->numVarPages) {
        memcpy(&logicalVariablePageId, buffer, sizeof(id_t));
        if (count == 0 || logicalVariablePageId == maxLogicalVariablePageId + 1) {
            maxLogicalVariablePageId = logicalVariablePageId;
            physicalVariablePageId++;
            moreToRead = !(readVariablePage(state, physicalVariablePageId));
            count++;
        } else {
            haveWrappedInMemory = logicalVariablePageId == maxLogicalVariablePageId - state->numVarPages + 1;
            break;
        }
    }

    if (count == 0)
        return 0;

    state->nextVarPageId = maxLogicalVariablePageId + 1;
    id_t minVarPageId = 0;
    if (haveWrappedInMemory) {
        id_t physicalPageIDOfSmallestData = logicalVariablePageId % state->numVarPages;
        readVariablePage(state, physicalPageIDOfSmallestData);
        memcpy(&(state->minVarRecordId), (int8_t *)buffer + sizeof(id_t), state->keySize);
        memcpy(&minVarPageId, buffer, sizeof(id_t));
        state->minVarRecordId++;
    }

    state->numAvailVarPages = state->numVarPages + minVarPageId - maxLogicalVariablePageId - 1;
    state->currentVarLoc = state->nextVarPageId % state->numVarPages * state->pageSize + state->variableDataHeaderSize;

    return 0;
}

/**
 * @brief   Prints the initialization stats of the given embedDB state
 * @param   state   embedDB state structure
 */
void embedDBPrintInit(embedDBState *state) {
    printf("EmbedDB State Initialization Stats:\n");
    printf("Buffer size: %d  Page size: %d\n", state->bufferSizeInBlocks, state->pageSize);
    printf("Key size: %d Data size: %d %sRecord size: %d\n", state->keySize, state->dataSize, EMBEDDB_USING_VDATA(state->parameters) ? "Variable data pointer size: 4 " : "", state->recordSize);
    printf("Use index: %d  Max/min: %d Sum: %d Bmap: %d\n", EMBEDDB_USING_INDEX(state->parameters), EMBEDDB_USING_MAX_MIN(state->parameters), EMBEDDB_USING_SUM(state->parameters), EMBEDDB_USING_BMAP(state->parameters));
    printf("Header size: %d  Records per page: %d\n", state->headerSize, state->maxRecordsPerPage);
}

/**
 * @brief	Given a state, uses the first and last keys to estimate a slope of keys
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @return	Returns slope estimate float
 */
float embedDBCalculateSlope(embedDBState *state, void *buffer) {
    // simplistic slope calculation where the first two entries are used, should be improved

    uint32_t slopeX1, slopeX2;
    slopeX1 = 0;
    slopeX2 = EMBEDDB_GET_COUNT(buffer) - 1;

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
 * @param	state	embedDB algorithm state structure
 * @return	Returns max error integer.
 */
int32_t getMaxError(embedDBState *state, void *buffer) {
    if (state->keySize <= 4) {
        int32_t maxError = 0, currentError;
        uint32_t minKey = 0, currentKey = 0;
        memcpy(&minKey, embedDBGetMinKey(state, buffer), state->keySize);

        // get slope of keys within page
        float slope = embedDBCalculateSlope(state, buffer);

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
        memcpy(&minKey, embedDBGetMinKey(state, buffer), state->keySize);

        // get slope of keys within page
        float slope = embedDBCalculateSlope(state, state->buffer);  // this is incorrect, should be buffer. TODO: fix

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
 * @param	state	embedDB algorithm state structure
 */
void indexPage(embedDBState *state, uint32_t pageNumber) {
    if (SEARCH_METHOD == 2) {
        if (RADIX_BITS > 0) {
            radixsplineAddPoint(state->rdix, embedDBGetMinKey(state, state->buffer), pageNumber);
        } else {
            splineAdd(state->spl, embedDBGetMinKey(state, state->buffer), pageNumber);
        }
    }
}

/**
 * @brief	Puts a given key, data pair into structure.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBPut(embedDBState *state, void *key, void *data) {
    /* Copy record into block */
    count_t count = EMBEDDB_GET_COUNT(state->buffer);
    if (state->minKey != UINT32_MAX) {
        void *previousKey = NULL;
        if (count == 0) {
            readPage(state, (state->nextDataPageId - 1) % state->numDataPages);
            previousKey = ((int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER) +
                          (state->recordSize * (state->maxRecordsPerPage - 1)) + state->headerSize;
        } else {
            previousKey = (int8_t *)state->buffer + (state->recordSize * (count - 1)) + state->headerSize;
        }
        if (state->compareKey(key, previousKey) != 1) {
#ifdef PRINT_ERRORS
            printf("Keys must be strictly ascending order. Insert Failed.\n");
#endif
            return 1;
        }
    }

    /* Write current page if full */
    if (count >= state->maxRecordsPerPage) {
        // As the first buffer is the data write buffer, no manipulation is required
        id_t pageNum = writePage(state, state->buffer);

        indexPage(state, pageNum);

        /* Save record in index file */
        if (state->indexFile != NULL) {
            void *buf = (int8_t *)state->buffer + state->pageSize * (EMBEDDB_INDEX_WRITE_BUFFER);
            count_t idxcount = EMBEDDB_GET_COUNT(buf);
            if (idxcount >= state->maxIdxRecordsPerPage) {
                /* Save index page */
                writeIndexPage(state, buf);

                idxcount = 0;
                initBufferPage(state, EMBEDDB_INDEX_WRITE_BUFFER);

                /* Add page id to minimum value spot in page */
                id_t *ptr = (id_t *)((int8_t *)buf + 8);
                *ptr = pageNum;
            }

            EMBEDDB_INC_COUNT(buf);

            /* Copy record onto index page */
            void *bm = EMBEDDB_GET_BITMAP(state->buffer);
            memcpy((void *)((int8_t *)buf + EMBEDDB_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);
        }

        updateAverageKeyDifference(state, state->buffer);
        updateMaxiumError(state, state->buffer);

        count = 0;
        initBufferPage(state, 0);
    }

    /* Copy record onto page */
    memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize, key, state->keySize);
    memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize + state->keySize, data, state->dataSize);

    /* Copy variable data offset if using variable data*/
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        uint32_t dataLocation;
        if (state->recordHasVarData) {
            dataLocation = state->currentVarLoc % (state->numVarPages * state->pageSize);
        } else {
            dataLocation = EMBEDDB_NO_VAR_DATA;
        }
        memcpy((int8_t *)state->buffer + (state->recordSize * count) + state->headerSize + state->keySize + state->dataSize, &dataLocation, sizeof(uint32_t));
    }

    /* Update count */
    EMBEDDB_INC_COUNT(state->buffer);

    /* Set minimum key for first record insert */
    if (state->minKey == UINT32_MAX)
        memcpy(&state->minKey, key, state->keySize);

    if (EMBEDDB_USING_MAX_MIN(state->parameters)) {
        /* Update MIN/MAX */
        void *ptr;
        if (count != 0) {
            /* Since keys are inserted in ascending order, every insert will
             * update max. Min will never change after first record. */
            ptr = EMBEDDB_GET_MAX_KEY(state->buffer, state);
            memcpy(ptr, key, state->keySize);

            ptr = EMBEDDB_GET_MIN_DATA(state->buffer, state);
            if (state->compareData(data, ptr) < 0)
                memcpy(ptr, data, state->dataSize);
            ptr = EMBEDDB_GET_MAX_DATA(state->buffer, state);
            if (state->compareData(data, ptr) > 0)
                memcpy(ptr, data, state->dataSize);
        } else {
            /* First record inserted */
            ptr = EMBEDDB_GET_MIN_KEY(state->buffer);
            memcpy(ptr, key, state->keySize);
            ptr = EMBEDDB_GET_MAX_KEY(state->buffer, state);
            memcpy(ptr, key, state->keySize);

            ptr = EMBEDDB_GET_MIN_DATA(state->buffer, state);
            memcpy(ptr, data, state->dataSize);
            ptr = EMBEDDB_GET_MAX_DATA(state->buffer, state);
            memcpy(ptr, data, state->dataSize);
        }
    }

    if (EMBEDDB_USING_BMAP(state->parameters)) {
        /* Update bitmap */
        char *bm = (char *)EMBEDDB_GET_BITMAP(state->buffer);
        state->updateBitmap(data, bm);
    }

    return 0;
}

void updateMaxiumError(embedDBState *state, void *buffer) {
    // Calculate error within the page
    int32_t maxError = getMaxError(state, buffer);
    if (state->maxError < maxError) {
        state->maxError = maxError;
    }
}

void updateAverageKeyDifference(embedDBState *state, void *buffer) {
    /* Update estimate of average key difference. */
    int32_t numBlocks = state->numDataPages - state->numAvailDataPages;
    if (numBlocks == 0)
        numBlocks = 1;

    if (state->keySize <= 4) {
        uint32_t maxKey = 0;
        memcpy(&maxKey, embedDBGetMaxKey(state, buffer), state->keySize);
        state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
    } else {
        uint64_t maxKey = 0;
        memcpy(&maxKey, embedDBGetMaxKey(state, buffer), state->keySize);
        state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
    }
}

/**
 * @brief	Puts the given key, data, and variable length data into the structure.
 * @param	state			embedDB algorithm state structure
 * @param	key				Key for record
 * @param	data			Data for record
 * @param	variableData	Variable length data for record
 * @param	length			Length of the variable length data in bytes
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBPutVar(embedDBState *state, void *key, void *data, void *variableData, uint32_t length) {
    if (!EMBEDDB_USING_VDATA(state->parameters)) {
#ifdef PRINT_ERRORS
        printf("Error: Can't insert variable data because it is not enabled\n");
#endif
        return -1;
    }

    // Insert their data

    /*
     * Check that there is enough space remaining in this page to start the insert of the variable
     * data here and if the data page will be written in embedDBGet
     */
    void *buf = (int8_t *)state->buffer + state->pageSize * (EMBEDDB_VAR_WRITE_BUFFER(state->parameters));
    if (state->currentVarLoc % state->pageSize > state->pageSize - 4 || EMBEDDB_GET_COUNT(state->buffer) >= state->maxRecordsPerPage) {
        writeVariablePage(state, buf);
        initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));
        // Move data writing location to the beginning of the next page, leaving the room for the header
        state->currentVarLoc += state->pageSize - state->currentVarLoc % state->pageSize + state->variableDataHeaderSize;
    }

    if (variableData == NULL) {
        // Var data enabled, but not provided
        state->recordHasVarData = 0;
        return embedDBPut(state, key, data);
    }

    // Perform the regular insert
    state->recordHasVarData = 1;
    int8_t r;
    if ((r = embedDBPut(state, key, data)) != 0) {
        return r;
    }

    // Update the header to include the maximum key value stored on this page
    memcpy((int8_t *)buf + sizeof(id_t), key, state->keySize);

    // Write the length of the data item into the buffer
    memcpy((uint8_t *)buf + state->currentVarLoc % state->pageSize, &length, sizeof(uint32_t));
    state->currentVarLoc += 4;

    // Check if we need to write after doing that
    if (state->currentVarLoc % state->pageSize == 0) {
        writeVariablePage(state, buf);
        initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));

        // Update the header to include the maximum key value stored on this page
        memcpy((int8_t *)buf + sizeof(id_t), key, state->keySize);
        state->currentVarLoc += state->variableDataHeaderSize;
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
            initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));

            // Update the header to include the maximum key value stored on this page and account for page number
            memcpy((int8_t *)buf + sizeof(id_t), key, state->keySize);
            state->currentVarLoc += state->variableDataHeaderSize;
        }
    }
    return 0;
}

/**
 * @brief	Given a key, estimates the location of the key within the node.
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @param	key		Key for record
 */
int16_t embedDBEstimateKeyLocation(embedDBState *state, void *buffer, void *key) {
    // get slope to use for linear estimation of key location
    // return estimated location of the key
    float slope = embedDBCalculateSlope(state, buffer);

    uint64_t minKey = 0, thisKey = 0;
    memcpy(&minKey, embedDBGetMinKey(state, buffer), state->keySize);
    memcpy(&thisKey, key, state->keySize);

    return (thisKey - minKey) / slope;
}

/**
 * @brief	Given a key, searches the node for the key. If interior node, returns child record number containing next page id to follow. If leaf node, returns if of first record with that key or (<= key). Returns -1 if key is not found.
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Pointer to in-memory buffer holding node
 * @param	key		Key for record
 * @param	range	1 if range query so return pointer to first record <= key, 0 if exact query so much return first exact match record
 */
id_t embedDBSearchNode(embedDBState *state, void *buffer, void *key, int8_t range) {
    int16_t first, last, middle, count;
    int8_t compare;
    void *mkey;

    count = EMBEDDB_GET_COUNT(buffer);
    middle = embedDBEstimateKeyLocation(state, buffer, key);

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
        mkey = (int8_t *)buffer + state->headerSize + (state->recordSize * middle);
        compare = state->compareKey(mkey, key);
        if (compare < 0) {
            first = middle + 1;
        } else if (compare == 0) {
            return middle;
        } else {
            last = middle - 1;
        }
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
 * @param	state		embedDB algorithm state structure
 * @param 	numReads	Tracks total number of reads for statistics
 * @param	buf			buffer to store page with desired record
 * @param	key			Key for the record to search for
 * @param	pageId		Page id to start search from
 * @param 	low			Lower bound for the page the record could be found on
 * @param 	high		Upper bound for the page the record could be found on
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t linearSearch(embedDBState *state, int16_t *numReads, void *buf, void *key, int32_t pageId, int32_t low, int32_t high) {
    int32_t pageError = 0;
    int32_t physPageId;
    while (1) {
        /* Move logical page number to physical page id based on location of first data page */
        physPageId = pageId % state->numDataPages;

        if (pageId > high || pageId < low || low > high || pageId < state->minDataPageId || pageId >= state->nextDataPageId) {
            return -1;
        }

        /* Read page into buffer. If 0 not returned, there was an error */
        id_t start = state->numReads;
        if (readPage(state, physPageId) != 0) {
            return -1;
        }
        *numReads += state->numReads - start;

        if (state->compareKey(key, embedDBGetMinKey(state, buf)) < 0) { /* Key is less than smallest record in block. */
            high = --pageId;
            pageError++;
        } else if (state->compareKey(key, embedDBGetMaxKey(state, buf)) > 0) { /* Key is larger than largest record in block. */
            low = ++pageId;
            pageError++;
        } else {
            /* Found correct block */
            return 0;
        }
    }
}

/**
 * @brief	Given a key, searches for data associated with
 *          that key in embedDB buffer using embedDBSearchNode.
 *          Note: Space for data must be already allocated.
 * @param	state	embedDB algorithm state structure
 * @param   buffer  pointer to embedDB buffer
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @param   range
 * @return	Return non-negative integer representing offset if success. -1 value if error.
 */
int8_t searchBuffer(embedDBState *state, void *buffer, void *key, void *data) {
    // return -1 if there is nothing in the buffer
    if (EMBEDDB_GET_COUNT(buffer) == 0) {
        return NO_RECORD_FOUND;
    }
    // find index of record inside of the write buffer
    id_t nextId = embedDBSearchNode(state, buffer, key, 0);
    // return 0 if found
    if (nextId != NO_RECORD_FOUND) {
        // Key found
        memcpy(data, (void *)((int8_t *)buffer + state->headerSize + state->recordSize * nextId + state->keySize), state->dataSize);
        return nextId;
    }
    // Key not found
    return NO_RECORD_FOUND;
}

/**
 * @brief	Given a key, returns data associated with key.
 * 			Note: Space for data must be already allocated.
 * 			Data is copied from database into data buffer.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBGet(embedDBState *state, void *key, void *data) {
    void *outputBuffer = state->buffer;
    if (state->nextDataPageId == 0) {
        int8_t success = searchBuffer(state, outputBuffer, key, data);
        if (success == 0) return success;

#ifdef PRINT_ERRORS
        printf("ERROR: No data in database.\n");
#endif
        return -1;
    }

    uint64_t thisKey = 0;
    memcpy(&thisKey, key, state->keySize);

    void *buf = (int8_t *)state->buffer + state->pageSize;
    int16_t numReads = 0;

    // if write buffer is not empty
    if ((EMBEDDB_GET_COUNT(outputBuffer) != 0)) {
        // get the max/min key from output buffer
        uint64_t bufMaxKey = 0;
        uint64_t bufMinKey = 0;
        memcpy(&bufMaxKey, embedDBGetMaxKey(state, outputBuffer), state->keySize);
        memcpy(&bufMinKey, embedDBGetMinKey(state, outputBuffer), state->keySize);
        // return -1 if key is not in buffer
        if (thisKey > bufMaxKey) return -1;
        // if key >= buffer's min, check buffer
        if (thisKey >= bufMinKey) {
            return (searchBuffer(state, outputBuffer, key, data));
        }
    }

#if SEARCH_METHOD == 0
    /* Perform a modified binary search that uses info on key location sequence for first placement. */

    // Guess logical page id
    uint32_t pageId;
    if (state->compareKey(key, (void *)&(state->minKey)) < 0) {
        pageId = state->minDataPageId;
    } else {
        pageId = (thisKey - state->minKey) / (state->maxRecordsPerPage * state->avgKeyDiff) + state->minDataPageId;

        if (pageId >= state->nextDataPageId)
            pageId = state->nextDataPageId - 1; /* Logical page would be beyond maximum. Set to last page. */
    }

    int32_t offset = 0;
    uint32_t first = state->minDataPageId, last = state->nextDataPageId - 1;
    while (1) {
        /* Read page into buffer */
        if (readPage(state, pageId % state->numDataPages) != 0)
            return -1;
        numReads++;

        if (first >= last)
            break;

        if (state->compareKey(key, embedDBGetMinKey(state, buf)) < 0) {
            /* Key is less than smallest record in block. */
            last = pageId - 1;
            uint64_t minKey = 0;
            memcpy(&minKey, embedDBGetMinKey(state, buf), state->keySize);
            offset = (thisKey - minKey) / (state->maxRecordsPerPage * state->avgKeyDiff) - 1;
            if (pageId + offset < first)
                offset = first - pageId;
            pageId += offset;

        } else if (state->compareKey(key, embedDBGetMaxKey(state, buf)) > 0) {
            /* Key is larger than largest record in block. */
            first = pageId + 1;
            uint64_t maxKey = 0;
            memcpy(&maxKey, embedDBGetMaxKey(state, buf), state->keySize);
            offset = (thisKey - maxKey) / (state->maxRecordsPerPage * state->avgKeyDiff) + 1;
            if (pageId + offset > last)
                offset = last - pageId;
            pageId += offset;
        } else {
            /* Found correct block */
            break;
        }
    }
#elif SEARCH_METHOD == 1
    /* Regular binary search */
    uint32_t first = state->minDataPageId, last = state->nextDataPageId - 1;
    uint32_t pageId = (first + last) / 2;
    while (1) {
        /* Read page into buffer */
        if (readPage(state, pageId % state->numDataPages) != 0)
            return -1;
        numReads++;

        if (first >= last)
            break;

        if (state->compareKey(key, embedDBGetMinKey(state, buf)) < 0) {
            /* Key is less than smallest record in block. */
            last = pageId - 1;
            pageId = (first + last) / 2;
        } else if (state->compareKey(key, embedDBGetMaxKey(state, buf)) > 0) {
            /* Key is larger than largest record in block. */
            first = pageId + 1;
            pageId = (first + last) / 2;
        } else {
            /* Found correct block */
            break;
        }
    }
#elif SEARCH_METHOD == 2
    /* Spline search */
    uint32_t location, lowbound, highbound;
    if (RADIX_BITS > 0) {
        radixsplineFind(state->rdix, key, state->compareKey, &location, &lowbound, &highbound);
    } else {
        splineFind(state->spl, key, state->compareKey, &location, &lowbound, &highbound);
    }

    // Check if the currently buffered page is the correct one
    if (!(lowbound <= state->bufferedPageId &&
          highbound >= state->bufferedPageId &&
          state->compareKey(embedDBGetMinKey(state, buf), key) <= 0 &&
          state->compareKey(embedDBGetMaxKey(state, buf), key) >= 0)) {
        if (linearSearch(state, &numReads, buf, key, location, lowbound, highbound) == -1) {
            return -1;
        }
    }

#endif
    id_t nextId = embedDBSearchNode(state, buf, key, 0);

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
 * 			Data is copied from database into data buffer.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @param	varData	Return variable for variable data as a embedDBVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	Return 0 if success. Non-zero value if error.
 * 			-1 : Error reading file or failed memory allocation
 * 			1  : Variable data was deleted to make room for newer data
 */
int8_t embedDBGetVar(embedDBState *state, void *key, void *data, embedDBVarDataStream **varData) {
    if (!EMBEDDB_USING_VDATA(state->parameters)) {
#ifdef PRINT_ERRORS
        printf("ERROR: embedDBGetVar called when not using variable data\n");
#endif
        return 0;
    }

    // get pointer for output buffer
    void *outputBuffer = (int8_t *)state->buffer;
    // search output buffer for recrd, mem copy fixed record into data
    int recordNum = searchBuffer(state, outputBuffer, key, data);
    // if there are records found in the output buffer
    if (recordNum != NO_RECORD_FOUND) {
        // flush variable record buffer to storage to read later on
        embedDBFlushVar(state);
        // copy contents of write buffer to read buffer for embedDBSetupVarDataStream()
        readToWriteBuf(state);
        // else if there are records in the file system, mem cpy fixed record into data
    } else if (embedDBGet(state, key, data) == RECORD_FOUND) {
        // get pointer from the read buffer
        void *buf = (int8_t *)state->buffer + (state->pageSize * EMBEDDB_DATA_READ_BUFFER);
        // retrieve offset
        recordNum = embedDBSearchNode(state, buf, key, 0);
    } else {
        return NO_RECORD_FOUND;
    }

    int8_t setupResult = embedDBSetupVarDataStream(state, key, varData, recordNum);

    switch (setupResult) {
        case 0:
            /* code */
            return 0;
        case 1:
            return 1;
        case 2:
        case 3:
            return -1;
    }

    return -1;
}

/**
 * @brief	Initialize iterator on embedDB structure.
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 */
void embedDBInitIterator(embedDBState *state, embedDBIterator *it) {
    /* Build query bitmap (if used) */
    it->queryBitmap = NULL;
    if (EMBEDDB_USING_BMAP(state->parameters)) {
        /* Verify that bitmap index is useful (must have set either min or max data value) */
        if (it->minData != NULL || it->maxData != NULL) {
            it->queryBitmap = calloc(1, state->bitmapSize);
            state->buildBitmapFromRange(it->minData, it->maxData, it->queryBitmap);
        }
    }

#ifdef PRINT_ERRORS
    if (!EMBEDDB_USING_BMAP(state->parameters)) {
        printf("WARN: Iterator not using index. If this is not intended, ensure that the embedDBState is using a bitmap and was initialized with an index file\n");
    } else if (!EMBEDDB_USING_INDEX(state->parameters)) {
        printf("WARN: Iterator not using index to full extent. If this is not intended, ensure that the embedDBState was initialized with an index file\n");
    }
#endif

    // Determine which data page should be the first examined if there is a min key
    if (it->minKey != NULL && SEARCH_METHOD == 2) {
        /* Spline search */
        uint32_t location, lowbound, highbound;
        if (RADIX_BITS > 0) {
            radixsplineFind(state->rdix, it->minKey, state->compareKey, &location, &lowbound, &highbound);
        } else {
            splineFind(state->spl, it->minKey, state->compareKey, &location, &lowbound, &highbound);
        }

        // Use the low bound as the start for our search
        it->nextDataPage = max(lowbound, state->minDataPageId);
    } else {
        it->nextDataPage = state->minDataPageId;
    }
    it->nextDataRec = 0;
}

/**
 * @brief	Close iterator after use.
 * @param	it		embedDB iterator structure
 */
void embedDBCloseIterator(embedDBIterator *it) {
    if (it->queryBitmap != NULL) {
        free(it->queryBitmap);
    }
}

/**
 * @brief   Flushes variable write buffer to storage and updates variable record pointer accordingly.
 * @param   state   algorithm state structure
 */
void embedDBFlushVar(embedDBState *state) {
    // only flush variable buffer
    writeVariablePage(state, (int8_t *)state->buffer + EMBEDDB_VAR_WRITE_BUFFER(state->parameters) * state->pageSize);
    state->fileInterface->flush(state->varFile);
    // init new buffer
    initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));
    // determine how many bytes are left
    int temp = state->pageSize - (state->currentVarLoc % state->pageSize);
    // create new offset
    state->currentVarLoc += temp + state->variableDataHeaderSize;
}

/**
 * @brief	Flushes output buffer.
 * @param	state	algorithm state structure
 */
int8_t embedDBFlush(embedDBState *state) {
    // As the first buffer is the data write buffer, no address change is required
    id_t pageNum = writePage(state, (int8_t *)state->buffer + EMBEDDB_DATA_WRITE_BUFFER * state->pageSize);
    state->fileInterface->flush(state->dataFile);

    indexPage(state, pageNum);

    if (EMBEDDB_USING_INDEX(state->parameters)) {
        void *buf = (int8_t *)state->buffer + state->pageSize * (EMBEDDB_INDEX_WRITE_BUFFER);
        count_t idxcount = EMBEDDB_GET_COUNT(buf);
        EMBEDDB_INC_COUNT(buf);

        /* Copy record onto index page */
        void *bm = EMBEDDB_GET_BITMAP(state->buffer);
        memcpy((void *)((int8_t *)buf + EMBEDDB_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);

        writeIndexPage(state, buf);
        state->fileInterface->flush(state->indexFile);

        /* Reinitialize buffer */
        initBufferPage(state, EMBEDDB_INDEX_WRITE_BUFFER);
    }

    /* Reinitialize buffer */
    initBufferPage(state, EMBEDDB_DATA_WRITE_BUFFER);

    // Flush var data page
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        // send write buffer pointer to write variable page
        writeVariablePage(state, (int8_t *)state->buffer + EMBEDDB_VAR_WRITE_BUFFER(state->parameters) * state->pageSize);
        state->fileInterface->flush(state->varFile);
        // init new buffer
        initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));
        // determine how many bytes are left
        int temp = state->pageSize - (state->currentVarLoc % state->pageSize);
        // create new offset
        state->currentVarLoc += temp + state->variableDataHeaderSize;
    }
    return 0;
}

/**
 * @brief	Iterates through a page in the read buffer.
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @return	ITERATE_MATCH if successful, ITERATE_NO_MORE_RECORDS if record is out of bounds, and ITERATE_NO_MATCH if record is not in page.
 */
int8_t iterateReadBuffer(embedDBState *state, embedDBIterator *it, void *key, void *data) {
    //  Keep reading record until we find one that matches the query
    int8_t *buf = (int8_t *)state->buffer + EMBEDDB_DATA_READ_BUFFER * state->pageSize;
    uint32_t pageRecordCount = EMBEDDB_GET_COUNT(buf);

    while (it->nextDataRec < pageRecordCount) {
        memcpy(key, buf + state->headerSize + it->nextDataRec * state->recordSize, state->keySize);
        memcpy(data, buf + state->headerSize + it->nextDataRec * state->recordSize + state->keySize, state->dataSize);
        it->nextDataRec++;
        // Check record
        if (it->minKey != NULL && state->compareKey(key, it->minKey) < 0)
            continue;
        if (it->maxKey != NULL && state->compareKey(key, it->maxKey) > 0)
            return ITERATE_NO_MORE_RECORDS;
        if (it->minData != NULL && state->compareData(data, it->minData) < 0)
            continue;
        if (it->maxData != NULL && state->compareData(data, it->maxData) > 0)
            continue;
        // If we make it here, the record matches the query
        return ITERATE_MATCH;
    }
    // If we make it here, no records in loaded page matches the query.
    return ITERATE_NO_MATCH;
}

/**
 * @brief	Return next key, data pair for iterator.
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @return	1 if successful, 0 if no more records
 */
int8_t embedDBNext(embedDBState *state, embedDBIterator *it, void *key, void *data) {
    while (1) {
        // return 0 since all pages including buffer has been read.
        if (it->nextDataPage > (state->nextDataPageId)) return 0;
        // if we have reached the end, read from output buffer if it is not empty
        if (it->nextDataPage == (state->nextDataPageId)) {
            // point to outputBuffer
            void *outputBuffer = (int8_t *)state->buffer;
            // if there are no records in the buffer, return
            if (EMBEDDB_GET_COUNT(outputBuffer) == 0) return 0;
            // else, place write buffer in read
            readToWriteBuf(state);
            // search read buffer
            int i = iterateReadBuffer(state, it, key, data);
            return (i != ITERATE_NO_MATCH) ? i : 0;
        }
        // If we are just starting to read a new page and we have a query bitmap
        if (it->nextDataRec == 0 && it->queryBitmap != NULL) {
            // Find what index page determines if we should read the data page
            uint32_t indexPage = it->nextDataPage / state->maxIdxRecordsPerPage;
            uint16_t indexRec = it->nextDataPage % state->maxIdxRecordsPerPage;

            if (state->indexFile != NULL && indexPage >= state->minIndexPageId && indexPage < state->nextIdxPageId) {
                // If the index page that contains this data page exists, else we must read the data page regardless cause we don't have the index saved for it

                if (readIndexPage(state, indexPage % state->numIndexPages) != 0) {
#ifdef PRINT_ERRORS
                    printf("ERROR: Failed to read index page %i (%i)\n", indexPage, indexPage % state->numIndexPages);
#endif
                    return 0;
                }

                // Get bitmap for data page in question
                void *indexBM = (int8_t *)state->buffer + EMBEDDB_INDEX_READ_BUFFER * state->pageSize + EMBEDDB_IDX_HEADER_SIZE + indexRec * state->bitmapSize;

                // Determine if we should read the data page
                if (!bitmapOverlap(it->queryBitmap, indexBM, state->bitmapSize)) {
                    // Do not read this data page, try the next one
                    it->nextDataPage++;
                    continue;
                }
            }
        }

        if (readPage(state, it->nextDataPage % state->numDataPages) != 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to read data page %i (%i)\n", it->nextDataPage, it->nextDataPage % state->numDataPages);
#endif
            return 0;
        }

        int8_t i = iterateReadBuffer(state, it, key, data);
        if (i != ITERATE_NO_MATCH) return i;
        // Finished reading through whole data page and didn't find a match
        it->nextDataPage++;
        it->nextDataRec = 0;
        // Try next data page by looping back to top
    }
}

/**
 * @brief	Return next key, data, variable data set for iterator
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @param	varData	Return variable for variable data as a embedDBVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	1 if successful, 0 if no more records
 */
int8_t embedDBNextVar(embedDBState *state, embedDBIterator *it, void *key, void *data, embedDBVarDataStream **varData) {
    if (!EMBEDDB_USING_VDATA(state->parameters)) {
#ifdef PRINT_ERRORS
        printf("ERROR: embedDBNextVar called when not using variable data\n");
#endif
        return 0;
    }

    // ensure record exists
    int8_t r = embedDBNext(state, it, key, data);
    if (!r) {
        return 0;
    }

    void *outputBuffer = (int8_t *)state->buffer;
    if (it->nextDataPage == 0 && (EMBEDDB_GET_COUNT(outputBuffer) > 0)) {
        embedDBFlushVar(state);
    }

    // Get the vardata address from the record
    count_t recordNum = it->nextDataRec - 1;
    int8_t setupResult = embedDBSetupVarDataStream(state, key, varData, recordNum);
    switch (setupResult) {
        case 0:
        case 1:
            return 1;
        case 2:
        case 3:
            return 0;
    }

    return 0;
}

/**
 * @brief Setup varDataStream object to return the variable data for a record
 * @param	state	embedDB algorithm state structure
 * @param   key     Key for the record
 * @param   varData Return variable for variable data as a embedDBVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return  Returns 0 if sucessfull or no variable data for the record, 1 if the records variable data was overwritten, 2 if the page failed to read, and 3 if the memorey failed to allocate.
 */
int8_t embedDBSetupVarDataStream(embedDBState *state, void *key, embedDBVarDataStream **varData, id_t recordNumber) {
    // create pointer to read buffer
    void *dataBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;
    // create pointer for record inside read buffer
    void *record = (int8_t *)dataBuf + state->headerSize + recordNumber * state->recordSize;
    // create pointer for variable record which is an offset to approximate location
    uint32_t varDataAddr = 0;
    memcpy(&varDataAddr, (int8_t *)record + state->keySize + state->dataSize, sizeof(uint32_t));
    // No variable data for the record, return 0
    if (varDataAddr == EMBEDDB_NO_VAR_DATA) {
        *varData = NULL;
        return 0;
    }

    // Check if the variable data associated with this key has been overwritten due to file wrap around
    if (state->compareKey(key, &state->minVarRecordId) < 0) {
        *varData = NULL;
        return 1;
    }

    uint32_t pageNum = (varDataAddr / state->pageSize) % state->numVarPages;

    // Read in page
    if (readVariablePage(state, pageNum) != 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: embedDB failed to read variable page\n");
#endif
        return 2;
    }

    // Get length of variable data
    void *varBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);
    uint32_t pageOffset = varDataAddr % state->pageSize;
    uint32_t dataLen = 0;
    memcpy(&dataLen, (int8_t *)varBuf + pageOffset, sizeof(uint32_t));

    // Move var data address to the beginning of the data, past the data length
    varDataAddr = (varDataAddr + sizeof(uint32_t)) % (state->numVarPages * state->pageSize);

    // If we end up on the page boundary, we need to move past the header
    if (varDataAddr % state->pageSize == 0) {
        varDataAddr += state->variableDataHeaderSize;
        varDataAddr %= (state->numVarPages * state->pageSize);
    }

    // Create varDataStream
    embedDBVarDataStream *varDataStream = malloc(sizeof(embedDBVarDataStream));
    if (varDataStream == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to alloc memory for embedDBVarDataStream\n");
#endif
        return 3;
    }

    varDataStream->dataStart = varDataAddr;
    varDataStream->totalBytes = dataLen;
    varDataStream->bytesRead = 0;
    varDataStream->fileOffset = varDataAddr;

    *varData = varDataStream;
    return 0;
}

/**
 * @brief	Reads data from variable data stream into the given buffer.
 * @param	state	embedDB algorithm state structure
 * @param	stream	Variable data stream
 * @param	buffer	Buffer to read data into
 * @param	length	Number of bytes to read (Must be <= buffer size)
 * @return	Number of bytes read
 */
uint32_t embedDBVarDataStreamRead(embedDBState *state, embedDBVarDataStream *stream, void *buffer, uint32_t length) {
    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Cannot pass null buffer to embedDBVarDataStreamRead\n");
#endif
        return 0;
    }
    // Read in var page containing the data to read
    uint32_t pageNum = (stream->fileOffset / state->pageSize) % state->numVarPages;

    if (readVariablePage(state, pageNum) != 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: Couldn't read variable data page %d\n", pageNum);
#endif
        return 0;
    }

    // Keep reading in data until the buffer is full
    void *varDataBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);
    uint32_t amtRead = 0;
    while (amtRead < length && stream->bytesRead < stream->totalBytes) {
        uint16_t pageOffset = stream->fileOffset % state->pageSize;
        uint32_t amtToRead = min(stream->totalBytes - stream->bytesRead, min(state->pageSize - pageOffset, length - amtRead));
        memcpy((int8_t *)buffer + amtRead, (int8_t *)varDataBuf + pageOffset, amtToRead);
        amtRead += amtToRead;
        stream->bytesRead += amtToRead;
        stream->fileOffset += amtToRead;

        // If we need to keep reading, read the next page
        if (amtRead < length && stream->bytesRead < stream->totalBytes) {
            pageNum = (pageNum + 1) % state->numVarPages;
            if (readVariablePage(state, pageNum) != 0) {
#ifdef PRINT_ERRORS
                printf("ERROR: Couldn't read variable data page %d\n", pageNum);
#endif
                return 0;
            }
            // Skip past the header
            stream->fileOffset += state->variableDataHeaderSize;
        }
    }
    return amtRead;
}

/**
 * @brief	Prints statistics.
 * @param	state	embedDB state structure
 */
void embedDBPrintStats(embedDBState *state) {
    printf("Num reads: %d\n", state->numReads);
    printf("Buffer hits: %d\n", state->bufferHits);
    printf("Num writes: %d\n", state->numWrites);
    printf("Num index reads: %d\n", state->numIdxReads);
    printf("Num index writes: %d\n", state->numIdxWrites);
    printf("Max Error: %d\n", state->maxError);

    if (SEARCH_METHOD == 2) {
        if (RADIX_BITS > 0) {
            splinePrint(state->rdix->spl);
            radixsplinePrint(state->rdix);
        } else {
            splinePrint(state->spl);
        }
    }
}

/**
 * @brief	Writes page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Buffer for writing out page
 * @return	Return page number if success, -1 if error.
 */
id_t writePage(embedDBState *state, void *buffer) {
    if (state->dataFile == NULL)
        return -1;

    /* Always writes to next page number. Returned to user. */
    id_t pageNum = state->nextDataPageId++;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->numAvailDataPages <= 0) {
        // Erase pages to make space for new data
        state->numAvailDataPages += state->eraseSizeInPages;
        state->minDataPageId += state->eraseSizeInPages;
        if (state->cleanSpline)
            cleanSpline(state, &state->minKey);
        // Estimate the smallest key now. Could determine exactly by reading this page
        state->minKey += state->eraseSizeInPages * state->maxRecordsPerPage * state->avgKeyDiff;
    }

    /* Seek to page location in file */
    int32_t val = state->fileInterface->write(buffer, pageNum % state->numDataPages, state->pageSize, state->dataFile);
    if (val == 0) {
#ifdef PRINT_ERRORS
        printf("Failed to write data page: %i (%i)\n", pageNum, pageNum % state->numDataPages);
#endif
        return -1;
    }

    state->numAvailDataPages--;
    state->numWrites++;

    return pageNum;
}

/**
 * @brief	Calculates the number of spline points not in use by embedDB and deltes them
 * @param	state	embedDB algorithm state structure
 * @param	key 	The minimim key embedDB still needs points for
 * @return	Returns the number of points deleted
 */
uint32_t cleanSpline(embedDBState *state, void *key) {
    uint32_t numPointsErased = 0;
    void *currentPoint;
    for (size_t i = 0; i < state->spl->count; i++) {
        currentPoint = splinePointLocation(state->spl, i);
        int8_t compareResult = state->compareKey(currentPoint, key);
        if (compareResult < 0)
            numPointsErased++;
        else
            break;
    }
    if (state->spl->count - numPointsErased == 1)
        numPointsErased--;
    splineErase(state->spl, numPointsErased);
    return numPointsErased;
}

/**
 * @brief	Writes index page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Buffer to use for writing index page
 * @return	Return page number if success, -1 if error.
 */
id_t writeIndexPage(embedDBState *state, void *buffer) {
    if (state->indexFile == NULL)
        return -1;

    /* Always writes to next page number. Returned to user. */
    id_t pageNum = state->nextIdxPageId++;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->numAvailIndexPages <= 0) {
        // Erase index pages to make room for new page
        state->numAvailIndexPages += state->eraseSizeInPages;
        state->minIndexPageId += state->eraseSizeInPages;
    }

    /* Seek to page location in file */
    int32_t val = state->fileInterface->write(buffer, pageNum % state->numIndexPages, state->pageSize, state->indexFile);
    if (val == 0) {
#ifdef PRINT_ERRORS
        printf("Failed to write index page: %i (%i)\n", pageNum, pageNum % state->numIndexPages);
#endif
        return -1;
    }

    state->numAvailIndexPages--;
    state->numIdxWrites++;

    return pageNum;
}

/**
 * @brief	Writes variable data page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	buffer	Buffer to use to write page to storage
 * @return	Return page number if success, -1 if error.
 */
id_t writeVariablePage(embedDBState *state, void *buffer) {
    if (state->varFile == NULL) {
        return -1;
    }

    // Make sure the address being witten to wraps around
    id_t physicalPageId = state->nextVarPageId % state->numVarPages;

    // Erase data if needed
    if (state->numAvailVarPages <= 0) {
        state->numAvailVarPages += state->eraseSizeInPages;
        // Last page that is deleted
        id_t pageNum = (physicalPageId + state->eraseSizeInPages - 1) % state->numVarPages;

        // Read in that page so we can update which records we still have the data for
        if (readVariablePage(state, pageNum) != 0) {
            return -1;
        }
        void *buf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters) + sizeof(id_t);
        memcpy(&state->minVarRecordId, buf, state->keySize);
        state->minVarRecordId += 1;  // Add one because the result from the last line is a record that is erased
    }

    // Add logical page number to data page
    void *buf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_WRITE_BUFFER(state->parameters);
    memcpy(buf, &state->nextVarPageId, sizeof(id_t));

    // Write to file
    uint32_t val = state->fileInterface->write(buffer, physicalPageId, state->pageSize, state->varFile);
    if (val == 0) {
#ifndef PRINT
        printf("Failed to write vardata page: %i\n", state->nextVarPageId);
#endif
        return -1;
    }

    state->nextVarPageId++;
    state->numAvailVarPages--;
    state->numWrites++;

    return state->nextVarPageId - 1;
}

/**
 * @brief	Reads given page from storage.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readPage(embedDBState *state, id_t pageNum) {
    /* Check if page is currently in buffer */
    if (pageNum == state->bufferedPageId) {
        state->bufferHits++;
        return 0;
    }

    // point to write buffer
    void *buf = (int8_t *)state->buffer + state->pageSize;

    /* Page is not in buffer. Read from storage. */
    /* Read page into start of buffer 1 */
    if (0 == state->fileInterface->read(buf, pageNum, state->pageSize, state->dataFile))
        return -1;

    state->numReads++;
    state->bufferedPageId = pageNum;
    return 0;
}

/**
 * @brief	Memcopies write buffer to the read buffer.
 * @param	state	embedDB algorithm state structure
 */
void readToWriteBuf(embedDBState *state) {
    // point to read buffer
    void *readBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;
    // point to write buffer
    void *writeBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_WRITE_BUFFER;
    // copy write buffer to the read buffer.
    memcpy(readBuf, writeBuf, state->pageSize);
}

/**
 * @brief	Memcopies variable write buffer to the read buffer.
 * @param	state	embedDB algorithm state structure
 */
void readToWriteBufVar(embedDBState *state) {
    // point to read buffer
    void *readBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);
    // point to write buffer
    void *writeBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_WRITE_BUFFER(state->parameters);
    // copy write buffer to the read buffer.
    memcpy(readBuf, writeBuf, state->pageSize);
}

/**
 * @brief	Reads given index page from storage.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readIndexPage(embedDBState *state, id_t pageNum) {
    /* Check if page is currently in buffer */
    if (pageNum == state->bufferedIndexPageId) {
        state->bufferHits++;
        return 0;
    }

    void *buf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_READ_BUFFER;

    /* Page is not in buffer. Read from storage. */
    /* Read page into start of buffer */
    if (0 == state->fileInterface->read(buf, pageNum, state->pageSize, state->indexFile))
        return -1;

    state->numIdxReads++;
    state->bufferedIndexPageId = pageNum;
    return 0;
}

/**
 * @brief	Reads given variable data page from storage
 * @param 	state 	embedDB algorithm state structure
 * @param 	pageNum Page number to read
 * @return 	Return 0 if success, -1 if error
 */
int8_t readVariablePage(embedDBState *state, id_t pageNum) {
    // Check if page is currently in buffer
    if (pageNum == state->bufferedVarPage) {
        state->bufferHits++;
        return 0;
    }

    // Get buffer to read into
    void *buf = (int8_t *)state->buffer + EMBEDDB_VAR_READ_BUFFER(state->parameters) * state->pageSize;

    // Read in one page worth of data
    if (state->fileInterface->read(buf, pageNum, state->pageSize, state->varFile) == 0) {
        return -1;
    }

    // Track stats
    state->numReads++;
    state->bufferedVarPage = pageNum;
    return 0;
}

/**
 * @brief	Resets statistics.
 * @param	state	embedDB state structure
 */
void embedDBResetStats(embedDBState *state) {
    state->numReads = 0;
    state->numWrites = 0;
    state->bufferHits = 0;
    state->numIdxReads = 0;
    state->numIdxWrites = 0;
}

/**
 * @brief	Closes structure and frees any dynamic space.
 * @param	state	embedDB state structure
 */
void embedDBClose(embedDBState *state) {
    if (state->dataFile != NULL) {
        state->fileInterface->close(state->dataFile);
    }
    if (state->indexFile != NULL) {
        state->fileInterface->close(state->indexFile);
    }
    if (state->varFile != NULL) {
        state->fileInterface->close(state->varFile);
    }
    if (SEARCH_METHOD == 2) {  // Spline
        if (RADIX_BITS > 0) {
            radixsplineClose(state->rdix);
            free(state->rdix);
            state->rdix = NULL;
            // Spl already freed by radixsplineClose
            state->spl = NULL;
        } else {
            splineClose(state->spl);
            free(state->spl);
            state->spl = NULL;
        }
    }
}
