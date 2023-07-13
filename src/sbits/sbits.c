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

#include "sbits.h"

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

/**
 * Number of spline points to be allocated. This is a set amount and will not grow.
 * The amount you need will depend on how much your key rate varies and what maxSplineError is set during sbits initialization.
 */
#define ALLOCATED_SPLINE_POINTS 300

/* Helper Functions */
int8_t sbitsInitData(sbitsState *state);
int8_t sbitsInitDataFromFile(sbitsState *state);
int8_t sbitsInitIndex(sbitsState *state);
int8_t sbitsInitIndexFromFile(sbitsState *state);
int8_t sbitsInitVarData(sbitsState *state);
int8_t sbitsInitVarDataFromFile(sbitsState *state);
void updateAverageKeyDifference(sbitsState *state, void *buffer);
void sbitsInitSplineFromFile(sbitsState *state);
int32_t getMaxError(sbitsState *state, void *buffer);
void updateMaxiumError(sbitsState *state, void *buffer);

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
 * @param   indexMaxError   max error of indexing structure (spline)
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

    state->indexMaxError = indexMaxError;

    /* Calculate block header size */

    /* Header size depends on bitmap size: 6 + X bytes: 4 byte id, 2 for record count, X for bitmap. */
    state->headerSize = 6 + state->bitmapSize;
    if (SBITS_USING_MAX_MIN(state->parameters))
        state->headerSize += state->keySize * 2 + state->dataSize * 2;

    /* Flags to show that these values have not been initalized with actual data yet */
    state->minKey = UINT32_MAX;
    state->bufferedPageId = -1;
    state->bufferedIndexPageId = -1;
    state->bufferedVarPage = -1;

    /* Calculate number of records per page */
    state->maxRecordsPerPage = (state->pageSize - state->headerSize) / state->recordSize;
    printf("Header size: %d  Records per page: %d\n", state->headerSize, state->maxRecordsPerPage);

    /* Initialize max error to maximum records per page */
    state->maxError = state->maxRecordsPerPage;

    /* Allocate first page of buffer as output page */
    initBufferPage(state, 0);

    if (state->numDataPages < (SBITS_USING_INDEX(state->parameters) * 2 + 2) * state->eraseSizeInPages) {
        printf("ERROR: Number of pages allocated must be at least twice erase block size for SBITS and four times when using indexing. Memory pages: %d\n", state->numDataPages);
        return -1;
    }

    /* Initalize the spline or radix spline structure if either are to be used */
    if (SEARCH_METHOD == 2) {
        if (RADIX_BITS > 0) {
            initRadixSpline(state, ALLOCATED_SPLINE_POINTS, RADIX_BITS);
        } else {
            state->spl = malloc(sizeof(spline));
            splineInit(state->spl, ALLOCATED_SPLINE_POINTS, indexMaxError, state->keySize);
        }
    }

    /* Allocate file for data*/
    int8_t dataInitResult = 0;
    dataInitResult = sbitsInitData(state);

    if (dataInitResult != 0) {
        return dataInitResult;
    }

    /* Allocate file and buffer for index */
    int8_t indexInitResult = 0;
    if (SBITS_USING_INDEX(state->parameters)) {
        if (state->bufferSizeInBlocks < 4) {
            printf("ERROR: SBITS using index requires at least 4 page buffers.\n");
            return -1;
        } else {
            indexInitResult = sbitsInitIndex(state);
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
    if (SBITS_USING_VDATA(state->parameters)) {
        if (state->bufferSizeInBlocks < 4 + (SBITS_USING_INDEX(state->parameters) ? 2 : 0)) {
            printf("ERROR: SBITS using variable records requires at least 4 page buffers if there is no index and 6 if there is.\n");
            return -1;
        } else {
            varDataInitResult = sbitsInitVarData(state);
        }
        return varDataInitResult;
    } else {
        state->varFile = NULL;
        state->numVarPages = 0;
    }

    resetStats(state);
    return 0;
}

int8_t sbitsInitData(sbitsState *state) {
    state->nextDataPageId = 0;
    state->avgKeyDiff = 1;
    state->nextDataPageId = 0;
    state->numAvailDataPages = state->numDataPages;
    state->minDataPageId = 0;

    if (state->dataFile == NULL) {
        printf("ERROR: No data file provided!\n");
        return -1;
    }

    /* Setup data file. */
    if (!SBITS_RESETING_DATA(state->parameters)) {
        int8_t openStatus = state->fileInterface->open(state->dataFile, SBITS_FILE_MODE_R_PLUS_B);
        if (openStatus) {
            return sbitsInitDataFromFile(state);
        }
        printf("No existing data file found. Attempting to initialize a new one.\n");
    }

    int8_t openStatus = state->fileInterface->open(state->dataFile, SBITS_FILE_MODE_W_PLUS_B);
    if (!openStatus) {
        printf("Error: Can't open data file!\n");
        return -1;
    }

    return 0;
}

int8_t sbitsInitDataFromFile(sbitsState *state) {
    printf("Attempt to initialize from existing data file\n");
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
        memcpy(&minKey, sbitsGetMinKey(state, buffer), state->keySize);
        state->minKey = minKey;
    } else {
        uint64_t minKey = 0;
        memcpy(&minKey, sbitsGetMinKey(state, buffer), state->keySize);
        state->minKey = minKey;
    }

    /* Put largest key back into the buffer */
    readPage(state, state->nextDataPageId - 1);

    updateAverageKeyDifference(state, buffer);
    if (SEARCH_METHOD == 2) {
        sbitsInitSplineFromFile(state);
    }

    return 0;
}

void sbitsInitSplineFromFile(sbitsState *state) {
    id_t pageNumberToRead = state->minDataPageId;
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_DATA_READ_BUFFER;
    id_t pagesRead = 0;
    id_t numberOfPagesToRead = state->nextDataPageId - state->minDataPageId;
    while (pagesRead < numberOfPagesToRead) {
        readPage(state, pageNumberToRead % state->numDataPages);
        if (RADIX_BITS > 0) {
            radixsplineAddPoint(state->rdix, sbitsGetMinKey(state, buffer), pageNumberToRead++);
        } else {
            splineAdd(state->spl, sbitsGetMinKey(state, buffer), pageNumberToRead++);
        }
        pagesRead++;
    }
}

int8_t sbitsInitIndex(sbitsState *state) {
    /* Setup index file. */

    /* 4 for id, 2 for count, 2 unused, 4 for minKey (pageId), 4 for maxKey (pageId) */
    state->maxIdxRecordsPerPage = (state->pageSize - 16) / state->bitmapSize;

    /* Allocate third page of buffer as index output page */
    initBufferPage(state, SBITS_INDEX_WRITE_BUFFER);

    /* Add page id to minimum value spot in page */
    void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_INDEX_WRITE_BUFFER);
    id_t *ptr = ((id_t *)((int8_t *)buf + 8));
    *ptr = state->nextDataPageId;

    state->nextIdxPageId = 0;
    state->numAvailIndexPages = state->numIndexPages;
    state->minIndexPageId = 0;

    if (state->numIndexPages < state->eraseSizeInPages * 2) {
        printf("ERROR: Minimum index space is two erase blocks\n");
        return -1;
    }

    if (state->numIndexPages % state->eraseSizeInPages != 0) {
        printf("ERROR: Ensure index space is a multiple of erase block size\n");
        return -1;
    }

    if (state->indexFile == NULL) {
        printf("ERROR: No index file provided!\n");
        return -1;
    }

    if (!SBITS_RESETING_DATA(state->parameters)) {
        int8_t openStatus = state->fileInterface->open(state->indexFile, SBITS_FILE_MODE_R_PLUS_B);
        if (openStatus) {
            return sbitsInitIndexFromFile(state);
        }
        printf("Unable to open index file. Attempting to initialize a new one.\n");
    }

    int8_t openStatus = state->fileInterface->open(state->indexFile, SBITS_FILE_MODE_W_PLUS_B);
    if (!openStatus) {
        printf("Error: Can't open index file!\n");
        return -1;
    }

    return 0;
}

int8_t sbitsInitIndexFromFile(sbitsState *state) {
    printf("Attempting to initialize from existing index file\n");

    id_t logicalIndexPageId = 0;
    id_t maxLogicaIndexPageId = 0;
    id_t physicalIndexPageId = 0;

    /* This will become zero if there is no more to read */
    int8_t moreToRead = !(readIndexPage(state, physicalIndexPageId));

    bool haveWrappedInMemory = false;
    int count = 0;
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_INDEX_READ_BUFFER;

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

int8_t sbitsInitVarData(sbitsState *state) {
    // Initialize variable data outpt buffer
    initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

    state->variableDataHeaderSize = state->keySize + sizeof(id_t);
    state->currentVarLoc = state->variableDataHeaderSize;
    state->minVarRecordId = 0;
    state->numAvailVarPages = state->numVarPages;
    state->nextVarPageId = 0;

    if (!SBITS_RESETING_DATA(state->parameters)) {
        int8_t openResult = state->fileInterface->open(state->varFile, SBITS_FILE_MODE_R_PLUS_B);
        if (openResult) {
            return sbitsInitVarDataFromFile(state);
        }
        printf("Unable to open variable data file. Attempting to initialize a new one.\n");
    }

    int8_t openResult = state->fileInterface->open(state->varFile, SBITS_FILE_MODE_W_PLUS_B);
    if (!openResult) {
        printf("Error: Can't open variable data file!\n");
        return -1;
    }

    printf("Variable data pages: %d\n", state->numVarPages);
    return 0;
}

int8_t sbitsInitVarDataFromFile(sbitsState *state) {
    printf("Attempting to initialize from existing variable data file.\n");
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
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
        float slope = sbitsCalculateSlope(state, state->buffer);  // this is incorrect, should be buffer. TODO: fix

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
void indexPage(sbitsState *state, uint32_t pageNumber) {
    if (SEARCH_METHOD == 2) {
        if (RADIX_BITS > 0) {
            radixsplineAddPoint(state->rdix, sbitsGetMinKey(state, state->buffer), pageNumber);
        } else {
            splineAdd(state->spl, sbitsGetMinKey(state, state->buffer), pageNumber);
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
    if (state->minKey != UINT32_MAX) {
        void *previousKey = NULL;
        if (count == 0) {
            readPage(state, (state->nextDataPageId - 1) % state->numDataPages);
            previousKey = ((int8_t *)state->buffer + state->pageSize * SBITS_DATA_READ_BUFFER) +
                          (state->recordSize * (state->maxRecordsPerPage - 1)) + state->headerSize;
        } else {
            previousKey = (int8_t *)state->buffer + (state->recordSize * (count - 1)) + state->headerSize;
        }
        if (state->compareKey(key, previousKey) != 1) {
            printf("Keys must be scritcly ascending order. Insert Failed.\n");
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

        updateAverageKeyDifference(state, state->buffer);
        updateMaxiumError(state, state->buffer);

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
    if (state->minKey == UINT32_MAX)
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

void updateMaxiumError(sbitsState *state, void *buffer) {
    // Calculate error within the page
    int32_t maxError = getMaxError(state, buffer);
    if (state->maxError < maxError) {
        state->maxError = maxError;
    }
}

void updateAverageKeyDifference(sbitsState *state, void *buffer) {
    /* Update estimate of average key difference. */
    int32_t numBlocks = state->nextDataPageId;
    if (numBlocks == 0)
        numBlocks = 1;

    if (state->keySize <= 4) {
        uint32_t maxKey = 0;
        memcpy(&maxKey, sbitsGetMaxKey(state, buffer), state->keySize);
        state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
    } else {
        uint64_t maxKey = 0;
        memcpy(&maxKey, sbitsGetMaxKey(state, buffer), state->keySize);
        state->avgKeyDiff = (maxKey - state->minKey) / numBlocks / state->maxRecordsPerPage;
    }
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
    if (!SBITS_USING_VDATA(state->parameters)) {
        printf("Error: Can't insert variable data because it is not enabled\n");
        return -1;
    }

    // Insert their data

    /*
     * Check that there is enough space remaining in this page to start the insert of the variable
     * data here and if the data page will be written in sbitsGet
     */
    void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_VAR_WRITE_BUFFER(state->parameters));
    if (state->currentVarLoc % state->pageSize > state->pageSize - 4 || SBITS_GET_COUNT(state->buffer) >= state->maxRecordsPerPage) {
        writeVariablePage(state, buf);
        initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));
        // Move data writing location to the beginning of the next page, leaving the room for the header
        state->currentVarLoc += state->pageSize - state->currentVarLoc % state->pageSize + state->variableDataHeaderSize;
    }

    if (variableData == NULL) {
        // Var data enabled, but not provided
        state->recordHasVarData = 0;
        return sbitsPut(state, key, data);
    }

    // Perform the regular insert
    state->recordHasVarData = 1;
    int8_t r;
    if ((r = sbitsPut(state, key, data)) != 0) {
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
        initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

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
            initBufferPage(state, SBITS_VAR_WRITE_BUFFER(state->parameters));

            // Update the header to include the maximum key value stored on this page and account for page number
            memcpy((int8_t *)buf + sizeof(id_t), key, state->keySize);
            state->currentVarLoc += state->variableDataHeaderSize;
        }
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
 * @param	range	1 if range query so return pointer to first record <= key, 0 if exact query so much return first exact match record
 */
id_t sbitsSearchNode(sbitsState *state, void *buffer, void *key, int8_t range) {
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
    if (state->nextDataPageId == 0) {
        printf("ERROR: No data in database.\n");
        return -1;
    }

    void *buf = (int8_t *)state->buffer + state->pageSize;
    int16_t numReads = 0;

    uint64_t thisKey = 0;
    memcpy(&thisKey, key, state->keySize);

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

        if (state->compareKey(key, sbitsGetMinKey(state, buf)) < 0) {
            /* Key is less than smallest record in block. */
            last = pageId - 1;
            uint64_t minKey = 0;
            memcpy(&minKey, sbitsGetMinKey(state, buf), state->keySize);
            offset = (thisKey - minKey) / (state->maxRecordsPerPage * state->avgKeyDiff) - 1;
            if (pageId + offset < first)
                offset = first - pageId;
            pageId += offset;

        } else if (state->compareKey(key, sbitsGetMaxKey(state, buf)) > 0) {
            /* Key is larger than largest record in block. */
            first = pageId + 1;
            uint64_t maxKey = 0;
            memcpy(&maxKey, sbitsGetMaxKey(state, buf), state->keySize);
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

        if (state->compareKey(key, sbitsGetMinKey(state, buf)) < 0) {
            /* Key is less than smallest record in block. */
            last = pageId - 1;
            pageId = (first + last) / 2;
        } else if (state->compareKey(key, sbitsGetMaxKey(state, buf)) > 0) {
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
          state->compareKey(sbitsGetMinKey(state, buf), key) <= 0 &&
          state->compareKey(sbitsGetMaxKey(state, buf), key) >= 0)) {
        if (linearSearch(state, &numReads, buf, key, location, lowbound, highbound) == -1) {
            return -1;
        }
    }

#endif
    id_t nextId = sbitsSearchNode(state, buf, key, 0);

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
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @param	varData	Return variable for variable data as a sbitsVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	Return 0 if success. Non-zero value if error.
 * 			-1 : Error reading file
 * 			1  : Variable data was deleted to make room for newer data
 */
int8_t sbitsGetVar(sbitsState *state, void *key, void *data, sbitsVarDataStream **varData) {
    // Get the fixed data
    int8_t r = sbitsGet(state, key, data);
    if (r != 0) {
        return r;
    }

    // Now the input buffer contains the record, so we can use that to find the variable data
    void *buf = (int8_t *)state->buffer + state->pageSize;
    id_t recordNum = sbitsSearchNode(state, buf, key, 0);

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
        printf("No data to read\n");
        return -1;
    }

    // Get pointer to the beginning of the data
    uint16_t bufPos = varDataOffset % state->pageSize;
    // Get length of data and move to the data portion of the record
    uint32_t dataLength;
    memcpy(&dataLength, (int8_t *)ptr + bufPos, sizeof(uint32_t));

    // Move var data address to the beginning of the data, past the data length
    varDataOffset = (varDataOffset + sizeof(uint32_t)) % (state->numVarPages * state->pageSize);

    // Create varDataStream
    sbitsVarDataStream *varDataStream = malloc(sizeof(sbitsVarDataStream));
    if (varDataStream == NULL) {
        printf("ERROR: Failed to alloc memory for sbitsVarDataStream\n");
        return 0;
    }

    varDataStream->dataStart = varDataOffset;
    varDataStream->totalBytes = dataLength;
    varDataStream->bytesRead = 0;
    varDataStream->fileOffset = varDataOffset;

    *varData = varDataStream;

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
    if (SBITS_USING_BMAP(state->parameters)) {
        /* Verify that bitmap index is useful (must have set either min or max data value) */
        if (it->minData != NULL || it->maxData != NULL) {
            it->queryBitmap = calloc(1, state->bitmapSize);
            state->buildBitmapFromRange(it->minData, it->maxData, it->queryBitmap);
        }
    }

    if (!SBITS_USING_BMAP(state->parameters)) {
        printf("WARN: Iterator not using index. If this is not intended, ensure that the sbitsState is using a bitmap and was initialized with an index file\n");
    } else if (!SBITS_USING_INDEX(state->parameters)) {
        printf("WARN: Iterator not using index to full extent. If this is not intended, ensure that the sbitsState was initialized with an index file\n");
    }

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
 * @param	it		SBITS iterator structure
 */
void sbitsCloseIterator(sbitsIterator *it) {
    if (it->queryBitmap != NULL) {
        free(it->queryBitmap);
    }
}

/**
 * @brief	Flushes output buffer.
 * @param	state	algorithm state structure
 */
int8_t sbitsFlush(sbitsState *state) {
    // As the first buffer is the data write buffer, no address change is required
    id_t pageNum = writePage(state, (int8_t *)state->buffer + SBITS_DATA_WRITE_BUFFER * state->pageSize);
    state->fileInterface->flush(state->dataFile);

    indexPage(state, pageNum);

    if (SBITS_USING_INDEX(state->parameters)) {
        void *buf = (int8_t *)state->buffer + state->pageSize * (SBITS_INDEX_WRITE_BUFFER);
        count_t idxcount = SBITS_GET_COUNT(buf);
        SBITS_INC_COUNT(buf);

        /* Copy record onto index page */
        void *bm = SBITS_GET_BITMAP(state->buffer);
        memcpy((void *)((int8_t *)buf + SBITS_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);

        writeIndexPage(state, buf);
        state->fileInterface->flush(state->indexFile);

        /* Reinitialize buffer */
        initBufferPage(state, SBITS_INDEX_WRITE_BUFFER);
    }

    /* Reinitialize buffer */
    initBufferPage(state, SBITS_DATA_WRITE_BUFFER);

    // Flush var data page
    if (SBITS_USING_VDATA(state->parameters)) {
        writeVariablePage(state, (int8_t *)state->buffer + SBITS_VAR_WRITE_BUFFER(state->parameters) * state->pageSize);
        state->fileInterface->flush(state->varFile);
    }
    return 0;
}

/**
 * @brief	Return next key, data pair for iterator.
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @return	1 if successful, 0 if no more records
 */
int8_t sbitsNext(sbitsState *state, sbitsIterator *it, void *key, void *data) {
    while (1) {
        if (it->nextDataPage >= state->nextDataPageId) {
            return 0;
        }

        // If we are just starting to read a new page and we have a query bitmap
        if (it->nextDataRec == 0 && it->queryBitmap != NULL) {
            // Find what index page determines if we should read the data page
            uint32_t indexPage = it->nextDataPage / state->maxIdxRecordsPerPage;
            uint16_t indexRec = it->nextDataPage % state->maxIdxRecordsPerPage;

            if (state->indexFile != NULL && indexPage >= state->minIndexPageId && indexPage < state->nextIdxPageId) {
                // If the index page that contains this data page exists, else we must read the data page regardless cause we don't have the index saved for it

                if (readIndexPage(state, indexPage % state->numIndexPages) != 0) {
                    printf("ERROR: Failed to read index page %lu (%lu)\n", indexPage, indexPage % state->numIndexPages);
                    return 0;
                }

                // Get bitmap for data page in question
                void *indexBM = (int8_t *)state->buffer + SBITS_INDEX_READ_BUFFER * state->pageSize + SBITS_IDX_HEADER_SIZE + indexRec * state->bitmapSize;

                // Determine if we should read the data page
                if (!bitmapOverlap(it->queryBitmap, indexBM, state->bitmapSize)) {
                    // Do not read this data page, try the next one
                    it->nextDataPage++;
                    continue;
                }
            }
        }

        if (readPage(state, it->nextDataPage % state->numDataPages) != 0) {
            printf("ERROR: Failed to read data page %lu (%lu)\n", it->nextDataPage, it->nextDataPage % state->numDataPages);
            return 0;
        }

        // Keep reading record until we find one that matches the query
        int8_t *buf = (int8_t *)state->buffer + SBITS_DATA_READ_BUFFER * state->pageSize;
        uint32_t pageRecordCount = SBITS_GET_COUNT(buf);
        while (it->nextDataRec < pageRecordCount) {
            // Get record
            memcpy(key, buf + state->headerSize + it->nextDataRec * state->recordSize, state->keySize);
            memcpy(data, buf + state->headerSize + it->nextDataRec * state->recordSize + state->keySize, state->dataSize);
            it->nextDataRec++;

            // Check record
            if (it->minKey != NULL && state->compareKey(key, it->minKey) < 0)
                continue;
            if (it->maxKey != NULL && state->compareKey(key, it->maxKey) > 0)
                return 0;
            if (it->minData != NULL && state->compareData(data, it->minData) < 0)
                continue;
            if (it->maxData != NULL && state->compareData(data, it->maxData) > 0)
                continue;

            // If we make it here, the record matches the query
            return 1;
        }

        // Finished reading through whole data page and didn't find a match
        it->nextDataPage++;
        it->nextDataRec = 0;

        // Try next data page by looping back to top
    }
}

/**
 * @brief	Return next key, data, variable data set for iterator
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @param	varData	Return variable for variable data as a sbitsVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	1 if successful, 0 if no more records
 */
int8_t sbitsNextVar(sbitsState *state, sbitsIterator *it, void *key, void *data, sbitsVarDataStream **varData) {
    if (SBITS_USING_VDATA(state->parameters)) {
        int8_t r = sbitsNext(state, it, key, data);
        if (!r) {
            return 0;
        }

        // Get the vardata address from the record
        count_t recordNum = it->nextDataRec - 1;
        void *dataBuf = (int8_t *)state->buffer + state->pageSize * SBITS_DATA_READ_BUFFER;
        void *record = (int8_t *)dataBuf + state->headerSize + recordNum * state->recordSize;
        uint32_t varDataAddr = 0;
        memcpy(&varDataAddr, (int8_t *)record + state->keySize + state->dataSize, sizeof(uint32_t));

        if (varDataAddr == SBITS_NO_VAR_DATA) {
            *varData = NULL;
            return 1;
        }

        uint32_t pageNum = (varDataAddr / state->pageSize) % state->numVarPages;
        uint32_t pageOffset = varDataAddr % state->pageSize;

        // Read in page
        if (readVariablePage(state, pageNum) != 0) {
            printf("ERROR: sbitsNextVar failed to read variable page\n");
            return 0;
        }

        // Get length of variable data
        void *varBuf = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
        uint32_t dataLen = 0;
        memcpy(&dataLen, (int8_t *)varBuf + pageOffset, sizeof(uint32_t));

        // Move var data address to the beginning of the data, past the data length
        varDataAddr = (varDataAddr + sizeof(uint32_t)) % (state->numVarPages * state->pageSize);

        // Create varDataStream
        sbitsVarDataStream *varDataStream = malloc(sizeof(sbitsVarDataStream));
        if (varDataStream == NULL) {
            printf("ERROR: Failed to alloc memory for sbitsVarDataStream\n");
            return 0;
        }

        varDataStream->dataStart = varDataAddr;
        varDataStream->totalBytes = dataLen;
        varDataStream->bytesRead = 0;
        varDataStream->fileOffset = varDataAddr;

        *varData = varDataStream;

        return 1;
    } else {
        printf("ERROR: sbitsNextVar called when not using variable data\n");
        return 0;
    }
}

/**
 * @brief	Reads data from variable data stream into the given buffer.
 * @param	state	SBITS algorithm state structure
 * @param	stream	Variable data stream
 * @param	buffer	Buffer to read data into
 * @param	length	Number of bytes to read (Must be <= buffer size)
 * @return	Number of bytes read
 */
uint32_t sbitsVarDataStreamRead(sbitsState *state, sbitsVarDataStream *stream, void *buffer, uint32_t length) {
    if (buffer == NULL) {
        printf("ERROR: Cannot pass null buffer to sbitsVarDataStreamRead\n");
        return 0;
    }

    // Read in var page containing the data to read
    uint32_t pageNum = (stream->fileOffset / state->pageSize) % state->numVarPages;
    if (readVariablePage(state, pageNum) != 0) {
        printf("ERROR: Couldn't read variable data page %d\n", pageNum);
        return 0;
    }

    // Keep reading in data until the buffer is full
    void *varDataBuf = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
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
                printf("ERROR: Couldn't read variable data page %d\n", pageNum);
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
 * @param	state	SBITS algorithm state structure
 * @param	buffer	Buffer for writing out page
 * @return	Return page number if success, -1 if error.
 */
id_t writePage(sbitsState *state, void *buffer) {
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

        // Estimate the smallest key now. Could determine exactly by reading this page
        state->minKey += state->eraseSizeInPages * state->maxRecordsPerPage * state->avgKeyDiff;
    }

    /* Seek to page location in file */
    int32_t val = state->fileInterface->write(buffer, pageNum % state->numDataPages, state->pageSize, state->dataFile);
    if (val == 0) {
        printf("Failed to write data page: %lu (%lu)\n", pageNum, pageNum % state->numDataPages);
        return -1;
    }

    state->numAvailDataPages--;
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
        printf("Failed to write index page: %lu (%lu)\n", pageNum, pageNum % state->numIndexPages);
        return -1;
    }

    state->numAvailIndexPages--;
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
        void *buf = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters) + sizeof(id_t);
        memcpy(&state->minVarRecordId, buf, state->keySize);
        state->minVarRecordId += 1;  // Add one because the result from the last line is a record that is erased
    }

    // Add logical page number to data page
    void *buf = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_WRITE_BUFFER(state->parameters);
    memcpy(buf, &state->nextVarPageId, sizeof(id_t));

    // Write to file
    uint32_t val = state->fileInterface->write(buffer, physicalPageId, state->pageSize, state->varFile);
    if (val == 0) {
        printf("Failed to write vardata page: %lu\n", state->nextVarPageId);
        return -1;
    }

    state->nextVarPageId++;
    state->numAvailVarPages--;
    state->numWrites++;

    return state->nextVarPageId - 1;
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

    /* Page is not in buffer. Read from storage. */
    /* Read page into start of buffer 1 */
    if (0 == state->fileInterface->read(buf, pageNum, state->pageSize, state->dataFile))
        return -1;

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
 * @param 	state 	SBITS algorithm state structure
 * @param 	pageNum Page number to read
 * @return 	Return 0 if success, -1 if error
 */
int8_t readVariablePage(sbitsState *state, id_t pageNum) {
    // Check if page is currently in buffer
    if (pageNum == state->bufferedVarPage) {
        state->bufferHits++;
        return 0;
    }

    // Get buffer to read into
    void *buf = (int8_t *)state->buffer + SBITS_VAR_READ_BUFFER(state->parameters) * state->pageSize;

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
