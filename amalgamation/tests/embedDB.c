#include "./EmbedDB.h"
/************************************************************embedDB.c************************************************************/
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

/************************************************************schema.c************************************************************/
/******************************************************************************/
/**
 * @file        schema.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the schema for EmbedDB query interface
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

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness) {
    embedDBSchema* schema = malloc(sizeof(embedDBSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < numCols; i++) {
        int8_t sign = colSignedness[i];
        uint8_t colSize = colSizes[i];
        totalSize += colSize;
        if (colSize <= 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Column size must be greater than zero\n");
#endif
            return NULL;
        }
        if (sign == embedDB_COLUMN_SIGNED) {
            schema->columnSizes[i] = -colSizes[i];
        } else if (sign == embedDB_COLUMN_UNSIGNED) {
            schema->columnSizes[i] = colSizes[i];
        } else {
#ifdef PRINT_ERRORS
            printf("ERROR: Must only use embedDB_COLUMN_SIGNED or embedDB_COLUMN_UNSIGNED to describe column signedness\n");
#endif
            return NULL;
        }
    }

    return schema;
}

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema) {
    embedDBSchema* copy = malloc(sizeof(embedDBSchema));
    if (copy == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    if (copy->columnSizes == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema) {
    uint16_t size = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        size += abs(schema->columnSizes[i]);
    }
    return size;
}

void printSchema(embedDBSchema* schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", embedDB_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
}

/************************************************************only-include-duplicate.c************************************************************/
              
/************************************************************embedDB.c************************************************************/
 
/************************************************************utilityFunctions.c************************************************************/
/******************************************************************************/
/**
 * @file        utilityFunctions.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       This file contains some utility functions to be used with embedDB.
 *              These include functions required to use the bitmap option, and a
 *              comparator for comparing keys. They can be modified or implemented
 *              differently depending on the application.
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

embedDBState *defaultInitializedState() {
    embedDBState *state = calloc(1, sizeof(embedDBState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("Failed to allocate memory for state.\n");
#endif
        return NULL;
    }

    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

    /* Address level parameters */
    state->numDataPages = 20000;  // Enough for 620,000 records
    state->numIndexPages = 44;    // Enough for 676,544 records
    state->eraseSizeInPages = 4;

    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Initialize embedDB structure */
    if (embedDBInit(state, 1) != 0) {
#ifdef PRINT_ERRORS
        printf("Initialization error.\n");
#endif
        free(state->buffer);
        free(state->fileInterface);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state);
        return NULL;
    }

    return state;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and max value. */
void buildBitmapInt8FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

int8_t inBitmapInt8(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt16(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt16(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint64_t *)bm = UINT64_MAX; /* Everything */
        return;
    } else {
        uint64_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt64(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint64_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt64(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint64_t *)bm = maxMap;
                return;
            }
        }
        *(uint64_t *)bm = minMap & maxMap;
    }
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

int8_t int64Comparator(void *a, void *b) {
    int64_t result = *((int64_t *)a) - *((int64_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

typedef struct {
    char *filename;
    FILE *file;
} FILE_INFO;

void *setupFile(char *filename) {
    FILE_INFO *fileInfo = malloc(sizeof(FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->file = NULL;
    return fileInfo;
}

void tearDownFile(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo->file != NULL)
        fclose(fileInfo->file);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageSize * pageNum, SEEK_SET);
    return fread(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageNum * pageSize, SEEK_SET);
    return fwrite(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_CLOSE(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fclose(fileInfo->file);
    fileInfo->file = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    return fflush(fileInfo->file) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "w+b");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "r+b");
    } else {
        return 0;
    }

    if (fileInfo->file == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getFileInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}

/************************************************************only-include-inline-comments.c************************************************************/
     // foo  // a + b?   /* I find your lack of faith disturbing*/    // I have a bad feeling about this   // /* Stay on target. */   // alsdfkjsdlf     //

/************************************************************embedDB.c************************************************************/
 
/************************************************************only-include.c************************************************************/
       


/************************************************************advancedQueries.c************************************************************/
/******************************************************************************/
/**
 * @file        advancedQueries.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the advanced query interface for EmbedDB
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

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    // Check the sign bits of the most significant bytes
    int sign1 = bytes1[numBytes - 1] & 0x80;
    int sign2 = bytes2[numBytes - 1] & 0x80;

    if (sign1 != sign2) {
        // Different signs, negative number is smaller
        return (sign1 ? -1 : 1);
    }

    // Same sign, perform regular byte-wise comparison
    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	0 or 1 to indicate if inequality is true
 */
int8_t compare(void* a, uint8_t operation, void* b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void* num1, const void* num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
    switch (operation) {
        case SELECT_GT:
            return compFunc(a, b, numBytes) > 0;
        case SELECT_LT:
            return compFunc(a, b, numBytes) < 0;
        case SELECT_GTE:
            return compFunc(a, b, numBytes) >= 0;
        case SELECT_LTE:
            return compFunc(a, b, numBytes) <= 0;
        case SELECT_EQ:
            return compFunc(a, b, numBytes) == 0;
        case SELECT_NEQ:
            return compFunc(a, b, numBytes) != 0;
        default:
            return 0;
    }
}

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator) {
    return operator->next(operator);
}

void initTableScan(embedDBOperator* operator) {
    if (operator->input != NULL) {
#ifdef PRINT_ERRORS
        printf("WARNING: TableScan operator should not have an input operator\n");
#endif
    }
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: TableScan operator needs its schema defined\n");
#endif
        return;
    }

    if (operator->schema->numCols<2) {
#ifdef PRINT_ERRORS
        printf("ERROR: When creating a table scan, you must include at least two columns: one for the key and one for the data from the iterator\n");
#endif
        return;
    }

    // Check that the provided key schema matches what is in the state
    embedDBState* embedDBstate = (embedDBState*)(((void**)operator->state)[0]);
    if (operator->schema->columnSizes[0] <= 0 || abs(operator->schema->columnSizes[0]) != embedDBstate->keySize) {
#ifdef PRINT_ERRORS
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
#endif
        return;
    }
    if (getRecordSizeFromSchema(operator->schema) != (embedDBstate->keySize + embedDBstate->dataSize)) {
#ifdef PRINT_ERRORS
        printf("ERROR: Size of provided schema doesn't match the size that will be returned by the provided iterator\n");
#endif
        return;
    }

    // Init buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextTableScan(embedDBOperator* operator) {
    // Check that a schema was set
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Must provide a base schema for a table scan operator\n");
#endif
        return 0;
    }

    // Get next record
    embedDBState* state = (embedDBState*)(((void**)operator->state)[0]);
    embedDBIterator* it = (embedDBIterator*)(((void**)operator->state)[1]);
    if (!embedDBNext(state, it, operator->recordBuffer, (int8_t*)operator->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(embedDBOperator* operator) {
    embedDBFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
    free(operator->state);
    operator->state = NULL;
}

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: All parameters must be provided to create a TableScan operator\n");
#endif
        return NULL;
    }

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }

    operator->state = malloc(2 * sizeof(void*));
    if (operator->state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }
    memcpy(operator->state, &state, sizeof(void*));
    memcpy((int8_t*)operator->state + sizeof(void*), &it, sizeof(void*));

    operator->schema = copySchema(baseSchema);
    operator->input = NULL;
    operator->recordBuffer = NULL;

    operator->init = initTableScan;
    operator->next = nextTableScan;
    operator->close = closeTableScan;

    return operator;
}

void initProjection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Get state
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection schema\n");
#endif
            return;
        }
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection while building schema\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
        }
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextProjection(embedDBOperator* operator) {
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Get next record
    if (operator->input->next(operator->input)) {
        for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            if (col == cols[nextProjCol]) {
                memcpy((int8_t*)operator->recordBuffer + nextProjColPos, (int8_t*)operator->input->recordBuffer + curColPos, colSize);
                nextProjColPos += colSize;
                nextProjCol++;
            }
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. Zero indexed. Column indexes must be strictly increasing i.e. columns must stay in the same order, can only remove columns from input
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols) {
    // Ensure column numbers are strictly increasing
    uint8_t lastCol = cols[0];
    for (uint8_t i = 1; i < numCols; i++) {
        if (cols[i] <= lastCol) {
#ifdef PRINT_ERRORS
            printf("ERROR: Columns in a projection must be strictly ascending for performance reasons");
#endif
            return NULL;
        }
        lastCol = cols[i];
    }
    // Create state
    uint8_t* state = malloc(numCols + 1);
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }
    state[0] = numCols;
    memcpy(state + 1, cols, numCols);

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initProjection;
    operator->next = nextProjection;
    operator->close = closeProjection;

    return operator;
}

void initSelection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = copySchema(operator->input->schema);
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextSelection(embedDBOperator* operator) {
    embedDBSchema* schema = operator->input->schema;

    int8_t colNum = *(int8_t*)operator->state;
    uint16_t colPos = getColOffsetFromSchema(schema, colNum);
    int8_t operation = *((int8_t*)operator->state + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (operator->input->next(operator->input)) {
        void* colData = (int8_t*)operator->input->recordBuffer + colPos;

        if (compare(colData, operation, *(void**)((int8_t*)operator->state + 2), isSigned, colSize)) {
            memcpy(operator->recordBuffer, operator->input->recordBuffer, getRecordSizeFromSchema(operator->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal) {
    int8_t* state = malloc(2 + sizeof(void*));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    state[0] = colNum;
    state[1] = operation;
    memcpy(state + 2, &compVal, sizeof(void*));

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initSelection;
    operator->next = nextSelection;
    operator->close = closeSelection;

    return operator;
}

/**
 * @brief	A private struct to hold the state of the aggregate operator
 */
struct aggregateInfo {
    int8_t (*groupfunc)(const void* lastRecord, const void* record);  // Function that determins if both records are in the same group
    embedDBAggregateFunc* functions;                                  // An array of aggregate functions
    uint32_t functionsLength;                                         // The length of the functions array
    void* lastRecordBuffer;                                           // Buffer for the last record read by input->next
    uint16_t bufferSize;                                              // Size of the input buffer (and lastRecordBuffer)
    int8_t isLastRecordUsable;                                        // Is the data in lastRecordBuffer usable for checking if the recently read record is in the same group? Is set to 0 at start, and also after the last record
};

void initAggregate(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Aggregate operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    struct aggregateInfo* state = operator->state;
    state->isLastRecordUsable = 0;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        operator->schema->numCols = state->functionsLength;
        operator->schema->columnSizes = malloc(state->functionsLength);
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < state->functionsLength; i++) {
            operator->schema->columnSizes[i] = state->functions[i].colSize;
            state->functions[i].colNum = i;
        }
    }

    // Init buffers
    state->bufferSize = getRecordSizeFromSchema(operator->input->schema);
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
    if (state->lastRecordBuffer == NULL) {
        state->lastRecordBuffer = malloc(state->bufferSize);
        if (state->lastRecordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
}

int8_t nextAggregate(embedDBOperator* operator) {
    struct aggregateInfo* state = operator->state;
    embedDBOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].reset != NULL) {
            state->functions[i].reset(state->functions + i, input->schema);
        }
    }

    int8_t recordsInGroup = 0;

    // Check flag used to indicate whether the last record read has been added to a group
    if (state->isLastRecordUsable) {
        recordsInGroup = 1;
        for (int i = 0; i < state->functionsLength; i++) {
            if (state->functions[i].add != NULL) {
                state->functions[i].add(state->functions + i, input->schema, state->lastRecordBuffer);
            }
        }
    }

    int8_t exitType = 0;
    while (input->next(input)) {
        // Check if record is in the same group as the last record
        if (!state->isLastRecordUsable || state->groupfunc(state->lastRecordBuffer, input->recordBuffer)) {
            recordsInGroup = 1;
            for (int i = 0; i < state->functionsLength; i++) {
                if (state->functions[i].add != NULL) {
                    state->functions[i].add(state->functions + i, input->schema, input->recordBuffer);
                }
            }
        } else {
            exitType = 1;
            break;
        }

        // Save this record
        memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);
        state->isLastRecordUsable = 1;
    }

    if (!recordsInGroup) {
        return 0;
    }

    if (exitType == 0) {
        // Exited because ran out of records, so all read records have been added to a group
        state->isLastRecordUsable = 0;
    }

    // Perform final compute on all functions
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].compute != NULL) {
            state->functions[i].compute(state->functions + i, operator->schema, operator->recordBuffer, state->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);

    return 1;
}

void closeAggregate(embedDBOperator* operator) {
    operator->input->close(operator->input);
    operator->input = NULL;
    embedDBFreeSchema(&operator->schema);
    free(((struct aggregateInfo*)operator->state)->lastRecordBuffer);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength) {
    struct aggregateInfo* state = malloc(sizeof(struct aggregateInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    state->groupfunc = groupfunc;
    state->functions = functions;
    state->functionsLength = functionsLength;
    state->lastRecordBuffer = NULL;

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initAggregate;
    operator->next = nextAggregate;
    operator->close = closeAggregate;

    return operator;
}

struct keyJoinInfo {
    embedDBOperator* input2;
    int8_t firstCall;
};

void initKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The first columns of the two tables must be the key and must be the same size. Make sure you haven't projected them out.\n");
#endif
        return;
    }

    // Setup schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        operator->schema->numCols = schema1->numCols + schema2->numCols;
        operator->schema->columnSizes = malloc(operator->schema->numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        memcpy(operator->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(operator->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
    }

    // Allocate recordBuffer
    operator->recordBuffer = malloc(getRecordSizeFromSchema(operator->schema));
    if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
        return;
    }

    state->firstCall = 1;
}

int8_t nextKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // We've already used this match
    void* record1 = input1->recordBuffer;
    void* record2 = input2->recordBuffer;

    int8_t colSize = abs(schema1->columnSizes[0]);

    if (state->firstCall) {
        state->firstCall = 0;

        if (!input1->next(input1) || !input2->next(input2)) {
            // If this case happens, you goofed, but I'll handle it anyway
            return 0;
        }
        goto check;
    }

    while (1) {
        // Advance the input with the smaller value
        int8_t comp = compareUnsignedNumbers(record1, record2, colSize);
        if (comp == 0) {
            // Move both forward because if they match at this point, they've already been matched
            if (!input1->next(input1) || !input2->next(input2)) {
                return 0;
            }
        } else if (comp < 0) {
            // Move record 1 forward
            if (!input1->next(input1)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        } else {
            // Move record 2 forward
            if (!input2->next(input2)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        }

    check:
        // See if these records join
        if (compareUnsignedNumbers(record1, record2, colSize) == 0) {
            // Copy both records into the output
            uint16_t record1Size = getRecordSizeFromSchema(schema1);
            memcpy(operator->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t*)operator->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    input1->close(input1);
    input2->close(input2);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2) {
    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }

    struct keyJoinInfo* state = malloc(sizeof(struct keyJoinInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }
    state->input2 = input2;

    operator->input = input1;
    operator->state = state;
    operator->recordBuffer = NULL;
    operator->schema = NULL;
    operator->init = initKeyJoin;
    operator->next = nextKeyJoin;
    operator->close = closeKeyJoin;

    return operator;
}

void countReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    *(uint32_t*)aggFunc->state = 0;
}

void countAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    (*(uint32_t*)aggFunc->state)++;
}

void countCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(uint32_t));
}

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate() {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = countReset;
    aggFunc->add = countAdd;
    aggFunc->compute = countCompute;
    aggFunc->state = malloc(sizeof(uint32_t));
    aggFunc->colSize = 4;
    return aggFunc;
}

void sumReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    if (abs(inputSchema->columnSizes[*((uint8_t*)aggFunc->state + sizeof(int64_t))]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    *(int64_t*)aggFunc->state = 0;
}

void sumAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    uint8_t colNum = *((uint8_t*)aggFunc->state + sizeof(int64_t));
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)recordBuffer + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t*)aggFunc->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t*)aggFunc->state) += val;
    }
}

void sumCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(int64_t));
}

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = sumReset;
    aggFunc->add = sumAdd;
    aggFunc->compute = sumCompute;
    aggFunc->state = malloc(sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t*)aggFunc->state + sizeof(int64_t)) = colNum;
    aggFunc->colSize = -8;
    return aggFunc;
}

struct minMaxState {
    uint8_t colNum;  // Which column of input to use
    void* current;   // The value currently regarded as the min/max
};

void minReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for min aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0xff, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as -1, not MAX_INT
        memset((int8_t*)state->current + colSize - 1, 0x7f, 1);
    }
}

void minAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_LT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

void minMaxCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), ((struct minMaxState*)aggFunc->state)->current, abs(outputSchema->columnSizes[aggFunc->colNum]));
}

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = minReset;
    aggFunc->add = minAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

void maxReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for max aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as 0, not MIN_INT
        memset((int8_t*)state->current + colSize - 1, 0x80, 1);
    }
}

void maxAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_GT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = maxReset;
    aggFunc->add = maxAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

struct avgState {
    uint8_t colNum;   // Column to take avg of
    int8_t isSigned;  // Is input column signed?
    uint32_t count;   // Count of records seen in group so far
    int64_t sum;      // Sum of records seen in group so far
};

void avgReset(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct avgState* state = aggFunc->state;
    if (abs(inputSchema->columnSizes[state->colNum]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    state->count = 0;
    state->sum = 0;
    state->isSigned = embedDB_IS_COL_SIGNED(inputSchema->columnSizes[state->colNum]);
}

void avgAdd(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct avgState* state = aggFunc->state;
    uint8_t colNum = state->colNum;
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)record + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        state->sum += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        val += (uint64_t)state->sum;
        memcpy(&state->sum, &val, sizeof(uint64_t));
    }
    state->count++;
}

void avgCompute(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    struct avgState* state = aggFunc->state;
    if (aggFunc->colSize == 8) {
        double avg = state->sum / (double)state->count;
        if (state->isSigned) {
            avg = state->sum / (double)state->count;
        } else {
            avg = (uint64_t)state->sum / (double)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(double));
    } else {
        float avg;
        if (state->isSigned) {
            avg = state->sum / (float)state->count;
        } else {
            avg = (uint64_t)state->sum / (float)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(float));
    }
}

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    struct avgState* state = malloc(sizeof(struct avgState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    aggFunc->state = state;
    if (outputFloatSize > 8 || (outputFloatSize < 8 && outputFloatSize > 4)) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 8.");
#endif
        aggFunc->colSize = 8;
    } else if (outputFloatSize < 4) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 4.");
#endif
        aggFunc->colSize = 4;
    } else {
        aggFunc->colSize = outputFloatSize;
    }
    aggFunc->reset = avgReset;
    aggFunc->add = avgAdd;
    aggFunc->compute = avgCompute;

    return aggFunc;
}

/**
 * @brief	Completely free a chain of functions recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator) {
    if ((*operator)->input != NULL) {
        embedDBFreeOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->state != NULL) {
        free((*operator)->state);
        (*operator)->state = NULL;
    }
    if ((*operator)->schema != NULL) {
        embedDBFreeSchema(&(*operator)->schema);
    }
    if ((*operator)->recordBuffer != NULL) {
        free((*operator)->recordBuffer);
        (*operator)->recordBuffer = NULL;
    }
    free(*operator);
    (*operator) = NULL;
}

/************************************************************schema.c************************************************************/
/******************************************************************************/
/**
 * @file        schema.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the schema for EmbedDB query interface
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

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness) {
    embedDBSchema* schema = malloc(sizeof(embedDBSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < numCols; i++) {
        int8_t sign = colSignedness[i];
        uint8_t colSize = colSizes[i];
        totalSize += colSize;
        if (colSize <= 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Column size must be greater than zero\n");
#endif
            return NULL;
        }
        if (sign == embedDB_COLUMN_SIGNED) {
            schema->columnSizes[i] = -colSizes[i];
        } else if (sign == embedDB_COLUMN_UNSIGNED) {
            schema->columnSizes[i] = colSizes[i];
        } else {
#ifdef PRINT_ERRORS
            printf("ERROR: Must only use embedDB_COLUMN_SIGNED or embedDB_COLUMN_UNSIGNED to describe column signedness\n");
#endif
            return NULL;
        }
    }

    return schema;
}

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema) {
    embedDBSchema* copy = malloc(sizeof(embedDBSchema));
    if (copy == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    if (copy->columnSizes == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema) {
    uint16_t size = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        size += abs(schema->columnSizes[i]);
    }
    return size;
}

void printSchema(embedDBSchema* schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", embedDB_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
}

/************************************************************utilityFunctions.c************************************************************/
/******************************************************************************/
/**
 * @file        utilityFunctions.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       This file contains some utility functions to be used with embedDB.
 *              These include functions required to use the bitmap option, and a
 *              comparator for comparing keys. They can be modified or implemented
 *              differently depending on the application.
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

embedDBState *defaultInitializedState() {
    embedDBState *state = calloc(1, sizeof(embedDBState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("Failed to allocate memory for state.\n");
#endif
        return NULL;
    }

    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

    /* Address level parameters */
    state->numDataPages = 20000;  // Enough for 620,000 records
    state->numIndexPages = 44;    // Enough for 676,544 records
    state->eraseSizeInPages = 4;

    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Initialize embedDB structure */
    if (embedDBInit(state, 1) != 0) {
#ifdef PRINT_ERRORS
        printf("Initialization error.\n");
#endif
        free(state->buffer);
        free(state->fileInterface);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state);
        return NULL;
    }

    return state;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and max value. */
void buildBitmapInt8FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

int8_t inBitmapInt8(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt16(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt16(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint64_t *)bm = UINT64_MAX; /* Everything */
        return;
    } else {
        uint64_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt64(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint64_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt64(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint64_t *)bm = maxMap;
                return;
            }
        }
        *(uint64_t *)bm = minMap & maxMap;
    }
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

int8_t int64Comparator(void *a, void *b) {
    int64_t result = *((int64_t *)a) - *((int64_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

typedef struct {
    char *filename;
    FILE *file;
} FILE_INFO;

void *setupFile(char *filename) {
    FILE_INFO *fileInfo = malloc(sizeof(FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->file = NULL;
    return fileInfo;
}

void tearDownFile(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo->file != NULL)
        fclose(fileInfo->file);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageSize * pageNum, SEEK_SET);
    return fread(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageNum * pageSize, SEEK_SET);
    return fwrite(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_CLOSE(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fclose(fileInfo->file);
    fileInfo->file = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    return fflush(fileInfo->file) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "w+b");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "r+b");
    } else {
        return 0;
    }

    if (fileInfo->file == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getFileInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}

/************************************************************only-include-duplicate.c************************************************************/
              
/************************************************************radixspline.c************************************************************/
/******************************************************************************/
/**
 * @file        radixspline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of radix spline for embedded devices.
 *              Based on "RadixSpline: a single-pass learned index" by
 *              A. Kipf, R. Marcus, A. van Renen, M. Stoian, A. Kemper,
 *              T. Kraska, and T. Neumann
 *              https://github.com/learnedsystems/RadixSpline
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

/**
 * @brief   Build the radix table
 * @param   rsdix       Radix spline structure
 * @param   keys        Data points to be indexed
 * @param   numKeys     Number of data items
 */
void radixsplineBuild(radixspline *rsidx, void **keys, uint32_t numKeys) {
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;

    for (uint32_t i = 0; i < numKeys; i++) {
        void *key;
        memcpy(&key, keys + i, sizeof(void *));
        radixsplineAddPoint(rsidx, key, i);
    }
}

/**
 * @brief   Rebuild the radix table with new shift amount
 * @param   rsdix       Radix spline structure
 * @param   spl         Spline structure
 * @param   radixSize   Size of radix table
 * @param   shiftAmount Difference in shift amount between current radix table and desired radix table
 */
void radixsplineRebuild(radixspline *rsidx, int8_t radixSize, int8_t shiftAmount) {
    // radixsplinePrint(rsidx);
    rsidx->prevPrefix = rsidx->prevPrefix >> shiftAmount;

    for (id_t i = 0; i < rsidx->size / pow(2, shiftAmount); i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, (int8_t *)rsidx->table + (i << shiftAmount) * rsidx->keySize, rsidx->keySize);
    }
    uint64_t maxKey = UINT64_MAX;
    for (id_t i = rsidx->size / pow(2, shiftAmount); i < rsidx->size; i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, &maxKey, rsidx->keySize);
    }
}

/**
 * @brief	Add a point to be indexed by the radix spline structure
 * @param	rsdix	Radix spline structure
 * @param	key		New point to be indexed by radix spline
 * @param   page    Page number for spline point to add
 */
void radixsplineAddPoint(radixspline *rsidx, void *key, uint32_t page) {
    splineAdd(rsidx->spl, key, page);

    // Return if not using Radix table
    if (rsidx->radixSize == 0) {
        return;
    }

    // Determine if need to update radix table based on adding point to spline
    if (rsidx->spl->count <= rsidx->pointsSeen)
        return;  // Nothing to do

    // take the last point that was added to spline
    key = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);

    // Initialize table and minKey on first key added
    if (rsidx->pointsSeen == 0) {
        rsidx->table = malloc(sizeof(id_t) * rsidx->size);
        uint64_t maxKey = UINT64_MAX;
        for (int32_t counter = 1; counter < rsidx->size; counter++) {
            memcpy(rsidx->table + counter, &maxKey, sizeof(id_t));
        }
        rsidx->minKey = key;
    }

    // Check if prefix will fit in radix table
    uint64_t keyDiff;
    if (rsidx->keySize <= 4) {
        uint32_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    } else {
        uint64_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    }

    uint8_t bitsToRepresentKey = ceil(log2f((float)keyDiff));
    int8_t newShiftSize;
    if (bitsToRepresentKey < rsidx->radixSize) {
        newShiftSize = 0;
    } else {
        newShiftSize = bitsToRepresentKey - rsidx->radixSize;
    }

    // if the shift size changes, need to remake table from scratch using new shift size
    if (newShiftSize > rsidx->shiftSize) {
        radixsplineRebuild(rsidx, rsidx->radixSize, newShiftSize - rsidx->shiftSize);
        rsidx->shiftSize = newShiftSize;
    }

    id_t prefix = keyDiff >> rsidx->shiftSize;
    if (prefix != rsidx->prevPrefix) {
        // Make all new rows in the radix table point to the last point seen
        for (id_t pr = rsidx->prevPrefix; pr < prefix; pr++) {
            memcpy(rsidx->table + pr, &rsidx->pointsSeen, sizeof(id_t));
        }

        rsidx->prevPrefix = prefix;
    }

    memcpy(rsidx->table + prefix, &rsidx->pointsSeen, sizeof(id_t));

    rsidx->pointsSeen++;
}

/**
 * @brief	Initialize an empty radix spline index of given size
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInit(radixspline *rsidx, spline *spl, int8_t radixSize, uint8_t keySize) {
    rsidx->spl = spl;
    rsidx->radixSize = radixSize;
    rsidx->keySize = keySize;
    rsidx->shiftSize = 0;
    rsidx->size = pow(2, radixSize);

    /* Determine the prefix size (shift bits) based on min and max keys */
    rsidx->minKey = spl->points;

    /* Initialize points seen */
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	rsidx		Array to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixBinarySearch(radixspline *rsidx, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    void *arr = rsidx->spl->points;

    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;
        void *midKey = splinePointLocation(rsidx->spl, mid);
        void *midKeyMinusOne = splinePointLocation(rsidx->spl, mid - 1);
        if (compareKey(midKey, key) >= 0 && compareKey(midKeyMinusOne, key) <= 0)
            return mid;

        if (compareKey(midKey, key) > 0)
            return radixBinarySearch(rsidx, low, mid - 1, key, compareKey);

        return radixBinarySearch(rsidx, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Initialize and build a radix spline index of given size using pre-built spline structure.
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keys		Keys to be indexed
 * @param	numKeys 	Number of keys in `keys`
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInitBuild(radixspline *rsidx, spline *spl, uint32_t radixSize, void **keys, uint32_t numKeys, uint8_t keySize) {
    radixsplineInit(rsidx, spl, radixSize, keySize);
    radixsplineBuild(rsidx, keys, numKeys);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using radix table.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntry(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    /* Use radix table to find range of spline points */

    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    uint32_t prefix = (keyVal - minKeyVal) >> rsidx->shiftSize;

    uint32_t begin, end;

    // Determine end, use next higher radix point if within bounds, unless key is exactly prefix
    if (keyVal == ((uint64_t)prefix << rsidx->shiftSize)) {
        memcpy(&end, rsidx->table + prefix, sizeof(id_t));
    } else {
        if ((prefix + 1) < rsidx->size) {
            memcpy(&end, rsidx->table + (prefix + 1), sizeof(id_t));
        } else {
            memcpy(&end, rsidx->table + (rsidx->size - 1), sizeof(id_t));
        }
    }

    // check end is in bounds since radix table values are initiated to INT_MAX
    if (end >= rsidx->spl->count) {
        end = rsidx->spl->count - 1;
    }

    // use previous adjacent radix point for lower bounds
    if (prefix == 0) {
        begin = 0;
    } else {
        memcpy(&begin, rsidx->table + (prefix - 1), sizeof(id_t));
    }

    return radixBinarySearch(rsidx, begin, end, key, compareKey);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using binary search.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return  Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntryBinarySearch(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    return radixBinarySearch(rsidx, 0, rsidx->spl->count - 1, key, compareKey);
}

/**
 * @brief	Estimate location of key in data using spline points.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 * @param	compareKey	Function to compare keys
 * @return	Estimated page number that contains key
 */
size_t radixsplineEstimateLocation(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    if (keyVal < minKeyVal)
        return 0;

    size_t index;
    if (rsidx->radixSize == 0) {
        /* Get index using binary search */
        index = radixsplineGetEntryBinarySearch(rsidx, key, compareKey);
    } else {
        /* Get index using radix table */
        index = radixsplineGetEntry(rsidx, key, compareKey);
    }

    /* Interpolate between two spline points */
    void *down = splinePointLocation(rsidx->spl, index - 1);
    void *up = splinePointLocation(rsidx->spl, index);

    uint64_t downKey = 0, upKey = 0;
    memcpy(&downKey, down, rsidx->keySize);
    memcpy(&upKey, up, rsidx->keySize);

    uint32_t upPage = 0;
    uint32_t downPage = 0;
    memcpy(&upPage, (int8_t *)up + rsidx->spl->keySize, sizeof(uint32_t));
    memcpy(&downPage, (int8_t *)down + rsidx->spl->keySize, sizeof(uint32_t));

    /* Keydiff * slope + y */
    uint32_t estimatedPage = (uint32_t)((keyVal - downKey) * (upPage - downPage) / (long double)(upKey - downKey)) + downPage;
    return estimatedPage > upPage ? upPage : estimatedPage;
}

/**
 * @brief	Finds a value using index. Returns predicted location and low and high error bounds.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param   compareKey  Function to compare keys
 * @param	loc		    Return of predicted location
 * @param	low		    Return of low bound on predicted location
 * @param	high	    Return of high bound on predicted location
 */
void radixsplineFind(radixspline *rsidx, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    /* Estimate location */
    id_t locationEstimate = radixsplineEstimateLocation(rsidx, key, compareKey);
    memcpy(loc, &locationEstimate, sizeof(id_t));

    /* Set error bounds based on maxError from spline construction */
    id_t lowEstimate = (rsidx->spl->maxError > locationEstimate) ? 0 : locationEstimate - rsidx->spl->maxError;
    memcpy(low, &lowEstimate, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);
    uint64_t lastKey = 0;
    memcpy(&lastKey, lastSplinePoint, rsidx->keySize);
    id_t highEstimate = (locationEstimate + rsidx->spl->maxError > lastKey) ? lastKey : locationEstimate + rsidx->spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief	Print radix spline structure.
 * @param	rsidx	Radix spline structure
 */
void radixsplinePrint(radixspline *rsidx) {
    if (rsidx == NULL || rsidx->radixSize == 0) {
        printf("No radix spline index to print.\n");
        return;
    }

    printf("Radix table (%u):\n", rsidx->size);
    // for (id_t i=0; i < 20; i++)
    uint64_t minKeyVal = 0;
    id_t tableVal;
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
    for (id_t i = 0; i < rsidx->size; i++) {
        printf("[" TO_BINARY_PATTERN "] ", TO_BINARY((uint8_t)(i)));
        memcpy(&tableVal, rsidx->table + i, sizeof(id_t));
        printf("(%lu): --> %u\n", (i << rsidx->shiftSize) + minKeyVal, tableVal);
    }
    printf("\n");
}

/**
 * @brief	Returns size of radix spline index structure in bytes
 * @param	rsidx	Radix spline structure
 */
size_t radixsplineSize(radixspline *rsidx) {
    return sizeof(rsidx) + rsidx->size * sizeof(uint32_t) + splineSize(rsidx->spl);
}

/**
 * @brief	Closes and frees space for radix spline index structure
 * @param	rsidx	Radix spline structure
 */
void radixsplineClose(radixspline *rsidx) {
    splineClose(rsidx->spl);
    free(rsidx->spl);
    free(rsidx->table);
}

/************************************************************embedDB.c************************************************************/
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

/************************************************************spline.c************************************************************/
/******************************************************************************/
/**
 * @file        spline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of spline.
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

/**
 * @brief   Initialize a spline structure with given maximum size and error.
 * @param   spl        Spline structure
 * @param   size       Maximum size of spline
 * @param   maxError   Maximum error allowed in spline
 * @param   keySize    Size of key in bytes
 * @return  Returns 0 if successful and -1 if not
 */
int8_t splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize) {
    if (size < 2) {
#ifdef PRINT_ERRORS
        printf("ERROR: The size of the spline must be at least two points.");
#endif
        return -1;
    }
    uint8_t pointSize = sizeof(uint32_t) + keySize;
    spl->count = 0;
    spl->pointsStartIndex = 0;
    spl->eraseSize = 1;
    spl->size = size;
    spl->maxError = maxError;
    spl->points = (void *)malloc(pointSize * size);
    spl->tempLastPoint = 0;
    spl->keySize = keySize;
    spl->lastKey = malloc(keySize);
    spl->lower = malloc(pointSize);
    spl->upper = malloc(pointSize);
    spl->firstSplinePoint = malloc(pointSize);
    spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief    Check if first line is to the left (counter-clockwise) of the second.
 */
static inline int8_t splineIsLeft(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 > y2 * x1;
}

/**
 * @brief    Check if first line is to the right (clockwise) of the second.
 */
static inline int8_t splineIsRight(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 < y2 * x1;
}

/**
 * @brief   Adds point to spline structure
 * @param   spl     Spline structure
 * @param   key     Data key to be added (must be incrementing)
 * @param   page    Page number for spline point to add
 */
void splineAdd(spline *spl, void *key, uint32_t page) {
    spl->numAddCalls++;
    /* Check if no spline points are currently empty */
    if (spl->numAddCalls == 1) {
        /* Add first point in data set to spline. */
        void *firstPoint = splinePointLocation(spl, 0);
        memcpy(firstPoint, key, spl->keySize);
        memcpy(((int8_t *)firstPoint + spl->keySize), &page, sizeof(uint32_t));
        /* Log first point for wrap around purposes */
        memcpy(spl->firstSplinePoint, key, spl->keySize);
        memcpy(((int8_t *)spl->firstSplinePoint + spl->keySize), &page, sizeof(uint32_t));
        spl->count++;
        memcpy(spl->lastKey, key, spl->keySize);
        return;
    }

    /* Check if there is only one spline point (need to initialize upper and lower limits using 2nd point) */
    if (spl->numAddCalls == 2) {
        /* Initialize upper and lower limits using second (unique) data point */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy(((int8_t *)spl->lower + spl->keySize), &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy(((int8_t *)spl->upper + spl->keySize), &upperPage, sizeof(uint32_t));
        memcpy(spl->lastKey, key, spl->keySize);
        spl->lastLoc = page;
        return;
    }

    /* Skip duplicates */
    uint64_t keyVal = 0, lastKeyVal = 0;
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&lastKeyVal, spl->lastKey, spl->keySize);

    if (keyVal <= lastKeyVal)
        return;

    /* Last point added to spline, check if previous point is temporary - overwrite previous point if temporary */
    if (spl->tempLastPoint != 0) {
        spl->count--;
    }

    uint32_t lastPage = 0;
    uint64_t lastPointKey = 0, upperKey = 0, lowerKey = 0;
    void *lastPointLocation = splinePointLocation(spl, spl->count - 1);
    memcpy(&lastPointKey, lastPointLocation, spl->keySize);
    memcpy(&upperKey, spl->upper, spl->keySize);
    memcpy(&lowerKey, spl->lower, spl->keySize);
    memcpy(&lastPage, (int8_t *)lastPointLocation + spl->keySize, sizeof(uint32_t));

    uint64_t xdiff, upperXDiff, lowerXDiff = 0;
    uint32_t ydiff, upperYDiff = 0;
    int64_t lowerYDiff = 0; /* This may be negative */

    xdiff = keyVal - lastPointKey;
    ydiff = page - lastPage;
    upperXDiff = upperKey - lastPointKey;
    memcpy(&upperYDiff, (int8_t *)spl->upper + spl->keySize, sizeof(uint32_t));
    upperYDiff -= lastPage;
    lowerXDiff = lowerKey - lastPointKey;
    memcpy(&lowerYDiff, (int8_t *)spl->lower + spl->keySize, sizeof(uint32_t));
    lowerYDiff -= lastPage;

    if (spl->count >= spl->size)
        splineErase(spl, spl->eraseSize);

    /* Check if next point still in error corridor */
    if (splineIsLeft(xdiff, ydiff, upperXDiff, upperYDiff) == 1 ||
        splineIsRight(xdiff, ydiff, lowerXDiff, lowerYDiff) == 1) {
        /* Point is not in error corridor. Add previous point to spline. */
        void *nextSplinePoint = splinePointLocation(spl, spl->count);
        memcpy(nextSplinePoint, spl->lastKey, spl->keySize);
        memcpy((int8_t *)nextSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
        spl->count++;
        spl->tempLastPoint = 0;

        /* Update upper and lower limits. */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
    } else {
        /* Check if must update upper or lower limits */

        /* Upper limit */
        if (splineIsLeft(upperXDiff, upperYDiff, xdiff, page + spl->maxError - lastPage) == 1) {
            memcpy(spl->upper, key, spl->keySize);
            uint32_t upperPage = page + spl->maxError;
            memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
        }

        /* Lower limit */
        if (splineIsRight(lowerXDiff, lowerYDiff, xdiff, (page < spl->maxError ? 0 : page - spl->maxError) - lastPage) == 1) {
            memcpy(spl->lower, key, spl->keySize);
            uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
            memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        }
    }

    spl->lastLoc = page;

    /* Add last key on spline if not already there. */
    /* This will get overwritten the next time a new spline point is added */
    memcpy(spl->lastKey, key, spl->keySize);
    void *tempSplinePoint = splinePointLocation(spl, spl->count);
    memcpy(tempSplinePoint, spl->lastKey, spl->keySize);
    memcpy((int8_t *)tempSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
    spl->count++;

    spl->tempLastPoint = 1;
}

/**
 * @brief   Removes points from the spline
 * @param   spl         The spline structure to search
 * @param   numPoints   The number of points to remove from the spline
 * @return  Returns zero if successful and one if not
 */
int splineErase(spline *spl, uint32_t numPoints) {
    /* If the user tries to delete more points than they allocated or deleting would only leave one spline point */
    if (numPoints > spl->count || spl->count - numPoints == 1)
        return 1;
    if (numPoints == 0)
        return 0;

    spl->count -= numPoints;
    spl->pointsStartIndex = (spl->pointsStartIndex + numPoints) % spl->size;
    if (spl->count == 0)
        spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief	Builds a spline structure given a sorted data set. GreedySplineCorridor
 * implementation from "Smooth interpolating histograms with error guarantees"
 * (BNCOD'08) by T. Neumann and S. Michel.
 * @param	spl			Spline structure
 * @param	data		Array of sorted data
 * @param	size		Number of values in array
 * @param	maxError	Maximum error for each spline
 */
void splineBuild(spline *spl, void **data, id_t size, size_t maxError) {
    spl->maxError = maxError;

    for (id_t i = 0; i < size; i++) {
        void *key;
        memcpy(&key, data + i, sizeof(void *));
        splineAdd(spl, key, i);
    }
}

/**
 * @brief    Print a spline structure.
 * @param    spl     Spline structure
 */
void splinePrint(spline *spl) {
    if (spl == NULL) {
        printf("No spline to print.\n");
        return;
    }
    printf("Spline max error (%i):\n", spl->maxError);
    printf("Spline points (%li):\n", spl->count);
    uint64_t keyVal = 0;
    uint32_t page = 0;
    for (id_t i = 0; i < spl->count; i++) {
        void *point = splinePointLocation(spl, i);
        memcpy(&keyVal, point, spl->keySize);
        memcpy(&page, (int8_t *)point + spl->keySize, sizeof(uint32_t));
        printf("[%i]: (%li, %i)\n", i, keyVal, page);
    }
    printf("\n");
}

/**
 * @brief    Return spline structure size in bytes.
 * @param    spl     Spline structure
 * @return   size of the spline in bytes
 */
uint32_t splineSize(spline *spl) {
    return sizeof(spline) + (spl->size * (spl->keySize + sizeof(uint32_t)));
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	arr			Array of spline points to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t pointsBinarySearch(spline *spl, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        // If mid is zero, then low = 0 and high = 1. Therefore there is only one spline segment and we return 1, the upper bound.
        if (mid == 0) {
            return 1;
        }

        void *midSplinePoint = splinePointLocation(spl, mid);
        void *midSplineMinusOnePoint = splinePointLocation(spl, mid - 1);

        if (compareKey(midSplinePoint, key) >= 0 && compareKey(midSplineMinusOnePoint, key) <= 0)
            return mid;

        if (compareKey(midSplinePoint, key) > 0)
            return pointsBinarySearch(spl, low, mid - 1, key, compareKey);

        return pointsBinarySearch(spl, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Estimate the page number of a given key
 * @param	spl			The spline structure to search
 * @param	key			The key to search for
 * @param	compareKey	Function to compare keys
 * @param	loc			A return value for the best estimate of which page the key is on
 * @param	low			A return value for the smallest page that it could be on
 * @param	high		A return value for the largest page it could be on
 */
void splineFind(spline *spl, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    size_t pointIdx;
    uint64_t keyVal = 0, smallestKeyVal = 0, largestKeyVal = 0;
    void *smallestSplinePoint = splinePointLocation(spl, 0);
    void *largestSplinePoint = splinePointLocation(spl, spl->count - 1);
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&smallestKeyVal, smallestSplinePoint, spl->keySize);
    memcpy(&largestKeyVal, largestSplinePoint, spl->keySize);

    if (compareKey(key, splinePointLocation(spl, 0)) < 0 || spl->count <= 1) {
        // Key is smaller than any we have on record
        uint32_t lowEstimate, highEstimate, locEstimate = 0;
        memcpy(&lowEstimate, (int8_t *)spl->firstSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(&highEstimate, (int8_t *)smallestSplinePoint + spl->keySize, sizeof(uint32_t));
        locEstimate = (lowEstimate + highEstimate) / 2;

        memcpy(loc, &locEstimate, sizeof(uint32_t));
        memcpy(low, &lowEstimate, sizeof(uint32_t));
        memcpy(high, &highEstimate, sizeof(uint32_t));
        return;
    } else if (compareKey(key, splinePointLocation(spl, spl->count - 1)) > 0) {
        memcpy(loc, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(low, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(high, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        return;
    } else {
        // Perform a binary seach to find the spline point above the key we're looking for
        pointIdx = pointsBinarySearch(spl, 0, spl->count - 1, key, compareKey);
    }

    // Interpolate between two spline points
    void *downKey = splinePointLocation(spl, pointIdx - 1);
    uint32_t downPage = 0;
    memcpy(&downPage, (int8_t *)downKey + spl->keySize, sizeof(uint32_t));
    void *upKey = splinePointLocation(spl, pointIdx);
    uint32_t upPage = 0;
    memcpy(&upPage, (int8_t *)upKey + spl->keySize, sizeof(uint32_t));
    uint64_t downKeyVal = 0, upKeyVal = 0;
    memcpy(&downKeyVal, downKey, spl->keySize);
    memcpy(&upKeyVal, upKey, spl->keySize);

    // Estimate location as page number
    // Keydiff * slope + y
    id_t locationEstimate = (id_t)((keyVal - downKeyVal) * (upPage - downPage) / (long double)(upKeyVal - downKeyVal)) + downPage;
    memcpy(loc, &locationEstimate, sizeof(id_t));

    // Set error bounds based on maxError from spline construction
    id_t lowEstiamte = (spl->maxError > locationEstimate) ? 0 : locationEstimate - spl->maxError;
    memcpy(low, &lowEstiamte, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(spl, spl->count - 1);
    uint32_t lastSplinePointPage = 0;
    memcpy(&lastSplinePointPage, (int8_t *)lastSplinePoint + spl->keySize, sizeof(uint32_t));
    id_t highEstimate = (locationEstimate + spl->maxError > lastSplinePointPage) ? lastSplinePointPage : locationEstimate + spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief    Free memory allocated for spline structure.
 * @param    spl        Spline structure
 */
void splineClose(spline *spl) {
    free(spl->points);
    free(spl->lastKey);
    free(spl->lower);
    free(spl->upper);
    free(spl->firstSplinePoint);
}

/**
 * @brief   Returns a pointer to the location of the specified spline point in memory. Note that this method does not check if there is a point there, so it may be garbage data.
 * @param   spl         The spline structure that contains the points
 * @param   pointIndex  The index of the point to return a pointer to
 */
void *splinePointLocation(spline *spl, size_t pointIndex) {
    return (int8_t *)spl->points + (((pointIndex + spl->pointsStartIndex) % spl->size) * (spl->keySize + sizeof(uint32_t)));
}

/************************************************************only-include-inline-comments.c************************************************************/
     // foo  // a + b?   /* I find your lack of faith disturbing*/    // I have a bad feeling about this   // /* Stay on target. */   // alsdfkjsdlf     //


/************************************************************schema.c************************************************************/
/******************************************************************************/
/**
 * @file        schema.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the schema for EmbedDB query interface
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

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness) {
    embedDBSchema* schema = malloc(sizeof(embedDBSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < numCols; i++) {
        int8_t sign = colSignedness[i];
        uint8_t colSize = colSizes[i];
        totalSize += colSize;
        if (colSize <= 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Column size must be greater than zero\n");
#endif
            return NULL;
        }
        if (sign == embedDB_COLUMN_SIGNED) {
            schema->columnSizes[i] = -colSizes[i];
        } else if (sign == embedDB_COLUMN_UNSIGNED) {
            schema->columnSizes[i] = colSizes[i];
        } else {
#ifdef PRINT_ERRORS
            printf("ERROR: Must only use embedDB_COLUMN_SIGNED or embedDB_COLUMN_UNSIGNED to describe column signedness\n");
#endif
            return NULL;
        }
    }

    return schema;
}

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema) {
    embedDBSchema* copy = malloc(sizeof(embedDBSchema));
    if (copy == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    if (copy->columnSizes == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema) {
    uint16_t size = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        size += abs(schema->columnSizes[i]);
    }
    return size;
}

void printSchema(embedDBSchema* schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", embedDB_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
}

/************************************************************radixspline.c************************************************************/
/******************************************************************************/
/**
 * @file        radixspline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of radix spline for embedded devices.
 *              Based on "RadixSpline: a single-pass learned index" by
 *              A. Kipf, R. Marcus, A. van Renen, M. Stoian, A. Kemper,
 *              T. Kraska, and T. Neumann
 *              https://github.com/learnedsystems/RadixSpline
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

/**
 * @brief   Build the radix table
 * @param   rsdix       Radix spline structure
 * @param   keys        Data points to be indexed
 * @param   numKeys     Number of data items
 */
void radixsplineBuild(radixspline *rsidx, void **keys, uint32_t numKeys) {
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;

    for (uint32_t i = 0; i < numKeys; i++) {
        void *key;
        memcpy(&key, keys + i, sizeof(void *));
        radixsplineAddPoint(rsidx, key, i);
    }
}

/**
 * @brief   Rebuild the radix table with new shift amount
 * @param   rsdix       Radix spline structure
 * @param   spl         Spline structure
 * @param   radixSize   Size of radix table
 * @param   shiftAmount Difference in shift amount between current radix table and desired radix table
 */
void radixsplineRebuild(radixspline *rsidx, int8_t radixSize, int8_t shiftAmount) {
    // radixsplinePrint(rsidx);
    rsidx->prevPrefix = rsidx->prevPrefix >> shiftAmount;

    for (id_t i = 0; i < rsidx->size / pow(2, shiftAmount); i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, (int8_t *)rsidx->table + (i << shiftAmount) * rsidx->keySize, rsidx->keySize);
    }
    uint64_t maxKey = UINT64_MAX;
    for (id_t i = rsidx->size / pow(2, shiftAmount); i < rsidx->size; i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, &maxKey, rsidx->keySize);
    }
}

/**
 * @brief	Add a point to be indexed by the radix spline structure
 * @param	rsdix	Radix spline structure
 * @param	key		New point to be indexed by radix spline
 * @param   page    Page number for spline point to add
 */
void radixsplineAddPoint(radixspline *rsidx, void *key, uint32_t page) {
    splineAdd(rsidx->spl, key, page);

    // Return if not using Radix table
    if (rsidx->radixSize == 0) {
        return;
    }

    // Determine if need to update radix table based on adding point to spline
    if (rsidx->spl->count <= rsidx->pointsSeen)
        return;  // Nothing to do

    // take the last point that was added to spline
    key = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);

    // Initialize table and minKey on first key added
    if (rsidx->pointsSeen == 0) {
        rsidx->table = malloc(sizeof(id_t) * rsidx->size);
        uint64_t maxKey = UINT64_MAX;
        for (int32_t counter = 1; counter < rsidx->size; counter++) {
            memcpy(rsidx->table + counter, &maxKey, sizeof(id_t));
        }
        rsidx->minKey = key;
    }

    // Check if prefix will fit in radix table
    uint64_t keyDiff;
    if (rsidx->keySize <= 4) {
        uint32_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    } else {
        uint64_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    }

    uint8_t bitsToRepresentKey = ceil(log2f((float)keyDiff));
    int8_t newShiftSize;
    if (bitsToRepresentKey < rsidx->radixSize) {
        newShiftSize = 0;
    } else {
        newShiftSize = bitsToRepresentKey - rsidx->radixSize;
    }

    // if the shift size changes, need to remake table from scratch using new shift size
    if (newShiftSize > rsidx->shiftSize) {
        radixsplineRebuild(rsidx, rsidx->radixSize, newShiftSize - rsidx->shiftSize);
        rsidx->shiftSize = newShiftSize;
    }

    id_t prefix = keyDiff >> rsidx->shiftSize;
    if (prefix != rsidx->prevPrefix) {
        // Make all new rows in the radix table point to the last point seen
        for (id_t pr = rsidx->prevPrefix; pr < prefix; pr++) {
            memcpy(rsidx->table + pr, &rsidx->pointsSeen, sizeof(id_t));
        }

        rsidx->prevPrefix = prefix;
    }

    memcpy(rsidx->table + prefix, &rsidx->pointsSeen, sizeof(id_t));

    rsidx->pointsSeen++;
}

/**
 * @brief	Initialize an empty radix spline index of given size
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInit(radixspline *rsidx, spline *spl, int8_t radixSize, uint8_t keySize) {
    rsidx->spl = spl;
    rsidx->radixSize = radixSize;
    rsidx->keySize = keySize;
    rsidx->shiftSize = 0;
    rsidx->size = pow(2, radixSize);

    /* Determine the prefix size (shift bits) based on min and max keys */
    rsidx->minKey = spl->points;

    /* Initialize points seen */
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	rsidx		Array to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixBinarySearch(radixspline *rsidx, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    void *arr = rsidx->spl->points;

    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;
        void *midKey = splinePointLocation(rsidx->spl, mid);
        void *midKeyMinusOne = splinePointLocation(rsidx->spl, mid - 1);
        if (compareKey(midKey, key) >= 0 && compareKey(midKeyMinusOne, key) <= 0)
            return mid;

        if (compareKey(midKey, key) > 0)
            return radixBinarySearch(rsidx, low, mid - 1, key, compareKey);

        return radixBinarySearch(rsidx, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Initialize and build a radix spline index of given size using pre-built spline structure.
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keys		Keys to be indexed
 * @param	numKeys 	Number of keys in `keys`
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInitBuild(radixspline *rsidx, spline *spl, uint32_t radixSize, void **keys, uint32_t numKeys, uint8_t keySize) {
    radixsplineInit(rsidx, spl, radixSize, keySize);
    radixsplineBuild(rsidx, keys, numKeys);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using radix table.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntry(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    /* Use radix table to find range of spline points */

    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    uint32_t prefix = (keyVal - minKeyVal) >> rsidx->shiftSize;

    uint32_t begin, end;

    // Determine end, use next higher radix point if within bounds, unless key is exactly prefix
    if (keyVal == ((uint64_t)prefix << rsidx->shiftSize)) {
        memcpy(&end, rsidx->table + prefix, sizeof(id_t));
    } else {
        if ((prefix + 1) < rsidx->size) {
            memcpy(&end, rsidx->table + (prefix + 1), sizeof(id_t));
        } else {
            memcpy(&end, rsidx->table + (rsidx->size - 1), sizeof(id_t));
        }
    }

    // check end is in bounds since radix table values are initiated to INT_MAX
    if (end >= rsidx->spl->count) {
        end = rsidx->spl->count - 1;
    }

    // use previous adjacent radix point for lower bounds
    if (prefix == 0) {
        begin = 0;
    } else {
        memcpy(&begin, rsidx->table + (prefix - 1), sizeof(id_t));
    }

    return radixBinarySearch(rsidx, begin, end, key, compareKey);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using binary search.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return  Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntryBinarySearch(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    return radixBinarySearch(rsidx, 0, rsidx->spl->count - 1, key, compareKey);
}

/**
 * @brief	Estimate location of key in data using spline points.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 * @param	compareKey	Function to compare keys
 * @return	Estimated page number that contains key
 */
size_t radixsplineEstimateLocation(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    if (keyVal < minKeyVal)
        return 0;

    size_t index;
    if (rsidx->radixSize == 0) {
        /* Get index using binary search */
        index = radixsplineGetEntryBinarySearch(rsidx, key, compareKey);
    } else {
        /* Get index using radix table */
        index = radixsplineGetEntry(rsidx, key, compareKey);
    }

    /* Interpolate between two spline points */
    void *down = splinePointLocation(rsidx->spl, index - 1);
    void *up = splinePointLocation(rsidx->spl, index);

    uint64_t downKey = 0, upKey = 0;
    memcpy(&downKey, down, rsidx->keySize);
    memcpy(&upKey, up, rsidx->keySize);

    uint32_t upPage = 0;
    uint32_t downPage = 0;
    memcpy(&upPage, (int8_t *)up + rsidx->spl->keySize, sizeof(uint32_t));
    memcpy(&downPage, (int8_t *)down + rsidx->spl->keySize, sizeof(uint32_t));

    /* Keydiff * slope + y */
    uint32_t estimatedPage = (uint32_t)((keyVal - downKey) * (upPage - downPage) / (long double)(upKey - downKey)) + downPage;
    return estimatedPage > upPage ? upPage : estimatedPage;
}

/**
 * @brief	Finds a value using index. Returns predicted location and low and high error bounds.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param   compareKey  Function to compare keys
 * @param	loc		    Return of predicted location
 * @param	low		    Return of low bound on predicted location
 * @param	high	    Return of high bound on predicted location
 */
void radixsplineFind(radixspline *rsidx, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    /* Estimate location */
    id_t locationEstimate = radixsplineEstimateLocation(rsidx, key, compareKey);
    memcpy(loc, &locationEstimate, sizeof(id_t));

    /* Set error bounds based on maxError from spline construction */
    id_t lowEstimate = (rsidx->spl->maxError > locationEstimate) ? 0 : locationEstimate - rsidx->spl->maxError;
    memcpy(low, &lowEstimate, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);
    uint64_t lastKey = 0;
    memcpy(&lastKey, lastSplinePoint, rsidx->keySize);
    id_t highEstimate = (locationEstimate + rsidx->spl->maxError > lastKey) ? lastKey : locationEstimate + rsidx->spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief	Print radix spline structure.
 * @param	rsidx	Radix spline structure
 */
void radixsplinePrint(radixspline *rsidx) {
    if (rsidx == NULL || rsidx->radixSize == 0) {
        printf("No radix spline index to print.\n");
        return;
    }

    printf("Radix table (%u):\n", rsidx->size);
    // for (id_t i=0; i < 20; i++)
    uint64_t minKeyVal = 0;
    id_t tableVal;
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
    for (id_t i = 0; i < rsidx->size; i++) {
        printf("[" TO_BINARY_PATTERN "] ", TO_BINARY((uint8_t)(i)));
        memcpy(&tableVal, rsidx->table + i, sizeof(id_t));
        printf("(%lu): --> %u\n", (i << rsidx->shiftSize) + minKeyVal, tableVal);
    }
    printf("\n");
}

/**
 * @brief	Returns size of radix spline index structure in bytes
 * @param	rsidx	Radix spline structure
 */
size_t radixsplineSize(radixspline *rsidx) {
    return sizeof(rsidx) + rsidx->size * sizeof(uint32_t) + splineSize(rsidx->spl);
}

/**
 * @brief	Closes and frees space for radix spline index structure
 * @param	rsidx	Radix spline structure
 */
void radixsplineClose(radixspline *rsidx) {
    splineClose(rsidx->spl);
    free(rsidx->spl);
    free(rsidx->table);
}

/************************************************************embedDB.c************************************************************/
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

/************************************************************only-include-duplicate.c************************************************************/
              
/************************************************************spline.c************************************************************/
/******************************************************************************/
/**
 * @file        spline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of spline.
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

/**
 * @brief   Initialize a spline structure with given maximum size and error.
 * @param   spl        Spline structure
 * @param   size       Maximum size of spline
 * @param   maxError   Maximum error allowed in spline
 * @param   keySize    Size of key in bytes
 * @return  Returns 0 if successful and -1 if not
 */
int8_t splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize) {
    if (size < 2) {
#ifdef PRINT_ERRORS
        printf("ERROR: The size of the spline must be at least two points.");
#endif
        return -1;
    }
    uint8_t pointSize = sizeof(uint32_t) + keySize;
    spl->count = 0;
    spl->pointsStartIndex = 0;
    spl->eraseSize = 1;
    spl->size = size;
    spl->maxError = maxError;
    spl->points = (void *)malloc(pointSize * size);
    spl->tempLastPoint = 0;
    spl->keySize = keySize;
    spl->lastKey = malloc(keySize);
    spl->lower = malloc(pointSize);
    spl->upper = malloc(pointSize);
    spl->firstSplinePoint = malloc(pointSize);
    spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief    Check if first line is to the left (counter-clockwise) of the second.
 */
static inline int8_t splineIsLeft(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 > y2 * x1;
}

/**
 * @brief    Check if first line is to the right (clockwise) of the second.
 */
static inline int8_t splineIsRight(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 < y2 * x1;
}

/**
 * @brief   Adds point to spline structure
 * @param   spl     Spline structure
 * @param   key     Data key to be added (must be incrementing)
 * @param   page    Page number for spline point to add
 */
void splineAdd(spline *spl, void *key, uint32_t page) {
    spl->numAddCalls++;
    /* Check if no spline points are currently empty */
    if (spl->numAddCalls == 1) {
        /* Add first point in data set to spline. */
        void *firstPoint = splinePointLocation(spl, 0);
        memcpy(firstPoint, key, spl->keySize);
        memcpy(((int8_t *)firstPoint + spl->keySize), &page, sizeof(uint32_t));
        /* Log first point for wrap around purposes */
        memcpy(spl->firstSplinePoint, key, spl->keySize);
        memcpy(((int8_t *)spl->firstSplinePoint + spl->keySize), &page, sizeof(uint32_t));
        spl->count++;
        memcpy(spl->lastKey, key, spl->keySize);
        return;
    }

    /* Check if there is only one spline point (need to initialize upper and lower limits using 2nd point) */
    if (spl->numAddCalls == 2) {
        /* Initialize upper and lower limits using second (unique) data point */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy(((int8_t *)spl->lower + spl->keySize), &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy(((int8_t *)spl->upper + spl->keySize), &upperPage, sizeof(uint32_t));
        memcpy(spl->lastKey, key, spl->keySize);
        spl->lastLoc = page;
        return;
    }

    /* Skip duplicates */
    uint64_t keyVal = 0, lastKeyVal = 0;
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&lastKeyVal, spl->lastKey, spl->keySize);

    if (keyVal <= lastKeyVal)
        return;

    /* Last point added to spline, check if previous point is temporary - overwrite previous point if temporary */
    if (spl->tempLastPoint != 0) {
        spl->count--;
    }

    uint32_t lastPage = 0;
    uint64_t lastPointKey = 0, upperKey = 0, lowerKey = 0;
    void *lastPointLocation = splinePointLocation(spl, spl->count - 1);
    memcpy(&lastPointKey, lastPointLocation, spl->keySize);
    memcpy(&upperKey, spl->upper, spl->keySize);
    memcpy(&lowerKey, spl->lower, spl->keySize);
    memcpy(&lastPage, (int8_t *)lastPointLocation + spl->keySize, sizeof(uint32_t));

    uint64_t xdiff, upperXDiff, lowerXDiff = 0;
    uint32_t ydiff, upperYDiff = 0;
    int64_t lowerYDiff = 0; /* This may be negative */

    xdiff = keyVal - lastPointKey;
    ydiff = page - lastPage;
    upperXDiff = upperKey - lastPointKey;
    memcpy(&upperYDiff, (int8_t *)spl->upper + spl->keySize, sizeof(uint32_t));
    upperYDiff -= lastPage;
    lowerXDiff = lowerKey - lastPointKey;
    memcpy(&lowerYDiff, (int8_t *)spl->lower + spl->keySize, sizeof(uint32_t));
    lowerYDiff -= lastPage;

    if (spl->count >= spl->size)
        splineErase(spl, spl->eraseSize);

    /* Check if next point still in error corridor */
    if (splineIsLeft(xdiff, ydiff, upperXDiff, upperYDiff) == 1 ||
        splineIsRight(xdiff, ydiff, lowerXDiff, lowerYDiff) == 1) {
        /* Point is not in error corridor. Add previous point to spline. */
        void *nextSplinePoint = splinePointLocation(spl, spl->count);
        memcpy(nextSplinePoint, spl->lastKey, spl->keySize);
        memcpy((int8_t *)nextSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
        spl->count++;
        spl->tempLastPoint = 0;

        /* Update upper and lower limits. */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
    } else {
        /* Check if must update upper or lower limits */

        /* Upper limit */
        if (splineIsLeft(upperXDiff, upperYDiff, xdiff, page + spl->maxError - lastPage) == 1) {
            memcpy(spl->upper, key, spl->keySize);
            uint32_t upperPage = page + spl->maxError;
            memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
        }

        /* Lower limit */
        if (splineIsRight(lowerXDiff, lowerYDiff, xdiff, (page < spl->maxError ? 0 : page - spl->maxError) - lastPage) == 1) {
            memcpy(spl->lower, key, spl->keySize);
            uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
            memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        }
    }

    spl->lastLoc = page;

    /* Add last key on spline if not already there. */
    /* This will get overwritten the next time a new spline point is added */
    memcpy(spl->lastKey, key, spl->keySize);
    void *tempSplinePoint = splinePointLocation(spl, spl->count);
    memcpy(tempSplinePoint, spl->lastKey, spl->keySize);
    memcpy((int8_t *)tempSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
    spl->count++;

    spl->tempLastPoint = 1;
}

/**
 * @brief   Removes points from the spline
 * @param   spl         The spline structure to search
 * @param   numPoints   The number of points to remove from the spline
 * @return  Returns zero if successful and one if not
 */
int splineErase(spline *spl, uint32_t numPoints) {
    /* If the user tries to delete more points than they allocated or deleting would only leave one spline point */
    if (numPoints > spl->count || spl->count - numPoints == 1)
        return 1;
    if (numPoints == 0)
        return 0;

    spl->count -= numPoints;
    spl->pointsStartIndex = (spl->pointsStartIndex + numPoints) % spl->size;
    if (spl->count == 0)
        spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief	Builds a spline structure given a sorted data set. GreedySplineCorridor
 * implementation from "Smooth interpolating histograms with error guarantees"
 * (BNCOD'08) by T. Neumann and S. Michel.
 * @param	spl			Spline structure
 * @param	data		Array of sorted data
 * @param	size		Number of values in array
 * @param	maxError	Maximum error for each spline
 */
void splineBuild(spline *spl, void **data, id_t size, size_t maxError) {
    spl->maxError = maxError;

    for (id_t i = 0; i < size; i++) {
        void *key;
        memcpy(&key, data + i, sizeof(void *));
        splineAdd(spl, key, i);
    }
}

/**
 * @brief    Print a spline structure.
 * @param    spl     Spline structure
 */
void splinePrint(spline *spl) {
    if (spl == NULL) {
        printf("No spline to print.\n");
        return;
    }
    printf("Spline max error (%i):\n", spl->maxError);
    printf("Spline points (%li):\n", spl->count);
    uint64_t keyVal = 0;
    uint32_t page = 0;
    for (id_t i = 0; i < spl->count; i++) {
        void *point = splinePointLocation(spl, i);
        memcpy(&keyVal, point, spl->keySize);
        memcpy(&page, (int8_t *)point + spl->keySize, sizeof(uint32_t));
        printf("[%i]: (%li, %i)\n", i, keyVal, page);
    }
    printf("\n");
}

/**
 * @brief    Return spline structure size in bytes.
 * @param    spl     Spline structure
 * @return   size of the spline in bytes
 */
uint32_t splineSize(spline *spl) {
    return sizeof(spline) + (spl->size * (spl->keySize + sizeof(uint32_t)));
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	arr			Array of spline points to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t pointsBinarySearch(spline *spl, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        // If mid is zero, then low = 0 and high = 1. Therefore there is only one spline segment and we return 1, the upper bound.
        if (mid == 0) {
            return 1;
        }

        void *midSplinePoint = splinePointLocation(spl, mid);
        void *midSplineMinusOnePoint = splinePointLocation(spl, mid - 1);

        if (compareKey(midSplinePoint, key) >= 0 && compareKey(midSplineMinusOnePoint, key) <= 0)
            return mid;

        if (compareKey(midSplinePoint, key) > 0)
            return pointsBinarySearch(spl, low, mid - 1, key, compareKey);

        return pointsBinarySearch(spl, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Estimate the page number of a given key
 * @param	spl			The spline structure to search
 * @param	key			The key to search for
 * @param	compareKey	Function to compare keys
 * @param	loc			A return value for the best estimate of which page the key is on
 * @param	low			A return value for the smallest page that it could be on
 * @param	high		A return value for the largest page it could be on
 */
void splineFind(spline *spl, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    size_t pointIdx;
    uint64_t keyVal = 0, smallestKeyVal = 0, largestKeyVal = 0;
    void *smallestSplinePoint = splinePointLocation(spl, 0);
    void *largestSplinePoint = splinePointLocation(spl, spl->count - 1);
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&smallestKeyVal, smallestSplinePoint, spl->keySize);
    memcpy(&largestKeyVal, largestSplinePoint, spl->keySize);

    if (compareKey(key, splinePointLocation(spl, 0)) < 0 || spl->count <= 1) {
        // Key is smaller than any we have on record
        uint32_t lowEstimate, highEstimate, locEstimate = 0;
        memcpy(&lowEstimate, (int8_t *)spl->firstSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(&highEstimate, (int8_t *)smallestSplinePoint + spl->keySize, sizeof(uint32_t));
        locEstimate = (lowEstimate + highEstimate) / 2;

        memcpy(loc, &locEstimate, sizeof(uint32_t));
        memcpy(low, &lowEstimate, sizeof(uint32_t));
        memcpy(high, &highEstimate, sizeof(uint32_t));
        return;
    } else if (compareKey(key, splinePointLocation(spl, spl->count - 1)) > 0) {
        memcpy(loc, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(low, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(high, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        return;
    } else {
        // Perform a binary seach to find the spline point above the key we're looking for
        pointIdx = pointsBinarySearch(spl, 0, spl->count - 1, key, compareKey);
    }

    // Interpolate between two spline points
    void *downKey = splinePointLocation(spl, pointIdx - 1);
    uint32_t downPage = 0;
    memcpy(&downPage, (int8_t *)downKey + spl->keySize, sizeof(uint32_t));
    void *upKey = splinePointLocation(spl, pointIdx);
    uint32_t upPage = 0;
    memcpy(&upPage, (int8_t *)upKey + spl->keySize, sizeof(uint32_t));
    uint64_t downKeyVal = 0, upKeyVal = 0;
    memcpy(&downKeyVal, downKey, spl->keySize);
    memcpy(&upKeyVal, upKey, spl->keySize);

    // Estimate location as page number
    // Keydiff * slope + y
    id_t locationEstimate = (id_t)((keyVal - downKeyVal) * (upPage - downPage) / (long double)(upKeyVal - downKeyVal)) + downPage;
    memcpy(loc, &locationEstimate, sizeof(id_t));

    // Set error bounds based on maxError from spline construction
    id_t lowEstiamte = (spl->maxError > locationEstimate) ? 0 : locationEstimate - spl->maxError;
    memcpy(low, &lowEstiamte, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(spl, spl->count - 1);
    uint32_t lastSplinePointPage = 0;
    memcpy(&lastSplinePointPage, (int8_t *)lastSplinePoint + spl->keySize, sizeof(uint32_t));
    id_t highEstimate = (locationEstimate + spl->maxError > lastSplinePointPage) ? lastSplinePointPage : locationEstimate + spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief    Free memory allocated for spline structure.
 * @param    spl        Spline structure
 */
void splineClose(spline *spl) {
    free(spl->points);
    free(spl->lastKey);
    free(spl->lower);
    free(spl->upper);
    free(spl->firstSplinePoint);
}

/**
 * @brief   Returns a pointer to the location of the specified spline point in memory. Note that this method does not check if there is a point there, so it may be garbage data.
 * @param   spl         The spline structure that contains the points
 * @param   pointIndex  The index of the point to return a pointer to
 */
void *splinePointLocation(spline *spl, size_t pointIndex) {
    return (int8_t *)spl->points + (((pointIndex + spl->pointsStartIndex) % spl->size) * (spl->keySize + sizeof(uint32_t)));
}

/************************************************************only-include.c************************************************************/
       


/************************************************************advancedQueries.c************************************************************/
/******************************************************************************/
/**
 * @file        advancedQueries.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the advanced query interface for EmbedDB
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

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    // Check the sign bits of the most significant bytes
    int sign1 = bytes1[numBytes - 1] & 0x80;
    int sign2 = bytes2[numBytes - 1] & 0x80;

    if (sign1 != sign2) {
        // Different signs, negative number is smaller
        return (sign1 ? -1 : 1);
    }

    // Same sign, perform regular byte-wise comparison
    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	0 or 1 to indicate if inequality is true
 */
int8_t compare(void* a, uint8_t operation, void* b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void* num1, const void* num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
    switch (operation) {
        case SELECT_GT:
            return compFunc(a, b, numBytes) > 0;
        case SELECT_LT:
            return compFunc(a, b, numBytes) < 0;
        case SELECT_GTE:
            return compFunc(a, b, numBytes) >= 0;
        case SELECT_LTE:
            return compFunc(a, b, numBytes) <= 0;
        case SELECT_EQ:
            return compFunc(a, b, numBytes) == 0;
        case SELECT_NEQ:
            return compFunc(a, b, numBytes) != 0;
        default:
            return 0;
    }
}

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator) {
    return operator->next(operator);
}

void initTableScan(embedDBOperator* operator) {
    if (operator->input != NULL) {
#ifdef PRINT_ERRORS
        printf("WARNING: TableScan operator should not have an input operator\n");
#endif
    }
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: TableScan operator needs its schema defined\n");
#endif
        return;
    }

    if (operator->schema->numCols<2) {
#ifdef PRINT_ERRORS
        printf("ERROR: When creating a table scan, you must include at least two columns: one for the key and one for the data from the iterator\n");
#endif
        return;
    }

    // Check that the provided key schema matches what is in the state
    embedDBState* embedDBstate = (embedDBState*)(((void**)operator->state)[0]);
    if (operator->schema->columnSizes[0] <= 0 || abs(operator->schema->columnSizes[0]) != embedDBstate->keySize) {
#ifdef PRINT_ERRORS
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
#endif
        return;
    }
    if (getRecordSizeFromSchema(operator->schema) != (embedDBstate->keySize + embedDBstate->dataSize)) {
#ifdef PRINT_ERRORS
        printf("ERROR: Size of provided schema doesn't match the size that will be returned by the provided iterator\n");
#endif
        return;
    }

    // Init buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextTableScan(embedDBOperator* operator) {
    // Check that a schema was set
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Must provide a base schema for a table scan operator\n");
#endif
        return 0;
    }

    // Get next record
    embedDBState* state = (embedDBState*)(((void**)operator->state)[0]);
    embedDBIterator* it = (embedDBIterator*)(((void**)operator->state)[1]);
    if (!embedDBNext(state, it, operator->recordBuffer, (int8_t*)operator->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(embedDBOperator* operator) {
    embedDBFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
    free(operator->state);
    operator->state = NULL;
}

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: All parameters must be provided to create a TableScan operator\n");
#endif
        return NULL;
    }

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }

    operator->state = malloc(2 * sizeof(void*));
    if (operator->state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }
    memcpy(operator->state, &state, sizeof(void*));
    memcpy((int8_t*)operator->state + sizeof(void*), &it, sizeof(void*));

    operator->schema = copySchema(baseSchema);
    operator->input = NULL;
    operator->recordBuffer = NULL;

    operator->init = initTableScan;
    operator->next = nextTableScan;
    operator->close = closeTableScan;

    return operator;
}

void initProjection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Get state
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection schema\n");
#endif
            return;
        }
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection while building schema\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
        }
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextProjection(embedDBOperator* operator) {
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Get next record
    if (operator->input->next(operator->input)) {
        for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            if (col == cols[nextProjCol]) {
                memcpy((int8_t*)operator->recordBuffer + nextProjColPos, (int8_t*)operator->input->recordBuffer + curColPos, colSize);
                nextProjColPos += colSize;
                nextProjCol++;
            }
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. Zero indexed. Column indexes must be strictly increasing i.e. columns must stay in the same order, can only remove columns from input
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols) {
    // Ensure column numbers are strictly increasing
    uint8_t lastCol = cols[0];
    for (uint8_t i = 1; i < numCols; i++) {
        if (cols[i] <= lastCol) {
#ifdef PRINT_ERRORS
            printf("ERROR: Columns in a projection must be strictly ascending for performance reasons");
#endif
            return NULL;
        }
        lastCol = cols[i];
    }
    // Create state
    uint8_t* state = malloc(numCols + 1);
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }
    state[0] = numCols;
    memcpy(state + 1, cols, numCols);

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initProjection;
    operator->next = nextProjection;
    operator->close = closeProjection;

    return operator;
}

void initSelection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = copySchema(operator->input->schema);
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextSelection(embedDBOperator* operator) {
    embedDBSchema* schema = operator->input->schema;

    int8_t colNum = *(int8_t*)operator->state;
    uint16_t colPos = getColOffsetFromSchema(schema, colNum);
    int8_t operation = *((int8_t*)operator->state + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (operator->input->next(operator->input)) {
        void* colData = (int8_t*)operator->input->recordBuffer + colPos;

        if (compare(colData, operation, *(void**)((int8_t*)operator->state + 2), isSigned, colSize)) {
            memcpy(operator->recordBuffer, operator->input->recordBuffer, getRecordSizeFromSchema(operator->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal) {
    int8_t* state = malloc(2 + sizeof(void*));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    state[0] = colNum;
    state[1] = operation;
    memcpy(state + 2, &compVal, sizeof(void*));

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initSelection;
    operator->next = nextSelection;
    operator->close = closeSelection;

    return operator;
}

/**
 * @brief	A private struct to hold the state of the aggregate operator
 */
struct aggregateInfo {
    int8_t (*groupfunc)(const void* lastRecord, const void* record);  // Function that determins if both records are in the same group
    embedDBAggregateFunc* functions;                                  // An array of aggregate functions
    uint32_t functionsLength;                                         // The length of the functions array
    void* lastRecordBuffer;                                           // Buffer for the last record read by input->next
    uint16_t bufferSize;                                              // Size of the input buffer (and lastRecordBuffer)
    int8_t isLastRecordUsable;                                        // Is the data in lastRecordBuffer usable for checking if the recently read record is in the same group? Is set to 0 at start, and also after the last record
};

void initAggregate(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Aggregate operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    struct aggregateInfo* state = operator->state;
    state->isLastRecordUsable = 0;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        operator->schema->numCols = state->functionsLength;
        operator->schema->columnSizes = malloc(state->functionsLength);
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < state->functionsLength; i++) {
            operator->schema->columnSizes[i] = state->functions[i].colSize;
            state->functions[i].colNum = i;
        }
    }

    // Init buffers
    state->bufferSize = getRecordSizeFromSchema(operator->input->schema);
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
    if (state->lastRecordBuffer == NULL) {
        state->lastRecordBuffer = malloc(state->bufferSize);
        if (state->lastRecordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
}

int8_t nextAggregate(embedDBOperator* operator) {
    struct aggregateInfo* state = operator->state;
    embedDBOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].reset != NULL) {
            state->functions[i].reset(state->functions + i, input->schema);
        }
    }

    int8_t recordsInGroup = 0;

    // Check flag used to indicate whether the last record read has been added to a group
    if (state->isLastRecordUsable) {
        recordsInGroup = 1;
        for (int i = 0; i < state->functionsLength; i++) {
            if (state->functions[i].add != NULL) {
                state->functions[i].add(state->functions + i, input->schema, state->lastRecordBuffer);
            }
        }
    }

    int8_t exitType = 0;
    while (input->next(input)) {
        // Check if record is in the same group as the last record
        if (!state->isLastRecordUsable || state->groupfunc(state->lastRecordBuffer, input->recordBuffer)) {
            recordsInGroup = 1;
            for (int i = 0; i < state->functionsLength; i++) {
                if (state->functions[i].add != NULL) {
                    state->functions[i].add(state->functions + i, input->schema, input->recordBuffer);
                }
            }
        } else {
            exitType = 1;
            break;
        }

        // Save this record
        memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);
        state->isLastRecordUsable = 1;
    }

    if (!recordsInGroup) {
        return 0;
    }

    if (exitType == 0) {
        // Exited because ran out of records, so all read records have been added to a group
        state->isLastRecordUsable = 0;
    }

    // Perform final compute on all functions
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].compute != NULL) {
            state->functions[i].compute(state->functions + i, operator->schema, operator->recordBuffer, state->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);

    return 1;
}

void closeAggregate(embedDBOperator* operator) {
    operator->input->close(operator->input);
    operator->input = NULL;
    embedDBFreeSchema(&operator->schema);
    free(((struct aggregateInfo*)operator->state)->lastRecordBuffer);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength) {
    struct aggregateInfo* state = malloc(sizeof(struct aggregateInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    state->groupfunc = groupfunc;
    state->functions = functions;
    state->functionsLength = functionsLength;
    state->lastRecordBuffer = NULL;

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initAggregate;
    operator->next = nextAggregate;
    operator->close = closeAggregate;

    return operator;
}

struct keyJoinInfo {
    embedDBOperator* input2;
    int8_t firstCall;
};

void initKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The first columns of the two tables must be the key and must be the same size. Make sure you haven't projected them out.\n");
#endif
        return;
    }

    // Setup schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        operator->schema->numCols = schema1->numCols + schema2->numCols;
        operator->schema->columnSizes = malloc(operator->schema->numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        memcpy(operator->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(operator->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
    }

    // Allocate recordBuffer
    operator->recordBuffer = malloc(getRecordSizeFromSchema(operator->schema));
    if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
        return;
    }

    state->firstCall = 1;
}

int8_t nextKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // We've already used this match
    void* record1 = input1->recordBuffer;
    void* record2 = input2->recordBuffer;

    int8_t colSize = abs(schema1->columnSizes[0]);

    if (state->firstCall) {
        state->firstCall = 0;

        if (!input1->next(input1) || !input2->next(input2)) {
            // If this case happens, you goofed, but I'll handle it anyway
            return 0;
        }
        goto check;
    }

    while (1) {
        // Advance the input with the smaller value
        int8_t comp = compareUnsignedNumbers(record1, record2, colSize);
        if (comp == 0) {
            // Move both forward because if they match at this point, they've already been matched
            if (!input1->next(input1) || !input2->next(input2)) {
                return 0;
            }
        } else if (comp < 0) {
            // Move record 1 forward
            if (!input1->next(input1)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        } else {
            // Move record 2 forward
            if (!input2->next(input2)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        }

    check:
        // See if these records join
        if (compareUnsignedNumbers(record1, record2, colSize) == 0) {
            // Copy both records into the output
            uint16_t record1Size = getRecordSizeFromSchema(schema1);
            memcpy(operator->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t*)operator->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    input1->close(input1);
    input2->close(input2);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2) {
    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }

    struct keyJoinInfo* state = malloc(sizeof(struct keyJoinInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }
    state->input2 = input2;

    operator->input = input1;
    operator->state = state;
    operator->recordBuffer = NULL;
    operator->schema = NULL;
    operator->init = initKeyJoin;
    operator->next = nextKeyJoin;
    operator->close = closeKeyJoin;

    return operator;
}

void countReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    *(uint32_t*)aggFunc->state = 0;
}

void countAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    (*(uint32_t*)aggFunc->state)++;
}

void countCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(uint32_t));
}

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate() {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = countReset;
    aggFunc->add = countAdd;
    aggFunc->compute = countCompute;
    aggFunc->state = malloc(sizeof(uint32_t));
    aggFunc->colSize = 4;
    return aggFunc;
}

void sumReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    if (abs(inputSchema->columnSizes[*((uint8_t*)aggFunc->state + sizeof(int64_t))]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    *(int64_t*)aggFunc->state = 0;
}

void sumAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    uint8_t colNum = *((uint8_t*)aggFunc->state + sizeof(int64_t));
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)recordBuffer + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t*)aggFunc->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t*)aggFunc->state) += val;
    }
}

void sumCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(int64_t));
}

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = sumReset;
    aggFunc->add = sumAdd;
    aggFunc->compute = sumCompute;
    aggFunc->state = malloc(sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t*)aggFunc->state + sizeof(int64_t)) = colNum;
    aggFunc->colSize = -8;
    return aggFunc;
}

struct minMaxState {
    uint8_t colNum;  // Which column of input to use
    void* current;   // The value currently regarded as the min/max
};

void minReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for min aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0xff, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as -1, not MAX_INT
        memset((int8_t*)state->current + colSize - 1, 0x7f, 1);
    }
}

void minAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_LT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

void minMaxCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), ((struct minMaxState*)aggFunc->state)->current, abs(outputSchema->columnSizes[aggFunc->colNum]));
}

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = minReset;
    aggFunc->add = minAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

void maxReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for max aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as 0, not MIN_INT
        memset((int8_t*)state->current + colSize - 1, 0x80, 1);
    }
}

void maxAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_GT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = maxReset;
    aggFunc->add = maxAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

struct avgState {
    uint8_t colNum;   // Column to take avg of
    int8_t isSigned;  // Is input column signed?
    uint32_t count;   // Count of records seen in group so far
    int64_t sum;      // Sum of records seen in group so far
};

void avgReset(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct avgState* state = aggFunc->state;
    if (abs(inputSchema->columnSizes[state->colNum]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    state->count = 0;
    state->sum = 0;
    state->isSigned = embedDB_IS_COL_SIGNED(inputSchema->columnSizes[state->colNum]);
}

void avgAdd(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct avgState* state = aggFunc->state;
    uint8_t colNum = state->colNum;
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)record + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        state->sum += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        val += (uint64_t)state->sum;
        memcpy(&state->sum, &val, sizeof(uint64_t));
    }
    state->count++;
}

void avgCompute(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    struct avgState* state = aggFunc->state;
    if (aggFunc->colSize == 8) {
        double avg = state->sum / (double)state->count;
        if (state->isSigned) {
            avg = state->sum / (double)state->count;
        } else {
            avg = (uint64_t)state->sum / (double)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(double));
    } else {
        float avg;
        if (state->isSigned) {
            avg = state->sum / (float)state->count;
        } else {
            avg = (uint64_t)state->sum / (float)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(float));
    }
}

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    struct avgState* state = malloc(sizeof(struct avgState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    aggFunc->state = state;
    if (outputFloatSize > 8 || (outputFloatSize < 8 && outputFloatSize > 4)) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 8.");
#endif
        aggFunc->colSize = 8;
    } else if (outputFloatSize < 4) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 4.");
#endif
        aggFunc->colSize = 4;
    } else {
        aggFunc->colSize = outputFloatSize;
    }
    aggFunc->reset = avgReset;
    aggFunc->add = avgAdd;
    aggFunc->compute = avgCompute;

    return aggFunc;
}

/**
 * @brief	Completely free a chain of functions recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator) {
    if ((*operator)->input != NULL) {
        embedDBFreeOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->state != NULL) {
        free((*operator)->state);
        (*operator)->state = NULL;
    }
    if ((*operator)->schema != NULL) {
        embedDBFreeSchema(&(*operator)->schema);
    }
    if ((*operator)->recordBuffer != NULL) {
        free((*operator)->recordBuffer);
        (*operator)->recordBuffer = NULL;
    }
    free(*operator);
    (*operator) = NULL;
}


/************************************************************utilityFunctions.c************************************************************/
/******************************************************************************/
/**
 * @file        utilityFunctions.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       This file contains some utility functions to be used with embedDB.
 *              These include functions required to use the bitmap option, and a
 *              comparator for comparing keys. They can be modified or implemented
 *              differently depending on the application.
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

embedDBState *defaultInitializedState() {
    embedDBState *state = calloc(1, sizeof(embedDBState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("Failed to allocate memory for state.\n");
#endif
        return NULL;
    }

    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

    /* Address level parameters */
    state->numDataPages = 20000;  // Enough for 620,000 records
    state->numIndexPages = 44;    // Enough for 676,544 records
    state->eraseSizeInPages = 4;

    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Initialize embedDB structure */
    if (embedDBInit(state, 1) != 0) {
#ifdef PRINT_ERRORS
        printf("Initialization error.\n");
#endif
        free(state->buffer);
        free(state->fileInterface);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state);
        return NULL;
    }

    return state;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and max value. */
void buildBitmapInt8FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

int8_t inBitmapInt8(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt16(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt16(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint64_t *)bm = UINT64_MAX; /* Everything */
        return;
    } else {
        uint64_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt64(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint64_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt64(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint64_t *)bm = maxMap;
                return;
            }
        }
        *(uint64_t *)bm = minMap & maxMap;
    }
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

int8_t int64Comparator(void *a, void *b) {
    int64_t result = *((int64_t *)a) - *((int64_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

typedef struct {
    char *filename;
    FILE *file;
} FILE_INFO;

void *setupFile(char *filename) {
    FILE_INFO *fileInfo = malloc(sizeof(FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->file = NULL;
    return fileInfo;
}

void tearDownFile(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo->file != NULL)
        fclose(fileInfo->file);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageSize * pageNum, SEEK_SET);
    return fread(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageNum * pageSize, SEEK_SET);
    return fwrite(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_CLOSE(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fclose(fileInfo->file);
    fileInfo->file = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    return fflush(fileInfo->file) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "w+b");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "r+b");
    } else {
        return 0;
    }

    if (fileInfo->file == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getFileInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}

/************************************************************only-include-inline-comments.c************************************************************/
     // foo  // a + b?   /* I find your lack of faith disturbing*/    // I have a bad feeling about this   // /* Stay on target. */   // alsdfkjsdlf     //

/************************************************************only-include.c************************************************************/
       


/************************************************************radixspline.c************************************************************/
/******************************************************************************/
/**
 * @file        radixspline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of radix spline for embedded devices.
 *              Based on "RadixSpline: a single-pass learned index" by
 *              A. Kipf, R. Marcus, A. van Renen, M. Stoian, A. Kemper,
 *              T. Kraska, and T. Neumann
 *              https://github.com/learnedsystems/RadixSpline
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

/**
 * @brief   Build the radix table
 * @param   rsdix       Radix spline structure
 * @param   keys        Data points to be indexed
 * @param   numKeys     Number of data items
 */
void radixsplineBuild(radixspline *rsidx, void **keys, uint32_t numKeys) {
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;

    for (uint32_t i = 0; i < numKeys; i++) {
        void *key;
        memcpy(&key, keys + i, sizeof(void *));
        radixsplineAddPoint(rsidx, key, i);
    }
}

/**
 * @brief   Rebuild the radix table with new shift amount
 * @param   rsdix       Radix spline structure
 * @param   spl         Spline structure
 * @param   radixSize   Size of radix table
 * @param   shiftAmount Difference in shift amount between current radix table and desired radix table
 */
void radixsplineRebuild(radixspline *rsidx, int8_t radixSize, int8_t shiftAmount) {
    // radixsplinePrint(rsidx);
    rsidx->prevPrefix = rsidx->prevPrefix >> shiftAmount;

    for (id_t i = 0; i < rsidx->size / pow(2, shiftAmount); i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, (int8_t *)rsidx->table + (i << shiftAmount) * rsidx->keySize, rsidx->keySize);
    }
    uint64_t maxKey = UINT64_MAX;
    for (id_t i = rsidx->size / pow(2, shiftAmount); i < rsidx->size; i++) {
        memcpy((int8_t *)rsidx->table + i * rsidx->keySize, &maxKey, rsidx->keySize);
    }
}

/**
 * @brief	Add a point to be indexed by the radix spline structure
 * @param	rsdix	Radix spline structure
 * @param	key		New point to be indexed by radix spline
 * @param   page    Page number for spline point to add
 */
void radixsplineAddPoint(radixspline *rsidx, void *key, uint32_t page) {
    splineAdd(rsidx->spl, key, page);

    // Return if not using Radix table
    if (rsidx->radixSize == 0) {
        return;
    }

    // Determine if need to update radix table based on adding point to spline
    if (rsidx->spl->count <= rsidx->pointsSeen)
        return;  // Nothing to do

    // take the last point that was added to spline
    key = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);

    // Initialize table and minKey on first key added
    if (rsidx->pointsSeen == 0) {
        rsidx->table = malloc(sizeof(id_t) * rsidx->size);
        uint64_t maxKey = UINT64_MAX;
        for (int32_t counter = 1; counter < rsidx->size; counter++) {
            memcpy(rsidx->table + counter, &maxKey, sizeof(id_t));
        }
        rsidx->minKey = key;
    }

    // Check if prefix will fit in radix table
    uint64_t keyDiff;
    if (rsidx->keySize <= 4) {
        uint32_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    } else {
        uint64_t keyVal = 0, minKeyVal = 0;
        memcpy(&keyVal, key, rsidx->keySize);
        memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
        keyDiff = keyVal - minKeyVal;
    }

    uint8_t bitsToRepresentKey = ceil(log2f((float)keyDiff));
    int8_t newShiftSize;
    if (bitsToRepresentKey < rsidx->radixSize) {
        newShiftSize = 0;
    } else {
        newShiftSize = bitsToRepresentKey - rsidx->radixSize;
    }

    // if the shift size changes, need to remake table from scratch using new shift size
    if (newShiftSize > rsidx->shiftSize) {
        radixsplineRebuild(rsidx, rsidx->radixSize, newShiftSize - rsidx->shiftSize);
        rsidx->shiftSize = newShiftSize;
    }

    id_t prefix = keyDiff >> rsidx->shiftSize;
    if (prefix != rsidx->prevPrefix) {
        // Make all new rows in the radix table point to the last point seen
        for (id_t pr = rsidx->prevPrefix; pr < prefix; pr++) {
            memcpy(rsidx->table + pr, &rsidx->pointsSeen, sizeof(id_t));
        }

        rsidx->prevPrefix = prefix;
    }

    memcpy(rsidx->table + prefix, &rsidx->pointsSeen, sizeof(id_t));

    rsidx->pointsSeen++;
}

/**
 * @brief	Initialize an empty radix spline index of given size
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInit(radixspline *rsidx, spline *spl, int8_t radixSize, uint8_t keySize) {
    rsidx->spl = spl;
    rsidx->radixSize = radixSize;
    rsidx->keySize = keySize;
    rsidx->shiftSize = 0;
    rsidx->size = pow(2, radixSize);

    /* Determine the prefix size (shift bits) based on min and max keys */
    rsidx->minKey = spl->points;

    /* Initialize points seen */
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	rsidx		Array to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixBinarySearch(radixspline *rsidx, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    void *arr = rsidx->spl->points;

    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;
        void *midKey = splinePointLocation(rsidx->spl, mid);
        void *midKeyMinusOne = splinePointLocation(rsidx->spl, mid - 1);
        if (compareKey(midKey, key) >= 0 && compareKey(midKeyMinusOne, key) <= 0)
            return mid;

        if (compareKey(midKey, key) > 0)
            return radixBinarySearch(rsidx, low, mid - 1, key, compareKey);

        return radixBinarySearch(rsidx, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Initialize and build a radix spline index of given size using pre-built spline structure.
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keys		Keys to be indexed
 * @param	numKeys 	Number of keys in `keys`
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInitBuild(radixspline *rsidx, spline *spl, uint32_t radixSize, void **keys, uint32_t numKeys, uint8_t keySize) {
    radixsplineInit(rsidx, spl, radixSize, keySize);
    radixsplineBuild(rsidx, keys, numKeys);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using radix table.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntry(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    /* Use radix table to find range of spline points */

    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    uint32_t prefix = (keyVal - minKeyVal) >> rsidx->shiftSize;

    uint32_t begin, end;

    // Determine end, use next higher radix point if within bounds, unless key is exactly prefix
    if (keyVal == ((uint64_t)prefix << rsidx->shiftSize)) {
        memcpy(&end, rsidx->table + prefix, sizeof(id_t));
    } else {
        if ((prefix + 1) < rsidx->size) {
            memcpy(&end, rsidx->table + (prefix + 1), sizeof(id_t));
        } else {
            memcpy(&end, rsidx->table + (rsidx->size - 1), sizeof(id_t));
        }
    }

    // check end is in bounds since radix table values are initiated to INT_MAX
    if (end >= rsidx->spl->count) {
        end = rsidx->spl->count - 1;
    }

    // use previous adjacent radix point for lower bounds
    if (prefix == 0) {
        begin = 0;
    } else {
        memcpy(&begin, rsidx->table + (prefix - 1), sizeof(id_t));
    }

    return radixBinarySearch(rsidx, begin, end, key, compareKey);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing key using binary search.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param	compareKey	Function to compare keys
 * @return  Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t radixsplineGetEntryBinarySearch(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    return radixBinarySearch(rsidx, 0, rsidx->spl->count - 1, key, compareKey);
}

/**
 * @brief	Estimate location of key in data using spline points.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 * @param	compareKey	Function to compare keys
 * @return	Estimated page number that contains key
 */
size_t radixsplineEstimateLocation(radixspline *rsidx, void *key, int8_t compareKey(void *, void *)) {
    uint64_t keyVal = 0, minKeyVal = 0;
    memcpy(&keyVal, key, rsidx->keySize);
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);

    if (keyVal < minKeyVal)
        return 0;

    size_t index;
    if (rsidx->radixSize == 0) {
        /* Get index using binary search */
        index = radixsplineGetEntryBinarySearch(rsidx, key, compareKey);
    } else {
        /* Get index using radix table */
        index = radixsplineGetEntry(rsidx, key, compareKey);
    }

    /* Interpolate between two spline points */
    void *down = splinePointLocation(rsidx->spl, index - 1);
    void *up = splinePointLocation(rsidx->spl, index);

    uint64_t downKey = 0, upKey = 0;
    memcpy(&downKey, down, rsidx->keySize);
    memcpy(&upKey, up, rsidx->keySize);

    uint32_t upPage = 0;
    uint32_t downPage = 0;
    memcpy(&upPage, (int8_t *)up + rsidx->spl->keySize, sizeof(uint32_t));
    memcpy(&downPage, (int8_t *)down + rsidx->spl->keySize, sizeof(uint32_t));

    /* Keydiff * slope + y */
    uint32_t estimatedPage = (uint32_t)((keyVal - downKey) * (upPage - downPage) / (long double)(upKey - downKey)) + downPage;
    return estimatedPage > upPage ? upPage : estimatedPage;
}

/**
 * @brief	Finds a value using index. Returns predicted location and low and high error bounds.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param   compareKey  Function to compare keys
 * @param	loc		    Return of predicted location
 * @param	low		    Return of low bound on predicted location
 * @param	high	    Return of high bound on predicted location
 */
void radixsplineFind(radixspline *rsidx, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    /* Estimate location */
    id_t locationEstimate = radixsplineEstimateLocation(rsidx, key, compareKey);
    memcpy(loc, &locationEstimate, sizeof(id_t));

    /* Set error bounds based on maxError from spline construction */
    id_t lowEstimate = (rsidx->spl->maxError > locationEstimate) ? 0 : locationEstimate - rsidx->spl->maxError;
    memcpy(low, &lowEstimate, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(rsidx->spl, rsidx->spl->count - 1);
    uint64_t lastKey = 0;
    memcpy(&lastKey, lastSplinePoint, rsidx->keySize);
    id_t highEstimate = (locationEstimate + rsidx->spl->maxError > lastKey) ? lastKey : locationEstimate + rsidx->spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief	Print radix spline structure.
 * @param	rsidx	Radix spline structure
 */
void radixsplinePrint(radixspline *rsidx) {
    if (rsidx == NULL || rsidx->radixSize == 0) {
        printf("No radix spline index to print.\n");
        return;
    }

    printf("Radix table (%u):\n", rsidx->size);
    // for (id_t i=0; i < 20; i++)
    uint64_t minKeyVal = 0;
    id_t tableVal;
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
    for (id_t i = 0; i < rsidx->size; i++) {
        printf("[" TO_BINARY_PATTERN "] ", TO_BINARY((uint8_t)(i)));
        memcpy(&tableVal, rsidx->table + i, sizeof(id_t));
        printf("(%lu): --> %u\n", (i << rsidx->shiftSize) + minKeyVal, tableVal);
    }
    printf("\n");
}

/**
 * @brief	Returns size of radix spline index structure in bytes
 * @param	rsidx	Radix spline structure
 */
size_t radixsplineSize(radixspline *rsidx) {
    return sizeof(rsidx) + rsidx->size * sizeof(uint32_t) + splineSize(rsidx->spl);
}

/**
 * @brief	Closes and frees space for radix spline index structure
 * @param	rsidx	Radix spline structure
 */
void radixsplineClose(radixspline *rsidx) {
    splineClose(rsidx->spl);
    free(rsidx->spl);
    free(rsidx->table);
}

/************************************************************spline.c************************************************************/
/******************************************************************************/
/**
 * @file        spline.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of spline.
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

/**
 * @brief   Initialize a spline structure with given maximum size and error.
 * @param   spl        Spline structure
 * @param   size       Maximum size of spline
 * @param   maxError   Maximum error allowed in spline
 * @param   keySize    Size of key in bytes
 * @return  Returns 0 if successful and -1 if not
 */
int8_t splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize) {
    if (size < 2) {
#ifdef PRINT_ERRORS
        printf("ERROR: The size of the spline must be at least two points.");
#endif
        return -1;
    }
    uint8_t pointSize = sizeof(uint32_t) + keySize;
    spl->count = 0;
    spl->pointsStartIndex = 0;
    spl->eraseSize = 1;
    spl->size = size;
    spl->maxError = maxError;
    spl->points = (void *)malloc(pointSize * size);
    spl->tempLastPoint = 0;
    spl->keySize = keySize;
    spl->lastKey = malloc(keySize);
    spl->lower = malloc(pointSize);
    spl->upper = malloc(pointSize);
    spl->firstSplinePoint = malloc(pointSize);
    spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief    Check if first line is to the left (counter-clockwise) of the second.
 */
static inline int8_t splineIsLeft(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 > y2 * x1;
}

/**
 * @brief    Check if first line is to the right (clockwise) of the second.
 */
static inline int8_t splineIsRight(uint64_t x1, int64_t y1, uint64_t x2, int64_t y2) {
    return y1 * x2 < y2 * x1;
}

/**
 * @brief   Adds point to spline structure
 * @param   spl     Spline structure
 * @param   key     Data key to be added (must be incrementing)
 * @param   page    Page number for spline point to add
 */
void splineAdd(spline *spl, void *key, uint32_t page) {
    spl->numAddCalls++;
    /* Check if no spline points are currently empty */
    if (spl->numAddCalls == 1) {
        /* Add first point in data set to spline. */
        void *firstPoint = splinePointLocation(spl, 0);
        memcpy(firstPoint, key, spl->keySize);
        memcpy(((int8_t *)firstPoint + spl->keySize), &page, sizeof(uint32_t));
        /* Log first point for wrap around purposes */
        memcpy(spl->firstSplinePoint, key, spl->keySize);
        memcpy(((int8_t *)spl->firstSplinePoint + spl->keySize), &page, sizeof(uint32_t));
        spl->count++;
        memcpy(spl->lastKey, key, spl->keySize);
        return;
    }

    /* Check if there is only one spline point (need to initialize upper and lower limits using 2nd point) */
    if (spl->numAddCalls == 2) {
        /* Initialize upper and lower limits using second (unique) data point */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy(((int8_t *)spl->lower + spl->keySize), &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy(((int8_t *)spl->upper + spl->keySize), &upperPage, sizeof(uint32_t));
        memcpy(spl->lastKey, key, spl->keySize);
        spl->lastLoc = page;
        return;
    }

    /* Skip duplicates */
    uint64_t keyVal = 0, lastKeyVal = 0;
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&lastKeyVal, spl->lastKey, spl->keySize);

    if (keyVal <= lastKeyVal)
        return;

    /* Last point added to spline, check if previous point is temporary - overwrite previous point if temporary */
    if (spl->tempLastPoint != 0) {
        spl->count--;
    }

    uint32_t lastPage = 0;
    uint64_t lastPointKey = 0, upperKey = 0, lowerKey = 0;
    void *lastPointLocation = splinePointLocation(spl, spl->count - 1);
    memcpy(&lastPointKey, lastPointLocation, spl->keySize);
    memcpy(&upperKey, spl->upper, spl->keySize);
    memcpy(&lowerKey, spl->lower, spl->keySize);
    memcpy(&lastPage, (int8_t *)lastPointLocation + spl->keySize, sizeof(uint32_t));

    uint64_t xdiff, upperXDiff, lowerXDiff = 0;
    uint32_t ydiff, upperYDiff = 0;
    int64_t lowerYDiff = 0; /* This may be negative */

    xdiff = keyVal - lastPointKey;
    ydiff = page - lastPage;
    upperXDiff = upperKey - lastPointKey;
    memcpy(&upperYDiff, (int8_t *)spl->upper + spl->keySize, sizeof(uint32_t));
    upperYDiff -= lastPage;
    lowerXDiff = lowerKey - lastPointKey;
    memcpy(&lowerYDiff, (int8_t *)spl->lower + spl->keySize, sizeof(uint32_t));
    lowerYDiff -= lastPage;

    if (spl->count >= spl->size)
        splineErase(spl, spl->eraseSize);

    /* Check if next point still in error corridor */
    if (splineIsLeft(xdiff, ydiff, upperXDiff, upperYDiff) == 1 ||
        splineIsRight(xdiff, ydiff, lowerXDiff, lowerYDiff) == 1) {
        /* Point is not in error corridor. Add previous point to spline. */
        void *nextSplinePoint = splinePointLocation(spl, spl->count);
        memcpy(nextSplinePoint, spl->lastKey, spl->keySize);
        memcpy((int8_t *)nextSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
        spl->count++;
        spl->tempLastPoint = 0;

        /* Update upper and lower limits. */
        memcpy(spl->lower, key, spl->keySize);
        uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        memcpy(spl->upper, key, spl->keySize);
        uint32_t upperPage = page + spl->maxError;
        memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
    } else {
        /* Check if must update upper or lower limits */

        /* Upper limit */
        if (splineIsLeft(upperXDiff, upperYDiff, xdiff, page + spl->maxError - lastPage) == 1) {
            memcpy(spl->upper, key, spl->keySize);
            uint32_t upperPage = page + spl->maxError;
            memcpy((int8_t *)spl->upper + spl->keySize, &upperPage, sizeof(uint32_t));
        }

        /* Lower limit */
        if (splineIsRight(lowerXDiff, lowerYDiff, xdiff, (page < spl->maxError ? 0 : page - spl->maxError) - lastPage) == 1) {
            memcpy(spl->lower, key, spl->keySize);
            uint32_t lowerPage = page < spl->maxError ? 0 : page - spl->maxError;
            memcpy((int8_t *)spl->lower + spl->keySize, &lowerPage, sizeof(uint32_t));
        }
    }

    spl->lastLoc = page;

    /* Add last key on spline if not already there. */
    /* This will get overwritten the next time a new spline point is added */
    memcpy(spl->lastKey, key, spl->keySize);
    void *tempSplinePoint = splinePointLocation(spl, spl->count);
    memcpy(tempSplinePoint, spl->lastKey, spl->keySize);
    memcpy((int8_t *)tempSplinePoint + spl->keySize, &spl->lastLoc, sizeof(uint32_t));
    spl->count++;

    spl->tempLastPoint = 1;
}

/**
 * @brief   Removes points from the spline
 * @param   spl         The spline structure to search
 * @param   numPoints   The number of points to remove from the spline
 * @return  Returns zero if successful and one if not
 */
int splineErase(spline *spl, uint32_t numPoints) {
    /* If the user tries to delete more points than they allocated or deleting would only leave one spline point */
    if (numPoints > spl->count || spl->count - numPoints == 1)
        return 1;
    if (numPoints == 0)
        return 0;

    spl->count -= numPoints;
    spl->pointsStartIndex = (spl->pointsStartIndex + numPoints) % spl->size;
    if (spl->count == 0)
        spl->numAddCalls = 0;
    return 0;
}

/**
 * @brief	Builds a spline structure given a sorted data set. GreedySplineCorridor
 * implementation from "Smooth interpolating histograms with error guarantees"
 * (BNCOD'08) by T. Neumann and S. Michel.
 * @param	spl			Spline structure
 * @param	data		Array of sorted data
 * @param	size		Number of values in array
 * @param	maxError	Maximum error for each spline
 */
void splineBuild(spline *spl, void **data, id_t size, size_t maxError) {
    spl->maxError = maxError;

    for (id_t i = 0; i < size; i++) {
        void *key;
        memcpy(&key, data + i, sizeof(void *));
        splineAdd(spl, key, i);
    }
}

/**
 * @brief    Print a spline structure.
 * @param    spl     Spline structure
 */
void splinePrint(spline *spl) {
    if (spl == NULL) {
        printf("No spline to print.\n");
        return;
    }
    printf("Spline max error (%i):\n", spl->maxError);
    printf("Spline points (%li):\n", spl->count);
    uint64_t keyVal = 0;
    uint32_t page = 0;
    for (id_t i = 0; i < spl->count; i++) {
        void *point = splinePointLocation(spl, i);
        memcpy(&keyVal, point, spl->keySize);
        memcpy(&page, (int8_t *)point + spl->keySize, sizeof(uint32_t));
        printf("[%i]: (%li, %i)\n", i, keyVal, page);
    }
    printf("\n");
}

/**
 * @brief    Return spline structure size in bytes.
 * @param    spl     Spline structure
 * @return   size of the spline in bytes
 */
uint32_t splineSize(spline *spl) {
    return sizeof(spline) + (spl->size * (spl->keySize + sizeof(uint32_t)));
}

/**
 * @brief	Performs a recursive binary search on the spine points for a key
 * @param	arr			Array of spline points to search through
 * @param	low		    Lower search bound (Index of spline point)
 * @param	high	    Higher search bound (Index of spline point)
 * @param	key		    Key to search for
 * @param	compareKey	Function to compare keys
 * @return	Index of spline point that is the upper end of the spline segment that contains the key
 */
size_t pointsBinarySearch(spline *spl, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        // If mid is zero, then low = 0 and high = 1. Therefore there is only one spline segment and we return 1, the upper bound.
        if (mid == 0) {
            return 1;
        }

        void *midSplinePoint = splinePointLocation(spl, mid);
        void *midSplineMinusOnePoint = splinePointLocation(spl, mid - 1);

        if (compareKey(midSplinePoint, key) >= 0 && compareKey(midSplineMinusOnePoint, key) <= 0)
            return mid;

        if (compareKey(midSplinePoint, key) > 0)
            return pointsBinarySearch(spl, low, mid - 1, key, compareKey);

        return pointsBinarySearch(spl, mid + 1, high, key, compareKey);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Estimate the page number of a given key
 * @param	spl			The spline structure to search
 * @param	key			The key to search for
 * @param	compareKey	Function to compare keys
 * @param	loc			A return value for the best estimate of which page the key is on
 * @param	low			A return value for the smallest page that it could be on
 * @param	high		A return value for the largest page it could be on
 */
void splineFind(spline *spl, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high) {
    size_t pointIdx;
    uint64_t keyVal = 0, smallestKeyVal = 0, largestKeyVal = 0;
    void *smallestSplinePoint = splinePointLocation(spl, 0);
    void *largestSplinePoint = splinePointLocation(spl, spl->count - 1);
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&smallestKeyVal, smallestSplinePoint, spl->keySize);
    memcpy(&largestKeyVal, largestSplinePoint, spl->keySize);

    if (compareKey(key, splinePointLocation(spl, 0)) < 0 || spl->count <= 1) {
        // Key is smaller than any we have on record
        uint32_t lowEstimate, highEstimate, locEstimate = 0;
        memcpy(&lowEstimate, (int8_t *)spl->firstSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(&highEstimate, (int8_t *)smallestSplinePoint + spl->keySize, sizeof(uint32_t));
        locEstimate = (lowEstimate + highEstimate) / 2;

        memcpy(loc, &locEstimate, sizeof(uint32_t));
        memcpy(low, &lowEstimate, sizeof(uint32_t));
        memcpy(high, &highEstimate, sizeof(uint32_t));
        return;
    } else if (compareKey(key, splinePointLocation(spl, spl->count - 1)) > 0) {
        memcpy(loc, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(low, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        memcpy(high, (int8_t *)largestSplinePoint + spl->keySize, sizeof(uint32_t));
        return;
    } else {
        // Perform a binary seach to find the spline point above the key we're looking for
        pointIdx = pointsBinarySearch(spl, 0, spl->count - 1, key, compareKey);
    }

    // Interpolate between two spline points
    void *downKey = splinePointLocation(spl, pointIdx - 1);
    uint32_t downPage = 0;
    memcpy(&downPage, (int8_t *)downKey + spl->keySize, sizeof(uint32_t));
    void *upKey = splinePointLocation(spl, pointIdx);
    uint32_t upPage = 0;
    memcpy(&upPage, (int8_t *)upKey + spl->keySize, sizeof(uint32_t));
    uint64_t downKeyVal = 0, upKeyVal = 0;
    memcpy(&downKeyVal, downKey, spl->keySize);
    memcpy(&upKeyVal, upKey, spl->keySize);

    // Estimate location as page number
    // Keydiff * slope + y
    id_t locationEstimate = (id_t)((keyVal - downKeyVal) * (upPage - downPage) / (long double)(upKeyVal - downKeyVal)) + downPage;
    memcpy(loc, &locationEstimate, sizeof(id_t));

    // Set error bounds based on maxError from spline construction
    id_t lowEstiamte = (spl->maxError > locationEstimate) ? 0 : locationEstimate - spl->maxError;
    memcpy(low, &lowEstiamte, sizeof(id_t));
    void *lastSplinePoint = splinePointLocation(spl, spl->count - 1);
    uint32_t lastSplinePointPage = 0;
    memcpy(&lastSplinePointPage, (int8_t *)lastSplinePoint + spl->keySize, sizeof(uint32_t));
    id_t highEstimate = (locationEstimate + spl->maxError > lastSplinePointPage) ? lastSplinePointPage : locationEstimate + spl->maxError;
    memcpy(high, &highEstimate, sizeof(id_t));
}

/**
 * @brief    Free memory allocated for spline structure.
 * @param    spl        Spline structure
 */
void splineClose(spline *spl) {
    free(spl->points);
    free(spl->lastKey);
    free(spl->lower);
    free(spl->upper);
    free(spl->firstSplinePoint);
}

/**
 * @brief   Returns a pointer to the location of the specified spline point in memory. Note that this method does not check if there is a point there, so it may be garbage data.
 * @param   spl         The spline structure that contains the points
 * @param   pointIndex  The index of the point to return a pointer to
 */
void *splinePointLocation(spline *spl, size_t pointIndex) {
    return (int8_t *)spl->points + (((pointIndex + spl->pointsStartIndex) % spl->size) * (spl->keySize + sizeof(uint32_t)));
}

/************************************************************advancedQueries.c************************************************************/
/******************************************************************************/
/**
 * @file        advancedQueries.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the advanced query interface for EmbedDB
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

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    // Check the sign bits of the most significant bytes
    int sign1 = bytes1[numBytes - 1] & 0x80;
    int sign2 = bytes2[numBytes - 1] & 0x80;

    if (sign1 != sign2) {
        // Different signs, negative number is smaller
        return (sign1 ? -1 : 1);
    }

    // Same sign, perform regular byte-wise comparison
    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	0 or 1 to indicate if inequality is true
 */
int8_t compare(void* a, uint8_t operation, void* b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void* num1, const void* num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
    switch (operation) {
        case SELECT_GT:
            return compFunc(a, b, numBytes) > 0;
        case SELECT_LT:
            return compFunc(a, b, numBytes) < 0;
        case SELECT_GTE:
            return compFunc(a, b, numBytes) >= 0;
        case SELECT_LTE:
            return compFunc(a, b, numBytes) <= 0;
        case SELECT_EQ:
            return compFunc(a, b, numBytes) == 0;
        case SELECT_NEQ:
            return compFunc(a, b, numBytes) != 0;
        default:
            return 0;
    }
}

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator) {
    return operator->next(operator);
}

void initTableScan(embedDBOperator* operator) {
    if (operator->input != NULL) {
#ifdef PRINT_ERRORS
        printf("WARNING: TableScan operator should not have an input operator\n");
#endif
    }
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: TableScan operator needs its schema defined\n");
#endif
        return;
    }

    if (operator->schema->numCols<2) {
#ifdef PRINT_ERRORS
        printf("ERROR: When creating a table scan, you must include at least two columns: one for the key and one for the data from the iterator\n");
#endif
        return;
    }

    // Check that the provided key schema matches what is in the state
    embedDBState* embedDBstate = (embedDBState*)(((void**)operator->state)[0]);
    if (operator->schema->columnSizes[0] <= 0 || abs(operator->schema->columnSizes[0]) != embedDBstate->keySize) {
#ifdef PRINT_ERRORS
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
#endif
        return;
    }
    if (getRecordSizeFromSchema(operator->schema) != (embedDBstate->keySize + embedDBstate->dataSize)) {
#ifdef PRINT_ERRORS
        printf("ERROR: Size of provided schema doesn't match the size that will be returned by the provided iterator\n");
#endif
        return;
    }

    // Init buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextTableScan(embedDBOperator* operator) {
    // Check that a schema was set
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Must provide a base schema for a table scan operator\n");
#endif
        return 0;
    }

    // Get next record
    embedDBState* state = (embedDBState*)(((void**)operator->state)[0]);
    embedDBIterator* it = (embedDBIterator*)(((void**)operator->state)[1]);
    if (!embedDBNext(state, it, operator->recordBuffer, (int8_t*)operator->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(embedDBOperator* operator) {
    embedDBFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
    free(operator->state);
    operator->state = NULL;
}

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: All parameters must be provided to create a TableScan operator\n");
#endif
        return NULL;
    }

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }

    operator->state = malloc(2 * sizeof(void*));
    if (operator->state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }
    memcpy(operator->state, &state, sizeof(void*));
    memcpy((int8_t*)operator->state + sizeof(void*), &it, sizeof(void*));

    operator->schema = copySchema(baseSchema);
    operator->input = NULL;
    operator->recordBuffer = NULL;

    operator->init = initTableScan;
    operator->next = nextTableScan;
    operator->close = closeTableScan;

    return operator;
}

void initProjection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Get state
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection schema\n");
#endif
            return;
        }
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection while building schema\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
        }
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextProjection(embedDBOperator* operator) {
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Get next record
    if (operator->input->next(operator->input)) {
        for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            if (col == cols[nextProjCol]) {
                memcpy((int8_t*)operator->recordBuffer + nextProjColPos, (int8_t*)operator->input->recordBuffer + curColPos, colSize);
                nextProjColPos += colSize;
                nextProjCol++;
            }
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. Zero indexed. Column indexes must be strictly increasing i.e. columns must stay in the same order, can only remove columns from input
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols) {
    // Ensure column numbers are strictly increasing
    uint8_t lastCol = cols[0];
    for (uint8_t i = 1; i < numCols; i++) {
        if (cols[i] <= lastCol) {
#ifdef PRINT_ERRORS
            printf("ERROR: Columns in a projection must be strictly ascending for performance reasons");
#endif
            return NULL;
        }
        lastCol = cols[i];
    }
    // Create state
    uint8_t* state = malloc(numCols + 1);
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }
    state[0] = numCols;
    memcpy(state + 1, cols, numCols);

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initProjection;
    operator->next = nextProjection;
    operator->close = closeProjection;

    return operator;
}

void initSelection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = copySchema(operator->input->schema);
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextSelection(embedDBOperator* operator) {
    embedDBSchema* schema = operator->input->schema;

    int8_t colNum = *(int8_t*)operator->state;
    uint16_t colPos = getColOffsetFromSchema(schema, colNum);
    int8_t operation = *((int8_t*)operator->state + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (operator->input->next(operator->input)) {
        void* colData = (int8_t*)operator->input->recordBuffer + colPos;

        if (compare(colData, operation, *(void**)((int8_t*)operator->state + 2), isSigned, colSize)) {
            memcpy(operator->recordBuffer, operator->input->recordBuffer, getRecordSizeFromSchema(operator->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal) {
    int8_t* state = malloc(2 + sizeof(void*));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    state[0] = colNum;
    state[1] = operation;
    memcpy(state + 2, &compVal, sizeof(void*));

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initSelection;
    operator->next = nextSelection;
    operator->close = closeSelection;

    return operator;
}

/**
 * @brief	A private struct to hold the state of the aggregate operator
 */
struct aggregateInfo {
    int8_t (*groupfunc)(const void* lastRecord, const void* record);  // Function that determins if both records are in the same group
    embedDBAggregateFunc* functions;                                  // An array of aggregate functions
    uint32_t functionsLength;                                         // The length of the functions array
    void* lastRecordBuffer;                                           // Buffer for the last record read by input->next
    uint16_t bufferSize;                                              // Size of the input buffer (and lastRecordBuffer)
    int8_t isLastRecordUsable;                                        // Is the data in lastRecordBuffer usable for checking if the recently read record is in the same group? Is set to 0 at start, and also after the last record
};

void initAggregate(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Aggregate operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    struct aggregateInfo* state = operator->state;
    state->isLastRecordUsable = 0;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        operator->schema->numCols = state->functionsLength;
        operator->schema->columnSizes = malloc(state->functionsLength);
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < state->functionsLength; i++) {
            operator->schema->columnSizes[i] = state->functions[i].colSize;
            state->functions[i].colNum = i;
        }
    }

    // Init buffers
    state->bufferSize = getRecordSizeFromSchema(operator->input->schema);
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
    if (state->lastRecordBuffer == NULL) {
        state->lastRecordBuffer = malloc(state->bufferSize);
        if (state->lastRecordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
}

int8_t nextAggregate(embedDBOperator* operator) {
    struct aggregateInfo* state = operator->state;
    embedDBOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].reset != NULL) {
            state->functions[i].reset(state->functions + i, input->schema);
        }
    }

    int8_t recordsInGroup = 0;

    // Check flag used to indicate whether the last record read has been added to a group
    if (state->isLastRecordUsable) {
        recordsInGroup = 1;
        for (int i = 0; i < state->functionsLength; i++) {
            if (state->functions[i].add != NULL) {
                state->functions[i].add(state->functions + i, input->schema, state->lastRecordBuffer);
            }
        }
    }

    int8_t exitType = 0;
    while (input->next(input)) {
        // Check if record is in the same group as the last record
        if (!state->isLastRecordUsable || state->groupfunc(state->lastRecordBuffer, input->recordBuffer)) {
            recordsInGroup = 1;
            for (int i = 0; i < state->functionsLength; i++) {
                if (state->functions[i].add != NULL) {
                    state->functions[i].add(state->functions + i, input->schema, input->recordBuffer);
                }
            }
        } else {
            exitType = 1;
            break;
        }

        // Save this record
        memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);
        state->isLastRecordUsable = 1;
    }

    if (!recordsInGroup) {
        return 0;
    }

    if (exitType == 0) {
        // Exited because ran out of records, so all read records have been added to a group
        state->isLastRecordUsable = 0;
    }

    // Perform final compute on all functions
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].compute != NULL) {
            state->functions[i].compute(state->functions + i, operator->schema, operator->recordBuffer, state->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);

    return 1;
}

void closeAggregate(embedDBOperator* operator) {
    operator->input->close(operator->input);
    operator->input = NULL;
    embedDBFreeSchema(&operator->schema);
    free(((struct aggregateInfo*)operator->state)->lastRecordBuffer);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength) {
    struct aggregateInfo* state = malloc(sizeof(struct aggregateInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    state->groupfunc = groupfunc;
    state->functions = functions;
    state->functionsLength = functionsLength;
    state->lastRecordBuffer = NULL;

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initAggregate;
    operator->next = nextAggregate;
    operator->close = closeAggregate;

    return operator;
}

struct keyJoinInfo {
    embedDBOperator* input2;
    int8_t firstCall;
};

void initKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The first columns of the two tables must be the key and must be the same size. Make sure you haven't projected them out.\n");
#endif
        return;
    }

    // Setup schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        operator->schema->numCols = schema1->numCols + schema2->numCols;
        operator->schema->columnSizes = malloc(operator->schema->numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        memcpy(operator->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(operator->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
    }

    // Allocate recordBuffer
    operator->recordBuffer = malloc(getRecordSizeFromSchema(operator->schema));
    if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
        return;
    }

    state->firstCall = 1;
}

int8_t nextKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // We've already used this match
    void* record1 = input1->recordBuffer;
    void* record2 = input2->recordBuffer;

    int8_t colSize = abs(schema1->columnSizes[0]);

    if (state->firstCall) {
        state->firstCall = 0;

        if (!input1->next(input1) || !input2->next(input2)) {
            // If this case happens, you goofed, but I'll handle it anyway
            return 0;
        }
        goto check;
    }

    while (1) {
        // Advance the input with the smaller value
        int8_t comp = compareUnsignedNumbers(record1, record2, colSize);
        if (comp == 0) {
            // Move both forward because if they match at this point, they've already been matched
            if (!input1->next(input1) || !input2->next(input2)) {
                return 0;
            }
        } else if (comp < 0) {
            // Move record 1 forward
            if (!input1->next(input1)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        } else {
            // Move record 2 forward
            if (!input2->next(input2)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        }

    check:
        // See if these records join
        if (compareUnsignedNumbers(record1, record2, colSize) == 0) {
            // Copy both records into the output
            uint16_t record1Size = getRecordSizeFromSchema(schema1);
            memcpy(operator->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t*)operator->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    input1->close(input1);
    input2->close(input2);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2) {
    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }

    struct keyJoinInfo* state = malloc(sizeof(struct keyJoinInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }
    state->input2 = input2;

    operator->input = input1;
    operator->state = state;
    operator->recordBuffer = NULL;
    operator->schema = NULL;
    operator->init = initKeyJoin;
    operator->next = nextKeyJoin;
    operator->close = closeKeyJoin;

    return operator;
}

void countReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    *(uint32_t*)aggFunc->state = 0;
}

void countAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    (*(uint32_t*)aggFunc->state)++;
}

void countCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(uint32_t));
}

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate() {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = countReset;
    aggFunc->add = countAdd;
    aggFunc->compute = countCompute;
    aggFunc->state = malloc(sizeof(uint32_t));
    aggFunc->colSize = 4;
    return aggFunc;
}

void sumReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    if (abs(inputSchema->columnSizes[*((uint8_t*)aggFunc->state + sizeof(int64_t))]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    *(int64_t*)aggFunc->state = 0;
}

void sumAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    uint8_t colNum = *((uint8_t*)aggFunc->state + sizeof(int64_t));
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)recordBuffer + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t*)aggFunc->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t*)aggFunc->state) += val;
    }
}

void sumCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(int64_t));
}

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = sumReset;
    aggFunc->add = sumAdd;
    aggFunc->compute = sumCompute;
    aggFunc->state = malloc(sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t*)aggFunc->state + sizeof(int64_t)) = colNum;
    aggFunc->colSize = -8;
    return aggFunc;
}

struct minMaxState {
    uint8_t colNum;  // Which column of input to use
    void* current;   // The value currently regarded as the min/max
};

void minReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for min aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0xff, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as -1, not MAX_INT
        memset((int8_t*)state->current + colSize - 1, 0x7f, 1);
    }
}

void minAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_LT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

void minMaxCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), ((struct minMaxState*)aggFunc->state)->current, abs(outputSchema->columnSizes[aggFunc->colNum]));
}

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = minReset;
    aggFunc->add = minAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

void maxReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for max aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as 0, not MIN_INT
        memset((int8_t*)state->current + colSize - 1, 0x80, 1);
    }
}

void maxAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_GT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = maxReset;
    aggFunc->add = maxAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

struct avgState {
    uint8_t colNum;   // Column to take avg of
    int8_t isSigned;  // Is input column signed?
    uint32_t count;   // Count of records seen in group so far
    int64_t sum;      // Sum of records seen in group so far
};

void avgReset(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct avgState* state = aggFunc->state;
    if (abs(inputSchema->columnSizes[state->colNum]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    state->count = 0;
    state->sum = 0;
    state->isSigned = embedDB_IS_COL_SIGNED(inputSchema->columnSizes[state->colNum]);
}

void avgAdd(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct avgState* state = aggFunc->state;
    uint8_t colNum = state->colNum;
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)record + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        state->sum += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        val += (uint64_t)state->sum;
        memcpy(&state->sum, &val, sizeof(uint64_t));
    }
    state->count++;
}

void avgCompute(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    struct avgState* state = aggFunc->state;
    if (aggFunc->colSize == 8) {
        double avg = state->sum / (double)state->count;
        if (state->isSigned) {
            avg = state->sum / (double)state->count;
        } else {
            avg = (uint64_t)state->sum / (double)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(double));
    } else {
        float avg;
        if (state->isSigned) {
            avg = state->sum / (float)state->count;
        } else {
            avg = (uint64_t)state->sum / (float)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(float));
    }
}

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    struct avgState* state = malloc(sizeof(struct avgState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    aggFunc->state = state;
    if (outputFloatSize > 8 || (outputFloatSize < 8 && outputFloatSize > 4)) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 8.");
#endif
        aggFunc->colSize = 8;
    } else if (outputFloatSize < 4) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 4.");
#endif
        aggFunc->colSize = 4;
    } else {
        aggFunc->colSize = outputFloatSize;
    }
    aggFunc->reset = avgReset;
    aggFunc->add = avgAdd;
    aggFunc->compute = avgCompute;

    return aggFunc;
}

/**
 * @brief	Completely free a chain of functions recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator) {
    if ((*operator)->input != NULL) {
        embedDBFreeOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->state != NULL) {
        free((*operator)->state);
        (*operator)->state = NULL;
    }
    if ((*operator)->schema != NULL) {
        embedDBFreeSchema(&(*operator)->schema);
    }
    if ((*operator)->recordBuffer != NULL) {
        free((*operator)->recordBuffer);
        (*operator)->recordBuffer = NULL;
    }
    free(*operator);
    (*operator) = NULL;
}

