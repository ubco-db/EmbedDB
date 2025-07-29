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
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "query-interface/activeRules.h"

#if defined(ARDUINO)
#include "serial_c_iface.h"
#endif

/* Helper Functions */
int8_t embedDBInitData(embedDBState *state);
int8_t embedDBInitDataFromFile(embedDBState *state);
int8_t embedDBInitDataFromFileWithRecordLevelConsistency(embedDBState *state);
int8_t embedDBInitIndex(embedDBState *state);
int8_t embedDBInitIndexFromFile(embedDBState *state);
int8_t embedDBInitVarData(embedDBState *state);
int8_t embedDBInitVarDataFromFile(embedDBState *state);
int8_t shiftRecordLevelConsistencyBlocks(embedDBState *state);
void embedDBInitSplineFromFile(embedDBState *state);
int32_t getMaxError(embedDBState *state, void *buffer);
void updateMaxiumError(embedDBState *state, void *buffer);
int8_t embedDBSetupVarDataStream(embedDBState *state, void *key, embedDBVarDataStream **varData, id_t recordNumber);
uint32_t cleanSpline(embedDBState *state, uint32_t minPageNumber);
void readToWriteBuf(embedDBState *state);
void readToWriteBufVar(embedDBState *state);

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

    /* check the number of allocated pages is a multiple of the erase size */
    if (state->numDataPages % state->eraseSizeInPages != 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The number of allocated data pages must be divisible by the erase size in pages.\n");
#endif
        return -1;
    }

    if (state->numDataPages < (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters) ? 4 : 2) * state->eraseSizeInPages) {
#ifdef PRINT_ERRORS
        printf("ERROR: The minimum number of data pages is twice the eraseSizeInPages or 4 times the eraseSizeInPages if using record-level consistency.\n");
#endif
        return -1;
    }

    state->recordSize = state->keySize + state->dataSize;
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        if (state->numVarPages % state->eraseSizeInPages != 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: The number of allocated variable data pages must be divisible by the erase size in pages.\n");
#endif
            return -1;
        }
        state->recordSize += 4;
    }

    state->indexMaxError = indexMaxError;

    /* Calculate block header size */

    /* Header size depends on bitmap size: 6 + X bytes: 4 byte id, 2 for record count, X for bitmap. */
    state->headerSize = 6;
    if (EMBEDDB_USING_INDEX(state->parameters)) {
        if (state->numIndexPages % state->eraseSizeInPages != 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: The number of allocated index pages must be divisible by the erase size in pages.\n");
#endif
            return -1;
        }
        state->headerSize += state->bitmapSize;
    }

    if (EMBEDDB_USING_MAX_MIN(state->parameters))
        state->headerSize += state->keySize * 2 + state->dataSize * 2;

    /* Flags to show that these values have not been initalized with actual data yet */
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

    /* Initalize the spline structure if being used */
    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        if (state->numSplinePoints < 4) {
#ifdef PRINT_ERRORS
            printf("ERROR: Unable to setup spline with less than 4 points.");
#endif
            return -1;
        }
        state->spl = malloc(sizeof(spline));
        splineInit(state->spl, state->numSplinePoints, indexMaxError, state->keySize);
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
    state->nextDataPageId = 0;
    state->numAvailDataPages = state->numDataPages;
    state->minDataPageId = 0;

    if (state->dataFile == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: No data file provided!\n");
#endif
        return -1;
    }

    if (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters)) {
        state->numAvailDataPages -= (state->eraseSizeInPages * 2);
        state->nextRLCPhysicalPageLocation = state->eraseSizeInPages;
        state->rlcPhysicalStartingPage = state->eraseSizeInPages;
    }

    /* Setup data file. */
    int8_t openStatus = 0;
    if (!EMBEDDB_RESETING_DATA(state->parameters)) {
        openStatus = state->fileInterface->open(state->dataFile, EMBEDDB_FILE_MODE_R_PLUS_B);
        if (openStatus) {
            if (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters)) {
                return embedDBInitDataFromFileWithRecordLevelConsistency(state);
            } else {
                return embedDBInitDataFromFile(state);
            }
        }
    } else {
        openStatus = state->fileInterface->open(state->dataFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    }

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
    uint32_t count = 0;
    count_t blockSize = state->eraseSizeInPages;
    bool validData = false;
    bool hasData = false;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;

    /* This will become zero if there is no more to read */
    int8_t moreToRead = !(readPage(state, physicalPageId));

    /* this handles the case where the first page may have been erased, so has junk data and we actually need to start from the second page */
    uint32_t i = 0;
    int8_t numRecords = 0;
    while (moreToRead && i < 2) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        validData = logicalPageId % state->numDataPages == count;
        numRecords = EMBEDDB_GET_COUNT(buffer);
        if (validData && numRecords > 0 && numRecords < state->maxRecordsPerPage + 1) {
            hasData = true;
            maxLogicalPageId = logicalPageId;
            physicalPageId++;
            updateMaxiumError(state, buffer);
            count++;
            i = 2;
        } else {
            physicalPageId += blockSize;
            count += blockSize;
        }
        moreToRead = !(readPage(state, physicalPageId));
        i++;
    }

    /* if we have no valid data, we just have an empty file can can start from the scratch */
    if (!hasData)
        return 0;

    while (moreToRead && count < state->numDataPages) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        validData = logicalPageId % state->numDataPages == count;
        if (validData && logicalPageId == maxLogicalPageId + 1) {
            maxLogicalPageId = logicalPageId;
            physicalPageId++;
            updateMaxiumError(state, buffer);
            moreToRead = !(readPage(state, physicalPageId));
            count++;
        } else {
            break;
        }
    }

    /*
     * Now we need to find where the page with the smallest key that is still valid.
     * The default case is we have not wrapped and the page number for the physical page with the smallest key is 0.
     */
    id_t physicalPageIDOfSmallestData = 0;

    /* check if data exists at this location */
    if (moreToRead && count < state->numDataPages) {
        /* find where the next block boundary is */
        id_t pagesToBlockBoundary = blockSize - (count % blockSize);

        /* go to the next block boundary */
        physicalPageId = (physicalPageId + pagesToBlockBoundary) % state->numDataPages;
        moreToRead = !(readPage(state, physicalPageId));

        /* there should have been more to read becuase the file should not be empty at this point if it was not empty at the previous block */
        if (!moreToRead) {
            return -1;
        }

        /* check if data is valid or if it is junk */
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        validData = logicalPageId % state->numDataPages == physicalPageId;

        /* this means we have wrapped and our start is actually here */
        if (validData) {
            physicalPageIDOfSmallestData = physicalPageId;
        }
    }

    state->nextDataPageId = maxLogicalPageId + 1;
    readPage(state, physicalPageIDOfSmallestData);
    memcpy(&(state->minDataPageId), buffer, sizeof(id_t));
    state->numAvailDataPages = state->numDataPages + state->minDataPageId - maxLogicalPageId - 1;

    /* Put largest key back into the buffer */
    readPage(state, (state->nextDataPageId - 1) % state->numDataPages);

    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        embedDBInitSplineFromFile(state);
    }

    return 0;
}

int8_t embedDBInitDataFromFileWithRecordLevelConsistency(embedDBState *state) {
    id_t logicalPageId = 0;
    id_t maxLogicalPageId = 0;
    id_t physicalPageId = 0;
    uint32_t count = 0;
    count_t blockSize = state->eraseSizeInPages;
    bool validData = false;
    bool hasPermanentData = false;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;

    /* This will become zero if there is no more to read */
    int8_t moreToRead = !(readPage(state, physicalPageId));

    /* This handles the case that the first three pages may not have valid data in them.
     * They may be either an erased page or pages for record-level consistency.
     */
    uint32_t i = 0;
    int8_t numRecords = 0;
    while (moreToRead && i < 4) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        validData = logicalPageId % state->numDataPages == count;
        numRecords = EMBEDDB_GET_COUNT(buffer);
        if (validData && numRecords > 0 && numRecords < state->maxRecordsPerPage + 1) {
            /* Setup for next loop so it does not have to worry about setting the initial values */
            hasPermanentData = true;
            maxLogicalPageId = logicalPageId;
            physicalPageId++;
            updateMaxiumError(state, buffer);
            count++;
            i = 4;
        } else {
            physicalPageId += blockSize;
            count += blockSize;
        }
        moreToRead = !(readPage(state, physicalPageId));
        i++;
    }

    if (hasPermanentData) {
        while (moreToRead && count < state->numDataPages) {
            memcpy(&logicalPageId, buffer, sizeof(id_t));
            validData = logicalPageId % state->numDataPages == count;
            if (validData && logicalPageId == maxLogicalPageId + 1) {
                maxLogicalPageId = logicalPageId;
                physicalPageId++;
                updateMaxiumError(state, buffer);
                moreToRead = !(readPage(state, physicalPageId));
                count++;
            } else {
                break;
            }
        }
    } else {
        /* Case where the there is no permanent pages written, but we may still have record-level consistency records in block 2 */
        count = 0;
        physicalPageId = 0;
    }

    /* find where the next block boundary is */
    id_t pagesToBlockBoundary = blockSize - (count % blockSize);
    /* if we are on a block-boundary, we erase the next page in case the erase failed and then skip to the start of the next block */
    if (pagesToBlockBoundary == blockSize) {
        int8_t eraseSuccess = state->fileInterface->erase(count, count + blockSize, state->pageSize, state->dataFile);
        if (!eraseSuccess) {
#ifdef PRINT_ERRORS
            printf("Error: Unable to erase data page during recovery!\n");
#endif
            return -1;
        }
    }

    /* go to the next block boundary */
    physicalPageId = (physicalPageId + pagesToBlockBoundary) % state->numDataPages;
    state->rlcPhysicalStartingPage = physicalPageId;
    state->nextRLCPhysicalPageLocation = physicalPageId;

    /* record-level consistency recovery algorithm */
    uint32_t numPagesRead = 0;
    uint32_t numPagesToRead = blockSize * 2;
    uint32_t rlcMaxLogicialPageNumber = UINT32_MAX;
    uint32_t rlcMaxRecordCount = UINT32_MAX;
    uint32_t rlcMaxPage = UINT32_MAX;
    moreToRead = !(readPage(state, physicalPageId));
    while (moreToRead && numPagesRead < numPagesToRead) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        /* If the next logical page number is not the one after the max data page, we can just skip to the next page.
         * We also need to read the page if there are no permanent records but the logicalPageId is zero, as this indicates we have record-level consistency records
         */
        if (logicalPageId == maxLogicalPageId + 1 || (logicalPageId == 0 && !hasPermanentData)) {
            uint32_t numRecords = EMBEDDB_GET_COUNT(buffer);
            if (rlcMaxRecordCount == UINT32_MAX || numRecords > rlcMaxRecordCount) {
                rlcMaxRecordCount = numRecords;
                rlcMaxLogicialPageNumber = logicalPageId;
                rlcMaxPage = numPagesRead;
            }
        }
        physicalPageId = (physicalPageId + 1) % state->numDataPages;
        moreToRead = !(readPage(state, physicalPageId));
        numPagesRead++;
    }

    /* need to find larged record-level consistency page to place back into the buffer and either one or both of the record-level consistency pages */
    uint32_t eraseStartingPage = 0;
    uint32_t eraseEndingPage = 0;
    uint32_t numBlocksToErase = 0;
    if (rlcMaxLogicialPageNumber == UINT32_MAX) {
        eraseStartingPage = state->rlcPhysicalStartingPage % state->numDataPages;
        numBlocksToErase = 2;
    } else {
        state->nextRLCPhysicalPageLocation = (state->rlcPhysicalStartingPage + rlcMaxPage + 1) % state->numDataPages;
        /* need to read the max page into read buffer again so we can copy into the write buffer */
        int8_t readSuccess = readPage(state, (state->rlcPhysicalStartingPage + rlcMaxPage) % state->numDataPages);
        if (readSuccess != 0) {
#ifdef PRINT_ERRORS
            printf("Error: Can't read page in data file that was previously read!\n");
#endif
            return -1;
        }
        memcpy(state->buffer, buffer, state->pageSize);
        eraseStartingPage = (state->rlcPhysicalStartingPage + (rlcMaxPage < blockSize ? blockSize : 0)) % state->numDataPages;
        numBlocksToErase = 1;
    }

    for (uint32_t i = 0; i < numBlocksToErase; i++) {
        eraseEndingPage = eraseStartingPage + blockSize;
        int8_t eraseSuccess = state->fileInterface->erase(eraseStartingPage, eraseEndingPage, state->pageSize, state->dataFile);
        if (!eraseSuccess) {
#ifdef PRINT_ERRORS
            printf("Error: Unable to erase pages in data file!\n");
#endif
            return -1;
        }
        eraseStartingPage = eraseEndingPage % state->numDataPages;
    }

    /* if we don't have any permanent data, we can just return now that the record-level consistency records have been handled */
    if (!hasPermanentData) {
        return 0;
    }

    /* Now check if we have wrapped after the record level consistency.
     * The default case is we start at beginning of data file.
     */
    id_t physicalPageIDOfSmallestData = 0;

    physicalPageId = (state->rlcPhysicalStartingPage + 2 * blockSize) % state->numDataPages;
    int8_t readSuccess = readPage(state, physicalPageId);
    if (readSuccess == 0) {
        memcpy(&logicalPageId, buffer, sizeof(id_t));
        validData = logicalPageId % state->numDataPages == physicalPageId;

        /* this means we have wrapped and our start is actually here */
        if (validData) {
            physicalPageIDOfSmallestData = physicalPageId;
        }
    }

    state->nextDataPageId = maxLogicalPageId + 1;
    readPage(state, physicalPageIDOfSmallestData);
    memcpy(&(state->minDataPageId), buffer, sizeof(id_t));
    state->numAvailDataPages = state->numDataPages + state->minDataPageId - maxLogicalPageId - 1 - (2 * blockSize);

    /* Put largest key back into the buffer */
    readPage(state, (state->nextDataPageId - 1) % state->numDataPages);
    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
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
        splineAdd(state->spl, embedDBGetMinKey(state, buffer), pageNumberToRead++);
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
    state->minVarRecordId = UINT64_MAX;
    state->numAvailVarPages = state->numVarPages;
    state->nextVarPageId = 0;

    if (!EMBEDDB_RESETING_DATA(state->parameters) && (state->nextDataPageId > 0 || EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters))) {
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
    id_t logicalVariablePageId = 0;
    id_t maxLogicalVariablePageId = 0;
    id_t physicalVariablePageId = 0;
    id_t count = 0;
    count_t blockSize = state->eraseSizeInPages;
    bool validData = false;
    bool hasData = false;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);

    /* This will equal 0 if there are no pages to read */
    int8_t moreToRead = !(readVariablePage(state, physicalVariablePageId));

    /* this handles the case where the first page may have been erased, so has junk data and we actually need to start from the second page */
    uint32_t i = 0;
    while (moreToRead && i < 2) {
        memcpy(&logicalVariablePageId, buffer, sizeof(id_t));
        validData = logicalVariablePageId % state->numVarPages == count;
        if (validData) {
            uint64_t largestVarRecordId = 0;
            /* Fetch the largest key value for which we have data on this page */
            memcpy(&largestVarRecordId, (int8_t *)buffer + sizeof(id_t), state->keySize);
            /*
             * Since 0 is a valid first page and a valid record key, we may have a case where this data is valid.
             * So we go to the next page to check if it is valid as well.
             */
            if (logicalVariablePageId != 0 || largestVarRecordId != 0) {
                i = 2;
                hasData = true;
                maxLogicalVariablePageId = logicalVariablePageId;
            }
            physicalVariablePageId++;
            count++;
        } else {
            id_t pagesToBlockBoundary = blockSize - (count % blockSize);
            physicalVariablePageId += pagesToBlockBoundary;
            count += pagesToBlockBoundary;
            i++;
        }
        moreToRead = !(readVariablePage(state, physicalVariablePageId));
    }

    /* if we have no valid data, we just have an empty file can can start from the scratch */
    if (!hasData)
        return 0;

    while (moreToRead && count < state->numVarPages) {
        memcpy(&logicalVariablePageId, buffer, sizeof(id_t));
        validData = logicalVariablePageId % state->numVarPages == count;
        if (validData && logicalVariablePageId == maxLogicalVariablePageId + 1) {
            maxLogicalVariablePageId = logicalVariablePageId;
            physicalVariablePageId++;
            moreToRead = !(readVariablePage(state, physicalVariablePageId));
            count++;
        } else {
            break;
        }
    }

    /*
     * Now we need to find where the page with the smallest key that is still valid.
     * The default case is we have not wrapped and the page number for the physical page with the smallest key is 0.
     */
    id_t physicalPageIDOfSmallestData = 0;

    /* check if data exists at this location */
    if (moreToRead && count < state->numVarPages) {
        /* find where the next block boundary is */
        id_t pagesToBlockBoundary = blockSize - (count % blockSize);

        /* go to the next block boundary */
        physicalVariablePageId = (physicalVariablePageId + pagesToBlockBoundary) % state->numVarPages;
        moreToRead = !(readVariablePage(state, physicalVariablePageId));

        /* there should have been more to read becuase the file should not be empty at this point if it was not empty at the previous block */
        if (!moreToRead) {
            return -1;
        }

        /* check if data is valid or if it is junk */
        memcpy(&logicalVariablePageId, buffer, sizeof(id_t));
        validData = logicalVariablePageId % state->numVarPages == physicalVariablePageId;

        /* this means we have wrapped and our start is actually here */
        if (validData) {
            physicalPageIDOfSmallestData = physicalVariablePageId;
        }
    }

    state->nextVarPageId = maxLogicalVariablePageId + 1;
    id_t minVarPageId = 0;
    int8_t readResult = readVariablePage(state, physicalPageIDOfSmallestData);
    if (readResult != 0) {
#ifdef PRINT_ERRORS
        printf("Error reading variable page with smallest data. \n");
#endif
        return -1;
    }

    memcpy(&minVarPageId, buffer, sizeof(id_t));

    /* If the smallest varPageId is 0, nothing was ever overwritten, so we have all the data */
    if (minVarPageId == 0) {
        void *dataBuffer;
        /* Using record level consistency where nothing was written to permanent storage yet but  */
        if (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters) && state->nextDataPageId == 0) {
            /* check the buffer for records  */
            dataBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_WRITE_BUFFER;
        } else {
            /* read page with smallest data we still have */
            dataBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;
            readResult = readPage(state, state->minDataPageId % state->numDataPages);
            if (readResult != 0) {
#ifdef PRINT_ERRORS
                printf("Error reading page in data file when recovering variable data. \n");
#endif
                return -1;
            }
        }

        /* Get smallest key from page and put it into the minVarRecordId */
        uint64_t minKey = 0;
        memcpy(&minKey, embedDBGetMinKey(state, dataBuffer), state->keySize);
        state->minVarRecordId = minKey;
    } else {
        /* We lose some records, but know for sure we have all records larger than this*/
        memcpy(&(state->minVarRecordId), (int8_t *)buffer + sizeof(id_t), state->keySize);
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
    if(EMBEDDB_GET_COUNT(buffer) == 0) slopeX2 = 0;
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
    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        splineAdd(state->spl, embedDBGetMinKey(state, state->buffer), pageNumber);
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
    if (state->nextDataPageId > 0 || count > 0) {
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
    bool wrotePage = false;
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

        updateMaxiumError(state, state->buffer);

        count = 0;
        initBufferPage(state, 0);
        wrotePage = true;
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

    /* If using record level consistency, we need to immediately write the updated page to storage */
    if (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters)) {
        /* Need to move record level consistency pointers if on a block boundary */
        if (wrotePage && state->nextDataPageId % state->eraseSizeInPages == 0) {
            /* move record-level consistency blocks */
            shiftRecordLevelConsistencyBlocks(state);
        }
        return writeTemporaryPage(state, state->buffer);
    }
    if(state->rules != NULL && state->rules[0] != NULL){
        executeRules(state, key, data);
    }

    return 0;
}

int8_t shiftRecordLevelConsistencyBlocks(embedDBState *state) {
    /* erase the record-level consistency blocks */

    /* TODO: Likely an optimisation here where we don't always need to erase the second block, but that will make this algorithm more complicated and the savings could be minimal */
    uint32_t numRecordLevelConsistencyPages = state->eraseSizeInPages * 2;
    uint32_t eraseStartingPage = state->rlcPhysicalStartingPage;
    uint32_t eraseEndingPage = 0;

    /* if we have wraped, we need to erase an additional block as the block we are shifting into is not empty */
    bool haveWrapped = (state->minDataPageId % state->numDataPages) == ((state->rlcPhysicalStartingPage + numRecordLevelConsistencyPages) % state->numDataPages);
    uint32_t numBlocksToErase = haveWrapped ? 2 : 3;

    /* Erase pages to make space for new data */
    for (size_t i = 0; i < numBlocksToErase; i++) {
        eraseEndingPage = eraseStartingPage + state->eraseSizeInPages;
        int8_t eraseSuccess = state->fileInterface->erase(eraseStartingPage, eraseEndingPage, state->pageSize, state->dataFile);
        if (!eraseSuccess) {
#ifdef PRINT_ERRORS
            printf("Error: Unable to erase pages in data file when shifting record level consistency blocks!\n");
#endif
            return -1;
        }
        eraseStartingPage = eraseEndingPage % state->numDataPages;
    }

    /* shift min data page if needed */
    if (haveWrapped) {
        /* Flag the pages as usable to EmbedDB */
        state->numAvailDataPages += state->eraseSizeInPages;
        state->minDataPageId += state->eraseSizeInPages;

        /* remove any spline points related to these pages */
        if (!EMBEDDB_DISABLED_SPLINE_CLEAN(state->parameters)) {
            cleanSpline(state, state->minDataPageId);
        }
    }

    /* shift record-level consistency blocks */
    state->rlcPhysicalStartingPage = (state->rlcPhysicalStartingPage + state->eraseSizeInPages) % state->numDataPages;
    state->nextRLCPhysicalPageLocation = state->rlcPhysicalStartingPage;

    return 0;
}

void updateMaxiumError(embedDBState *state, void *buffer) {
    // Calculate error within the page
    int32_t maxError = getMaxError(state, buffer);
    if (state->maxError < maxError) {
        state->maxError = maxError;
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
    if (state->currentVarLoc % state->pageSize > state->pageSize - 4 || (!(EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters)) && EMBEDDB_GET_COUNT(state->buffer) >= state->maxRecordsPerPage)) {
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

    if (state->minVarRecordId == UINT64_MAX) {
        memcpy(&state->minVarRecordId, key, state->keySize);
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

    if (EMBEDDB_USING_RECORD_LEVEL_CONSISTENCY(state->parameters)) {
        embedDBFlushVar(state);
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
 * @param 	high		Uper bound for the page the record could be found on
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t linearSearch(embedDBState *state, void *buf, void *key, int32_t pageId, int32_t low, int32_t high) {
    int32_t pageError = 0;
    int32_t physPageId;
    while (1) {
        /* Move logical page number to physical page id based on location of first data page */
        physPageId = pageId % state->numDataPages;

        if (pageId > high || pageId < low || low > high || pageId < state->minDataPageId || pageId >= state->nextDataPageId) {
            return -1;
        }

        /* Read page into buffer. If 0 not returned, there was an error */
        if (readPage(state, physPageId) != 0) {
            return -1;
        }

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

int8_t binarySearch(embedDBState *state, void *buffer, void *key) {
    uint32_t first = state->minDataPageId, last = state->nextDataPageId - 1;
    uint32_t pageId = (first + last) / 2;
    while (1) {
        /* Read page into buffer */
        if (readPage(state, pageId % state->numDataPages) != 0)
            return -1;

        if (first >= last)
            break;

        if (state->compareKey(key, embedDBGetMinKey(state, buffer)) < 0) {
            /* Key is less than smallest record in block. */
            last = pageId - 1;
            pageId = (first + last) / 2;
        } else if (state->compareKey(key, embedDBGetMaxKey(state, buffer)) > 0) {
            /* Key is larger than largest record in block. */
            first = pageId + 1;
            pageId = (first + last) / 2;
        } else {
            /* Found correct block */
            return 0;
        }
    }
}

int8_t splineSearch(embedDBState *state, void *buffer, void *key) {
    /* Spline search */
    uint32_t location, lowbound, highbound;
    splineFind(state->spl, key, state->compareKey, &location, &lowbound, &highbound);

    /* If the spline thinks the data is on a page smaller than the smallest data page we have, we know we don't have the data */
    if (highbound < state->minDataPageId) {
        return -1;
    }

    /* if the spline returns a lowbound lower than than the smallest page we have, we can move the lowbound and location up */
    if (lowbound < state->minDataPageId) {
        lowbound = state->minDataPageId;
        location = (lowbound + highbound) / 2;
    }

    // Check if the currently buffered page is the correct one
    if (!(lowbound <= state->bufferedPageId &&
          highbound >= state->bufferedPageId &&
          state->compareKey(embedDBGetMinKey(state, buffer), key) <= 0 &&
          state->compareKey(embedDBGetMaxKey(state, buffer), key) >= 0)) {
        if (linearSearch(state, buffer, key, location, lowbound, highbound) == -1) {
            return -1;
        }
    }
    return 0;
}

/**
 * @brief	Given a key, searches for data associated with
 *          that key in embedDB buffer using embedDBSearchNode.
 *          Note: Space for data must be already allocated.
 * @param	state	embedDB algorithm state structure
 * @param   buffer  pointer to embedDB buffer
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return non-negative integer representing offset if success. Non-zero value if error.
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
 * @return	Return 0 if success. Returns -2 if requested key is less than the minimum stored key. Non-zero value if error.
 */
int8_t embedDBGet(embedDBState *state, void *key, void *data) {
    void *outputBuffer = state->buffer;
    if (state->nextDataPageId == 0) {
        int8_t success = searchBuffer(state, outputBuffer, key, data);
        if (success != NO_RECORD_FOUND) {
            return 0;
        }
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
            return (searchBuffer(state, outputBuffer, key, data) != NO_RECORD_FOUND) ? 0 : NO_RECORD_FOUND;
        }
    }

    int8_t searchResult = 0;
    if (EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        /* Regular binary search */
        searchResult = binarySearch(state, buf, key);
    } else {
        /* Spline search */
        searchResult = splineSearch(state, buf, key);
    }

    if (searchResult != 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: embedDBGet was unable to find page to search for record\n");
#endif
        return -1;
    }

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
    void *outputBuffer = (int8_t *)state->buffer;

    // search output buffer for record, mem copy fixed record into data
    int8_t recordNum = searchBuffer(state, outputBuffer, key, data);

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

    /* Determine which data page should be the first examined if there is a min key and that we have spline points */
    if (state->spl->count != 0 && it->minKey != NULL && !(EMBEDDB_USING_BINARY_SEARCH(state->parameters))) {
        /* Spline search */
        uint32_t location, lowbound, highbound = 0;
        splineFind(state->spl, it->minKey, state->compareKey, &location, &lowbound, &highbound);

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
 * @brief	Flushes output buffer.
 * @param	state	algorithm state structure
 * @returns 0 if successul and a non-zero value otherwise
 */
int8_t embedDBFlushVar(embedDBState *state) {
    /* Check if we actually have any variable data in the buffer */
    if (state->currentVarLoc % state->pageSize == state->variableDataHeaderSize) {
        return 0;
    }

    // only flush variable buffer
    id_t writeResult = writeVariablePage(state, (int8_t *)state->buffer + EMBEDDB_VAR_WRITE_BUFFER(state->parameters) * state->pageSize);
    if (writeResult == -1) {
#ifdef PRINT_ERRORS
        printf("Failed to write variable data page during embedDBFlushVar.");
#endif
        return -1;
    }

    state->fileInterface->flush(state->varFile);
    // init new buffer
    initBufferPage(state, EMBEDDB_VAR_WRITE_BUFFER(state->parameters));
    // determine how many bytes are left
    int temp = state->pageSize - (state->currentVarLoc % state->pageSize);
    // create new offset
    state->currentVarLoc += temp + state->variableDataHeaderSize;
    return 0;
}

/**
 * @brief	Flushes output buffer.
 * @param	state	algorithm state structure
 * @returns 0 if successul and a non-zero value otherwise
 */
int8_t embedDBFlush(embedDBState *state) {
    // As the first buffer is the data write buffer, no address change is required
    int8_t *buffer = (int8_t *)state->buffer + EMBEDDB_DATA_WRITE_BUFFER * state->pageSize;
    if (EMBEDDB_GET_COUNT(buffer) < 1)
        return 0;

    id_t pageNum = writePage(state, buffer);
    if (pageNum == -1) {
#ifdef PRINT_ERRORS
        printf("Failed to write page during embedDBFlush.");
#endif
        return -1;
    }

    state->fileInterface->flush(state->dataFile);

    indexPage(state, pageNum);

    if (EMBEDDB_USING_INDEX(state->parameters)) {
        void *buf = (int8_t *)state->buffer + state->pageSize * (EMBEDDB_INDEX_WRITE_BUFFER);
        count_t idxcount = EMBEDDB_GET_COUNT(buf);
        EMBEDDB_INC_COUNT(buf);

        /* Copy record onto index page */
        void *bm = EMBEDDB_GET_BITMAP(state->buffer);
        memcpy((void *)((int8_t *)buf + EMBEDDB_IDX_HEADER_SIZE + state->bitmapSize * idxcount), bm, state->bitmapSize);

        id_t writeResult = writeIndexPage(state, buf);
        if (writeResult == -1) {
#ifdef PRINT_ERRORS
            printf("Failed to write index page during embedDBFlush.");
#endif
            return -1;
        }

        state->fileInterface->flush(state->indexFile);

        /* Reinitialize buffer */
        initBufferPage(state, EMBEDDB_INDEX_WRITE_BUFFER);
    }

    /* Reinitialize buffer */
    initBufferPage(state, EMBEDDB_DATA_WRITE_BUFFER);

    // Flush var data page
    if (EMBEDDB_USING_VDATA(state->parameters)) {
        int8_t varFlushResult = embedDBFlushVar(state);
        if (varFlushResult != 0) {
#ifdef PRINT_ERRORS
            printf("Failed to flush variable data page");
#endif
            return -1;
        }
    }
    return 0;
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
    int searchWriteBuf = 0;
    while (1) {
        if (it->nextDataPage > state->nextDataPageId) {
            return 0;
        }
        if (it->nextDataPage == state->nextDataPageId) {
            searchWriteBuf = 1;
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

        if (searchWriteBuf == 0 && readPage(state, it->nextDataPage % state->numDataPages) != 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to read data page %i (%i)\n", it->nextDataPage, it->nextDataPage % state->numDataPages);
#endif
            return 0;
        }

        // Keep reading record until we find one that matches the query
        int8_t *buf = searchWriteBuf == 0 ? (int8_t *)state->buffer + EMBEDDB_DATA_READ_BUFFER * state->pageSize : (int8_t *)state->buffer + EMBEDDB_DATA_WRITE_BUFFER * state->pageSize;
        uint32_t pageRecordCount = EMBEDDB_GET_COUNT(buf);
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
        readToWriteBuf(state);
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
    void *dataBuf = (int8_t *)state->buffer + state->pageSize * EMBEDDB_DATA_READ_BUFFER;
    void *record = (int8_t *)dataBuf + state->headerSize + recordNumber * state->recordSize;

    uint32_t varDataAddr = 0;
    memcpy(&varDataAddr, (int8_t *)record + state->keySize + state->dataSize, sizeof(uint32_t));
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

    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        splinePrint(state->spl);
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
    id_t physicalPageNum = pageNum % state->numDataPages;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->numAvailDataPages <= 0) {
        /* Erase pages to make space for new data */
        int8_t eraseResult = state->fileInterface->erase(physicalPageNum, physicalPageNum + state->eraseSizeInPages, state->pageSize, state->dataFile);
        if (eraseResult != 1) {
#ifdef PRINT_ERRORS
            printf("Failed to erase data page: %i (%i)\n", pageNum, physicalPageNum);
#endif
            return -1;
        }

        /* Flag the pages as usable to EmbedDB */
        state->numAvailDataPages += state->eraseSizeInPages;
        state->minDataPageId += state->eraseSizeInPages;

        /* remove any spline points related to these pages */
        if (!EMBEDDB_DISABLED_SPLINE_CLEAN(state->parameters)) {
            cleanSpline(state, state->minDataPageId);
        }
    }

    /* Seek to page location in file */
    int32_t val = state->fileInterface->write(buffer, physicalPageNum, state->pageSize, state->dataFile);
    if (val == 0) {
#ifdef PRINT_ERRORS
        printf("Failed to write data page: %i (%i)\n", pageNum, physicalPageNum);
#endif
        return -1;
    }

    state->numAvailDataPages--;
    state->numWrites++;

    return pageNum;
}

int8_t writeTemporaryPage(embedDBState *state, void *buffer) {
    if (state->dataFile == NULL) {
#ifdef PRINT_ERRORS
        printf("The dataFile in embedDBState was null.");
#endif
        return -3;
    }

    /* Setup page number in header */
    /* TODO: Maybe talk to Ramon about optimizing this */
    memcpy(buffer, &(state->nextDataPageId), sizeof(id_t));

    /* Wrap if needed */
    state->nextRLCPhysicalPageLocation %= state->numDataPages;

    /* If the nextPhysicalPage wrapped, we need to add the numDataPages to it to properly compare the page numbers below */
    uint32_t nextPage = state->nextRLCPhysicalPageLocation + (state->nextRLCPhysicalPageLocation < state->rlcPhysicalStartingPage ? state->numDataPages : 0);

    /* if the nextRLC physical page number would be outside the block, we wrap to the start of our record-level consistency blocks */
    if (nextPage - state->rlcPhysicalStartingPage >= state->eraseSizeInPages * 2) {
        state->nextRLCPhysicalPageLocation = state->rlcPhysicalStartingPage;
    }

    /* If in pageNum is second page in block, we erase the other record-level consistency block */
    if (state->nextRLCPhysicalPageLocation % state->eraseSizeInPages == 1) {
        uint32_t eraseStartingPage = state->rlcPhysicalStartingPage;
        count_t blockSize = state->eraseSizeInPages;
        if (state->nextRLCPhysicalPageLocation == eraseStartingPage + 1) {
            eraseStartingPage = (eraseStartingPage + blockSize) % state->numDataPages;
        }
        uint32_t eraseEndingPage = eraseStartingPage + blockSize;

        int8_t eraseSuccess = state->fileInterface->erase(eraseStartingPage, eraseEndingPage, state->pageSize, state->dataFile);
        if (!eraseSuccess) {
#ifdef PRINT_ERRORS
            printf("Failed to erase block starting at physical page %i in the data file.", state->nextRLCPhysicalPageLocation);
            return -2;
#endif
        }
    }

    /* Write temporary page to storage */
    int8_t writeSuccess = state->fileInterface->write(buffer, state->nextRLCPhysicalPageLocation++, state->pageSize, state->dataFile);
    if (!writeSuccess) {
#ifdef PRINT_ERRORS
        printf("Failed to write temporary page for record-level-consistency: Logical Page Number %i - Physical Page (%i)\n", state->nextDataPageId, state->nextRLCPhysicalPageLocation - 1);
#endif
        return -1;
    }

    return 0;
}

/**
 * @brief	Calculates the number of spline points not in use by embedDB and deletes them
 * @param	state	embedDB algorithm state structure
 * @param	key 	The minimim key embedDB still needs points for
 * @return	Returns the number of points deleted
 */
uint32_t cleanSpline(embedDBState *state, uint32_t minPageNumber) {
    uint32_t numPointsErased = 0;
    void *nextPoint;
    uint32_t currentPageNumber = 0;
    for (size_t i = 0; i < state->spl->count; i++) {
        nextPoint = splinePointLocation(state->spl, i + 1);
        memcpy(&currentPageNumber, (int8_t *)nextPoint + state->keySize, sizeof(uint32_t));
        if (currentPageNumber < minPageNumber) {
            numPointsErased++;
        } else {
            break;
        }
    }
    if (state->spl->count - numPointsErased < 2)
        numPointsErased -= 2 - (state->spl->count - numPointsErased);
    if (numPointsErased <= 0)
        return 0;
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
    id_t physicalPageNumber = pageNum % state->numIndexPages;

    /* Setup page number in header */
    memcpy(buffer, &(pageNum), sizeof(id_t));

    if (state->numAvailIndexPages <= 0) {
        // Erase index pages to make room for new page
        int8_t eraseResult = state->fileInterface->erase(physicalPageNumber, physicalPageNumber + state->eraseSizeInPages, state->pageSize, state->indexFile);
        if (eraseResult != 1) {
#ifdef PRINT_ERRORS
            printf("Failed to erase data page: %i (%i)\n", pageNum, physicalPageNumber);
#endif
            return -1;
        }
        state->numAvailIndexPages += state->eraseSizeInPages;
        state->minIndexPageId += state->eraseSizeInPages;
    }

    /* Seek to page location in file */
    int32_t val = state->fileInterface->write(buffer, physicalPageNumber, state->pageSize, state->indexFile);
    if (val == 0) {
#ifdef PRINT_ERRORS
        printf("Failed to write index page: %i (%i)\n", pageNum, physicalPageNumber);
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
        int8_t eraseResult = state->fileInterface->erase(physicalPageId, physicalPageId + state->eraseSizeInPages, state->pageSize, state->varFile);
        if (eraseResult != 1) {
#ifdef PRINT_ERRORS
            printf("Failed to erase data page: %i (%i)\n", state->nextVarPageId, physicalPageId);
#endif
            return -1;
        }
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
    if (!EMBEDDB_USING_BINARY_SEARCH(state->parameters)) {
        splineClose(state->spl);
        free(state->spl);
        state->spl = NULL;
    }
}
