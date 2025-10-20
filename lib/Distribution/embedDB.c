#include "./embedDB.h"

/******************************************************************************/
/**
 * @file        EmbedDB-Amalgamation
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code amalgamated into one file for easy distribution
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

#if defined(ARDUINO)
#endif

/**
 * @brief    Initialize a spline structure with given maximum size and error.
 * @param    spl        Spline structure
 * @param    size       Maximum size of spline
 * @param    maxError   Maximum error allowed in spline
 * @param    keySize    Size of key in bytes
 */
void splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize) {
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
    }

    /* Skip duplicates */
    uint64_t keyVal = 0, lastKeyVal = 0;
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&lastKeyVal, spl->lastKey, spl->keySize);

    if (keyVal <= lastKeyVal && spl->numAddCalls != 2)
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

    if (spl->count >= spl->size) {
        int8_t eraseResult = splineErase(spl, spl->eraseSize);
    }

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

        /* If we add a point, we might need to erase again */
        if (spl->count >= spl->size) {
            int8_t eraseResult = splineErase(spl, spl->eraseSize);
        }

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
    printf("Spline max error (%u):\n", spl->maxError);
    printf("Spline points (%lu):\n", spl->count);
    uint64_t keyVal = 0;
    uint32_t page = 0;
    for (id_t i = 0; i < spl->count; i++) {
        void *point = splinePointLocation(spl, i);
        memcpy(&keyVal, point, spl->keySize);
        memcpy(&page, (int8_t *)point + spl->keySize, sizeof(uint32_t));
        printf("[%u]: (%lu, %d)\n", i, keyVal, page);
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

#if defined(ARDUINO)
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
    if (EMBEDDB_GET_COUNT(buffer) == 0) slopeX2 = 0;
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
    if (state->rules != NULL && state->rules[0] != NULL) {
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

#if defined(ARDUINO)
#endif

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 * @param   colTypes        An array describing the type of the column. Use the defined constants embedDB_COLUMN_INT or embedDB_COLUMN_FLOAT
 */
embedDBSchema *embedDBCreateSchema(uint8_t numCols, int8_t *colSizes, int8_t *colSignedness, ColumnType *colTypes) {
    embedDBSchema *schema = malloc(sizeof(embedDBSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    schema->columnTypes = malloc(numCols * sizeof(ColumnType));
    memcpy(schema->columnTypes, colTypes, numCols * sizeof(ColumnType));

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
void embedDBFreeSchema(embedDBSchema **schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free((*schema)->columnTypes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void *createBufferFromSchema(embedDBSchema *schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema *copySchema(const embedDBSchema *schema) {
    embedDBSchema *copy = malloc(sizeof(embedDBSchema));
    if (copy == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    copy->columnTypes = malloc(schema->numCols * sizeof(ColumnType));
    if (copy->columnSizes == NULL || copy->columnTypes == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    memcpy(copy->columnTypes, schema->columnTypes, schema->numCols * sizeof(ColumnType));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema *schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema *schema) {
    uint16_t size = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        size += abs(schema->columnSizes[i]);
    }
    return size;
}

void printSchema(embedDBSchema *schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", embedDB_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
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

#if defined(ARDUINO)
#endif

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareUnsignedNumbers(const void *num1, const void *num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t *bytes1 = (const uint8_t *)num1;
    const uint8_t *bytes2 = (const uint8_t *)num2;

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
int8_t compareSignedNumbers(const void *num1, const void *num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t *bytes1 = (const uint8_t *)num1;
    const uint8_t *bytes2 = (const uint8_t *)num2;

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
int8_t compare(void *a, uint8_t operation, void *b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void *num1, const void *num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
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
int8_t exec(embedDBOperator *op) {
    return op->next(op);
}

void initTableScan(embedDBOperator *op) {
    if (op->input != NULL) {
#ifdef PRINT_ERRORS
        printf("WARNING: TableScan operator should not have an input operator\n");
#endif
    }
    if (op->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: TableScan operator needs its schema defined\n");
#endif
        return;
    }

    if (op->schema->numCols < 2) {
#ifdef PRINT_ERRORS
        printf("ERROR: When creating a table scan, you must include at least two columns: one for the key and one for the data from the iterator\n");
#endif
        return;
    }

    // Check that the provided key schema matches what is in the state
    embedDBState *embedDBstate = (embedDBState *)(((void **)op->state)[0]);
    if (op->schema->columnSizes[0] <= 0 || abs(op->schema->columnSizes[0]) != embedDBstate->keySize) {
#ifdef PRINT_ERRORS
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
#endif
        return;
    }
    if (getRecordSizeFromSchema(op->schema) != (embedDBstate->keySize + embedDBstate->dataSize)) {
#ifdef PRINT_ERRORS
        printf("ERROR: Size of provided schema doesn't match the size that will be returned by the provided iterator\n");
#endif
        return;
    }

    // Init buffer
    if (op->recordBuffer == NULL) {
        op->recordBuffer = createBufferFromSchema(op->schema);
        if (op->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextTableScan(embedDBOperator *op) {
    // Check that a schema was set
    if (op->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Must provide a base schema for a table scan operator\n");
#endif
        return 0;
    }

    // Get next record
    embedDBState *state = (embedDBState *)(((void **)op->state)[0]);
    embedDBIterator *it = (embedDBIterator *)(((void **)op->state)[1]);
    if (!embedDBNext(state, it, op->recordBuffer, (int8_t *)op->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(embedDBOperator *op) {
    embedDBFreeSchema(&op->schema);
    free(op->recordBuffer);
    op->recordBuffer = NULL;
    free(op->state);
    op->state = NULL;
}

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator *createTableScanOperator(embedDBState *state, embedDBIterator *it, embedDBSchema *baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: All parameters must be provided to create a TableScan operator\n");
#endif
        return NULL;
    }

    embedDBOperator *op = malloc(sizeof(embedDBOperator));
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }

    op->state = malloc(2 * sizeof(void *));
    if (op->state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }
    memcpy(op->state, &state, sizeof(void *));
    memcpy((int8_t *)op->state + sizeof(void *), &it, sizeof(void *));

    op->schema = copySchema(baseSchema);
    op->input = NULL;
    op->recordBuffer = NULL;

    op->init = initTableScan;
    op->next = nextTableScan;
    op->close = closeTableScan;

    return op;
}

void initProjection(embedDBOperator *op) {
    if (op->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    op->input->init(op->input);

    // Get state
    uint8_t numCols = *(uint8_t *)op->state;
    uint8_t *cols = (uint8_t *)op->state + 1;
    const embedDBSchema *inputSchema = op->input->schema;

    // Init output schema
    if (op->schema == NULL) {
        op->schema = malloc(sizeof(embedDBSchema));
        if (op->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection schema\n");
#endif
            return;
        }
        op->schema->numCols = numCols;
        op->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        op->schema->columnTypes = malloc(numCols * sizeof(ColumnType));
        if (op->schema->columnSizes == NULL || op->schema->columnTypes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection while building schema\n");
#endif
            return;
        }

        for (uint8_t i = 0; i < numCols; i++) {
            op->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
            op->schema->columnTypes[i] = inputSchema->columnTypes[cols[i]];
        }
    }

    // Init output buffer
    if (op->recordBuffer == NULL) {
        op->recordBuffer = createBufferFromSchema(op->schema);
        if (op->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextProjection(embedDBOperator *op) {
    uint8_t numCols = *(uint8_t *)op->state;
    uint8_t *cols = (uint8_t *)op->state + 1;
    embedDBSchema *inputSchema = op->input->schema;

    // Get next record
    if (op->input->next(op->input)) {
        uint16_t curColPos = 0;
        for (uint8_t colIdx = 0; colIdx < numCols; colIdx++) {
            uint8_t col = cols[colIdx];
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            uint16_t srcColPos = getColOffsetFromSchema(inputSchema, col);
            memcpy((int8_t *)op->recordBuffer + curColPos, (int8_t *)op->input->recordBuffer + srcColPos, colSize);
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(embedDBOperator *op) {
    op->input->close(op->input);

    embedDBFreeSchema(&op->schema);
    free(op->state);
    op->state = NULL;
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. Zero indexed. Column indexes must be strictly increasing i.e. columns must stay in the same order, can only remove columns from input
 */
embedDBOperator *createProjectionOperator(embedDBOperator *input, uint8_t numCols, uint8_t *cols) {
    // Create state
    uint8_t *state = malloc(numCols + 1);
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }
    state[0] = numCols;
    memcpy(state + 1, cols, numCols);

    embedDBOperator *op = malloc(sizeof(embedDBOperator));
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }

    op->state = state;
    op->input = input;
    op->schema = NULL;
    op->recordBuffer = NULL;
    op->init = initProjection;
    op->next = nextProjection;
    op->close = closeProjection;

    return op;
}

struct selectionInfo {
    int8_t colNum;
    int8_t operation;
    void *compVal;
};

void initSelection(embedDBOperator *op) {
    if (op->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    op->input->init(op->input);

    // Init output schema
    if (op->schema == NULL) {
        op->schema = copySchema(op->input->schema);
    }

    // Init output buffer
    if (op->recordBuffer == NULL) {
        op->recordBuffer = createBufferFromSchema(op->schema);
        if (op->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextSelection(embedDBOperator *op) {
    embedDBSchema *schema = op->input->schema;
    struct selectionInfo *state = op->state;

    int8_t colNum = state->colNum;
    uint16_t colPos = getColOffsetFromSchema(schema, colNum);
    int8_t operation = state->operation;
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (op->input->next(op->input)) {
        void *colData = (int8_t *)op->input->recordBuffer + colPos;
        if (compare(colData, operation, state->compVal, isSigned, colSize)) {
            memcpy(op->recordBuffer, op->input->recordBuffer, getRecordSizeFromSchema(op->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(embedDBOperator *op) {
    op->input->close(op->input);

    embedDBFreeSchema(&op->schema);
    free(op->state);
    op->state = NULL;
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator *createSelectionOperator(embedDBOperator *input, int8_t colNum, int8_t operation, void *compVal) {
    struct selectionInfo *state = malloc(sizeof(struct selectionInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->operation = operation;
    memcpy(&state->compVal, &compVal, sizeof(void *));

    embedDBOperator *op = malloc(sizeof(embedDBOperator));
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    op->state = state;
    op->input = input;
    op->schema = NULL;
    op->recordBuffer = NULL;
    op->init = initSelection;
    op->next = nextSelection;
    op->close = closeSelection;

    return op;
}

void initOrderBy(embedDBOperator *op) {
    if (op == NULL || op->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: ORDER BY: NULL input operator\n");
#endif
        return;
    }

    op->input->init(op->input);

    if (op->schema == NULL) {
        op->schema = copySchema(op->input->schema);
    }

    if (op->recordBuffer == NULL) {
        op->recordBuffer = createBufferFromSchema(op->schema);
        if (op->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: ORDER BY: Failed to allocate buffer\n");
#endif
            return;
        }
    }

    ((sortData *)op->state)->readBuffer = malloc(PAGE_SIZE);

    prepareSort(op);

    return;
}

int8_t nextOrderBy(embedDBOperator *op) {
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: ORDER BY: NULL input operator\n");
#endif
        return 0;
    }

    if (readNextRecord((sortData *)op->state, op->recordBuffer) != 0) {
        return 0;
    }

    return 1;
}

void closeOrderBy(embedDBOperator *op) {
    op->input->close(op->input);
    op->input = NULL;
    embedDBFreeSchema(&op->schema);

    closeSort(((sortData *)op->state)->fileIterator);
    free(((sortData *)op->state)->readBuffer);
    free(((sortData *)op->state)->fileIterator);

    free(op->state);
    op->state = NULL;
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

/**
 * @brief Create an operator that will reorder records based on a given direction
 *
 * @param dbState       The database state
 * @param input         The operator that this operator can pull records from
 * @param colNum        The column that is being sorted on
 * @param compareFn     The function being used to make comparisons between row data
 */
embedDBOperator *createOrderByOperator(embedDBState *dbState, embedDBOperator *input, int8_t colNum, int32_t limit, int8_t (*compareFn)(void *a, void *b)) {
    if (input == NULL || dbState == NULL || compareFn == NULL || colNum < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: ORDER BY: Invalid Input data\n");
#endif
        return NULL;
    }

    // Operator state
    struct sortData *state = malloc(sizeof(struct sortData));
    embedDBOperator *op = malloc(sizeof(embedDBOperator));

    if (state == NULL || op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: ORDER BY: malloc failed\n");
#endif
        return NULL;
    }

    state->fileInterface = dbState->fileInterface;
    state->colNum = colNum;
    state->compareFn = compareFn;
    state->tupleLimit = limit;

    op->state = state;
    op->input = input;
    op->schema = NULL;
    op->recordBuffer = NULL;
    op->init = initOrderBy;
    op->next = nextOrderBy;
    op->close = closeOrderBy;

    return op;
}

/**
 * @brief	A private struct to hold the state of the aggregate operator
 */
struct aggregateInfo {
    int8_t (*groupfunc)(const void *lastRecord, const void *record);  // Function that determins if both records are in the same group
    embedDBAggregateFunc *functions;                                  // An array of aggregate functions
    uint32_t functionsLength;                                         // The length of the functions array
    void *lastRecordBuffer;                                           // Buffer for the last record read by input->next
    uint16_t bufferSize;                                              // Size of the input buffer (and lastRecordBuffer)
    int8_t isLastRecordUsable;                                        // Is the data in lastRecordBuffer usable for checking if the recently read record is in the same group? Is set to 0 at start, and also after the last record
};

void initAggregate(embedDBOperator *op) {
    if (op->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Aggregate operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    op->input->init(op->input);

    struct aggregateInfo *state = op->state;
    state->isLastRecordUsable = 0;

    // Init output schema
    if (op->schema == NULL) {
        op->schema = malloc(sizeof(embedDBSchema));
        if (op->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        op->schema->numCols = state->functionsLength;
        op->schema->columnSizes = malloc(state->functionsLength);
        op->schema->columnTypes = malloc(state->functionsLength);
        if (op->schema->columnSizes == NULL || op->schema->columnTypes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < state->functionsLength; i++) {
            op->schema->columnSizes[i] = state->functions[i].colSize;
            state->functions[i].colNum = i;
        }
    }

    // Init buffers
    state->bufferSize = getRecordSizeFromSchema(op->input->schema);
    if (op->recordBuffer == NULL) {
        op->recordBuffer = createBufferFromSchema(op->schema);
        if (op->recordBuffer == NULL) {
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

int8_t nextAggregate(embedDBOperator *op) {
    struct aggregateInfo *state = op->state;
    embedDBOperator *input = op->input;

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
            state->functions[i].compute(state->functions + i, op->schema, op->recordBuffer, state->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);

    return 1;
}

void closeAggregate(embedDBOperator *op) {
    op->input->close(op->input);
    op->input = NULL;
    embedDBFreeSchema(&op->schema);
    free(((struct aggregateInfo *)op->state)->lastRecordBuffer);
    free(op->state);
    op->state = NULL;
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator *createAggregateOperator(embedDBOperator *input, int8_t (*groupfunc)(const void *lastRecord, const void *record), embedDBAggregateFunc *functions, uint32_t functionsLength) {
    struct aggregateInfo *state = malloc(sizeof(struct aggregateInfo));
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

    embedDBOperator *op = malloc(sizeof(embedDBOperator));
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    op->state = state;
    op->input = input;
    op->schema = NULL;
    op->recordBuffer = NULL;
    op->init = initAggregate;
    op->next = nextAggregate;
    op->close = closeAggregate;

    return op;
}

struct keyJoinInfo {
    embedDBOperator *input2;
    int8_t firstCall;
};

void initKeyJoin(embedDBOperator *op) {
    struct keyJoinInfo *state = op->state;
    embedDBOperator *input1 = op->input;
    embedDBOperator *input2 = state->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    embedDBSchema *schema1 = input1->schema;
    embedDBSchema *schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The first columns of the two tables must be the key and must be the same size. Make sure you haven't projected them out.\n");
#endif
        return;
    }

    // Setup schema
    if (op->schema == NULL) {
        op->schema = malloc(sizeof(embedDBSchema));
        if (op->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        op->schema->numCols = schema1->numCols + schema2->numCols;
        op->schema->columnSizes = malloc(op->schema->numCols * sizeof(int8_t));
        op->schema->columnTypes = malloc(op->schema->numCols * sizeof(ColumnType));
        if (op->schema->columnSizes == NULL || op->schema->columnTypes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        memcpy(op->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(op->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
        memcpy(op->schema->columnTypes, schema1->columnTypes, schema1->numCols);
        memcpy(op->schema->columnTypes + schema1->numCols, schema2->columnTypes, schema2->numCols);
    }

    // Allocate recordBuffer
    op->recordBuffer = malloc(getRecordSizeFromSchema(op->schema));
    if (op->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
        return;
    }

    state->firstCall = 1;
}

int8_t nextKeyJoin(embedDBOperator *op) {
    struct keyJoinInfo *state = op->state;
    embedDBOperator *input1 = op->input;
    embedDBOperator *input2 = state->input2;
    embedDBSchema *schema1 = input1->schema;
    embedDBSchema *schema2 = input2->schema;

    // We've already used this match
    void *record1 = input1->recordBuffer;
    void *record2 = input2->recordBuffer;

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
            memcpy(op->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t *)op->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(embedDBOperator *op) {
    struct keyJoinInfo *state = op->state;
    embedDBOperator *input1 = op->input;
    embedDBOperator *input2 = state->input2;
    input1->close(input1);
    input2->close(input2);

    embedDBFreeSchema(&op->schema);
    free(op->state);
    op->state = NULL;
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator *createKeyJoinOperator(embedDBOperator *input1, embedDBOperator *input2) {
    embedDBOperator *op = malloc(sizeof(embedDBOperator));
    if (op == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }

    struct keyJoinInfo *state = malloc(sizeof(struct keyJoinInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }
    state->input2 = input2;

    op->input = input1;
    op->state = state;
    op->recordBuffer = NULL;
    op->schema = NULL;
    op->init = initKeyJoin;
    op->next = nextKeyJoin;
    op->close = closeKeyJoin;

    return op;
}

void countReset(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema) {
    *(uint32_t *)aggFunc->state = 0;
}

void countAdd(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema, const void *recordBuffer) {
    (*(uint32_t *)aggFunc->state)++;
}

void countCompute(embedDBAggregateFunc *aggFunc, embedDBSchema *outputSchema, void *recordBuffer, const void *lastRecord) {
    // Put count in record
    memcpy((int8_t *)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(uint32_t));
}

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc *createCountAggregate() {
    embedDBAggregateFunc *aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = countReset;
    aggFunc->add = countAdd;
    aggFunc->compute = countCompute;
    aggFunc->state = malloc(sizeof(uint32_t));
    aggFunc->colSize = 4;
    return aggFunc;
}

void sumReset(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema) {
    if (abs(inputSchema->columnSizes[*((uint8_t *)aggFunc->state + sizeof(int64_t))]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    *(int64_t *)aggFunc->state = 0;
}

void sumAdd(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema, const void *recordBuffer) {
    uint8_t colNum = *((uint8_t *)aggFunc->state + sizeof(int64_t));
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void *colPos = (int8_t *)recordBuffer + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t *)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t *)aggFunc->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t *)aggFunc->state) += val;
    }
}

void sumCompute(embedDBAggregateFunc *aggFunc, embedDBSchema *outputSchema, void *recordBuffer, const void *lastRecord) {
    // Put count in record
    memcpy((int8_t *)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(int64_t));
}

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc *createSumAggregate(uint8_t colNum) {
    embedDBAggregateFunc *aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = sumReset;
    aggFunc->add = sumAdd;
    aggFunc->compute = sumCompute;
    aggFunc->state = malloc(sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t *)aggFunc->state + sizeof(int64_t)) = colNum;
    aggFunc->colSize = -8;
    return aggFunc;
}

struct minMaxState {
    uint8_t colNum;  // Which column of input to use
    void *current;   // The value currently regarded as the min/max
};

void minReset(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema) {
    struct minMaxState *state = aggFunc->state;
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
        memset((int8_t *)state->current + colSize - 1, 0x7f, 1);
    }
}

void minAdd(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema, const void *record) {
    struct minMaxState *state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void *newValue = (int8_t *)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_LT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

void minMaxCompute(embedDBAggregateFunc *aggFunc, embedDBSchema *outputSchema, void *recordBuffer, const void *lastRecord) {
    // Put count in record
    memcpy((int8_t *)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), ((struct minMaxState *)aggFunc->state)->current, abs(outputSchema->columnSizes[aggFunc->colNum]));
}

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc *createMinAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc *aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState *state = malloc(sizeof(struct minMaxState));
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

void maxReset(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema) {
    struct minMaxState *state = aggFunc->state;
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
        memset((int8_t *)state->current + colSize - 1, 0x80, 1);
    }
}

void maxAdd(embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema, const void *record) {
    struct minMaxState *state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void *newValue = (int8_t *)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_GT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc *createMaxAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc *aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState *state = malloc(sizeof(struct minMaxState));
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
    uint8_t colNum;      // Column to take avg of
    ColumnType colType;  // Column type
    uint32_t count;      // Count of records seen in group so far
    double sum;          // Sum of records seen in group so far
};

void avgReset(struct embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema) {
    struct avgState *state = aggFunc->state;
    state->colType = inputSchema->columnTypes[state->colNum];
    state->count = 0;
    state->sum = 0.0;
}

void avgAdd(struct embedDBAggregateFunc *aggFunc, embedDBSchema *inputSchema, const void *record) {
    struct avgState *state = aggFunc->state;
    uint8_t colNum = state->colNum;
    void *colPos = (int8_t *)record + getColOffsetFromSchema(inputSchema, colNum);
    switch (state->colType) {
        case embedDB_COLUMN_INT32: {
            int32_t val;
            memcpy(&val, colPos, sizeof(int32_t));
            state->sum += val;
            break;
        }
        case embedDB_COLUMN_UINT32: {
            uint32_t val;
            memcpy(&val, colPos, sizeof(uint32_t));
            state->sum += val;
            break;
        }
        case embedDB_COLUMN_INT64: {
            int64_t val;
            memcpy(&val, colPos, sizeof(int64_t));
            state->sum += val;
            break;
        }
        case embedDB_COLUMN_UINT64: {
            uint64_t val;
            memcpy(&val, colPos, sizeof(uint64_t));
            state->sum += val;
            break;
        }
        case embedDB_COLUMN_FLOAT: {
            float val;
            memcpy(&val, colPos, sizeof(float));
            state->sum += val;
            break;
        }
        case embedDB_COLUMN_DOUBLE: {
            double val = 0;
            memcpy(&val, colPos, sizeof(double));
            state->sum += val;
            break;
        }
        default:
#ifdef PRINT_ERRORS
            printf("WARNING: avgAdd encountered unsupported column type: %d\n", state->colType);
#endif
            return;
    }
    state->count++;
}

void avgCompute(struct embedDBAggregateFunc *aggFunc, embedDBSchema *outputSchema, void *recordBuffer, const void *lastRecord) {
    struct avgState *state = aggFunc->state;
    if (state->count == 0) {
        return;  // Avoid division by zero
    }

    void *outputPos = (int8_t *)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum);

    switch (state->colType) {
        case embedDB_COLUMN_INT32:
        case embedDB_COLUMN_UINT32:
        case embedDB_COLUMN_INT64:
        case embedDB_COLUMN_UINT64:
        case embedDB_COLUMN_FLOAT: {
            float avg = (float)(state->sum / state->count);
            memcpy(outputPos, &avg, sizeof(float));
            break;
        }
        case embedDB_COLUMN_DOUBLE: {
            double avg = state->sum / state->count;
            memcpy(outputPos, &avg, sizeof(double));
            break;
        }
        default:
#ifdef PRINT_ERRORS
            printf("WARNING: avgCompute encountered unsupported column type\n");
#endif
            return;
    }
}

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc *createAvgAggregate(uint8_t colNum, int8_t outputFloatSize) {
    embedDBAggregateFunc *aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    struct avgState *state = malloc(sizeof(struct avgState));
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
void embedDBFreeOperatorRecursive(embedDBOperator **op) {
    if ((*op)->input != NULL) {
        embedDBFreeOperatorRecursive(&(*op)->input);
    }
    if ((*op)->state != NULL) {
        free((*op)->state);
        (*op)->state = NULL;
    }
    if ((*op)->schema != NULL) {
        embedDBFreeSchema(&(*op)->schema);
    }
    if ((*op)->recordBuffer != NULL) {
        free((*op)->recordBuffer);
        (*op)->recordBuffer = NULL;
    }
    free(*op);
    (*op) = NULL;
}

/************************************************************embedDBUtility.c************************************************************/
/******************************************************************************/
/**
 * @file        embedDBUtility.c
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
    int64_t i1, i2;
    memcpy(&i1, a, sizeof(int64_t));
    memcpy(&i2, b, sizeof(int64_t));
    int64_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

int8_t floatComparator(void *a, void *b) {
    float f1, f2;
    memcpy(&f1, a, sizeof(float));
    memcpy(&f2, b, sizeof(float));
    return (f1 > f2) - (f1 < f2);
}

int8_t doubleComparator(void *a, void *b) {
    double f1, f2;
    memcpy(&f1, a, sizeof(double));
    memcpy(&f2, b, sizeof(double));
    return (f1 > f2) - (f1 < f2);
}
/************************************************************activeRules.c************************************************************/

activeRule *IF(activeRule *rule, uint8_t colNum, ActiveQueryType type) {
    rule->type = type;
    rule->colNum = colNum;
    return rule;
}

activeRule *IFCustom(activeRule *rule, uint8_t colNum, void *(*executeCustom)(activeRule *rule, void *key), CustomReturnType returnType) {
    rule->type = GET_CUSTOM;
    rule->colNum = colNum;
    rule->executeCustom = executeCustom;
    rule->returnType = returnType;
    return rule;
}

activeRule *is(activeRule *rule, SelectOperation operation, void *threshold) {
    rule->operation = operation;
    rule->threshold = threshold;
    return rule;
}

activeRule *ofLast(activeRule *rule, void *numLastEntries) {
    rule->numLastEntries = numLastEntries;
    return rule;
}

activeRule *where(activeRule *rule, void *minData, void *maxData) {
    rule->minData = minData;
    rule->maxData = maxData;
    return rule;
}

activeRule *then(activeRule *rule, void (*callback)(void *aggregateValue, void *currentValue, void *context)) {
    rule->callback = callback;
    return rule;
}

activeRule *createActiveRule(embedDBSchema *schema, void *context) {
    activeRule *rule = (activeRule *)malloc(sizeof(activeRule));
    if (rule != NULL) {
        rule->minData = NULL;  // Default to no min data
        rule->maxData = NULL;  // Default to no max data
        rule->schema = copySchema(schema);
        rule->context = context;
        rule->IF = IF;
        rule->IFCustom = IFCustom;
        rule->is = is;
        rule->ofLast = ofLast;
        rule->where = where;
        rule->then = then;
        rule->enabled = true;  // Default to enabled
    }
    return rule;
}

void executeRules(embedDBState *state, void *key, void *data) {
    for (int i = 0; i < state->numRules; i++) {
        if (state->rules[i]->enabled == false) {
            continue;  // Skip disabled rules
        }
        switch (state->rules[i]->type) {
            case GET_AVG:
                handleGetAvg(state, state->rules[i], key, data);
                break;
            case GET_MAX:
            case GET_MIN:
                handleGetMinMax(state, state->rules[i], key, data);
                break;
            case GET_CUSTOM:
                handleCustomQuery(state, state->rules[i], key, data);
                break;
            default:
                printf("ERROR: Unsupported rule type\n");
        }
    }
}

float GetAvg(embedDBState *state, activeRule *rule, void *key) {
    void **allocatedValues;
    embedDBOperator *op = createOperator(state, rule, &allocatedValues, key);

    void *recordBuffer = op->recordBuffer;
    float *C1 = (float *)((int8_t *)recordBuffer + 0);
    // Print as csv
    exec(op);
    float avg = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return avg;
}

int32_t GetMinMax32(embedDBState *state, activeRule *rule, void *key) {
    void **allocatedValues;
    embedDBOperator *op = createOperator(state, rule, &allocatedValues, key);

    void *recordBuffer = op->recordBuffer;
    int32_t *C1 = (int32_t *)((int8_t *)recordBuffer + 0);
    // Print as csv
    exec(op);
    int32_t minmax = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return minmax;
}

int64_t GetMinMax64(embedDBState *state, activeRule *rule, void *key) {
    void **allocatedValues;
    embedDBOperator *op = createOperator(state, rule, &allocatedValues, key);

    void *recordBuffer = op->recordBuffer;
    int64_t *C1 = (int64_t *)((int8_t *)recordBuffer + 0);
    // Print as csv
    exec(op);
    int64_t minmax = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return minmax;
}

embedDBOperator *createOperator(embedDBState *state, activeRule *rule, void ***allocatedValues, void *key) {
    embedDBIterator *it = (embedDBIterator *)malloc(sizeof(embedDBIterator));
    if (state->keySize == 4) {
        uint32_t minKeyVal = *(uint32_t *)key - (*(uint32_t *)rule->numLastEntries - 1);
        uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    } else if (state->keySize == 8) {
        uint64_t minKeyVal = *(uint64_t *)key - (*(uint64_t *)rule->numLastEntries - 1);
        uint64_t *minKeyPtr = (uint64_t *)malloc(sizeof(uint64_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    } else {
        printf("ERROR: Unsupported key size\n");
        return NULL;
    }

    it->maxKey = NULL;
    it->minData = rule->minData;
    it->maxData = rule->maxData;
    embedDBInitIterator(state, it);

    embedDBOperator *scanOp = createTableScanOperator(state, it, rule->schema);

    embedDBAggregateFunc *aggFunc = NULL;

    switch (rule->type) {
        case GET_AVG:
            aggFunc = createAvgAggregate(rule->colNum, 4);
            break;
        case GET_MAX:
            aggFunc = createMaxAggregate(rule->colNum, rule->schema->columnSizes[rule->colNum]);
            break;
        case GET_MIN:
            aggFunc = createMinAggregate(rule->colNum, rule->schema->columnSizes[rule->colNum]);
            break;
        default:
            printf("ERROR: Unsupported rule type\n");
    }

    embedDBAggregateFunc *aggFuncs = (embedDBAggregateFunc *)malloc(1 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *aggFunc;
    embedDBOperator *aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 1);
    aggOp->init(aggOp);

    free(aggFunc);

    *allocatedValues = (void **)malloc(2 * sizeof(void *));
    ((void **)*allocatedValues)[0] = it;
    ((void **)*allocatedValues)[1] = aggFuncs;

    return aggOp;
}

int8_t groupFunction(const void *lastRecord, const void *record) {
    return 1;
}

void executeComparison(activeRule *rule, void *aggregateValue, Comparator comparator, void *data) {
    int8_t comparisonResult = comparator(aggregateValue, rule->threshold);

    switch (rule->operation) {
        case GreaterThan:
            if (comparisonResult > 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case LessThan:
            if (comparisonResult < 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case GreaterThanOrEqual:
            if (comparisonResult >= 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case LessThanOrEqual:
            if (comparisonResult <= 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case Equal:
            if (comparisonResult == 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case NotEqual:
            if (comparisonResult != 0) rule->callback(aggregateValue, data, rule->context);
            break;
        default:
            printf("ERROR: Unsupported operation\n");
    }
}

void handleGetAvg(embedDBState *state, activeRule *rule, void *key, void *data) {
    float avg = GetAvg(state, rule, key);
    executeComparison(rule, &avg, floatComparator, data);
}

void handleGetMinMax(embedDBState *state, activeRule *rule, void *key, void *data) {
    int columnSize = abs(rule->schema->columnSizes[rule->colNum]);

    if (columnSize == 4) {  // 32-bit integer
        int32_t minmax = GetMinMax32(state, rule, key);
        executeComparison(rule, &minmax, int32Comparator, data);
    } else if (columnSize == 8) {  // 64-bit integer
        int64_t minmax = GetMinMax64(state, rule, key);
        executeComparison(rule, &minmax, int64Comparator, data);
    } else {
        printf("ERROR: Unsupported column size\n");
    }
}

void handleCustomQuery(embedDBState *state, activeRule *rule, void *key, void *data) {
    void *result = rule->executeCustom(rule, key);
    switch (rule->returnType) {
        case DBINT32:
            executeComparison(rule, result, int32Comparator, data);
            break;
        case DBINT64:
            executeComparison(rule, result, int64Comparator, data);
            break;
        case DBFLOAT:
            executeComparison(rule, result, floatComparator, data);
            break;
        case DBDOUBLE:
            executeComparison(rule, result, doubleComparator, data);
            break;
        default:
            printf("ERROR: Unsupported return type\n");
    }
}

/************************************************************adaptive_sort.c************************************************************/
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

// #define     DEBUG         1
// #define     DEBUG_OUTPUT  1
// #define     DEBUG_READ    1
// #define     DEBUG_HEAP    0

// #define ADAPTIVE_SORT_PRINT_FINISH

/**
 * Prints the contents of the heap. Used for debugging.
 */
void print_heap(char *buffer, int32_t heap_start_offset, int heap_size, int list_size, external_sort_t *es) {
    // Prints the heap
    int32_t aa;
    char *addr;
    int j;
    for (aa = 0; aa < 1; aa++) {
        addr = buffer + heap_start_offset;
        printf("heap: ");
        for (j = 0; j < heap_size; j++)
            printf(" %d", *(int32_t *)(addr - j * es->record_size));
        printf("| ");
    }
    printf("   ");

    // Prints the list
    for (aa = 0; aa < 1; aa++) {
        addr = buffer + es->page_size;
        printf("list: ");
        for (j = 0; j < list_size; j++)
            printf(" %d", *(int32_t *)(addr + j * es->record_size));
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
    uint8_t (*iterator)(void *state, void *buffer),
    void *iteratorState,
    void *tupleBuffer,
    void *outputFile,
    char *buffer,
    int bufferSizeInBlocks,
    external_sort_t *es,
    long *resultFilePtr,
    metrics_t *metric,
    int8_t (*compareFn)(void *a, void *b),
    int8_t runGenOnly,
    int8_t writeToReadRatio,
    void *sortData) {
    int16_t tuplesPerPage = (es->page_size - es->headerSize) / es->record_size;
    es->compare_fcn = compareFn;
    long lastWritePos = 0;
    int16_t i, status;
    int32_t numSublist = 0;
    void *addr;
    int32_t numShiftOutOutput = 0, numShiftIntoOutput = 0, numShiftOtherBlock = 0;

    /* Distribution estimation variables */
    int16_t avgDistinct = 0; /* Average # of distinct values per run. Multiplied by 10 so can do integer rather than float operations. */
    /* Note: Could be int8_t as larger than 255 is above cutoff for using MinSort. */
    uint8_t numDistinctInRun = 0; /* Number of distinct values in current run */

    int optimistic = true;
    if (optimistic) {
        // Do FLASH MinSort init first
#ifdef DEBUG
        printf("*Optimistic*\n");
#endif

        MinSortState ms;
        ms.buffer = buffer;
        ms.iteratorState = iteratorState;
        ms.memoryAvailable = bufferSizeInBlocks * es->page_size;
        ms.num_records = ((file_iterator_state_t *)iteratorState)->totalRecords;

        init_MinSort(&ms, es, metric, compareFn);
        avgDistinct = 16;

        int16_t numPasses = (int)ceil(log(es->num_pages / bufferSizeInBlocks) / log(bufferSizeInBlocks));
        int32_t nobSortCost = numPasses * (10 + writeToReadRatio) / 10;

#ifdef DEBUG
        printf("Adaptive calculation.\n");
        printf("NOB sort cost. # runs: %d", numSublist);
        printf(" # passes: %d cost: %d\n", numPasses, nobSortCost);
        printf("MinSort cost. Num sublists: %d ", numSublist);
        printf(" Avg. distinct/sublist: %d\n", avgDistinct / 10);
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
            char *outputBuffer = buffer + es->page_size;

            // Main sorting loop for min sort: fetches and writes sorted records in blocks
            while (next_MinSort(&ms, es, (char *)(outputBuffer + count * es->record_size + es->headerSize), metric, compareFn) != NULL) {
                // Store record in block (already done during call to next)
                // buf = (void *)(outputBuffer+count*es->record_size+es->headerSize);
                count++;

                // When a block is full, write it to the output file
                if (count == values_per_page) {
                    *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
                    *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

                    // Write block to the ouput file
                    if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                        return 9;  // Return error code if writing to the output file fails
                    }

                    count = 0;    /* Reset count for the next block */
                    blockIndex++; /* Update to next block id */
                    metric->num_writes++;

#ifdef DEBUG_OUTPUT
                    printf("Wrote output block. Block index: %d\n", blockIndex);
                    for (int k = 0; k < values_per_page; k++) {
                        printf("%3d: 1 Output Record: %d\n", k, outputBuffer + es->headerSize + k * es->record_size + es->key_offset);
                    }
#endif
                }
            }

            // Write last block if there are remaining records
            if (count > 0) {
                *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
                *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

                if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                    return 9;  // Return error code if writing to the output file fails
                }

                count = 0;    /* Reset count for the next block */
                blockIndex++; /* Update to next block id */
                metric->num_writes++;

#ifdef DEBUG_OUTPUT
                printf("Wrote output block. Block index: %d\n", blockIndex);
                for (int k = 0; k < values_per_page; k++) {
                    printf("%3d: 2 Output Record: %d\n", k, *(uint32_t *)(outputBuffer + es->headerSize + k * es->record_size + es->key_offset));
                }
#endif
            }

            close_MinSort(&ms, es);

            *resultFilePtr = 0;
            return 0;
        } else {
            optimistic = 0;
        }
    }

    if (!optimistic) {
        /*                                 */
        /* -----Replacement Selection----- */
        /*                                 */

        // Replacement selection variables
        int32_t recordsRead = 0;
        int32_t heapSize = 0;
        int32_t heapStartOffset = bufferSizeInBlocks * es->page_size - es->record_size;
        int32_t listSize = 0;

        void *lastOutputKey = malloc(es->record_size); /* Pointer to memory storing value of last key output */
        int8_t haveOutputKey = 0;
        int32_t sublistSize = 0; /* size in blocks */
        int32_t outputCount = 0; /* number of values in output block */
        int32_t recordsLeft = 0; /* number of records in buffer */
        void *heapVal, *inputVal;

        // Fill all blocks other than the first with tuples
        addr = buffer + es->page_size;
        for (i = 0; i < (bufferSizeInBlocks - 1) * tuplesPerPage; i++) {
            status = !iterator(sortData, addr);
            if (status == 0)
                break;
            recordsRead++;
            addr += es->record_size;
        }

        recordsLeft = recordsRead;

        // Update metrics
        metric->num_reads += bufferSizeInBlocks - 1;
        metric->num_runs++;

        // Build heap from tuples in filled blocks
        for (i = 0; i < recordsRead; i++) {
            addr -= es->record_size;
            memcpy(tupleBuffer, addr, es->record_size);
            metric->num_memcpys++;
            shiftUp_rev(buffer + heapStartOffset, tupleBuffer, heapSize, es, metric);
            heapSize++;
        }

        // Read each block and sort
        while (recordsLeft != 0) {
            recordsRead = 0;

            // Read in page
            addr = buffer + es->headerSize;
            for (i = 0; i < tuplesPerPage; i++) {
                status = !iterator(sortData, addr);
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
                in_memory_quick_sort(buffer + es->headerSize, (uint32_t)recordsRead, es->record_size, es->key_offset, es->compare_fcn, metric);
            } else if (heapSize < tuplesPerPage) {
                // May have enough records currently in heap to continue last block. TODO: Does this make sense? It will add to last run before starting new one.

                // Move everything in list to the heap
                for (listSize = listSize; listSize > 0; listSize--) {
                    shiftUp_rev(buffer + heapStartOffset, buffer + es->page_size + (listSize - 1) * es->record_size, heapSize, es, metric);
                    heapSize++;
                }

                // If first value in heap is smaller than lastOutputValue then start new sublist, otherwise continue with previous one.
                heapVal = buffer + heapStartOffset;
                if (lastOutputKey == NULL || es->compare_fcn(heapVal, lastOutputKey) < 0) {
                    // Start new sublist
                    numSublist++;

                    // Track number of distinct values per sublist
                    avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct / 10) * 10 / numSublist;
#ifdef DEBUG
                    printf("Number of distinct values in sublist: %d Running average: %d\n", numDistinctInRun, avgDistinct / 10);
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
            for (i = 0; i < tuplesPerPage; i++) {
                // Check if we've read all records from the current page
                if (recordsRead == 0) {
                    // Check if there are any records left
                    if (recordsLeft <= 0)
                        break;

                    // Just copy over from heap
                    memcpy(buffer + es->headerSize + i * es->record_size, buffer + heapStartOffset, es->record_size); /* Heap into input/output block */
                    metric->num_memcpys++;
                    outputCount++;
                    recordsLeft--;

                    // Restore heap
                    heapSize--;
                    if (heapSize > 0)
                        heapify_rev(buffer + heapStartOffset, buffer + heapStartOffset - heapSize * es->record_size, heapSize, es, metric);
                    continue;
                }

                heapVal = buffer + heapStartOffset;
                inputVal = buffer + es->headerSize + i * es->record_size;

                // Check if both heap top and current input value are smaller than the last output key
                // This indicates we need to start a new sorted sublist
                if (haveOutputKey && (es->compare_fcn(heapVal + es->key_offset, lastOutputKey + es->key_offset) < 0 || heapSize <= 0) && es->compare_fcn(inputVal + es->key_offset, lastOutputKey + es->key_offset) < 0) {
                    // Start a new sublist (as cannot use heap value or input value)
                    numSublist++;

                    // Track number of distinct values per sublist
                    avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct / 10) * 10 / numSublist;
#ifdef DEBUG
                    printf("Number of distinct values in sublist: %d Running average: %d\n", numDistinctInRun, avgDistinct / 10);
#endif
                    numDistinctInRun = 1;

                    // Convert unsorted list into heap
                    for (listSize = listSize; listSize > 0; listSize--) {
                        shiftUp_rev(buffer + heapStartOffset, buffer + es->page_size + (listSize - 1) * es->record_size, heapSize, es, metric);
                        heapSize++;
                    }

                    // Restart building the sublist
                    outputCount = 0;
                    haveOutputKey = 0;
                    sublistSize = 0;
                    recordsLeft += i;
                    i = -1;
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
                if ((es->compare_fcn(heapVal + es->key_offset, inputVal + es->key_offset) < 0 && (haveOutputKey == 0 || es->compare_fcn(heapVal + es->key_offset, lastOutputKey + es->key_offset) >= 0)) || (haveOutputKey && es->compare_fcn(inputVal + es->key_offset, lastOutputKey + es->key_offset) < 0)) {
                    // Use the heap value
                    memcpy(tupleBuffer, buffer + es->headerSize + i * es->record_size, es->record_size);              /* Input tuple into buffer */
                    memcpy(buffer + es->headerSize + i * es->record_size, buffer + heapStartOffset, es->record_size); /* Heap into input/output block */
                    metric->num_memcpys += 2;
                    // Determine if the value is different than the last one to estimate the number of distinct values
                    if (numDistinctInRun < 255 && haveOutputKey) {
                        // Value is different
                        metric->num_compar++;
                        if (es->compare_fcn(lastOutputKey + es->key_offset, inputVal + es->key_offset) < 0)
                            numDistinctInRun++;
                    }
                    // lastOutputKey = inputVal;
                    memcpy(lastOutputKey, inputVal, es->record_size);
                    metric->num_memcpys++;

                    // Find somewhere to put the input value
                    if (es->compare_fcn(tupleBuffer + es->key_offset, lastOutputKey + es->key_offset) < 0) {
                        // Restore heap
                        heapSize--;
                        if (heapSize > 0)
                            heapify_rev(buffer + heapStartOffset, buffer + heapStartOffset - heapSize * es->record_size, heapSize, es, metric);

                        // Put value into the unsorted list
                        memcpy(buffer + es->page_size + listSize * es->record_size, tupleBuffer, es->record_size);
                        metric->num_memcpys++;
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
                        if (es->compare_fcn(lastOutputKey + es->key_offset, inputVal + es->key_offset) < 0)
                            numDistinctInRun++;
                    }
                    // Use the newly read value. don't move it, it's in proper place.
                    // lastOutputKey = inputVal;       // Update the last key output
                    memcpy(lastOutputKey, inputVal, es->record_size);
                    metric->num_memcpys++;
                }
                haveOutputKey = 1;
                outputCount++;
                recordsLeft--;
#ifdef DEBUG_HEAP
                print_heap(buffer, heapStartOffset, heapSize, listSize, es);
#endif
                if (recordsLeft == 0)
                    break;
            }

            // Add Page Headers
            *((int32_t *)buffer) = sublistSize;
            *((int16_t *)(buffer + BLOCK_COUNT_OFFSET)) = (int8_t)outputCount;
            memcpy(tupleBuffer, buffer + (outputCount - 1) * es->record_size + es->headerSize, es->key_size);
            memcpy(lastOutputKey, tupleBuffer, es->record_size);
            metric->num_memcpys += 2;

            // Store the last key output temporarily in tuple buffer as once write out then read new block it would be gone
            // Write the output block
            ((file_iterator_state_t *)iteratorState)->fileInterface->writeRel(buffer, PAGE_SIZE, 1, outputFile);
            if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                // File write error
                free(lastOutputKey);
                return 9;
            }

#ifdef DEBUG_OUTPUT
            printf("Wrote block. Sublist: %d ", numSublist);
            printf(" Idx: %d\n", sublistSize);
            // printf("Offset: %lu\n",  ftell(outputFile)-es->page_size);
            for (int k = 0; k < tuplesPerPage; k++) {
                printf("%3d: 3 Output Record: %d\n", k, *(uint32_t *)(buffer + es->headerSize + k * es->record_size + es->key_offset));
            }
#endif

            metric->num_writes += 1;
            sublistSize++;
            outputCount = 0;
        } /* while records left */

        // free(lastOutputKey);
        numSublist = metric->num_runs;
#ifdef ADAPTIVE_SORT_PRINT
        printf("Gen time: %d\n", metric->genTime);
#endif

        // Track number of distinct values per sublist
        avgDistinct = avgDistinct + (numDistinctInRun - avgDistinct / 10) * 10 / numSublist;
#ifdef ADAPTIVE_SORT_PRINT
        printf("Final number of distinct values in sublist: %d Average: %d\n", numDistinctInRun, avgDistinct);
#endif
        numDistinctInRun = 0;
    } /* end pessmistic */

    // No merge phase necessary
    if (numSublist == 1) {
        ((file_iterator_state_t *)iteratorState)->fileInterface->flush(outputFile);
        *resultFilePtr = 0;
        return 0;
    }

    // Run generation phase only (DEBUG)
    if (runGenOnly)
        return 0;

    // lastWritePos = ftell(outputFile);
    lastWritePos = ((file_iterator_state_t *)iteratorState)->fileInterface->tell(outputFile);

    // if (avgDistinct/10 < nobSortCost)
    int bufferSizeBytes = (bufferSizeInBlocks - 1) * es->page_size;                        /* One of the buffers is used for a read buffer */
    int8_t sublistVersionPossible = (numSublist <= bufferSizeBytes / (SORT_KEY_SIZE + 4)); /* +4 is size of file offset pointer. Each record has a key and file offset. */

    if (sublistVersionPossible && avgDistinct > tuplesPerPage)
        avgDistinct = tuplesPerPage * 10;

    int16_t numPasses = (int)ceil(log(numSublist) / log(bufferSizeInBlocks));
    int32_t nobSortCost = numPasses * (10 + writeToReadRatio) / 10;

#ifdef ADAPTIVE_SORT_PRINT
    printf("Adaptive calculation.\n");
    printf("NOB sort cost. # runs: %d", numSublist);
    printf(" # passes: %d cost: %d\n", numPasses, nobSortCost);
    printf("MinSort cost. Num sublists: %d ", numSublist);
    printf(" Avg. distinct/sublist: %d\n", avgDistinct / 10);
#endif

    // Make decision to use either no output buffer sort or MinSort
    if (avgDistinct / 10 < nobSortCost) {
        /*               */
        /*    MinSort    */
        /*               */

        // If can buffer smallest value per sublist, can use a better performing version
        if (sublistVersionPossible) {
            // Use better performing version of minsort
#ifdef ADAPTIVE_SORT_PRINT
            printf("Performing MinSort with sorted sublists\n");
#endif
            ((file_iterator_state_t *)iteratorState)->file = outputFile;
            *resultFilePtr = 0;
            flash_minsort_sublist(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn, numSublist);
            *resultFilePtr = lastWritePos;
        } else {
            // Use normal version of minsort. Do not have enough space to index a value per sublist. Assumes data is not sorted in each region
#ifdef ADAPTIVE_SORT_PRINT
            printf("Performing MinSort\n");
#endif
            ((file_iterator_state_t *)iteratorState)->file = outputFile;
            flash_minsort(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn);
            *resultFilePtr = 0;
        }
    } else {
        /*                                   */
        /*    No Output Buffer Sort Merge    */
        /*                                   */

        /* ----- Merge phase: recursively combine M sublists ----- */
        long mergeSOW; /* start of write */
        int32_t currentBlockId = 0;
        long lastMergeStart = 0; /* start of read */
        long lastMergeEnd = lastWritePos;

        long *sublsFilePtr = (long *)malloc(sizeof(long) * bufferSizeInBlocks);         /* location of current record in file */
        int32_t *sublsBlkPos = (int32_t *)malloc(sizeof(int32_t) * bufferSizeInBlocks); /* current block of sublist being read */
        int32_t *blocksInSublist = (int32_t *)malloc(sizeof(int32_t) * bufferSizeInBlocks);

        int32_t *record1 = (int32_t *)malloc(sizeof(int32_t) * bufferSizeInBlocks); /* current record of each buffered block. (byte offset from start of buffer) */
        int32_t *record2 = (int32_t *)malloc(sizeof(int32_t) * bufferSizeInBlocks); /* current output block record stored in each buffered block (byte offset from start of buffer) */
        /* Output block uses record2 to store position of last to-output record inserted */
        int16_t run = 0;
        int8_t passNumber = 1;
        int32_t numRuns;
        int32_t resultRecOffset = -1; /* Number of records from start of buffer to the next record to output */
        int32_t resultBlock = -1;     /* Block containing next record to output */
        char isRecord2 = 0;           /* 1 if result record is from the output block but stored in a non outputblock */
        int32_t offset = 0;           /* Offset of current record being compared with current smallest record */
        int16_t heapSizeRecords;      /* Number of records in heap */
        char outputIsEmpty = 0;       /* Flag indicating if there are still more input records in sublist in output block */
        int16_t numTransferThisPass;
        int32_t blk = -1;
        int16_t space = 0;
        int16_t outputCursor;
        int8_t destBlk;
        int32_t other = 0;

        // Verify all memory has been allocated successfully
        if (record2 == NULL) {
            free(record1);
            free(sublsBlkPos);
            free(sublsFilePtr);
            free(blocksInSublist);
            return 9;
        }

        while (numSublist > 1) {
            // Check if can finish using minsort with sorted sublists
            // if (numSublist >= 32 && numSublist <= 64)// && avgDistinct/10 < 32)
            // {
            //     // Switch to MinSort to finish off
            //     printf("Finishing sort with MinSort with sorted sublists\n");
            //     ((file_iterator_state_t*) iteratorState)->file = outputFile;
            //     // *resultFilePtr = lastMergeStart;
            //     // fflush(outputFile);
            //     ((file_iterator_state_t *) iteratorState)->fileInterface->flush(outputFile);

            //     *resultFilePtr = lastMergeStart;
            //     flash_minsort_sublist(iteratorState, tupleBuffer, outputFile, buffer, bufferSizeBytes, es, resultFilePtr, metric, compareFn, numSublist);
            //     lastMergeStart = lastMergeEnd;
            //     *resultFilePtr = lastMergeStart;
            //     break; // Sort done
            // }

            // Wrap-around in memory space/file after every 3rd pass
            if (passNumber % 3 == 0) {
                lastWritePos = 0;
            }
#ifdef ADAPTIVE_SORT_PRINT
            printf("Pass number: %u  Comparisons: %lu  MemCopies: %lu  TransferIn: %lu  TransferOut: %lu TransferOther: %lu Other: %lu\n", passNumber, metric->num_compar, metric->num_memcpys, numShiftIntoOutput, numShiftOutOutput, numShiftOtherBlock, other);
#endif

            passNumber++;

            // perform a merge
            mergeSOW = lastWritePos;

            numRuns = (numSublist + bufferSizeInBlocks - 1) / bufferSizeInBlocks; /* Equivalent to CEIL(numSublist/bufferSizeInBlocks) */

            // perform runs
            long ptrLastBlock = lastMergeEnd;
            for (run = 0; run < numRuns; run++) {
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
                for (i = 0; i < sublistsInRun; i++) {
                    /* Read last block of sublist into buffer */
                    // fseek(outputFile, ptrLastBlock - es->page_size, SEEK_SET);
                    // if (0 == fread(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile))
                    // {   /* File read error */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 10;
                    // }

                    ((file_iterator_state_t *)iteratorState)->fileInterface->seek(ptrLastBlock - es->page_size, outputFile);
                    ((file_iterator_state_t *)iteratorState)->fileInterface->readRel(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile);
                    if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                        /* File read error */
                        free(record1);
                        free(record2);
                        free(sublsBlkPos);
                        free(sublsFilePtr);
                        return 10;
                    }

                    metric->num_reads += 1;
                    ptrLastBlock = ptrLastBlock - (*(int32_t *)&buffer[i * es->page_size]) * es->page_size - es->page_size;
                    blocksInSublist[i] = *(int32_t *)&buffer[i * es->page_size] + 1; /* Retrieve block id (indexed from 0 - hence +1) to compute count of blocks in sublist */

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
                            if (es->compare_fcn(buffer + es->headerSize + es->key_offset, buffer + i * es->page_size + es->headerSize + es->key_offset) > 0) {
#ifdef DEBUG
                                void *buffer0Rec = (void *)buffer + es->headerSize;
                                void *currentRec = (void *)buffer + i * es->page_size + es->headerSize;
                                printf("Swapping in buffer 0. Current key: %d  New key: %d\n", *(uint32_t *)(buffer0Rec + es->key_offset), *(uint32_t *)(currentRec + es->key_offset));
#endif
                                // Perform swap
                                sublsBlkPos[i] = sublsFilePtr[0]; /* Note: Using subls_blk_pos[i] as a temp variable during swap */  // TODO: Update swap to not be variable length
                                sublsFilePtr[0] = sublsFilePtr[i];
                                sublsFilePtr[i] = sublsBlkPos[i];
                                sublsBlkPos[i] = blocksInSublist[i];
                                blocksInSublist[i] = blocksInSublist[0];
                                blocksInSublist[0] = sublsBlkPos[i];
                                sublsBlkPos[i] = 0; /* Reset variable back to 0 */
                            }
                        }
                    }
                }

                // Load in first blocks into buffer
                for (i = 0; i < sublistsInRun; i++) {
                    // fseek(outputFile, sublsFilePtr[i], SEEK_SET);
                    // if (0 == fread(&buffer[i * es->page_size], (size_t)es->page_size, 1, outputFile))
                    // {   /* Read error */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 10;
                    // }

                    // Read first block into buffer
                    ((file_iterator_state_t *)iteratorState)->fileInterface->seek(sublsFilePtr[i], outputFile);
                    ((file_iterator_state_t *)iteratorState)->fileInterface->readRel(&buffer[i * es->page_size], PAGE_SIZE, 1, outputFile);
                    if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                        // File read error
                        free(record1);
                        free(record2);
                        free(sublsBlkPos);
                        free(sublsFilePtr);
                        return 10;
                    }

                    metric->num_reads += 1;

#ifdef DEBUG_READ
                    void *firstRec = (void *)buffer + i * es->page_size + es->headerSize;
                    void *lastRec = (void *)buffer + i * es->page_size + es->headerSize + (*((int16_t *)(buffer + i * es->page_size + BLOCK_COUNT_OFFSET)) - 1) * es->record_size;
                    printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", i, (int32_t) * (buffer + i * es->page_size),
                           *((int16_t *)(buffer + i * es->page_size + BLOCK_COUNT_OFFSET)), *(uint32_t *)(firstRec + es->key_offset), *(uint32_t *)(lastRec + es->key_offset));
#endif
                    // Initialize record1 to start of each block and record2 to empty
                    record1[i] = i * es->page_size + es->headerSize;
                    record2[i] = -1;
                }

                // Perform the run
                while (1) {
                    // Find next smallest tuple
                    resultBlock = -1;
                    isRecord2 = 0;

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
                    for (; i < sublistsInRun; i++) {
                        if (record1[i] == -1)
                            continue; /* Sublist has no more records */

                        offset = record1[i];

                        metric->num_compar++;
                        if (0 < es->compare_fcn(buffer + resultRecOffset + es->key_offset, buffer + offset + es->key_offset)) { /* Record is smaller than current smallest record */
                            resultRecOffset = offset;
                            resultBlock = i;
                        }
                    }

                    // Find smallest value of last block, it might be scattered amongst other blocks
                    // Note: For loop code is assuming OUTPUT_BLOCK_ID is 0. Otherwise, i should start at 0 not 1 and must check if i == OUTPUT_BLOCK_ID.
                    for (i = 1; i < sublistsInRun; i++) {
                        if (record2[i] == -1)
                            continue; /* This block has no records from the output block */

                        /* Current value is at start of block in list 2 */
                        offset = i * es->page_size + es->headerSize;

                        if (resultBlock != -1)
                            metric->num_compar++;

                        if ((resultBlock == -1) || 0 < es->compare_fcn(buffer + resultRecOffset + es->key_offset, buffer + offset + es->key_offset)) { /* Record is smaller than current smallest record */
                            resultRecOffset = offset;
                            resultBlock = i;
                            isRecord2 = 1;
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
                    void *buf = (void *)buffer + resultRecOffset;
                    printf("Smallest Record: %d  From list: %d\n", *(uint32_t *)(buf + es->key_offset), resultBlock);
                    printf("List status: 0: (%d, %d) 1: (%d, %d) 2: (%d, %d) ResultList: %d\n", record1[0], record2[0],
                           record1[1], record2[1], record1[2], record2[2], resultBlock);

                    if (*(uint32_t *)(buf + es->key_offset) == 27391) {
                        /* Output all block contents */
                        for (int l = 0; l < 2; l++) {
                            printf("Current  block: %d  # records: %d\n", l, tuplesPerPage);
                            for (int k = 0; k < tuplesPerPage; k++) {
                                void *buf = (void *)(buffer + es->headerSize + k * es->record_size + l * es->page_size);
                                printf("%d: Record: %d  Address: %p\n", k, buf + es->key_size, buf);
                            }
                        }
                        printf("HERE\n");
                    }
#endif

                    /* Add smallest tuple to output position in buffer (may already be in output buffer) */
                    if (resultBlock != OUTPUT_BLOCK_ID) {
                        if ((record1[OUTPUT_BLOCK_ID] == record2[OUTPUT_BLOCK_ID]) && (record1[OUTPUT_BLOCK_ID] != -1)) { /* Output block does not have space for the result record */
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
                            void *buf = (void *)(buffer + record1[OUTPUT_BLOCK_ID]);
                            printf("Output record moved to list %d Key: %d\n", resultBlock, *(uint32_t *)(buf + es->key_size));
#endif
                            /* Move result record into output block (record1[output_block]==record2[output_block]) */
                            metric->num_memcpys++;
                            memcpy(buffer + record2[OUTPUT_BLOCK_ID], buffer + resultRecOffset, (size_t)es->record_size);

                            /* Move displaced output block record out of the temp buffer and into the output list (list2) of the result record's block */
                            if (isRecord2 == 0) { /* Smallest record is not originally from output block */
                                /* Result is from record1 list. Insert into heap of output records for block. */
                                if (record2[resultBlock] == -1)
                                    record2[resultBlock] = resultBlock * es->page_size + es->headerSize;
                                else
                                    record2[resultBlock] += es->record_size;
                                heapSizeRecords = (record2[resultBlock] + es->record_size - resultBlock * es->page_size) / es->record_size;
                                /* Buffered output record is in tuple_buffer */
                                shiftUp(buffer + resultBlock * es->page_size + es->headerSize, tupleBuffer, heapSizeRecords - 1, es, metric);
                            } else {
                                /* Result is from record2 list. Insert the displaced output value into record2 list */
                                heapSizeRecords = (record2[resultBlock] + es->record_size - resultBlock * es->page_size) / es->record_size;

                                /* Output record to be inserted is already stored in the tuple_buffer */
                                heapify(buffer + resultBlock * es->page_size + es->headerSize, tupleBuffer, heapSizeRecords, es, metric);
                            }

                            /* Displaced the output block's current record. Increment to next output block record. */
                            record1[OUTPUT_BLOCK_ID] += es->record_size;
                            if (record1[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + (*((int16_t *)(buffer + OUTPUT_BLOCK_ID * es->page_size + BLOCK_COUNT_OFFSET))) * es->record_size + es->headerSize)
                                // if (record1[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + tuplesPerPage*es->record_size + es->headerSize)
                                record1[OUTPUT_BLOCK_ID] = -1;
                        } else { /* Output block already has an empty slot for the result value. Only need to move result value into result list of output block. */
                            /* Move result record into output block */
                            metric->num_memcpys++;
                            memcpy(buffer + record2[OUTPUT_BLOCK_ID], buffer + resultRecOffset, (size_t)es->record_size);

                            if (isRecord2 == 1) {
                                /* is_record2: result value came from list2 of result block */
                                record2[resultBlock] -= es->record_size;

                                if (record2[resultBlock] < resultBlock * es->page_size + es->headerSize)
                                    record2[resultBlock] = -1;
                                else {
                                    /* Move last value to front of heap */
                                    heapSizeRecords = (record2[resultBlock] + es->record_size - resultBlock * es->page_size) / es->record_size;
                                    heapify(buffer + resultBlock * es->page_size + es->headerSize, buffer + record2[resultBlock] + es->record_size, heapSizeRecords, es, metric);
                                }
                            }
                        }

                        /* increment to next position of block that smallest value was read from */
                        if (isRecord2 == 0)
                            record1[resultBlock] += es->record_size;
                        /* end if smallestblock != output block */
                    } else /* The smallest value is already in output block, move it from record1 to record2 */
                    {
                        if (record2[resultBlock] != record1[resultBlock]) {
                            metric->num_memcpys++;
                            memcpy(buffer + record2[resultBlock], buffer + record1[resultBlock], (size_t)es->record_size);
                        }

                        record1[resultBlock] += es->record_size;
                    } /* end of adding smallest tuple to appropriate block */

                    /* Determine if block with smallest value has any more records in it */
                    if (record1[resultBlock] >= resultBlock * es->page_size + (*((int16_t *)(buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET))) * es->record_size + es->headerSize)
                        record1[resultBlock] = -1;

                    /* Output block is full, write it out */
                    if (record2[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + tuplesPerPage * es->record_size - es->record_size) {
                        // fseek(outputFile, lastWritePos, SEEK_SET);
                        // if (0 == fwrite(buffer + OUTPUT_BLOCK_ID * es->page_size, (size_t)es->page_size, 1, outputFile))
                        // {   /* File write error - Arduino prints 1st value nmemb times if nmemb != 1  */
                        //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                        //     return 9;
                        // }

                        // Setup block header
                        *((int32_t *)buffer) = currentBlockId++;
                        *((int16_t *)(buffer + BLOCK_COUNT_OFFSET)) = (int16_t)tuplesPerPage;

                        ((file_iterator_state_t *)iteratorState)->fileInterface->seek(lastWritePos, outputFile);
                        ((file_iterator_state_t *)iteratorState)->fileInterface->writeRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                        if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                            // File read error
                            free(record1);
                            free(record2);
                            free(sublsBlkPos);
                            free(sublsFilePtr);
                            return 10;
                        }

                        lastWritePos = ((file_iterator_state_t *)iteratorState)->fileInterface->tell(outputFile);
                        record2[OUTPUT_BLOCK_ID] = -1;
                        metric->num_writes++;
#ifdef DEBUG_OUTPUT
                        printf("Wrote output block: %d  # records: %d\n", *((int32_t *)buffer), tuplesPerPage);
                        for (int k = 0; k < tuplesPerPage; k++) {
                            void *buf = (void *)(buffer + es->headerSize + k * es->record_size);
                            printf("%3d: 4 Output Record: %d  Address: %p\n", k, *(uint32_t *)(buf + es->key_offset), buf);
                        }
#endif
                    }

                    /* Read in the next block of a sublist if buffered block is depleted (non-output block) */
                    if ((record1[resultBlock] == -1) && (sublsBlkPos[resultBlock] != -1) && (resultBlock != OUTPUT_BLOCK_ID)) {
                        /* check if we are finished with that sublist */
                        if (sublsBlkPos[resultBlock] >= blocksInSublist[resultBlock] - 1) {
                            sublsBlkPos[resultBlock] = -1; /* sublist is spent */
                            record1[resultBlock] = -1;
                        } else {
                            /* not finished with sublist read in next block of sublist */
                            sublsBlkPos[resultBlock]++;
                            sublsFilePtr[resultBlock] += es->page_size;

                            /* put any output records in this block into other blocks */
                            int32_t originPtr = resultBlock * es->page_size + es->headerSize;
                            int32_t destBlk = OUTPUT_BLOCK_ID;
                            int16_t numTransfer = (record2[resultBlock] - originPtr) / es->record_size + 1;

                            /* while there are still records left to move */
                            while (record2[resultBlock] != -1 && originPtr <= record2[resultBlock]) {
                                /* Find a block with space to store the record */
                                blk = -1;
                                space = 0;
                                while (blk == -1 && space == 0) {
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
                                        destBlk++; /* Go to next destination block if currently at the original block that had smallest value */

                                    if (destBlk > bufferSizeInBlocks) {
#ifdef ADAPTIVE_SORT_PRINT
                                        printf("Incorrect destination block. List 1: (%d, %d) List 2: (%d, %d) List 3: (%d, %d) ResultList: %d\n", record1[0], record2[0],
                                               record1[1], record2[1], record1[2], record2[2], resultBlock);

                                        /* Output all block contents */
                                        for (int l = 0; l < 3; l++) {
                                            printf("Current  block: %d  # records: %d\n", l, tuplesPerPage);
                                            for (int k = 0; k < tuplesPerPage; k++) {
                                                void *buf = (void *)(buffer + es->headerSize + k * es->record_size + l * es->page_size);
                                                printf("%d: Record: %d  Address: %p\n", k, buf + es->key_offset, buf);
                                            }
                                        }
#endif
                                    }
                                }

                                numTransferThisPass = space;
                                if (space > numTransfer)
                                    numTransferThisPass = numTransfer;
                                numTransfer -= numTransferThisPass;

                                if (destBlk == OUTPUT_BLOCK_ID) { /* Returning tuples back to output block */
                                    /* Position record1 input pointer at first space for record to be inserted */
                                    if (record1[destBlk] == -1) { /* There are no input records in sublist 0 currently in the block */
                                        record1[destBlk] = destBlk * es->page_size + (tuplesPerPage - numTransferThisPass) * es->record_size + es->headerSize;
                                        offset = record1[destBlk]; /* Remember first insert location */
                                        for (i = 0; i < numTransferThisPass; i++) {
#ifdef DEBUG
                                            void *buf = (void *)(buffer + originPtr);
                                            printf("Empty output block case. Moved output record back from list %d Key: %d\n", resultBlock, *(uint32_t *)(buf + es->key_offset));
#endif
                                            numShiftIntoOutput++;
                                            /* Get top value from heap */
                                            metric->num_memcpys++;
                                            memcpy(buffer + record1[destBlk], buffer + originPtr, (size_t)es->record_size);

                                            /* Fix heap */
                                            heapSizeRecords = (record2[resultBlock] + es->record_size - resultBlock * es->page_size) / es->record_size;
                                            heapSizeRecords--; /* Subtract 1 as going to use last record in heap as insert record */

                                            heapify(buffer + resultBlock * es->page_size + es->headerSize, (void *)(buffer + record2[resultBlock]), heapSizeRecords, es, metric);
                                            record1[destBlk] += es->record_size;
                                            record2[resultBlock] -= es->record_size;
                                        }
                                        record1[destBlk] = offset; /* Set pointer to first insert location */
                                    } else {
                                        for (i = 0; i < numTransferThisPass; i++) {
                                            record1[destBlk] = record1[destBlk] - es->record_size;
#ifdef DEBUG
                                            void *buf = (void *)(buffer + originPtr);
                                            printf("Moved output record back from list %d Key: %d\n", resultBlock, buf + es->key_offset);
#endif
                                            numShiftIntoOutput++;

                                            /* insertion sort type insert */
                                            int32_t insert_ptr = record1[destBlk];
                                            while (insert_ptr < destBlk * es->page_size + (tuplesPerPage - 1) * es->record_size) {
                                                metric->num_compar++;
#ifdef DEBUG
                                                void *buf = (void *)(buffer + insert_ptr + es->record_size);
                                                printf("Compare with list %d Key: %d\n", resultBlock, buf + es->key_offset);
#endif
                                                if (0 < es->compare_fcn(buffer + originPtr + es->key_offset, buffer + insert_ptr + es->record_size + es->key_offset)) {
                                                    /* shift next_val down */
                                                    metric->num_memcpys++;
                                                    memcpy(buffer + insert_ptr, buffer + insert_ptr + es->record_size, (size_t)es->record_size);
                                                } else
                                                    break;

                                                insert_ptr += es->record_size;
                                            }

                                            metric->num_memcpys++;
                                            memcpy(buffer + insert_ptr, buffer + originPtr, (size_t)es->record_size);
                                            originPtr += es->record_size;
                                        }
                                    }
                                } else {
                                    for (i = 0; i < numTransferThisPass; i++) {
                                        /* insert into a non output block, put into the record2 list of the block */
                                        if (record2[destBlk] == -1)
                                            record2[destBlk] = destBlk * es->page_size + es->headerSize; /* no other record2 values */
                                        else
                                            record2[destBlk] += es->record_size; /* other record2 values */

#ifdef DEBUG
                                        void *buf = (void *)(buffer + originPtr);
                                        printf("Moved output record to list %d Key: %d\n", destBlk, buf + es->key_offset);
#endif
                                        numShiftOtherBlock++;

                                        /* Insert at end of heap */
                                        int32_t heapSizeRecords = (record2[destBlk] + es->record_size - es->page_size * destBlk) / es->record_size;
                                        shiftUp(buffer + destBlk * es->page_size + es->headerSize, buffer + originPtr, heapSizeRecords - 1, es, metric);

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
                            ((file_iterator_state_t *)iteratorState)->fileInterface->seek(sublsFilePtr[resultBlock], outputFile);
                            ((file_iterator_state_t *)iteratorState)->fileInterface->readRel(buffer + resultBlock * es->page_size, PAGE_SIZE, 1, outputFile);
                            if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                                // File read error
                                free(record1);
                                free(record2);
                                free(sublsBlkPos);
                                free(sublsFilePtr);
                                return 10;
                            }

                            metric->num_reads += 1;
                            record2[resultBlock] = -1;
                            record1[resultBlock] = resultBlock * es->page_size + es->headerSize;
#ifdef DEBUG_READ
                            printf("Read block sublist: %d\n", resultBlock);
                            void *firstRec = (void *)buffer + resultBlock * es->page_size + es->headerSize;
                            void *lastRec = (void *)buffer + resultBlock * es->page_size + es->headerSize + (*((int16_t *)(buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET)) - 1) * es->record_size;
                            printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", resultBlock, (int32_t) * (buffer + resultBlock * es->page_size),
                                   *((int16_t *)(buffer + resultBlock * es->page_size + BLOCK_COUNT_OFFSET)), firstRec + es->key_offset, lastRec + es->key_offset);
#endif
                        }
                    } /* end if is the non output block empty */

                    /* Determine if there are no records from the output block left */
                    outputIsEmpty = 1;
                    if (record1[OUTPUT_BLOCK_ID] != -1) {
                        outputIsEmpty = 0;
                    } else {
                        for (i = 0; i < sublistsInRun; i++) {
                            if (i == OUTPUT_BLOCK_ID)
                                continue;

                            if (record2[i] != -1) {
                                outputIsEmpty = 0;
                                break;
                            }
                        }
                    }

                    // read in next block of sublist (output block)
                    if (outputIsEmpty && (-1 != sublsBlkPos[OUTPUT_BLOCK_ID])) {
                        /* check if we are finished with output blocks associated sublist */
                        if (sublsBlkPos[OUTPUT_BLOCK_ID] >= blocksInSublist[OUTPUT_BLOCK_ID] - 1) {
                            sublsBlkPos[OUTPUT_BLOCK_ID] = -1; /* sublist is spent */
                            record1[OUTPUT_BLOCK_ID] = -1;
                        } else {
                            /* sublist isn't empty read in next block of sublist */
                            sublsBlkPos[OUTPUT_BLOCK_ID]++;
                            sublsFilePtr[OUTPUT_BLOCK_ID] += es->page_size;

                            /* if the output block contains results they have to be temporarily stored in other blocks. */
                            if (record2[OUTPUT_BLOCK_ID] != -1) {
                                outputCursor = OUTPUT_BLOCK_ID * es->page_size + es->headerSize;
                                destBlk = 1;

                                /* While there are still output tuples to move */
                                while (outputCursor <= record2[OUTPUT_BLOCK_ID]) {
                                    /* find next block with space to store a tuple. Start at block 1 continue to block N where N>1 */
                                    blk = -1;
                                    space = 0;
                                    while (-1 == blk) {
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
                                    void *buf = (void *)(buffer + outputCursor);
                                    printf("Output list empty so moved record in output to list %d Key: %d\n", destBlk, *(uint32_t *)(buf + es->key_offset));
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

                            ((file_iterator_state_t *)iteratorState)->fileInterface->seek(sublsFilePtr[OUTPUT_BLOCK_ID], outputFile);
                            ((file_iterator_state_t *)iteratorState)->fileInterface->readRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                            if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                                // File read error
                                free(record1);
                                free(record2);
                                free(sublsBlkPos);
                                free(sublsFilePtr);
                                return 10;
                            }

                            int16_t numRecords = *((int16_t *)(buffer + BLOCK_COUNT_OFFSET));
#ifdef DEBUG_READ
                            printf("Read block sublist: 0\n");
                            void *firstRec = (void *)buffer + es->headerSize;
                            void *lastRec = (void *)buffer + es->headerSize + (*((int16_t *)(buffer + BLOCK_COUNT_OFFSET)) - 1) * es->record_size;
                            printf("Read Sublist: %d Block: %d NumRec: %d First key: %d Last key: %d\n", 0, (int32_t) * (buffer + 0 * es->page_size),
                                   *((int16_t *)(buffer + BLOCK_COUNT_OFFSET)), firstRec + es->key_offset, lastRec + es->key_offset);
#endif

                            metric->num_reads += 1;
                            record1[OUTPUT_BLOCK_ID] = OUTPUT_BLOCK_ID * es->page_size + es->headerSize;

                            /* put the results back into the output block, re-add them in reverse order from when we removed them (blocks N to 1)
                             * This will keep the blocks in sorted order.  */
                            if (record2[OUTPUT_BLOCK_ID] != -1) {
                                outputCursor = OUTPUT_BLOCK_ID * es->page_size + es->headerSize;

                                for (blk = 0; blk < sublistsInRun; blk++) {
                                    if (record2[blk] == -1)
                                        continue;

                                    if (blk == OUTPUT_BLOCK_ID)
                                        continue;

                                    int16_t blkCursor = blk * es->page_size + es->headerSize;
                                    int16_t limit = record2[blk];

                                    /* Output block read may not be full of input records. Only swap the input records. */
                                    i = 0;
                                    while (blkCursor <= limit && i < numRecords) {
                                        i++;
                                        metric->num_memcpys += 3;
                                        /* swap record */
                                        memcpy(tupleBuffer, buffer + blkCursor, (size_t)es->record_size);
                                        memcpy(buffer + blkCursor, buffer + outputCursor, (size_t)es->record_size);
                                        memcpy(buffer + outputCursor, tupleBuffer, (size_t)es->record_size);
                                        outputCursor += es->record_size;
                                        blkCursor += es->record_size;
                                        numShiftIntoOutput++;
                                    }
                                    /* Copy back to output block all remaining records into the free space in the output block */
                                    while (blkCursor <= limit) {
                                        metric->num_memcpys += 1;
                                        memcpy(buffer + outputCursor, buffer + blkCursor, (size_t)es->record_size);
                                        outputCursor += es->record_size;
                                        blkCursor += es->record_size;
                                        numShiftIntoOutput++;
                                        record2[blk] -= es->record_size;
                                    }
                                }

                                record1[OUTPUT_BLOCK_ID] = record2[OUTPUT_BLOCK_ID] + es->record_size;

                                if (record1[OUTPUT_BLOCK_ID] >= OUTPUT_BLOCK_ID * es->page_size + es->headerSize + numRecords * es->record_size)
                                    record1[OUTPUT_BLOCK_ID] = -1;
                            }
                        }
                        /*end of reading in next output block */
                    }
                    /* end of run */
                }

                if (record2[0] > 0) { /* Tuples in output block to write out */
                    // fseek(outputFile, lastWritePos, SEEK_SET);
                    // if (0 == fwrite(buffer + OUTPUT_BLOCK_ID * es->page_size, (size_t)es->page_size, 1, outputFile))
                    // {   /* File write error - arduino prints 1st value nmemb times if nmemb != 1 */
                    //     free(record1); free(record2); free(sublsBlkPos); free(sublsFilePtr);
                    //     return 9;
                    // }

                    // setup header
                    *((int32_t *)buffer) = currentBlockId;
                    *((int16_t *)(buffer + BLOCK_COUNT_OFFSET)) = (int16_t)(record2[0] - es->headerSize) / es->record_size + 1;
                    currentBlockId++;

                    ((file_iterator_state_t *)iteratorState)->fileInterface->seek(lastWritePos, outputFile);
                    ((file_iterator_state_t *)iteratorState)->fileInterface->writeRel(buffer + OUTPUT_BLOCK_ID * es->page_size, PAGE_SIZE, 1, outputFile);
                    if (((file_iterator_state_t *)iteratorState)->fileInterface->error(outputFile)) {
                        // File write error
                        free(record1);
                        free(record2);
                        free(sublsBlkPos);
                        free(sublsFilePtr);
                        return 10;
                    }

                    lastWritePos = ((file_iterator_state_t *)iteratorState)->fileInterface->tell(outputFile);
                    record2[OUTPUT_BLOCK_ID] = -1;
                    metric->num_writes += 1;

#ifdef DEBUG_OUTPUT
                    printf("Wrote output block here.\n");
                    for (int k = 0; k < tuplesPerPage; k++) {
                        void *buf = (void *)(buffer + es->headerSize + k * es->record_size);
                        printf("%3d: 5 Output Record: %d  Address: %p\n", k, *(uint32_t *)(buf + es->key_offset), buf);  // TODO: Update to no use test_record_t
                    }
#endif
                }

            } /* end of runs */

            numSublist = numRuns;      /* each run produces 1 sublist */
            lastMergeStart = mergeSOW; /* next merge reads where this one started writing */
            lastMergeEnd = lastWritePos;
        } /* end of merge */
        *resultFilePtr = lastMergeStart;
#ifdef ADAPTIVE_SORT_PRINT_FINISH
        printf("Complete. Comparisons: %u  Writes: %u  Reads: %u Memcpys:\n", metric->num_compar, metric->num_writes, metric->num_reads, metric->num_memcpys);
#endif

        /* cleanup */
        free(sublsFilePtr);
        free(sublsBlkPos);
        free(blocksInSublist);
        free(record1);
        free(record2);
    }

    return 0;
}

/************************************************************flash_minsort.c************************************************************/
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

// #define DEBUG 1
// #define DEBUG_OUTPUT 1
// #define DEBUG_READ 1

#ifndef INT_MAX
#define INT_MAX 0xFFFFFFFF
#endif

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

    // Read page into the buffer
    if (0 == is->fileInterface->read(ms->buffer, pageNum, es->page_size, fp)) {
        printf("MINSORT: Failed to read block.\n");
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
#ifdef FLASH_MINSORT_PRINT
    printf("Memory overhead: %d  Max regions: %d\r\n", 2 * es->key_size + INT_SIZE, j);
#endif
    ms->blocks_per_region = (uint32_t)ceil((double)ms->numBlocks / j);
    ms->numRegions = (uint32_t)ceil((double)ms->numBlocks / ms->blocks_per_region);

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
        readPageMinSort(ms, i, es, metric);  // Load block i into buffer
        regionIdx = i / ms->blocks_per_region;

        // Set inital value to first read.
        // ms->min[regionIdx] = getValuePtr(ms, 0, es);
        memcpy(getMinRegionPtr(ms, regionIdx, es), getValuePtr(ms, 0, es), es->key_size);
        metric->num_memcpys++;
        ms->min_initialized[regionIdx] = true;

        /* Process remaining records in the block */
        for (j = 1; j < ms->records_per_block; j++) {
            if (((i * ms->records_per_block) + j) < ms->num_records) {
                val = getValuePtr(ms, j, es);
                metric->num_compar++;

                /* Update region’s minimum if current record is smaller */
                if (compareFn(val, getMinRegionPtr(ms, regionIdx, es)) == -1) {
                    memcpy(getMinRegionPtr(ms, regionIdx, es), val, es->key_size);
                    metric->num_memcpys++;
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
        ms->regionIdx = INT_MAX;  // Reset the region index to indicate no region has been selected yet

        for (i = 0; i < ms->numRegions; i++) {
            metric->num_compar++;

            // If the current region has a valid minimum, and it's less than the current tuple, update the minimum
            if (ms->min_initialized[i] && (!ms->current_initialized || compareFn(getMinRegionPtr(ms, i, es), ms->current) == -1)) {
                memcpy(ms->current, getMinRegionPtr(ms, i, es), es->key_size);  // ms->current = ms->min[i];
                metric->num_memcpys++;
                ms->current_initialized = true;
                ms->regionIdx = i;  // Update the region index to the one containing the new minimum
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

        if (curBlk > ms->numBlocks) {
            break;
        }

        // Read the current block into the buffer if it's not already loaded
        if (curBlk != ms->lastBlockIdx) {
            readPageMinSort(ms, curBlk, es, metric);
        }

        for (i = startIndex % ms->records_per_block; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records) {
                break;  // Stop if we've reached the end of records in the block
            }

            dataVal = getValuePtr(ms, i, es);  // Pointer to the current record's value
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
                i++;  // Move to the next record
                ms->tuplesOut++;
                goto done;  // Exit the loop since we found the record we were looking for
            }
            metric->num_compar++;

            // If the current record is greater than the current minimum and is smaller than the next, update the next minimum
            if (compareFn(dataVal, ms->current) == 1 && (!ms->next_initialized || compareFn(dataVal, ms->next) == -1)) {
                memcpy(ms->next, dataVal, es->key_size);  // ms->next = dataVal;
                metric->num_memcpys++;
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

        if (curBlk >= ms->numBlocks) {
            break;
        }

        // If the block is not already loaded, read it into the buffer
        if (curBlk != ms->lastBlockIdx) {
            readPageMinSort(ms, curBlk, es, metric);
            i = 0;
        }

        // Search through the records in the block
        for (; i < ms->records_per_block; i++) {
            if (curBlk * ms->records_per_block + i >= ms->num_records) {
                break;  // Stop if we've reached the end of records in the block
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
                memcpy(ms->next, dataVal, es->key_size);  // Update the next tuple
                metric->num_memcpys++;
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
            memcpy(getMinRegionPtr(ms, ms->regionIdx, es), ms->next, es->key_size);  // Update the region's minimum
            metric->num_memcpys++;
            ms->next_initialized = false;
            ms->min_initialized[ms->regionIdx] = true;
        }

#ifdef DEBUG
        printf("Updated minimum in block to: %d\r\n", ms->min[ms->regionIdx]);
#endif
    }

    return tupleBuffer;  // Update the region's minimum
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
    int8_t (*compareFn)(void *a, void *b)) {
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
            count = 0;                                                 // Reset count for the next block

            // Write the block to the output file using the file interface's write method
            if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
                return 9;  // Return error code if writing to the output file fails
            }

#ifdef DEBUG
            printf("Wrote output block. Block index: %d\n", blockIndex);
            for (int k = 0; k < values_per_page; k++) {
                test_record_t *buf = (void *)(outputBuffer + es->headerSize + k * es->record_size);
                printf("%d: Output Record: %d\n", k, buf->key);
            }
#endif
            metric->num_writes++;
            blockIndex++;
        }
    }

    // Write the last block if there are remaining records
    if (count > 0) {
        *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
        *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

        if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, blockIndex, es->page_size, outputFile)) {
            return 9;  // Return error code if writing to the output file fails
        }
        metric->num_writes++;
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

    return 0;  // Successful completion
}

/************************************************************flash_minsort_sublist.c************************************************************/
/******************************************************************************/
/**
@file		flash_minsort_sublist.c
@author		Ramon Lawrence
@brief		Flash Minsort designed to handle regions that are sorted sublists.
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
#define DEBUG 1
#define DEBUG_OUTPUT 1
#define DEBUG_READ 1
*/

void readPage_sublist(MinSortStateSublist *ms, int pageNum, external_sort_t *es, metrics_t *metric) {
    file_iterator_state_t *is = (file_iterator_state_t *)ms->iteratorState;
    void *fp = is->file;

    // Read page into the buffer
    if (0 == is->fileInterface->read(ms->buffer, pageNum, es->page_size, fp)) {
        printf("MINSORT SUBLIST: Failed to read block.\n");
    }

    metric->num_reads++;
    ms->blocksRead++;
    ms->lastBlockIdx = pageNum;

#ifdef DEBUG_READ
    printf("Reading block: %d Offset: %lu\n", pageNum, offset);
    for (int k = 0; k < 31; k++) {
        test_record_t *buf = (void *)(ms->buffer + es->headerSize + k * es->record_size);
        printf("%d: Record: %d\n", k, buf->key);
    }
#endif
}

int32_t getBlockId(MinSortStateSublist *ms) {
    return *((int32_t *)(ms->buffer));
}

int16_t getNumRecordsBlock(MinSortStateSublist *ms) {
    return *((int16_t *)(ms->buffer + BLOCK_COUNT_OFFSET));
}

/* Returns a value of a tuple given a record number in a block (that has been previously buffered) */
void *getTuple_sublist(MinSortStateSublist *ms, int recordNum, external_sort_t *es) {
    // test_record_t *buf = (test_record_t*) (ms->buffer+es->headerSize+recordNum*es->record_size);
    // return buf->key;
    return (void *)(ms->buffer + es->headerSize + recordNum * es->record_size);
}

void init_MinSort_sublist(MinSortStateSublist *ms, external_sort_t *es, metrics_t *metric) {
    unsigned int i = 0, j = 0, regionIdx = 0;

    /* Operator statistics */
    ms->blocksRead = 0;
    ms->tuplesRead = 0;
    ms->tuplesOut = 0;
    ms->bytesRead = 0;

    ms->record_size = es->record_size;
    ms->numBlocks = es->num_pages;

    // Ignoring small variable overhead
    // j = (ms->memoryAvailable - 2 * SORT_KEY_SIZE - INT_SIZE) / SORT_KEY_SIZE;
    j = (ms->memoryAvailable) / (SORT_KEY_SIZE + sizeof(uint8_t));
#ifdef FLASH_MINSORT_PRINT
    printf("Memory overhead: %d  Max regions: %d\r\n", 2 * SORT_KEY_SIZE + INT_SIZE, j);
#endif
    // Memory allocation
    // Allocate minimum index in separate memory space (block 0 is input buffer, block 1 is output buffer)
    // Block 1 as output buffer is not being counted in this case,
    // TODO: Challenge with this as if given only 2 buffers then have no room for minimum index. Creating separate allocated arrays for now.
    // Note: Assuming MinSort does need actually count the output buffer for its use as it can produce records in iterator format and does not need an output buffer for this.
    ms->current = malloc(es->record_size);
    ms->next = malloc(es->record_size);

    ms->min = malloc(ms->numRegions * es->record_size);
    ms->min_set = malloc(ms->numRegions * sizeof(uint8_t));
    ms->offset = malloc(ms->numRegions * sizeof(long));
#ifdef FLASH_MINSORT_PRINT
    printf("Page size: %d, Memory size: %d Record size: %d, Number of records: %lu, Number of blocks: %d, Regions: %d\r\n",
           es->page_size, ms->memoryAvailable, ms->record_size, ms->num_records, ms->numBlocks, ms->numRegions);
#endif

    for (i = 0; i < ms->numRegions; i++)
        ms->min_set[i] = false;
    regionIdx = ms->numRegions - 1;

    /* Scan data to populate the minimum in each region */
    /* Read from back of output file to get start of each sublist (region) */

    /* Read last block of sublist into buffer */
    long lastBlock = ms->numBlocks - 1;
    while (lastBlock >= 0) {
        readPage_sublist(ms, lastBlock, es, metric);
        int numBlocksSublist = *(int32_t *)ms->buffer; /* Retrieve block id (indexed from 0) to compute count of blocks in sublist */

#if DEBUG
        printf("Read block: %d", lastBlock);
        printf(" Num: %d\n", numBlocksSublist);

        for (int k = 0; k < 31; k++) {
            test_record_t *buf = (void *)(ms->buffer + es->headerSize + k * es->record_size);
            printf("%d: Record: %d\n", k, buf->key);
        }
#endif
        lastBlock = lastBlock - numBlocksSublist;
        readPage_sublist(ms, lastBlock, es, metric);

        // val = getTuple_sublist(ms, 0, es);
        // ms->min[regionIdx] = val;
        memcpy(ms->min + es->record_size * regionIdx, getTuple_sublist(ms, 0, es), es->value_size);
        metric->num_memcpys++;
        ms->min_set[regionIdx] = true;
        ms->offset[regionIdx] = lastBlock * es->page_size + es->headerSize + ms->fileOffset;
#if DEBUG
        printf("New min. Index: %d", regionIdx);
        printf(" Min: %u", ms->min[regionIdx]);
        printf(" Offset: %lu\n", ms->offset[regionIdx]);
#endif
        regionIdx--;
        lastBlock--;
    }

#ifdef DEBUG
    printf("Region summary\n");
    for (i = 0; i < ms->numRegions; i++) {
        printf("Reg: %d", i);
        printf(" Min: %u", ms->min[i]);
        printf(" Offset: %lu\n", ms->offset[i]);
    }

#endif

    // ms->current = INT_MAX;
    // ms->next    = INT_MAX;
    // ms->lastBlockIdx = INT_MAX;
    ms->current_set = false;
    ms->next_set = false;
    ms->lastBlockIdx_set = false;
    ms->nextIdx = 0;
}

char *next_MinSort_sublist(MinSortStateSublist *ms, external_sort_t *es, void *tupleBuffer, metrics_t *metric) {
    unsigned int i, curBlk;
    unsigned long int startIndex;

    // Find the block with the minimum tuple value - otherwise continue on with last block
    if (ms->nextIdx == 0) {
        // Find new block as do not know location of next minimum tuple
        ms->current_set = false;
        ms->regionIdx_set = false;
        ms->next_set = false;

        for (i = 0; i < ms->numRegions; i++) {
            metric->num_compar++;

            // if (ms->min[i] < ms->current)
            // {   ms->current = ms->min[i];
            //     ms->regionIdx = i;
            // }

            // If min is set update current if current is not set or min is less than current
            if (ms->min_set[i] && (!ms->current_set || es->compare_fcn(ms->min + i * es->record_size + es->key_offset, ms->current + es->key_offset) < 0)) {
                memcpy(ms->current, ms->min + i * es->record_size, es->record_size);
                metric->num_memcpys++;
                ms->regionIdx = i;
                ms->regionIdx_set = true;
                ms->current_set = true;
            }
        }
        if (!ms->regionIdx_set)
            return NULL;  // Join complete - no more tuples

        // Determine current block and record index for next smallest value based on file offset
        startIndex = ms->offset[ms->regionIdx];
        i = (startIndex % es->page_size - es->headerSize) / ms->record_size;
        curBlk = startIndex / es->page_size;

        // Smallest value is at current index
        if (curBlk != ms->lastBlockIdx) {  // Read block into buffer
            readPage_sublist(ms, curBlk, es, metric);
        }
    } else {  // Use next record in current block
        i = ms->nextIdx;
    }

    memcpy(tupleBuffer, ms->buffer + ms->record_size * i + es->headerSize, ms->record_size);
    metric->num_memcpys++;

#ifdef DEBUG
    test_record_t *buf = (test_record_t *)(ms->buffer + es->headerSize + i * es->record_size);
    buf = (test_record_t *)tupleBuffer;
    printf("Returning tuple: %d\n", buf->key);
#endif

    // Advance to next tuple in block
    i++;
    ms->nextIdx = 0;

    if (i >= getNumRecordsBlock(ms)) {
        // Advance to next block
        i = 0;
        int32_t currentBlockId = getBlockId(ms);
        curBlk++;
        readPage_sublist(ms, curBlk, es, metric);
        if (currentBlockId >= getBlockId(ms)) {
            // Transitioned to a block in a new sublist
            // ms->min[ms->regionIdx] = INT_MAX;
            ms->offset[ms->regionIdx] = -1;
            ms->min_set[ms->regionIdx] = false;
        } else {
            // ms->min[ms->regionIdx] = getTuple_sublist(ms,0,es);
            ms->offset[ms->regionIdx] = curBlk * es->page_size + es->headerSize;
            memcpy(ms->min + es->record_size * ms->regionIdx, getTuple_sublist(ms, 0, es), es->value_size);
            metric->num_memcpys++;
            ms->min_set[ms->regionIdx] = true;
        }
    } else {
        // ms->min[ms->regionIdx] = getTuple_sublist(ms,i,es);
        ms->offset[ms->regionIdx] += es->record_size;
        memcpy(ms->min + es->record_size * ms->regionIdx, getTuple_sublist(ms, i, es), es->value_size);
        metric->num_memcpys++;
        ms->min_set[ms->regionIdx] = true;

        // Current tuple is set and each to min tuple
        if (ms->current_set && es->compare_fcn(ms->min + i * es->record_size + es->key_offset, ms->current + es->key_offset) == 0) {
            ms->nextIdx = i;
        }
        // if (ms->min[ms->regionIdx]  == ms->current)
        //     ms->nextIdx = i;
    }

#ifdef DEBUG
    printf("Updated minimum in block to: %d\r\n", ms->min[ms->regionIdx]);
#endif

    return tupleBuffer;
}

void close_MinSort_sublist(MinSortStateSublist *ms, external_sort_t *es) {
    /*
    printf("Tuples out:  %lu\r\n", ms->op.tuples_out);
    printf("Blocks read: %lu\r\n", ms->op.blocks_read);
    printf("Tuples read: %lu\r\n", ms->op.tuples_read);
    printf("Bytes read:  %lu\r\n", ms->op.bytes_read);
    */
}

/**
@brief      Flash Minsort implemented that has input file with sorted sublists.
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
@param      numSubList
                Number of sublists
*/
int flash_minsort_sublist(
    void *iteratorState,
    void *tupleBuffer,
    void *outputFile,
    char *buffer,
    int bufferSizeInBytes,
    external_sort_t *es,
    long *resultFilePtr,
    metrics_t *metric,
    int8_t (*compareFn)(void *a, void *b),
    long numSubList) {
#ifdef FLASH_MINSORT_PRINT
    printf("*Flash Minsort (sorted sublist version)*\n");
#endif

    MinSortStateSublist ms;
    ms.buffer = buffer;
    ms.iteratorState = iteratorState;
    ms.memoryAvailable = bufferSizeInBytes;
    ms.num_records = ((file_iterator_state_t *)iteratorState)->totalRecords;
    ms.numRegions = numSubList;
    ms.fileOffset = *resultFilePtr;

    init_MinSort_sublist(&ms, es, metric);
    int16_t count = 0;
    int32_t blockIndex = 0;
    int16_t values_per_page = (es->page_size - es->headerSize) / es->record_size;
    char *outputBuffer = buffer + es->page_size;
    unsigned long lastWritePos = ms.fileOffset + es->num_pages * es->page_size;

    // Write
    while (next_MinSort_sublist(&ms, es, (char *)(outputBuffer + count * es->record_size + es->headerSize), metric) != NULL) {
        // Store record in block (already done during call to next)
        count++;

        if (count == values_per_page) {                                // Write block
            *((int32_t *)outputBuffer) = blockIndex;                   /* Block index */
            *((int16_t *)(outputBuffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */
            count = 0;

            // Force seek to end of file as outputFile is also inputFile and have been reading it
            ((file_iterator_state_t *)iteratorState)->fileInterface->seek(lastWritePos, outputFile);
            // Write the block to the output file using the file interface's write method
            if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->writeRel(outputBuffer, es->page_size, 1, outputFile)) {
                return 9;  // Return error code if writing to the output file fails
            }

            lastWritePos += es->page_size;
            metric->num_writes += 1;
            /*
            printf("Loc2: %lu\n", ftell(outputFile));
                         if (blockIndex % 16 == 0)
                            printf("Last write pos: %lu Block: %d\n", lastWritePos, blockIndex);
                            */
#ifdef DEBUG_OUTPUT
            printf("Wrote output block. Block index: %d\n", blockIndex);
            for (int k = 0; k < values_per_page; k++) {
                test_record_t *buf = (void *)(outputBuffer + es->headerSize + k * es->record_size);
                printf("%d: Output Record: %d\n", k, buf->key);
            }
#endif
            blockIndex++;
        }
    }

    // Write the last block if there are remaining
    if (count > 0) {
        // fseek(outputFile, lastWritePos, SEEK_SET);
        ((file_iterator_state_t *)iteratorState)->fileInterface->seek(lastWritePos, outputFile);

        *((int32_t *)buffer) = blockIndex;                   /* Block index */
        *((int16_t *)(buffer + BLOCK_COUNT_OFFSET)) = count; /* Block record count */

        if (0 == ((file_iterator_state_t *)iteratorState)->fileInterface->write(outputBuffer, es->page_size, 1, outputFile)) {
            return 9;  // Return error code if writing to the output file fails
        }

        metric->num_writes += 1;
        blockIndex++;
        count = 0;
    }

    ((file_iterator_state_t *)iteratorState)->fileInterface->flush(outputFile);

    close_MinSort_sublist(&ms, es);

    *resultFilePtr = 0;
    free(ms.min);
    free(ms.offset);
    free(ms.current);
    free(ms.next);

    //    printf("Complete. Comparisons: %d  MemCopies: %d  TransferIn: %d  TransferOut: %d TransferOther: %d\n", metric->num_compar, metric->num_memcpys, numShiftIntoOutput, numShiftOutOutput, numShiftOtherBlock);

    return 0;
}

/************************************************************in_memory_sort.c************************************************************/
/******************************************************************************/
/**
@file
@author		Kris Wallperington
@brief		Implementation of an in-place, recursive quicksort written by the author.
@copyright	Copyright 2016
                                The University of British Columbia,
                                IonDB Project Contributors (see AUTHORS.md)
@par
                        Licensed under the Apache License, Version 2.0 (the "License");
                        you may not use this file except in compliance with the License.
                        You may obtain a copy of the License at
                                        http://www.apache.org/licenses/LICENSE-2.0
@par
                        Unless required by applicable law or agreed to in writing,
                        software distributed under the License is distributed on an
                        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                        either express or implied. See the License for the specific
                        language governing permissions and limitations under the
                        License.
*/
/******************************************************************************/
// TODO: quick sort throws a seg fault on pc when sorting large arrays (>20000). This may be due to the stack overflowing
//  from the recursive calls to in_memory_quick_sort_helper(...)

int8_t
merge_sort_int32_comparator(
    void *a,
    void *b) {
    int32_t result = *((int32_t *)a) - *((int32_t *)b);
    if (result < 0) return -1;
    if (result > 0) return 1;
    return 0;
}

void in_memory_swap(
    void *tmp_buffer,
    int value_size,
    char *a,
    char *b) {
    memcpy(tmp_buffer, a, value_size);
    memcpy(a, b, value_size);
    memcpy(b, tmp_buffer, value_size);
}

void *in_memory_quick_sort_partition(
    void *tmp_buffer,
    int value_size,
    int key_offset,
    int8_t (*compare_fcn)(void *a, void *b),
    char *low,
    char *high,
    metrics_t *metric) {
    char *pivot = low;
    char *lower_bound = low - value_size;
    char *upper_bound = high + value_size;

    while (1) {
        do {
            upper_bound -= value_size;
            metric->num_compar++;
        } while (compare_fcn(upper_bound + key_offset, pivot + key_offset) > 0);

        do {
            lower_bound += value_size;
            metric->num_compar++;
        } while (compare_fcn(lower_bound + key_offset, pivot + key_offset) < 0);

        if (lower_bound < upper_bound) {
            in_memory_swap(tmp_buffer, value_size, lower_bound, upper_bound);
            metric->num_memcpys += 3;
        } else {
            return upper_bound;
        }
    }
}

void in_memory_quick_sort_helper(
    void *tmp_buffer,
    uint32_t num_values,
    int value_size,
    int key_offset,
    int8_t (*compare_fcn)(void *a, void *b),
    char *low,
    char *high,
    metrics_t *metric) {
    if (low < high) {
        char *pivot = (char *)in_memory_quick_sort_partition(tmp_buffer, value_size, key_offset, compare_fcn, low, high, metric);

        in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, key_offset, compare_fcn, low, pivot, metric);
        in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, key_offset, compare_fcn, pivot + value_size, high, metric);
    }
}

int in_memory_quick_sort(
    void *data,
    uint32_t num_values,
    int value_size,
    int key_offset,
    int8_t (*compare_fcn)(void *a, void *b),
    metrics_t *metric) {
    void *tmp_buffer = malloc(value_size);
    if (NULL == tmp_buffer) return 8;

    /*void* low = data*/
    char *high = (char *)data + (num_values - 1) * value_size;
    in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, key_offset, compare_fcn, (char *)data, high, metric);

    free(tmp_buffer);

    return 0;
}
/************************************************************no_output_heap.c************************************************************/
/******************************************************************************/
/**
@file		replacement_heap.c
@author		Riley Jackson, Ramon Lawrence
@brief		File-based replacement selection
@copyright	Copyright 2019
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
 *Starts with empty root and recursively moves tuples into their empty parent. Stops when the input tuple can be inserted into the parent instead while maintaining sorted order.
 */
void heapify(char *buffer,
             void *input_tuple,
             int32_t size,
             external_sort_t *es,
             metrics_t *metric) {
    int32_t left, right, smallest;
    int32_t i = 0;
    while (1) {
        left = 2 * i + 1;
        right = left + 1;

        if (left >= size)
            break;

        // find if left or right is smallest
        metric->num_compar++;
        if (right < size && es->compare_fcn(buffer + right * es->record_size + es->key_offset, buffer + left * es->record_size + es->key_offset) < 0)
            smallest = right;
        else
            smallest = left;

        // is input tuple the smallest
        metric->num_compar++;
        if (es->compare_fcn(input_tuple + es->key_offset, buffer + smallest * es->record_size + es->key_offset) < 0)
            break;

        // Perform shift
        metric->num_memcpys++;
        memcpy(buffer + i * es->record_size, buffer + smallest * es->record_size, (size_t)es->record_size);
        i = smallest;
    }
    // insert the tuple
    metric->num_memcpys++;
    memcpy(buffer + i * es->record_size, input_tuple, (size_t)es->record_size);
}
/*
 * Shifts parent node of current child at idx into the child. Stops shifting parents and inserts the insert_tuple into the idx when
 * the idx points to the position where the input tuple belongs in sorted order.
 */
void shiftUp(char *buffer,
             void *input_tuple,
             int32_t idx,
             external_sort_t *es,
             metrics_t *metric) {
    int32_t parent;

    while (idx > 0) {
        parent = (idx - 1) / 2;

        metric->num_compar++;
        if (es->compare_fcn(input_tuple, buffer + parent * es->record_size + es->key_offset) >= 0) {
            break;
        }
        metric->num_memcpys++;
        memcpy(buffer + idx * es->record_size, buffer + parent * es->record_size, (size_t)es->record_size);
        idx = parent;
    }
    metric->num_memcpys++;
    memcpy(buffer + idx * es->record_size, input_tuple, (size_t)es->record_size);
}

/*
 *Starts with empty root and recursively moves tuples into their empty parent. Stops when the input tuple can be inserted into the parent instead while maintaining sorted order.
 * Heap function assumes root is at end of array and works backwards
 */
void heapify_rev(char *buffer,
                 void *input_tuple,
                 int32_t size,
                 external_sort_t *es,
                 metrics_t *metric) {
    int32_t left, right, smallest;
    int32_t i = 0;
    while (1) {
        left = 2 * i + 1;
        right = left + 1;

        if (left >= size)
            break;

        // find if left or right is smallest
        metric->num_compar++;
        if (right < size && es->compare_fcn(buffer - right * es->record_size + es->key_offset, buffer - left * es->record_size + es->key_offset) < 0)
            smallest = right;
        else
            smallest = left;

        // is input tuple the smallest
        metric->num_compar++;
        if (es->compare_fcn(input_tuple + es->key_offset, buffer - smallest * es->record_size + es->key_offset) < 0)
            break;

        // Perform shift
        metric->num_memcpys++;
        memcpy(buffer - i * es->record_size, buffer - smallest * es->record_size, (size_t)es->record_size);
        i = smallest;
    }
    // insert the tuple
    metric->num_memcpys++;
    memcpy(buffer - i * es->record_size, input_tuple, (size_t)es->record_size);
}
/*
 * Shifts parent node of current child at idx into the child. Stops shifting parents and inserts the insert_tuple into the idx when
 * the idx points to the position where the input tuple belongs in sorted order.
 * Heap function assumes root is at end of array and works backwards
 */
void shiftUp_rev(char *buffer,
                 void *input_tuple,
                 int32_t idx,
                 external_sort_t *es,
                 metrics_t *metric) {
    int32_t parent;

    while (idx > 0) {
        parent = (idx - 1) / 2;

        metric->num_compar++;
        if (es->compare_fcn(input_tuple + es->key_offset, buffer - parent * es->record_size + es->key_offset) >= 0) {
            break;
        }
        metric->num_memcpys++;
        memcpy(buffer - idx * es->record_size, buffer - parent * es->record_size, (size_t)es->record_size);
        idx = parent;
    }
    metric->num_memcpys++;
    memcpy(buffer - idx * es->record_size, input_tuple, (size_t)es->record_size);
}

/************************************************************sortWrapper.c************************************************************/

#define PRINT_METRIC

// External declaration for setupFile function
extern void *setupFile(const char *filename);

// Forward declaration for pure in-memory sort (no file I/O)
file_iterator_state_t *startPureMemorySort(sortData *data, embedDBOperator *op);

/**
 * @brief Pure in-memory sort that avoids file I/O completely for very small datasets
 * @param data Sort configuration data
 * @param op The operator to read data from
 * @return file_iterator_state_t* Iterator for reading sorted results from memory
 */
file_iterator_state_t *startPureMemorySort(sortData *data, embedDBOperator *op) {
    printf("DEBUG: Starting pure in-memory sort\n");

    int record_count = 0;
    while (exec(op->input)) {
        record_count++;
        if (record_count > 10) {  // Safety limit
            printf("ERROR: Too many records for pure in-memory sort\n");
            return NULL;
        }
    }

    printf("DEBUG: Found %d records for pure in-memory sort\n", record_count);

    if (record_count == 0) {
        printf("DEBUG: No records to sort\n");
        file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
        if (iteratorState == NULL) {
            return NULL;
        }
        iteratorState->file = NULL;
        iteratorState->fileInterface = data->fileInterface;
        iteratorState->currentRecord = 0;
        iteratorState->recordsRead = 0;
        iteratorState->recordsLeftInBlock = 0;
        iteratorState->recordSize = data->recordSize;
        iteratorState->totalRecords = 0;
        iteratorState->resultFile = 0;
        return iteratorState;
    }

    void *buffer = malloc(record_count * data->recordSize);
    if (buffer == NULL) {
        printf("ERROR: Failed to allocate memory for pure in-memory sort\n");
        return NULL;
    }

    op->input->close(op->input);
    op->input->init(op->input);

    int records_read = 0;
    while (exec(op->input) && records_read < record_count) {
        memcpy((char *)buffer + records_read * data->recordSize,
               op->input->recordBuffer,
               data->recordSize);
        records_read++;
    }

    printf("DEBUG: Read %d records into memory buffer\n", records_read);

    // Sort the records in memory using quicksort
    metrics_t metrics = {0};
    int sort_result = in_memory_quick_sort(buffer, records_read, data->recordSize, data->keyOffset, data->compareFn, &metrics);

    if (sort_result != 0) {
        printf("ERROR: In-memory sort failed\n");
        free(buffer);
        return NULL;
    }

    printf("DEBUG: Pure in-memory sort completed successfully\n");

    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
        printf("ERROR: Failed to allocate iterator state\n");
        free(buffer);
        return NULL;
    }

    iteratorState->file = buffer;
    iteratorState->fileInterface = data->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsRead = 0;
    iteratorState->recordsLeftInBlock = 0;
    iteratorState->recordSize = data->recordSize;
    iteratorState->totalRecords = records_read;
    iteratorState->resultFile = 0;

    return iteratorState;
}

/**
 * @brief Adds header information and writes buffer to file
 *
 * @param buffer            The buffer that is written. Should be atleast the size of pageSize
 * @param blockIndex        The block index
 * @param numberOfValues    The the number of database rows stored in the page
 * @param pageSize          The size of the page
 * @param fileInterface     The interface used to write the file
 * @param file              The file being written to
 * @return int8_t
 */
int8_t writePageWithHeader(void *buffer, const uint32_t blockIndex, const uint32_t numberOfValues, const uint32_t pageSize, const embedDBFileInterface *fileInterface, void *file) {
    memcpy(buffer, &blockIndex, sizeof(int32_t));
    memcpy(buffer + sizeof(uint32_t), &numberOfValues, sizeof(int16_t));

    fileInterface->write(buffer, blockIndex, pageSize, file);

    if (fileInterface->error(file)) {
        printf("ERROR: SORT: Failed to write unsorted data");
        return 1;
    }

    return 0;
}

/**
 * @brief               Writes row data from the input operator to a file
 *
 * @param data         The operator data
 * @param op            The previous operator
 * @param unsortedFile  A prexisting file that the row data will be writen to
 * @param recordSize    The size of the data
 * @param keySize       The size of the key
 * @param keyOffset     The offset of the key with in the record (# of bytes)
 * @return uint32_t     The total number of records written or 0 if an error occurs
 *
 */
uint32_t loadRowData(sortData *data, embedDBOperator *op, void *unsortedFile) {
    uint32_t count = 0;
    int32_t blockIndex = 0;
    int16_t valuesPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / data->recordSize;

    void *buffer = malloc(PAGE_SIZE);

    if (buffer == NULL) {
        printf("ERROR: SORT: buffer malloc failed");
        return 0;
    }

    // Write row data to file
    while (exec(op->input)) {
        // Write page to file when full
        if (count % valuesPerPage == 0 && count != 0) {
            if (writePageWithHeader(buffer, blockIndex, valuesPerPage, PAGE_SIZE, data->fileInterface, unsortedFile)) {
                free(buffer);
                buffer = NULL;
                return 0;
            }

            blockIndex++;
        }

        // Offset of the data in the page
        uint32_t rowOffset = count % valuesPerPage * data->recordSize + BLOCK_HEADER_SIZE;

        if (rowOffset + data->recordSize > PAGE_SIZE) {
            printf("ERROR: SORT: error calculating row offset");
            free(buffer);
            buffer = NULL;
            return 0;
        }

        // Write data to buffer
        memcpy((uint8_t *)buffer + rowOffset, op->input->recordBuffer, data->recordSize);

        count++;

        // temp limit for debugging
        if (data->tupleLimit != -1 && count >= data->tupleLimit)
            break;
    }

    // Write remaining records
    uint32_t numRemainingRecords = (count % valuesPerPage == 0 && count != 0) ? valuesPerPage : count % valuesPerPage;
    if (writePageWithHeader(buffer, blockIndex, numRemainingRecords, PAGE_SIZE, data->fileInterface, unsortedFile)) {
        free(buffer);
        buffer = NULL;
        return 0;
    }

    data->fileInterface->flush(unsortedFile);

    // Clean up
    free(buffer);
    buffer = NULL;

    return count;
}

/**
 * @brief Begins the sorting operation using row data from previous operator
 *
 * @param op The previous operator that will feed row data
 */
void prepareSort(embedDBOperator *op) {
    sortData *data = op->state;
    data->keyOffset = getColOffsetFromSchema(op->schema, data->colNum);
    data->recordSize = getRecordSizeFromSchema(op->schema);
    data->keySize = op->schema->columnSizes[data->colNum];

    // A columns size will be negative if the column is signed
    // and positive if value is unsigned
    if (data->keySize < 0) {
        data->keySize = -1 * data->keySize;
    }

#ifdef ARDUINO
    // For Arduino Due, use pure in-memory sort to completely avoid SD card I/O issues
    data->fileIterator = startPureMemorySort(data, op);
    if (data->fileIterator == NULL) {
        printf("ERROR: Pure memory sort failed\n");
        return;
    }
    return;
#endif

    // Set up files
    void *unsortedFile = setupFile(SORT_DATA_LOCATION);
    void *sortedFile = setupFile(SORT_ORDER_LOCATION);

    if (unsortedFile == NULL || sortedFile == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to open files while initializing ORDER BY operator");
#endif
        return;
    }

    const uint8_t unsortedOpen = data->fileInterface->open(unsortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    const uint8_t sortedOpen = data->fileInterface->open(sortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);

    if (!unsortedOpen || !sortedOpen) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to open files while initializing ORDER BY operator");
#endif
        return;
    }

    // Load row data
    data->count = loadRowData(data, op, unsortedFile);

    // Start sorting
    file_iterator_state_t *iteratorState = startSort(data, unsortedFile, sortedFile);
    if (iteratorState == NULL) {
        printf("ERROR: Sort failed");
        return;
    }

    // Finish
    iteratorState->file = sortedFile;
    data->fileInterface->close(unsortedFile);
    data->fileIterator = iteratorState;
}

/**
 * @brief The data given in the unsortedFile is sorted and stored in the sortedFile
 *
 * @param fileInterface             The file interface
 * @param unsortedFile              The file that is loaded with row data
 * @param sortedFile                An empty file
 * @param recordSize                The size of the records
 * @param count                     The total number of records stored in unsortedFile
 * @return file_iterator_state_t*   An iterator that is used to retrieve the sorted records
 */
file_iterator_state_t *startSort(sortData *data, void *unsortedFile, void *sortedFile) {
    // Initialize external_sort_t structure
    external_sort_t es;
    es.key_size = data->keySize;
    es.value_size = data->recordSize;
    es.record_size = data->recordSize;
    es.key_offset = data->keyOffset;
    es.headerSize = BLOCK_HEADER_SIZE;
    es.page_size = PAGE_SIZE;
    es.num_pages = (uint32_t)ceil((float)data->count / ((es.page_size - es.headerSize) / es.record_size));

// Reduce buffer size for Arduino
#ifdef ARDUINO
    const int buffer_max_pages = 1;  // Reduced to minimum for Arduino
#else
    const int buffer_max_pages = 4;
#endif

    char *buffer = malloc(buffer_max_pages * es.page_size + es.record_size);
    char *tuple_buffer = buffer + es.page_size * buffer_max_pages;

    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: SORT: buffer malloc failed m\n");
#endif
        return NULL;
    }

    // Prepare the file iterator data for sorting
    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        printf("Error: SORT: iterator malloc failed\n");
#endif
        free(buffer);
        buffer = NULL;
        return NULL;
    }

    iteratorState->file = unsortedFile;
    iteratorState->recordsRead = 0;
    iteratorState->totalRecords = data->count;  // Total records from the previous while loop
    iteratorState->recordSize = es.record_size;
    iteratorState->fileInterface = data->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsLeftInBlock = 0;
    iteratorState->resultFile = 0;

    data->fileIterator = iteratorState;

    // Metrics
    metrics_t metrics = initMetric();

    long result_file_ptr = 0;

    int err;
// Use simpler sort for Arduino with small datasets
#ifdef ARDUINO
    printf("DEBUG: Starting Arduino sort with %d records\n", data->count);
    if (data->count <= 100) {  // Use flash_minsort for all datasets on Arduino (more memory efficient)
        printf("DEBUG: Using flash_minsort for small dataset\n");
        err = flash_minsort(iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages * es.page_size, &es, &result_file_ptr, &metrics, data->compareFn);
    } else {
        printf("DEBUG: Using flash_minsort for large dataset\n");
        // Use flash_minsort for larger datasets (more memory efficient than adaptive_sort)
        err = flash_minsort(iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages * es.page_size, &es, &result_file_ptr, &metrics, data->compareFn);
    }
    printf("DEBUG: Arduino sort completed with error code: %d\n", err);
#else
    // Use adaptive sort on desktop
    int8_t runGenOnly = false;   // Run full sort operation
    int8_t writeReadRatio = 19;  // 1.97 * 10 => 19
    err = adaptive_sort(readNextRecord, iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages, &es, &result_file_ptr, &metrics, data->compareFn, runGenOnly, writeReadRatio, data);
#endif

#ifdef PRINT_METRIC
    printf("\tComplete. Comparisons: %d  Writes: %d  Reads: %d Memcpys: %d\n", metrics.num_compar, metrics.num_writes, metrics.num_reads, metrics.num_memcpys);
#endif

    iteratorState->resultFile = result_file_ptr;

#ifdef PRINT_ERRORS
    if (8 == err) {
        printf("Out of memory!\n");
    } else if (10 == err) {
        printf("File Read Error!\n");
    } else if (9 == err) {
        printf("File Write Error!\n");
    }
#endif

    // Reset file iterator
    iteratorState->recordsRead = 0;
    iteratorState->currentRecord = 0;

    // Clean up
    free(buffer);
    buffer = NULL;
    return iteratorState;
}

/**
 * @brief Reads the next record from the sorted file
 *
 * @param data     The ORDER BY operator data
 * @param buffer    A buffer that is the size of one record
 * @return uint8_t  0: if read was successful. other wise none zero
 */
uint8_t readNextRecord(void *data, void *buffer) {
    file_iterator_state_t *iteratorState = ((sortData *)data)->fileIterator;

    if (iteratorState->recordsRead >= iteratorState->totalRecords) {
        return 1;  // No more records left to read
    }

#ifdef ARDUINO
    // For pure memory sort on Arduino, read directly from memory buffer
    if (iteratorState->file != NULL && iteratorState->resultFile == 0) {
        memcpy(buffer, (char *)iteratorState->file + iteratorState->recordsRead * iteratorState->recordSize,
               iteratorState->recordSize);
        iteratorState->recordsRead++;
        iteratorState->currentRecord++;
        return 0;
    }
#endif

    uint32_t recordPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / iteratorState->recordSize;

    // Read next page if current buffer is empty
    if (iteratorState->currentRecord % recordPerPage == 0 || iteratorState->recordsRead == 0) {
        iteratorState->fileInterface->seek(iteratorState->currentRecord / recordPerPage * PAGE_SIZE + iteratorState->resultFile, iteratorState->file);
        iteratorState->fileInterface->readRel(((sortData *)data)->readBuffer, PAGE_SIZE, 1, iteratorState->file);

        if (((sortData *)data)->fileInterface->error(iteratorState->file)) {
            printf("ERROR: SORT: next record read failed");
            return 2;
        }
    }

    // Copy result to ouput buffer
    memcpy(buffer, ((sortData *)data)->readBuffer + BLOCK_HEADER_SIZE + iteratorState->recordSize * (iteratorState->currentRecord % recordPerPage), iteratorState->recordSize);
    iteratorState->recordsRead++;
    iteratorState->currentRecord++;

#ifdef DEBUG
    printf("DEBUG: ROWDATA from file:\n");
    for (int i = 0; i < iteratorState->recordSize - SORT_KEY_SIZE; i++) {
        printf("%2x ", ((uint8_t *)buffer)[i]);
    }
    printf("\n");
#endif

    return 0;
}

void closeSort(file_iterator_state_t *iteratorState) {
#ifdef ARDUINO
    // For pure memory sort, we need to free the memory buffer
    if (iteratorState->file != NULL && iteratorState->resultFile == 0) {
        free(iteratorState->file);
        iteratorState->file = NULL;
        return;
    }
#endif

    if (iteratorState->file != NULL) {
        iteratorState->fileInterface->close(iteratorState->file);
        iteratorState->file = NULL;
    }
}

/**
 * @brief Initalizes default metric values
 *
 * @return metrics_t
 */
metrics_t initMetric() {
    metrics_t metrics;
    metrics.num_reads = 0;
    metrics.num_compar = 0;
    metrics.num_memcpys = 0;
    metrics.num_runs = 0;
    metrics.num_writes = 0;
    return metrics;
}
