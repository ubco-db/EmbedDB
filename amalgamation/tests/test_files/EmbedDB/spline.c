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

#include "spline.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
