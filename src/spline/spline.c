/******************************************************************************/
/**
 * @file		spline.c
 * @author		Ramon Lawrence
 * @brief		Implementation of spline.
 * @copyright	Copyright 2021
 *                         The University of British Columbia,
 *                         Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 *         modification, are permitted provided that the following conditions are
 * met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *         this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *         this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *         AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *         ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *         CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *         SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *         INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *         CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *         ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/
#include "spline.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief    Initialize a spline structure with given maximum size and error.
 * @param    spl        Spline structure
 * @param    size       Maximum size of spline
 * @param    maxError   Maximum error allowed in spline
 * @param    keySize    Size of key in bytes
 */
void splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize) {
    spl->count = 0;
    spl->size = size;
    spl->maxError = maxError;
    spl->points = (point *)malloc(sizeof(point) * size);
    for (id_t i = 0; i < size; i++) {
        spl->points[i].key = malloc(keySize);
    }
    spl->tempLastPoint = 0;
    spl->keySize = keySize;
    spl->lastKey = malloc(keySize);
    spl->lower.key = malloc(keySize);
    spl->upper.key = malloc(keySize);
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
        memcpy(spl->points[0].key, key, spl->keySize);
        spl->points[0].page = page;
        spl->count++;
        memcpy(spl->lastKey, key, spl->keySize);
        return;
    }

    /* Check if there is only one spline point (need to initialize upper and lower limits using 2nd point) */
    if (spl->numAddCalls == 2) {
        /* Initialize upper and lower limits using second (unique) data point */
        memcpy(spl->lower.key, key, spl->keySize);
        spl->lower.page = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy(spl->upper.key, key, spl->keySize);
        spl->upper.page = page + spl->maxError;
        memcpy(spl->lastKey, key, spl->keySize);
        spl->lastLoc = page;
        return;
    }

    /* Skip duplicates */
    uint64_t keyVal = 0, lastKeyVal = 0;
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&lastKeyVal, spl->lastKey, spl->keySize);
    if (keyVal == lastKeyVal) {
        return;
    }

    assert(keyVal > lastKeyVal);

    /* Last point added to spline, check if previous point is temporary - overwrite previous point if temporary */
    if (spl->tempLastPoint != 0) {
        spl->count--;
    }
    point last;
    memcpy(&last, spl->points + spl->count - 1, sizeof(point));
    uint64_t lastPointKey = 0, upperKey = 0, lowerKey = 0;
    memcpy(&lastPointKey, last.key, spl->keySize);
    memcpy(&upperKey, spl->upper.key, spl->keySize);
    memcpy(&lowerKey, spl->lower.key, spl->keySize);

    uint64_t xdiff, upperXDiff, lowerXDiff;
    uint32_t ydiff, upperYDiff;
    int64_t lowerYDiff; /* This may be negative */

    xdiff = keyVal - lastPointKey;
    ydiff = page - last.page;
    upperXDiff = upperKey - lastPointKey;
    upperYDiff = spl->upper.page - last.page;
    lowerXDiff = lowerKey - lastPointKey;
    lowerYDiff = (int64_t)spl->lower.page - last.page;

    /* Check if next point still in error corridor */
    if (splineIsLeft(xdiff, ydiff, upperXDiff, upperYDiff) == 1 ||
        splineIsRight(xdiff, ydiff, lowerXDiff, lowerYDiff) == 1) {
        /* Point is not in error corridor. Add previous point to spline. */
        assert(spl->count < spl->size);
        memcpy(spl->points[spl->count].key, spl->lastKey, spl->keySize);
        spl->points[spl->count].page = spl->lastLoc;
        spl->count++;
        spl->tempLastPoint = 0;

        /* Update upper and lower limits. */
        memcpy(spl->lower.key, key, spl->keySize);
        spl->lower.page = page < spl->maxError ? 0 : page - spl->maxError;
        memcpy(spl->upper.key, key, spl->keySize);
        spl->upper.page = page + spl->maxError;
    } else {
        /* Check if must update upper or lower limits */

        /* Upper limit */
        if (splineIsLeft(upperXDiff, upperYDiff, xdiff, page + spl->maxError - last.page) == 1) {
            memcpy(spl->upper.key, key, spl->keySize);
            spl->upper.page = page + spl->maxError;
        }

        /* Lower limit */
        if (splineIsRight(lowerXDiff, lowerYDiff, xdiff, (page < spl->maxError ? 0 : page - spl->maxError) - last.page) == 1) {
            memcpy(spl->lower.key, key, spl->keySize);
            spl->lower.page = page < spl->maxError ? 0 : page - spl->maxError;
        }
    }

    spl->lastLoc = page;

    /* Add last key on spline if not already there. */
    /* This will get overwritten the next time a new spline point is added */
    memcpy(spl->lastKey, key, spl->keySize);
    assert(spl->count < spl->size);
    memcpy(spl->points[spl->count].key, spl->lastKey, spl->keySize);
    spl->points[spl->count].page = spl->lastLoc;
    spl->count++;

    spl->tempLastPoint = 1;
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
    printf("Spline max error (%lu):\n", spl->maxError);
    printf("Spline points (%lu):\n", spl->count);
    uint64_t keyVal = 0;
    for (id_t i = 0; i < spl->count; i++) {
        memcpy(&keyVal, spl->points[i].key, spl->keySize);
        printf("[%lu]: (%lu, %lu)\n", i, keyVal, spl->points[i].page);
    }
    printf("\n");
}

/**
 * @brief    Return spline structure size in bytes.
 * @param    spl     Spline structure
 * @return   size of the spline in bytes
 */
uint32_t splineSize(spline *spl) {
    return sizeof(spline) + (spl->count * sizeof(point));
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
size_t pointsBinarySearch(point *arr, int low, int high, void *key, int8_t compareKey(void *, void *)) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        // If mid is zero, then low = 0 and high = 1. Therefore there is only one spline segment and we return 1, the upper bound.
        if (mid == 0) {
            return 1;
        }

        if (compareKey(arr[mid].key, key) >= 0 && compareKey(arr[mid - 1].key, key) <= 0)
            return mid;

        if (compareKey(arr[mid].key, key) > 0)
            return pointsBinarySearch(arr, low, mid - 1, key, compareKey);

        return pointsBinarySearch(arr, mid + 1, high, key, compareKey);
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
    memcpy(&keyVal, key, spl->keySize);
    memcpy(&smallestKeyVal, spl->points[0].key, spl->keySize);
    memcpy(&largestKeyVal, spl->points[spl->count - 1].key, spl->keySize);

    if (compareKey(key, spl->points[0].key) < 0 || spl->count <= 1) {
        // Key is smaller than any we have on record
        *loc = *low = *high = spl->points[0].page;
        return;
    } else if (compareKey(key, spl->points[spl->count - 1].key) > 0) {
        *loc = *low = *high = spl->points[spl->count - 1].page;
        return;
    } else {
        // Perform a binary seach to find the spline point above the key we're looking for
        pointIdx = pointsBinarySearch(spl->points, 0, spl->count - 1, key, compareKey);
    }

    // Interpolate between two spline points
    point down = spl->points[pointIdx - 1];
    point up = spl->points[pointIdx];
    uint64_t downKeyVal = 0, upKeyVal = 0;
    memcpy(&downKeyVal, down.key, spl->keySize);
    memcpy(&upKeyVal, up.key, spl->keySize);

    // Estimate location as page number
    // Keydiff * slope + y
    *loc = (id_t)((keyVal - downKeyVal) * (up.page - down.page) / (long double)(upKeyVal - downKeyVal)) + down.page;

    // Set error bounds based on maxError from spline construction
    *low = (spl->maxError > *loc) ? 0 : *loc - spl->maxError;
    point lastSplinePoint = spl->points[spl->count - 1];
    *high = (*loc + spl->maxError > lastSplinePoint.page) ? lastSplinePoint.page : *loc + spl->maxError;
}

/**
 * @brief    Free memory allocated for spline structure.
 * @param    spl        Spline structure
 */
void splineClose(spline *spl) {
    for (id_t i = 0; i < spl->size; i++) {
        free(spl->points[i].key);
    }
    free(spl->points);
    free(spl->lastKey);
    free(spl->lower.key);
    free(spl->upper.key);
}
