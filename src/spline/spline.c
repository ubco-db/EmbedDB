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

/**
 * @brief    Initialize a spline structure with given maximum size.
 * @param    spl     Spline structure
 * @param    size    Maximum size of spline
 */
void splineInit(spline *spl, id_t size, size_t maxError) {
    spl->count = 0;
    spl->currentPointLoc = 0;
    spl->size = size;
    spl->maxError = maxError;
    spl->points = (point *)malloc(sizeof(point) * size);
    spl->tempLastPoint = 0;
}

/**
 * @brief    Check if first line is to the left (counter-clockwise) of the second.
 */
static inline int8_t splineIsLeft(id_t x1, int64_t y1, id_t x2, int64_t y2) {
    return y1 * x2 > y2 * x1;
}

/**
 * @brief    Check if first line is to the right (clockwise) of the second.
 */
static inline int8_t splineIsRight(id_t x1, int64_t y1, id_t x2, int64_t y2) {
    return y1 * x2 < y2 * x1;
}

/**
 * @brief    Adds point to spline structure]
 * @param    spl     Spline structure
 * @param    key     Data key to be added (must be incrementing)
 */
void splineAdd(spline *spl, id_t key) {
    /* Check if no spline points are currently empty */
    if (spl->currentPointLoc == 0) {
        /* Add first point in data set to spline. */
        spl->points[0].x = key;
        spl->points[0].y = 0;
        spl->count++;
        spl->lastKey = key;
        /* Update location of current point */
        spl->currentPointLoc++;
        return;
    }
    /* Check if there is only one spline point (need to initialize upper and
     * lower limits using 2nd point) */
    if (spl->currentPointLoc == 1) {
        /* Initialize upper and lower limits using second (unique) data point */
        spl->lower.x = key;
        spl->lower.y = spl->currentPointLoc < spl->maxError
                           ? 0
                           : spl->currentPointLoc - spl->maxError;
        spl->upper.x = key;
        spl->upper.y = spl->currentPointLoc + spl->maxError;
        spl->lastKey = key;
        spl->lastLoc = spl->currentPointLoc;

        spl->currentPointLoc++;
        return;
    }

    /* Skip duplicates */
    if (spl->lastKey == key) {
        spl->currentPointLoc++;
        return;
    }

    assert(key > spl->lastKey);

    /* Last point added to spline, check if previous point is temporary -
     * overwrite previous point if temporary */
    if (spl->tempLastPoint != 0) {
        spl->count--;
    }
    point last = spl->points[spl->count - 1];

    id_t xdiff, ydiff, upperXDiff, upperYDiff, lowerXDiff;
    int64_t lowerYDiff; /* This may be negative */

    xdiff = key - last.x;
    ydiff = spl->currentPointLoc - last.y;
    upperXDiff = spl->upper.x - last.x;
    upperYDiff = spl->upper.y - last.y;
    lowerXDiff = spl->lower.x - last.x;
    lowerYDiff = (int64_t)spl->lower.y - last.y;

    /* Check if next point still in error corridor */
    if (splineIsLeft(xdiff, ydiff, upperXDiff, upperYDiff) == 1 ||
        splineIsRight(xdiff, ydiff, lowerXDiff, lowerYDiff) == 1) {

        /* Point is not in error corridor. Add previous point tospline. */
        assert(spl->count < spl->size);
        spl->points[spl->count].x = spl->lastKey;
        spl->points[spl->count].y = spl->lastLoc;
        // printf("Added spline point: (%lu, %lu)\n", spl->points[spl->count].x,
        // spl->points[spl->count].y);
        spl->count++;
        spl->tempLastPoint = 0;

        /* Update upper and lower limits. */
        spl->lower.x = key;
        spl->lower.y = spl->currentPointLoc < spl->maxError
                           ? 0
                           : spl->currentPointLoc - spl->maxError;
        spl->upper.x = key;
        spl->upper.y = spl->currentPointLoc + spl->maxError;
    } else {
        /* Check if must update upper or lower limits */

        /* Upper limit */
        if (splineIsLeft(upperXDiff, upperYDiff, xdiff, spl->currentPointLoc + spl->maxError - last.y) == 1) {
            spl->upper.x = key;
            spl->upper.y = spl->currentPointLoc + spl->maxError;
        }

        /* Lower limit */
        if (splineIsRight(lowerXDiff, lowerYDiff, xdiff,
                          (spl->currentPointLoc < spl->maxError
                               ? 0
                               : spl->currentPointLoc - spl->maxError) -
                              last.y) == 1) {
            spl->lower.x = key;
            spl->lower.y = spl->currentPointLoc < spl->maxError
                               ? 0
                               : spl->currentPointLoc - spl->maxError;
        }
    }

    spl->lastLoc = spl->currentPointLoc;

    /* Update location of current point */
    spl->currentPointLoc++;

    /* Add last key on spline if not already there. */
    /* This will get overwritten the next time a new spline point is added */

    spl->lastKey = key;
    assert(spl->count < spl->size);
    spl->points[spl->count].x = spl->lastKey;
    spl->points[spl->count].y = spl->lastLoc;
    spl->count++;

    // TODO: Add flag when final point is added
    spl->tempLastPoint = 1;
}

/**
 * @brief  Builds a spline structure given a sorted data set. GreedySplineCorridor
 * implementation from "Smooth interpolating histograms with error guarantees"
 * (BNCOD'08) by T. Neumann and S. Michel.
 * @param  spl         Spline structure
 * @param  data        Array of sorted data
 * @param	size        Number of values in array
 * @param	maxError    Maximum error for each spline
 */
void splineBuild(spline *spl, id_t *data, id_t size, size_t maxError) {
    spl->maxError = maxError;

    for (id_t i = 0; i < size; i++) {
        splineAdd(spl, data[i]);
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
    for (id_t i = 0; i < spl->count; i++)
        printf("[%lu]: (%lu, %lu)\n", i, spl->points[i].x, spl->points[i].y);
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
 * @brief    Performs a recursive binary search for a given value
 * @param    arr     Array to search through
 * @param    low     Lower search bound
 * @param    high    Higher search bound
 * @param    x       Search term
 * @return   Returns the value found by the binary search
 */
size_t pointsBinarySearch(point *arr, int low, int high, int x) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        if (arr[mid].x >= x && arr[mid - 1].x <= x)
            return mid;

        if (arr[mid].x > x)
            return pointsBinarySearch(arr, low, mid - 1, x);

        return pointsBinarySearch(arr, mid + 1, high, x);
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
 * @param	spl		The spline structure to search
 * @param	key		The key to search for
 * @param	loc		A return value for the best estimate of which page the key is on
 * @param	low		A return value for the smallest page that it could be on
 * @param	high	A return value for the largest page it could be on
 */
void splineFind(spline *spl, id_t key, id_t *loc, id_t *low, id_t *high) {
    size_t pointIdx;

    if (key < spl->points[0].x || spl->count <= 1) {
        // Key is smaller than any we have on record
        *loc = *low = *high = spl->points[0].y;
        return;
    } else if (key > spl->points[spl->count - 1].x) {
        *loc = *low = *high = spl->points[spl->count - 1].y;
        return;
    } else {
        // Perform a binary seach to find the spline point above the key we're looking for
        pointIdx = pointsBinarySearch(spl->points, 0, spl->count - 1, key);
    }

    // Interpolate between two spline points
    point down = spl->points[pointIdx - 1];
    point up = spl->points[pointIdx];

    // Estimate location as page number
    // Keydiff * slope + y
    *loc = (key - down.x) * ((double)(up.y - down.y) / (up.x - down.x)) + down.y;

    // Set error bounds based on maxError from spline construction
    *low = (spl->maxError > *loc) ? 0 : *loc - spl->maxError;
    point lastSplinePoint = spl->points[spl->count - 1];
    *high = (*loc + spl->maxError > lastSplinePoint.y) ? lastSplinePoint.y : *loc + spl->maxError;
}
