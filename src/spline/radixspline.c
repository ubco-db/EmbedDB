/******************************************************************************/
/**
 * @file		radixspline.c
 * @author		Ramon Lawrence
 * @brief		Implementation of radix spline for embedded devices.
 *                         Based on "RadixSpline: a single-pass learned index" by
 *                         A. Kipf, R. Marcus, A. van Renen, M. Stoian, A. Kemper,
 *                         T. Kraska, and T. Neumann
 *                         https://github.com/learnedsystems/RadixSpline
 * @copyright	Copyright 2022
 *                         The University of British Columbia
 *                         Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 *         modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *         this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *         this list of conditions and the following disclaimer in the documentation
 *         and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *         may be used to endorse or promote products derived from this software without
 *         specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *         AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *         IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *         ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *         LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *         CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *         SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *         INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *         CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *         ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *         POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "radixspline.h"

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
        radixsplineAddPoint(rsidx, key);
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
 */
void radixsplineAddPoint(radixspline *rsidx, void *key) {
    splineAdd(rsidx->spl, key);

    // Return if not using Radix table
    if (rsidx->radixSize == 0) {
        return;
    }

    // Determine if need to update radix table based on adding point to spline
    if (rsidx->spl->count <= rsidx->pointsSeen)
        return; // Nothing to do

    // take the last point that was added to spline
    key = rsidx->spl->points[rsidx->spl->count - 1].key;

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
    rsidx->numPoints = rsidx->spl->currentPointLoc;
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
    rsidx->numPoints = 0;
    rsidx->shiftSize = 0;
    rsidx->size = pow(2, radixSize);

    /* Determine the prefix size (shift bits) based on min and max keys */
    rsidx->minKey = spl->points[0].key;

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
    point *arr = rsidx->spl->points;

    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        if (compareKey(arr[mid].key, key) >= 0 && compareKey(arr[mid - 1].key, key) <= 0)
            return mid;

        if (compareKey(arr[mid].key, key) > 0)
            return radixBinarySearch(rsidx, low, mid - 1, key, compareKey);

        return radixBinarySearch(rsidx, mid + 1, high, key, compareKey);
    }

    // We reach here when element is not present in array.
    // return the limiting upper/lower bound
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
    point down, up;
    memcpy(&down, rsidx->spl->points + (index - 1), sizeof(point));
    memcpy(&up, rsidx->spl->points + index, sizeof(point));

    uint64_t downKey = 0, upKey = 0;
    memcpy(&downKey, down.key, rsidx->keySize);
    memcpy(&upKey, up.key, rsidx->keySize);

    /* Keydiff * slope + y */
    uint32_t estimatedPage = (uint32_t)((keyVal - downKey) * (up.page - down.page) / (long double)(upKey - downKey)) + down.page;
    return estimatedPage > up.page ? up.page : estimatedPage;
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
    *loc = radixsplineEstimateLocation(rsidx, key, compareKey);

    /* Set error bounds based on maxError from spline construction */
    *low = (rsidx->spl->maxError > *loc) ? 0 : *loc - rsidx->spl->maxError;
    point lastSplinePoint;
    memcpy(&lastSplinePoint, rsidx->spl->points + (rsidx->spl->count - 1), sizeof(point));
    uint64_t lastKey = 0;
    memcpy(&lastKey, lastSplinePoint.key, rsidx->keySize);
    *high = (*loc + rsidx->spl->maxError > lastKey) ? lastKey : *loc + rsidx->spl->maxError;
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

    printf("Radix table (%lu):\n", rsidx->size);
    // for (id_t i=0; i < 20; i++)
    uint64_t minKeyVal = 0;
    id_t tableVal;
    memcpy(&minKeyVal, rsidx->minKey, rsidx->keySize);
    for (id_t i = 0; i < rsidx->size; i++) {
        printf("[" TO_BINARY_PATTERN "] ", TO_BINARY((uint8_t)(i)));
        memcpy(&tableVal, rsidx->table + i, sizeof(id_t));
        printf("(%lu): --> %lu\n", (i << rsidx->shiftSize) + minKeyVal, tableVal);
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
    free(rsidx->spl->points);
    free(rsidx->table);
}