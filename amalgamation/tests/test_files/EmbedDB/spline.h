/******************************************************************************/
/**
 * @file        spline.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Implementation of spline for embedded devices.
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

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#ifndef SPLINE_H
#define SPLINE_H

#include <stddef.h>
#include <stdint.h>

/* Define type for keys and location ids. */
typedef uint32_t id_t;

typedef struct spline_s spline;

struct spline_s {
    size_t count;            /* Number of points in spline */
    size_t size;             /* Maximum number of points */
    size_t pointsStartIndex; /* Index of the first spline point */
    void *points;            /* Array of points */
    void *upper;             /* Upper spline limit */
    void *lower;             /* Lower spline limit */
    void *firstSplinePoint;  /* First Point that was added to the spline */
    uint32_t lastLoc;        /* Location of previous spline key */
    void *lastKey;           /* Previous spline key */
    uint32_t eraseSize;      /* Size of points to erase if none can be cleaned */
    uint32_t maxError;       /* Maximum error */
    uint32_t numAddCalls;    /* Number of times the add method has been called */
    uint32_t tempLastPoint;  /* Last spline point is temporary if value is not 0 */
    uint8_t keySize;         /* Size of key in bytes */
};

/**
 * @brief   Initialize a spline structure with given maximum size and error.
 * @param   spl        Spline structure
 * @param   size       Maximum size of spline
 * @param   maxError   Maximum error allowed in spline
 * @param   keySize    Size of key in bytes
 * @return  Returns 0 if successful and -1 if not
 */
int8_t splineInit(spline *spl, id_t size, size_t maxError, uint8_t keySize);

/**
 * @brief	Builds a spline structure given a sorted data set. GreedySplineCorridor
 * implementation from "Smooth interpolating histograms with error guarantees"
 * (BNCOD'08) by T. Neumann and S. Michel.
 * @param	spl			Spline structure
 * @param	data		Array of sorted data
 * @param	size		Number of values in array
 * @param   maxError	Maximum error for each spline
 */
void splineBuild(spline *spl, void **data, id_t size, size_t maxError);

/**
 * @brief   Adds point to spline structure
 * @param   spl     Spline structure
 * @param   key     Data key to be added (must be incrementing)
 * @param   page    Page number for spline point to add
 */
void splineAdd(spline *spl, void *key, uint32_t page);

/**
 * @brief	Print a spline structure.
 * @param	spl	Spline structure
 */
void splinePrint(spline *spl);

/**
 * @brief	Return spline structure size in bytes.
 * @param	spl	Spline structure
 */
uint32_t splineSize(spline *spl);

/**
 * @brief	Estimate the page number of a given key
 * @param	spl			The spline structure to search
 * @param	key			The key to search for
 * @param	compareKey	Function to compare keys
 * @param	loc			A return value for the best estimate of which page the key is on
 * @param	low			A return value for the smallest page that it could be on
 * @param	high		A return value for the largest page it could be on
 */
void splineFind(spline *spl, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high);

/**
 * @brief    Free memory allocated for spline structure.
 * @param    spl        Spline structure
 */
void splineClose(spline *spl);

/**
 * @brief   Removes points from the spline
 * @param   spl         The spline structure to search
 * @param   numPoints   The number of points to remove from the spline
 * @return  Returns zero if successful and one if not
 */
int splineErase(spline *spl, uint32_t numPoints);

/**
 * @brief   Returns a pointer to the location of the specified spline point in memory. Note that this method does not check if there is a point there, so it may be garbage data.
 * @param   spl         The spline structure that contains the points
 * @param   pointIndex  The index of the point to return a pointer to
 */
void *splinePointLocation(spline *spl, size_t pointIndex);

#ifdef __cplusplus
}
#endif

#endif
