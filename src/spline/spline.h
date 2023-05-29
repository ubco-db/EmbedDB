/******************************************************************************/
/**
 * @file		spline.h
 * @author		Ramon Lawrence
 * @brief		Implementation of spline for embedded devices.
 * @copyright	Copyright 2021
 * 			The University of British Columbia,
 * 			Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/
#ifndef SPLINE_H
#define SPLINE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>

#if defined(ARDUINO)
#include "file/dataflash_c_iface.h"
#include "file/sdcard_c_iface.h"
#include "file/serial_c_iface.h"
#endif

/* Define type for keys and location ids. */
typedef uint32_t id_t;

typedef struct {
    id_t x;
    id_t y;
} point;

struct spline_s {
    size_t count;           /* Number of points in spline */
    size_t currentPointLoc; /* Location of current point */
    size_t size;            /* Maximum number of points */
    point *points;          /* Array of points */
    point upper;            /* Upper spline limit */
    point lower;            /* Lower spline limit */
    id_t lastLoc;           /* Location of previous spline key */
    id_t lastKey;           /* Previous spline key */
    uint32_t maxError;      /* Maximum error */
    id_t tempLastPoint;     /* Last spline point is temporary if value is not 0 */
};

typedef struct spline_s spline;

/**
 * @brief	Initialize a spline structure with given maximum size.
 * @param	spl		Spline structure
 * @param	size	Maximum size of spline
 */
void splineInit(spline *spl, id_t size, size_t maxError);

/**
 * @brief	Adds a new data point to index.
 * @param	spl	Spline structure
 * @param	key	Key to index (note: location is last point in array)
 */
void splineAdd(spline *spl, id_t minKey);

/**
 * @brief	Builds a spline structure given a sorted data set.
 * 			GreedySplineCorridor implementation from "Smooth interpolating histograms
 * 			with error guarantees" (BNCOD'08) by T. Neumann and S. Michel.
 * @param	spl			Spline structure
 * @param	data		Array of sorted data
 * @param	size		Number of values in array
 * @param	maxError	Maximum error for each spline
 */
void splineBuild(spline *spl, id_t *data, id_t size, size_t maxError);

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
 *
 * @param	spl		The spline structure to search
 * @param	key		The key to search for
 * @param	loc		A return value for the best estimate of which page the key is on
 * @param	low		A return value for the smallest page that it could be on
 * @param	high	A return value for the largest page it could be on
 */
void splineFind(spline *spl, id_t key, id_t *loc, id_t *low, id_t *high);

#ifdef __cplusplus
}
#endif

#endif