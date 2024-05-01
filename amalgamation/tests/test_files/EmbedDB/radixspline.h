/******************************************************************************/
/**
 * @file        radixspline.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for implementation of radix spline for
 *              embedded devices. Based on "RadixSpline: a single-pass
 *              learned index" by A. Kipf, R. Marcus, A. van Renen,
 *              M. Stoian, A. Kemper, T. Kraska, and T. Neumann
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

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#ifndef RADIXSPLINE_H
#define RADIXSPLINE_H

#include <math.h>
#include <stddef.h>
#include <stdint.h>

#define TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define TO_BINARY(byte)            \
    (byte & 0x80 ? '1' : '0'),     \
        (byte & 0x40 ? '1' : '0'), \
        (byte & 0x20 ? '1' : '0'), \
        (byte & 0x10 ? '1' : '0'), \
        (byte & 0x08 ? '1' : '0'), \
        (byte & 0x04 ? '1' : '0'), \
        (byte & 0x02 ? '1' : '0'), \
        (byte & 0x01 ? '1' : '0')

typedef struct radixspline_s radixspline;

#include "spline.h"

struct radixspline_s {
    spline *spl;      /* Spline with spline points */
    uint32_t size;    /* Size of radix table */
    id_t *table;      /* Radix table */
    int8_t shiftSize; /* Size of prefix/shift (in bits) */
    int8_t radixSize; /* Size of radix (in bits) */
    void *minKey;     /* Minimum key */
    id_t prevPrefix;  /* Prefix of most recently seen spline point */
    id_t pointsSeen;  /* Number of data points added to radix */
    uint8_t keySize;  /* Size of key in bytes */
};

typedef struct {
    id_t key;
    uint64_t sum;
} lookup_t;

/**
 * @brief   Build the radix table
 * @param   rsdix       Radix spline structure
 * @param   keys        Data points to be indexed
 * @param   numKeys     Number of data items
 */
void radixsplineBuild(radixspline *rsidx, void **keys, uint32_t numKeys);

/**
 * @brief	Initialize an empty radix spline index of given size
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInit(radixspline *rsidx, spline *spl, int8_t radixSize, uint8_t keySize);

/**
 * @brief	Initialize and build a radix spline index of given size using pre-built spline structure.
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	keys		Keys to be indexed
 * @param	numKeys 	Number of keys in `keys`
 * @param	keySize		Size of keys to be stored in radix table
 */
void radixsplineInitBuild(radixspline *rsidx, spline *spl, uint32_t radixSize, void **keys, uint32_t numKeys, uint8_t keySize);

/**
 * @brief	Add a point to be indexed by the radix spline structure
 * @param	rsdix	Radix spline structure
 * @param	key		New point to be indexed by radix spline
 * @param   page    Page number for spline point to add
 */
void radixsplineAddPoint(radixspline *rsidx, void *key, uint32_t page);

/**
 * @brief	Finds a value using index. Returns predicted location and low and high error bounds.
 * @param	rsidx	    Radix spline structure
 * @param	key		    Search key
 * @param   compareKey  Function to compare keys
 * @param	loc		    Return of predicted location
 * @param	low		    Return of low bound on predicted location
 * @param	high	    Return of high bound on predicted location
 */
void radixsplineFind(radixspline *rsidx, void *key, int8_t compareKey(void *, void *), id_t *loc, id_t *low, id_t *high);

/**
 * @brief	Print radix spline structure.
 * @param	rsidx	Radix spline structure
 */
void radixsplinePrint(radixspline *rsidx);

/**
 * @brief	Returns size of radix spline index structure in bytes
 * @param	rsidx	Radix spline structure
 */
size_t radixsplineSize(radixspline *rsidx);

/**
 * @brief	Closes and frees space for radix spline index structure
 * @param	rsidx	Radix spline structure
 */
void radixsplineClose(radixspline *rsidx);

#ifdef __cplusplus
}
#endif

#endif
