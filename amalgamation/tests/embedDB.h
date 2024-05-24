#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
/************************************************************embedDB.h************************************************************/
         
/************************************************************spline.h************************************************************/
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

/************************************************************radixspline.h************************************************************/
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

/************************************************************embedDB.h************************************************************/
/******************************************************************************/
/**
 * @file        embedDB.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for EmbedDB.
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

#ifndef embedDB_H_
#define embedDB_H_

#ifdef __cplusplus
extern "C" {
#endif     

/* Define type for page ids (physical and logical). */
typedef uint32_t id_t;

/* Define type for page record count. */
typedef uint16_t count_t;

#define EMBEDDB_USE_INDEX 1
#define EMBEDDB_USE_MAX_MIN 2
#define EMBEDDB_USE_SUM 4
#define EMBEDDB_USE_BMAP 8
#define EMBEDDB_USE_VDATA 16
#define EMBEDDB_RESET_DATA 32

#define EMBEDDB_USING_INDEX(x) ((x & EMBEDDB_USE_INDEX) > 0 ? 1 : 0)
#define EMBEDDB_USING_MAX_MIN(x) ((x & EMBEDDB_USE_MAX_MIN) > 0 ? 1 : 0)
#define EMBEDDB_USING_SUM(x) ((x & EMBEDDB_USE_SUM) > 0 ? 1 : 0)
#define EMBEDDB_USING_BMAP(x) ((x & EMBEDDB_USE_BMAP) > 0 ? 1 : 0)
#define EMBEDDB_USING_VDATA(x) ((x & EMBEDDB_USE_VDATA) > 0 ? 1 : 0)
#define EMBEDDB_RESETING_DATA(x) ((x & EMBEDDB_RESET_DATA) > 0 ? 1 : 0)

/* Offsets with header */
#define EMBEDDB_COUNT_OFFSET 4
#define EMBEDDB_BITMAP_OFFSET 6
// #define EMBEDDB_MIN_OFFSET		8
#define EMBEDDB_MIN_OFFSET 14
#define EMBEDDB_IDX_HEADER_SIZE 16

#define EMBEDDB_NO_VAR_DATA UINT32_MAX

#define max(a, b) ((a) > (b) ? (a) : (b))
#define min(a, b) ((a) < (b) ? (a) : (b))

#define EMBEDDB_GET_COUNT(x) *((count_t *)((int8_t *)x + EMBEDDB_COUNT_OFFSET))
#define EMBEDDB_INC_COUNT(x) *((count_t *)((int8_t *)x + EMBEDDB_COUNT_OFFSET)) = *((count_t *)((int8_t *)x + EMBEDDB_COUNT_OFFSET)) + 1

#define EMBEDDB_GET_BITMAP(x) ((void *)((int8_t *)x + EMBEDDB_BITMAP_OFFSET))

#define EMBEDDB_GET_MIN_KEY(x) ((void *)((int8_t *)x + EMBEDDB_MIN_OFFSET))
#define EMBEDDB_GET_MAX_KEY(x, y) ((void *)((int8_t *)x + EMBEDDB_MIN_OFFSET + y->keySize))

#define EMBEDDB_GET_MIN_DATA(x, y) ((void *)((int8_t *)x + EMBEDDB_MIN_OFFSET + y->keySize * 2))
#define EMBEDDB_GET_MAX_DATA(x, y) ((void *)((int8_t *)x + EMBEDDB_MIN_OFFSET + y->keySize * 2 + y->dataSize))

#define EMBEDDB_DATA_WRITE_BUFFER 0
#define EMBEDDB_DATA_READ_BUFFER 1
#define EMBEDDB_INDEX_WRITE_BUFFER 2
#define EMBEDDB_INDEX_READ_BUFFER 3
#define EMBEDDB_VAR_WRITE_BUFFER(x) ((x & EMBEDDB_USE_INDEX) ? 4 : 2)
#define EMBEDDB_VAR_READ_BUFFER(x) ((x & EMBEDDB_USE_INDEX) ? 5 : 3)

#define EMBEDDB_FILE_MODE_W_PLUS_B 0  // Open file as read/write, creates file if doesn't exist, overwrites if it does. aka "w+b"
#define EMBEDDB_FILE_MODE_R_PLUS_B 1  // Open file as read/write, file must exist, keeps data if it does. aka "r+b"

#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define BYTE_TO_BINARY(byte)       \
    (byte & 0x80 ? '1' : '0'),     \
        (byte & 0x40 ? '1' : '0'), \
        (byte & 0x20 ? '1' : '0'), \
        (byte & 0x10 ? '1' : '0'), \
        (byte & 0x08 ? '1' : '0'), \
        (byte & 0x04 ? '1' : '0'), \
        (byte & 0x02 ? '1' : '0'), \
        (byte & 0x01 ? '1' : '0')

#define BM_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c %c%c%c%c%c%c%c%c"
#define BM_TO_BINARY(bm)           \
    (bm & 0x8000 ? '1' : '0'),     \
        (bm & 0x4000 ? '1' : '0'), \
        (bm & 0x2000 ? '1' : '0'), \
        (bm & 0x1000 ? '1' : '0'), \
        (bm & 0x800 ? '1' : '0'),  \
        (bm & 0x400 ? '1' : '0'),  \
        (bm & 0x200 ? '1' : '0'),  \
        (bm & 0x100 ? '1' : '0'),  \
        (bm & 0x80 ? '1' : '0'),   \
        (bm & 0x40 ? '1' : '0'),   \
        (bm & 0x20 ? '1' : '0'),   \
        (bm & 0x10 ? '1' : '0'),   \
        (bm & 0x08 ? '1' : '0'),   \
        (bm & 0x04 ? '1' : '0'),   \
        (bm & 0x02 ? '1' : '0'),   \
        (bm & 0x01 ? '1' : '0')

#define NO_RECORD_FOUND -1
#define RECORD_FOUND 0
/**
 * @brief	An interface for embedDB to read/write to any storage medium at the page level of granularity
 */
typedef struct {
    /**
     * @brief	Reads a single page into the buffer
     * @param	buffer		Pre-allocated space where data is read into
     * @param	pageNum		Page number to read. Is treated as an offset from the beginning of the file
     * @param	pageSize	Number of bytes in a page
     * @param	file		The file to read from. This is the file data that was stored in embedDBState->dataFile etc
     * @return	1 for success and 0 for failure
     */
    int8_t (*read)(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file);

    /**
     * @brief	Writes a single page to file
     * @param	buffer		The data to write to file
     * @param	pageNum		Page number to write. Is treated as an offset from the beginning of the file
     * @param	pageSize	Number of bytes in a page
     * @param	file		The file data that was stored in embedDBState->dataFile etc
     * @return	1 for success and 0 for failure
     */
    int8_t (*write)(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file);

    /**
     * @brief	Closes the file
     * @return	1 for success and 0 for failure
     */
    int8_t (*close)(void *file);

    /**
     * @brief
     * @param	file	The data that was passed to embedDB
     * @param	flags	Flags that determine in which mode
     * @return	1 for success and 0 for failure
     */
    int8_t (*open)(void *file, uint8_t mode);

    /**
     * @brief	Flushes file
     * @return	1 for success and 0 for failure
     */
    int8_t (*flush)(void *file);
} embedDBFileInterface;

typedef struct {
    void *dataFile;                                                       /* File for storing data records. */
    void *indexFile;                                                      /* File for storing index records. */
    void *varFile;                                                        /* File for storing variable length data. */
    embedDBFileInterface *fileInterface;                                  /* Interface to the file storage */
    uint32_t numDataPages;                                                /* The number of pages will use for storing fixed records*/
    uint32_t numIndexPages;                                               /* The number of pages will use for storing the data index */
    uint32_t numVarPages;                                                 /* The number of pages will use for storing variable data */
    count_t eraseSizeInPages;                                             /* Erase size in pages */
    uint32_t numAvailDataPages;                                           /* Number of writable data pages left before needing to delete */
    uint32_t numAvailIndexPages;                                          /* Number of writable index pages left before needing to delete */
    uint32_t numAvailVarPages;                                            /* Number of writable var pages left before needing to delete */
    uint32_t minDataPageId;                                               /* Lowest logical data page id that is saved on file */
    uint32_t minIndexPageId;                                              /* Lowest logical index page id that is saved on file */
    uint64_t minVarRecordId;                                              /* Minimum record id that we still have variable data for */
    id_t nextDataPageId;                                                  /* Next logical page id. Page id is an incrementing value and may not always be same as physical page id. */
    id_t nextIdxPageId;                                                   /* Next logical page id for index. Page id is an incrementing value and may not always be same as physical page id. */
    id_t nextVarPageId;                                                   /* Page number of next var page to be written */
    id_t currentVarLoc;                                                   /* Current variable address offset to write at (bytes from beginning of file) */
    void *buffer;                                                         /* Pre-allocated memory buffer for use by algorithm */
    spline *spl;                                                          /* Spline model */
    uint32_t numSplinePoints;                                             /* Number of spline points to allocate */
    radixspline *rdix;                                                    /* Radix Spline search model */
    int32_t indexMaxError;                                                /* Max error for indexing structure (Spline or PGM) */
    int8_t bufferSizeInBlocks;                                            /* Size of buffer in blocks */
    count_t pageSize;                                                     /* Size of physical page on device */
    int8_t parameters;                                                    /* Parameter flags for indexing and bitmaps */
    int8_t keySize;                                                       /* Size of key in bytes (fixed-size records) */
    int8_t dataSize;                                                      /* Size of data in bytes (fixed-size records). Do not include space for variable size records if you are using them. */
    int8_t recordSize;                                                    /* Size of record in bytes (fixed-size records) */
    int8_t headerSize;                                                    /* Size of header in bytes (calculated during init()) */
    int8_t variableDataHeaderSize;                                        /* Size of page header in variable data files (calculated during init()) */
    int8_t bitmapSize;                                                    /* Size of bitmap in bytes */
    int8_t cleanSpline;                                                   /* Enables automatic spline cleaning */
    id_t avgKeyDiff;                                                      /* Estimate for difference between key values. Used for get() to predict location of record. */
    count_t maxRecordsPerPage;                                            /* Maximum records per page */
    count_t maxIdxRecordsPerPage;                                         /* Maximum index records per page */
    int8_t (*compareKey)(void *a, void *b);                               /* Function that compares two arbitrary keys passed as parameters */
    int8_t (*compareData)(void *a, void *b);                              /* Function that compares two arbitrary data values passed as parameters */
    void (*extractData)(void *data);                                      /* Given a record, function that extracts the data (key) value from that record */
    void (*buildBitmapFromRange)(void *minData, void *maxData, void *bm); /* Given a record, builds bitmap based on its data (key) value */
    void (*updateBitmap)(void *data, void *bm);                           /* Given a record, updates bitmap based on its data (key) value */
    int8_t (*inBitmap)(void *data, void *bm);                             /* Returns 1 if data (key) value is a valid value given the bitmap */
    uint64_t minKey;                                                      /* Minimum key */
    uint64_t maxKey;                                                      /* Maximum key */
    int32_t maxError;                                                     /* Maximum key error */
    id_t numWrites;                                                       /* Number of page writes */
    id_t numReads;                                                        /* Number of page reads */
    id_t numIdxWrites;                                                    /* Number of index page writes */
    id_t numIdxReads;                                                     /* Number of index page reads */
    id_t bufferHits;                                                      /* Number of pages returned from buffer rather than storage */
    id_t bufferedPageId;                                                  /* Page id currently in read buffer */
    id_t bufferedIndexPageId;                                             /* Index page id currently in index read buffer */
    id_t bufferedVarPage;                                                 /* Variable page id currently in variable read buffer */
    uint8_t recordHasVarData;                                             /* Internal flag to signal that the record currently being written has var data */
} embedDBState;

typedef struct {
    uint32_t nextDataPage; /* Next data page that the iterator should read */
    uint16_t nextDataRec;  /* Next record on the data page tat the iterator should read */
    void *minKey;
    void *maxKey;
    void *minData;
    void *maxData;
    void *queryBitmap;
} embedDBIterator;

typedef struct {
    uint32_t totalBytes; /* Total number of bytes in the stream */
    uint32_t bytesRead;  /* Number of bytes read so far */
    uint32_t dataStart;  /* Start of data as an offset in bytes from the beginning of the file */
    uint32_t fileOffset; /* Where the iterator should start reading data next time (offset from start of file) */
} embedDBVarDataStream;

typedef enum {
    ITERATE_NO_MATCH = -1,
    ITERATE_MATCH = 1,
    ITERATE_NO_MORE_RECORDS = 0
} IterateStatus;

/**
 * @brief	Initialize embedDB structure.
 * @param	state			embedDB algorithm state structure
 * @param	indexMaxError	Max error of indexing structure (spline)
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBInit(embedDBState *state, size_t indexMaxError);

/**
 * @brief   Prints the initialization stats of the given embedDB state
 * @param   state   embedDB state structure
 */
void embedDBPrintInit(embedDBState *state);

/**
 * @brief	Puts a given key, data pair into structure.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBPut(embedDBState *state, void *key, void *data);

/**
 * @brief	Puts the given key, data, and variable length data into the structure.
 * @param	state			embedDB algorithm state structure
 * @param	key				Key for record
 * @param	data			Data for record
 * @param	variableData	Variable length data for record
 * @param	length			Length of the variable length data in bytes
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBPutVar(embedDBState *state, void *key, void *data, void *variableData, uint32_t length);

/**
 * @brief	Given a key, returns data associated with key.
 * 			Note: Space for data must be already allocated.
 * 			Data is copied from database into data buffer.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t embedDBGet(embedDBState *state, void *key, void *data);

/**
 * @brief	Given a key, returns data associated with key.
 * 			Data is copied from database into data buffer.
 * @param	state	embedDB algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @param	varData	Return variable for variable data as a embedDBVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	Return 0 if success. Non-zero value if error.
 * 			-1 : Error reading file
 * 			1  : Variable data was deleted to make room for newer data
 */
int8_t embedDBGetVar(embedDBState *state, void *key, void *data, embedDBVarDataStream **varData);

/**
 * @brief	Initialize iterator on embedDB structure.
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 */
void embedDBInitIterator(embedDBState *state, embedDBIterator *it);

/**
 * @brief	Close iterator after use.
 * @param	it		embedDB iterator structure
 */
void embedDBCloseIterator(embedDBIterator *it);

/**
 * @brief	Return next key, data pair for iterator.
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @return	1 if successful, 0 if no more records
 */
int8_t embedDBNext(embedDBState *state, embedDBIterator *it, void *key, void *data);

/**
 * @brief	Return next key, data, variable data set for iterator
 * @param	state	embedDB algorithm state structure
 * @param	it		embedDB iterator state structure
 * @param	key		Return variable for key (Pre-allocated)
 * @param	data	Return variable for data (Pre-allocated)
 * @param	varData	Return variable for variable data as a embedDBVarDataStream (Unallocated). Returns NULL if no variable data. **Be sure to free the stream after you are done with it**
 * @return	1 if successful, 0 if no more records
 */
int8_t embedDBNextVar(embedDBState *state, embedDBIterator *it, void *key, void *data, embedDBVarDataStream **varData);

/**
 * @brief	Reads data from variable data stream into the given buffer.
 * @param	state	embedDB algorithm state structure
 * @param	stream	Variable data stream
 * @param	buffer	Buffer to read data into
 * @param	length	Number of bytes to read (Must be <= buffer size)
 * @return	Number of bytes read
 */
uint32_t embedDBVarDataStreamRead(embedDBState *state, embedDBVarDataStream *stream, void *buffer, uint32_t length);

/**
 * @brief	Flushes output buffer.
 * @param	state	embedDB algorithm state structure
 */
int8_t embedDBFlush(embedDBState *state);

/**
 * @brief	Reads given page from storage.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readPage(embedDBState *state, id_t pageNum);

/**
 * @brief	Reads given index page from storage.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readIndexPage(embedDBState *state, id_t pageNum);

/**
 * @brief	Reads given variable data page from storage
 * @param 	state 	embedDB algorithm state structure
 * @param 	pageNum Page number to read
 * @return 	Return 0 if success, -1 if error
 */
int8_t readVariablePage(embedDBState *state, id_t pageNum);

/**
 * @brief	Writes page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writePage(embedDBState *state, void *buffer);

/**
 * @brief	Writes index page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writeIndexPage(embedDBState *state, void *buffer);

/**
 * @brief	Writes variable data page in buffer to storage. Returns page number.
 * @param	state	embedDB algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writeVariablePage(embedDBState *state, void *buffer);

/**
 * @brief	Prints statistics.
 * @param	state	embedDB state structure
 */
void embedDBPrintStats(embedDBState *state);

/**
 * @brief	Resets statistics.
 * @param	state	embedDB state structure
 */
void embedDBResetStats(embedDBState *state);

/**
 * @brief	Closes structure and frees any dynamic space.
 * @param	state	embedDB state structure
 */
void embedDBClose(embedDBState *state);

#ifdef __cplusplus
}
#endif
#endif
/************************************************************schema.h************************************************************/
/******************************************************************************/
/**
 * @file        schema.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for the schema for EmbedDB query interface
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

#ifndef embedDB_SCHEMA_H_
#define embedDB_SCHEMA_H_ 

#define embedDB_COLUMN_SIGNED 0
#define embedDB_COLUMN_UNSIGNED 1
#define embedDB_IS_COL_SIGNED(colSize) (colSize < 0 ? 1 : 0)

/**
 * @brief	A struct to desribe the number and sizes of attributes contained in the data of a embedDB table
 */
typedef struct {
    uint8_t numCols;      // The number of columns in the table
    int8_t* columnSizes;  // A list of the sizes, in bytes, of each column. Negative numbers indicate signed columns while positive indicate an unsigned column
} embedDBSchema;

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of key & data columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness);

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema);

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema);

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema);

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum);

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema);

void printSchema(embedDBSchema* schema);

#endif

/************************************************************utilityFunctions.h************************************************************/
/******************************************************************************/
/**
 * @file        utilityFunctions.h
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

#ifdef __cplusplus
extern "C" {
#endif   

/* Constructors */
embedDBState *defaultInitializedState();

/* Bitmap functions */
void updateBitmapInt8(void *data, void *bm);
void buildBitmapInt8FromRange(void *min, void *max, void *bm);
int8_t inBitmapInt8(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void buildBitmapInt16FromRange(void *min, void *max, void *bm);
void updateBitmapInt64(void *data, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
void buildBitmapInt64FromRange(void *min, void *max, void *bm);

/* Recordwise functions */
int8_t int32Comparator(void *a, void *b);
int8_t int64Comparator(void *a, void *b);

/* File functions */
embedDBFileInterface *getFileInterface();
void *setupFile(char *filename);
void tearDownFile(void *file);

#ifdef __cplusplus
}
#endif

/************************************************************advancedQueries.h************************************************************/
/******************************************************************************/
/**
 * @file        advancedQueries.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for the advanced query interface for EmbedDB
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

#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H  

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
#define SELECT_EQ 4
#define SELECT_NEQ 5

typedef struct embedDBAggregateFunc {
    /**
     * @brief	Resets the state
     */
    void (*reset)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	record	The record being added
     */
    void (*add)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record);

    /**
     * @brief	Finalize aggregate result into the record buffer and modify the schema accordingly. Is called once right before aggroup returns.
     */
    void (*compute)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord);

    /**
     * @brief	A user-allocated space where the operator saves its state. E.g. a sum operator might have 4 bytes allocated to store the sum of all data
     */
    void* state;

    /**
     * @brief	How many bytes will the compute insert into the record
     */
    int8_t colSize;

    /**
     * @brief	Which column number should compute write to
     */
    uint8_t colNum;
} embedDBAggregateFunc;

typedef struct embedDBOperator {
    /**
     * @brief	The input operator to this operator
     */
    struct embedDBOperator* input;

    /**
     * @brief	Initialize the operator. Usually includes setting/calculating the output schema, allocating buffers, etc. Recursively inits input operator as its first action.
     */
    void (*init)(struct embedDBOperator* operator);

    /**
     * @brief	Puts the next tuple to be outputed by this operator into @c operator->recordBuffer. Needs to call next on the input operator if applicable
     * @return	Returns 0 or 1 to indicate whether a new tuple was outputted to operator->recordBuffer
     */
    int8_t (*next)(struct embedDBOperator* operator);

    /**
     * @brief	Recursively closes this operator and its input operator. Frees anything allocated in init.
     */
    void (*close)(struct embedDBOperator* operator);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* state;

    /**
     * @brief	The output schema of this operator
     */
    embedDBSchema* schema;

    /**
     * @brief	The output record of this operator
     */
    void* recordBuffer;
} embedDBOperator;

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator);

/**
 * @brief	Completely free a chain of operators recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator);

///////////////////////////////////////////
// Pre-built operators for basic queries //
///////////////////////////////////////////

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema);

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. *Zero indexed*
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols);

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal);

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength);

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2);

//////////////////////////////////
// Prebuilt aggregate functions //
//////////////////////////////////

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate();

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum);

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize);

#endif


/************************************************************utilityFunctions.h************************************************************/
/******************************************************************************/
/**
 * @file        utilityFunctions.h
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

#ifdef __cplusplus
extern "C" {
#endif   

/* Constructors */
embedDBState *defaultInitializedState();

/* Bitmap functions */
void updateBitmapInt8(void *data, void *bm);
void buildBitmapInt8FromRange(void *min, void *max, void *bm);
int8_t inBitmapInt8(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void buildBitmapInt16FromRange(void *min, void *max, void *bm);
void updateBitmapInt64(void *data, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
void buildBitmapInt64FromRange(void *min, void *max, void *bm);

/* Recordwise functions */
int8_t int32Comparator(void *a, void *b);
int8_t int64Comparator(void *a, void *b);

/* File functions */
embedDBFileInterface *getFileInterface();
void *setupFile(char *filename);
void tearDownFile(void *file);

#ifdef __cplusplus
}
#endif

/************************************************************spline.h************************************************************/
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

/************************************************************schema.h************************************************************/
/******************************************************************************/
/**
 * @file        schema.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for the schema for EmbedDB query interface
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

#ifndef embedDB_SCHEMA_H_
#define embedDB_SCHEMA_H_ 

#define embedDB_COLUMN_SIGNED 0
#define embedDB_COLUMN_UNSIGNED 1
#define embedDB_IS_COL_SIGNED(colSize) (colSize < 0 ? 1 : 0)

/**
 * @brief	A struct to desribe the number and sizes of attributes contained in the data of a embedDB table
 */
typedef struct {
    uint8_t numCols;      // The number of columns in the table
    int8_t* columnSizes;  // A list of the sizes, in bytes, of each column. Negative numbers indicate signed columns while positive indicate an unsigned column
} embedDBSchema;

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of key & data columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness);

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema);

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema);

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema);

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum);

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema);

void printSchema(embedDBSchema* schema);

#endif

/************************************************************radixspline.h************************************************************/
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

/************************************************************advancedQueries.h************************************************************/
/******************************************************************************/
/**
 * @file        advancedQueries.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for the advanced query interface for EmbedDB
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

#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H  

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
#define SELECT_EQ 4
#define SELECT_NEQ 5

typedef struct embedDBAggregateFunc {
    /**
     * @brief	Resets the state
     */
    void (*reset)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	record	The record being added
     */
    void (*add)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record);

    /**
     * @brief	Finalize aggregate result into the record buffer and modify the schema accordingly. Is called once right before aggroup returns.
     */
    void (*compute)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord);

    /**
     * @brief	A user-allocated space where the operator saves its state. E.g. a sum operator might have 4 bytes allocated to store the sum of all data
     */
    void* state;

    /**
     * @brief	How many bytes will the compute insert into the record
     */
    int8_t colSize;

    /**
     * @brief	Which column number should compute write to
     */
    uint8_t colNum;
} embedDBAggregateFunc;

typedef struct embedDBOperator {
    /**
     * @brief	The input operator to this operator
     */
    struct embedDBOperator* input;

    /**
     * @brief	Initialize the operator. Usually includes setting/calculating the output schema, allocating buffers, etc. Recursively inits input operator as its first action.
     */
    void (*init)(struct embedDBOperator* operator);

    /**
     * @brief	Puts the next tuple to be outputed by this operator into @c operator->recordBuffer. Needs to call next on the input operator if applicable
     * @return	Returns 0 or 1 to indicate whether a new tuple was outputted to operator->recordBuffer
     */
    int8_t (*next)(struct embedDBOperator* operator);

    /**
     * @brief	Recursively closes this operator and its input operator. Frees anything allocated in init.
     */
    void (*close)(struct embedDBOperator* operator);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* state;

    /**
     * @brief	The output schema of this operator
     */
    embedDBSchema* schema;

    /**
     * @brief	The output record of this operator
     */
    void* recordBuffer;
} embedDBOperator;

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator);

/**
 * @brief	Completely free a chain of operators recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator);

///////////////////////////////////////////
// Pre-built operators for basic queries //
///////////////////////////////////////////

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema);

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. *Zero indexed*
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols);

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal);

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength);

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2);

//////////////////////////////////
// Prebuilt aggregate functions //
//////////////////////////////////

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate();

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum);

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize);

#endif

