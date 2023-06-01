/******************************************************************************/
/**
 * @file		sbits.h
 * @author		Ramon Lawrence
 * @brief		This file is for sequential bitmap indexing for time series (SBITS).
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
#ifndef SBITS_H_
#define SBITS_H_

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#if defined(ARDUINO)
#include "file/dataflash_c_iface.h"
#include "file/sdcard_c_iface.h"
#include "file/serial_c_iface.h"
#endif

/* If using radix spline indexing */
#include "spline/radixspline.h"
#include "spline/spline.h"

/* If using PGM indexing */
/* TODO */

/* Define type for page ids (physical and logical). */
typedef uint32_t id_t;

/* Define type for page record count. */
typedef uint16_t count_t;

#define SBITS_USE_INDEX 1
#define SBITS_USE_MAX_MIN 2
#define SBITS_USE_SUM 4
#define SBITS_USE_BMAP 8
#define SBITS_USE_VDATA 16

#define SBITS_USING_INDEX(x) ((x & SBITS_USE_INDEX) > 0 ? 1 : 0)
#define SBITS_USING_MAX_MIN(x) ((x & SBITS_USE_MAX_MIN) > 0 ? 1 : 0)
#define SBITS_USING_SUM(x) ((x & SBITS_USE_SUM) > 0 ? 1 : 0)
#define SBITS_USING_BMAP(x) ((x & SBITS_USE_BMAP) > 0 ? 1 : 0)
#define SBITS_USING_VDATA(x) ((x & SBITS_USE_VDATA) > 0 ? 1 : 0)

/* Offsets with header */
#define SBITS_COUNT_OFFSET 4
#define SBITS_BITMAP_OFFSET 6
// #define SBITS_MIN_OFFSET		8
#define SBITS_MIN_OFFSET 14
#define SBITS_IDX_HEADER_SIZE 16

#define SBITS_NO_VAR_DATA UINT32_MAX

#define SBITS_GET_COUNT(x) *((count_t *)((int8_t *)x + SBITS_COUNT_OFFSET))
#define SBITS_INC_COUNT(x) *((count_t *)((int8_t *)x + SBITS_COUNT_OFFSET)) = *((count_t *)((int8_t *)x + SBITS_COUNT_OFFSET)) + 1

#define SBITS_GET_BITMAP(x) ((void *)((int8_t *)x + SBITS_BITMAP_OFFSET))

#define SBITS_GET_MIN_KEY(x, y) ((void *)((int8_t *)x + SBITS_MIN_OFFSET))
#define SBITS_GET_MAX_KEY(x, y) ((void *)((int8_t *)x + SBITS_MIN_OFFSET + y->keySize))

#define SBITS_GET_MIN_DATA(x, y) ((void *)((int8_t *)x + SBITS_MIN_OFFSET + y->keySize * 2))
#define SBITS_GET_MAX_DATA(x, y) ((void *)((int8_t *)x + SBITS_MIN_OFFSET + y->keySize * 2 + y->dataSize))

#define SBITS_DATA_WRITE_BUFFER 0
#define SBITS_DATA_READ_BUFFER 1
#define SBITS_INDEX_WRITE_BUFFER 2
#define SBITS_INDEX_READ_BUFFER 3
#define SBITS_VAR_WRITE_BUFFER(x) ((x & SBITS_USE_INDEX) ? 4 : 2)
#define SBITS_VAR_READ_BUFFER(x) ((x & SBITS_USE_INDEX) ? 5 : 3)

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

#define FILE_STORAGE 1
#define DATAFLASH_STORAGE 2

typedef struct {
    SD_FILE *file;                              /* File for storing data records. */
    SD_FILE *indexFile;                         /* File for storing index records. */
    SD_FILE *varFile;                           /* File for storing variable length data. */
    int8_t storageType;                         /* Storage type. 1 - for SD card file, 2 - for data flash memory */
    void *storage;                              /* Storage configuration */
    id_t startAddress;                          /* Start address in memory space */
    id_t endAddress;                            /* End address in memory space */
    count_t eraseSizeInPages;                   /* Erase size in pages */
    id_t startDataPage;                         /* Start data page number */
    id_t endDataPage;                           /* End data page number */
    id_t numAvailVarPages;                      /* Number of writable pages left before needing to delete */
    id_t varAddressStart;                       /* Start address for the variable data page */
    id_t varAddressEnd;                         /* End address for the variable data page */
    count_t numVarPages;                        /* Number of variable pages */
    id_t startIdxPage;                          /* Start index page number */
    id_t endIdxPage;                            /* End index page number */
    id_t firstDataPage;                         /* First data page number (physical location) */
    id_t firstDataPageId;                       /* First data page number (logical page id) */
    id_t currentVarLoc;                         /* Current variable address offset to write at (bytes from beginning of file) */
    id_t nextVarPageId;                         /* Page number of next var page to be written */
    id_t firstIdxPage;                          /* First data page number (physical location) */
    id_t erasedEndPage;                         /* Physical page number of last erased page */
    id_t erasedEndIdxPage;                      /* Physical page number of last erased index page */
    int8_t wrappedMemory;                       /* 1 if have wrapped around in memory, 0 otherwise */
    int8_t wrappedIdxMemory;                    /* 1 if have wrapped around in index memory, 0 otherwise */
    int8_t wrappedVariableMemory;               /* 1 if have wrapped around in variable data memory, 0 otherwise */
    id_t minVarRecordId;                        /* Minimum record id that we still have variable data for */
    void *buffer;                               /* Pre-allocated memory buffer for use by algorithm */
    spline *spl;                                /* Spline model */
    radixspline *rdix;                          /* Radix Spline search model */
    int32_t indexMaxError;                      /* Max error for indexing structure (Spline or PGM) */
    int8_t bufferSizeInBlocks;                  /* Size of buffer in blocks */
    count_t pageSize;                           /* Size of physical page on device */
    int8_t parameters;                          /* Parameter flags for indexing and bitmaps */
    int8_t keySize;                             /* Size of key in bytes (fixed-size records) */
    int8_t dataSize;                            /* Size of data in bytes (fixed-size records). Do not include space for variable size records if you are using them. */
    int8_t recordSize;                          /* Size of record in bytes (fixed-size records) */
    int8_t headerSize;                          /* Size of header in bytes (calculated during init()) */
    int8_t bitmapSize;                          /* Size of bitmap in bytes */
    id_t avgKeyDiff;                            /* Estimate for difference between key values. Used for get() to predict location of record. */
    id_t nextPageId;                            /* Next logical page id. Page id is an incrementing value and may not always be same as physical page id. */
    id_t nextPageWriteId;                       /* Physical page id of next page to write. */
    id_t nextIdxPageId;                         /* Next logical page id for index. Page id is an incrementing value and may not always be same as physical page id. */
    id_t nextIdxPageWriteId;                    /* Physical index page id of next page to write. */
    count_t maxRecordsPerPage;                  /* Maximum records per page */
    count_t maxIdxRecordsPerPage;               /* Maximum index records per page */
    int8_t (*compareKey)(void *a, void *b);     /* Function that compares two arbitrary keys passed as parameters */
    int8_t (*compareData)(void *a, void *b);    /* Function that compares two arbitrary data values passed as parameters */
    void (*extractData)(void *data);            /* Given a record, function that extracts the data (key) value from that record */
    void (*updateBitmap)(void *data, void *bm); /* Given a record, updates bitmap based on its data (key) value */
    int8_t (*inBitmap)(void *data, void *bm);   /* Returns 1 if data (key) value is a valid value given the bitmap */
    int32_t minKey;                             /* Minimum key */
    int32_t maxKey;                             /* Maximum key */
    int32_t maxError;                           /* Maximum key error */
    id_t numWrites;                             /* Number of page writes */
    id_t numReads;                              /* Number of page reads */
    id_t numIdxWrites;                          /* Number of index page writes */
    id_t numIdxReads;                           /* Number of index page reads */
    id_t bufferHits;                            /* Number of pages returned from buffer rather than storage */
    id_t bufferedPageId;                        /* Page id currently in read buffer */
    id_t bufferedIndexPageId;                   /* Index page id currently in index read buffer */
    uint8_t recordHasVarData;                   /* Internal flag to signal that the record currently being written has var data */
} sbitsState;

typedef struct {
    id_t lastIterPage;       /* Last page read by iterator */
    count_t lastIterRec;     /* Last record read by iterator */
    id_t lastIdxIterPage;    /* Last index page read by iterator */
    count_t lastIdxIterRec;  /* Last index record read by iterator */
    int8_t wrappedMemory;    /* 1 if have wrapped around in memory during iterator search, 0 otherwise */
    int8_t wrappedIdxMemory; /* 1 if have wrapped around in memory during index iterator search, 0 otherwise */
    void *minKey;
    void *maxKey;
    void *minData;
    void *maxData;
    void *queryBitmap;
} sbitsIterator;

/**
 * @brief	Initialize SBITS structure.
 * @param	state			SBITS algorithm state structure
 * @param	indexMaxError	Max error of indexing structure (spline or PGM)
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsInit(sbitsState *state, size_t indexMaxError);

/**
 * @brief	Puts a given key, data pair into structure.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsPut(sbitsState *state, void *key, void *data);

/**
 * @brief	Puts the given key, data, and variable length data into the structure.
 * @param	state			SBITS algorithm state structure
 * @param	key				Key for record
 * @param	data			Data for record
 * @param	variableData	Variable length data for record
 * @param	length			Length of the variable length data in bytes
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsPutVar(sbitsState *state, void *key, void *data, void *variableData, uint32_t length);

/**
 * @brief	Given a key, returns data associated with key.
 * 			Note: Space for data must be already allocated.
 * 			Data is copied from database into data buffer.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @return	Return 0 if success. Non-zero value if error.
 */
int8_t sbitsGet(sbitsState *state, void *key, void *data);

/**
 * @brief	Given a key, returns data associated with key.
 * 			Data is copied from database into data buffer.
 * @param	state	SBITS algorithm state structure
 * @param	key		Key for record
 * @param	data	Pre-allocated memory to copy data for record
 * @param	varData	Un-allocated memory where the variable length data will be returned
 * @return	Return 0 if success. Non-zero value if error.
 * 			-1 : Error reading file
 * 			1  : Variable data was deleted to make room for newer data
 */
int8_t sbitsGetVar(sbitsState *state, void *key, void *data, void **varData, uint32_t *length);

/**
 * @brief	Initialize iterator on sbits structure.
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 */
void sbitsInitIterator(sbitsState *state, sbitsIterator *it);

/**
 * @brief	Return next key, data pair for iterator.
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 * @param	key		Key for record
 * @param	data	Data for record
 */
int8_t sbitsNext(sbitsState *state, sbitsIterator *it, void **key, void **data);

/**
 * @brief	Return next key, data, variable data set for iterator
 * @param	state	SBITS algorithm state structure
 * @param	it		SBITS iterator state structure
 * @param	key		Key for record
 * @param	data	Data for record
 * @param	varData	Data for variable portion of record
 */
int8_t sbitsNextVar(sbitsState *state, sbitsIterator *it, void **key, void **data, void **varData);

/**
 * @brief	Flushes output buffer.
 * @param	state	SBITS algorithm state structure
 */
int8_t sbitsFlush(sbitsState *state);

/**
 * @brief	Reads given page from storage.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readPage(sbitsState *state, id_t pageNum);

/**
 * @brief	Reads given index page from storage.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return 0 if success, -1 if error.
 */
int8_t readIndexPage(sbitsState *state, id_t pageNum);

/**
 * @brief	Reads given variable data page from storage
 * @param 	state 	SBITS algorithm state structure
 * @param 	pageNum Page number to read
 * @return 	Return 0 if success, -1 if error
 */
int8_t readVariablePage(sbitsState *state, id_t pageNum);

/**
 * @brief	Writes page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writePage(sbitsState *state, void *buffer);

/**
 * @brief	Writes index page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writeIndexPage(sbitsState *state, void *buffer);

/**
 * @brief	Writes variable data page in buffer to storage. Returns page number.
 * @param	state	SBITS algorithm state structure
 * @param	pageNum	Page number to read
 * @return	Return page number if success, -1 if error.
 */
id_t writeVariablePage(sbitsState *state, void *buffer);

/**
 * @brief	Prints statistics.
 * @param	state	SBITS state structure
 */
void printStats(sbitsState *state);

/**
 * @brief	Resets statistics.
 * @param	state	SBITS state structure
 */
void resetStats(sbitsState *state);

/*
Bitmap related functions
*/
/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	SBITS state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(sbitsState *state, void *min, void *max, void *bm);

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	SBITS state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(sbitsState *state, void *min, void *max, void *bm);

#if defined(__cplusplus)
}
#endif

#endif
