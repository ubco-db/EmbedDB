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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "../spline/radixspline.h"
#include "../spline/spline.h"

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