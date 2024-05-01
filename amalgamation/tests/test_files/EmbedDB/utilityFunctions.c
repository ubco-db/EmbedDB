/******************************************************************************/
/**
 * @file        utilityFunctions.c
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

#include "utilityFunctions.h"

#include <string.h>

embedDBState *defaultInitializedState() {
    embedDBState *state = calloc(1, sizeof(embedDBState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("Failed to allocate memory for state.\n");
#endif
        return NULL;
    }

    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

    /* Address level parameters */
    state->numDataPages = 20000;  // Enough for 620,000 records
    state->numIndexPages = 44;    // Enough for 676,544 records
    state->eraseSizeInPages = 4;

    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Initialize embedDB structure */
    if (embedDBInit(state, 1) != 0) {
#ifdef PRINT_ERRORS
        printf("Initialization error.\n");
#endif
        free(state->buffer);
        free(state->fileInterface);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state);
        return NULL;
    }

    return state;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and max value. */
void buildBitmapInt8FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

int8_t inBitmapInt8(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt16(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt16(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 64-bit bitmap from (min, max) range.
 * @param	state	embedDB state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt64FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint64_t *)bm = UINT64_MAX; /* Everything */
        return;
    } else {
        uint64_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt64(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint64_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt64(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint64_t *)bm = maxMap;
                return;
            }
        }
        *(uint64_t *)bm = minMap & maxMap;
    }
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

int8_t int64Comparator(void *a, void *b) {
    int64_t result = *((int64_t *)a) - *((int64_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

typedef struct {
    char *filename;
    FILE *file;
} FILE_INFO;

void *setupFile(char *filename) {
    FILE_INFO *fileInfo = malloc(sizeof(FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->file = NULL;
    return fileInfo;
}

void tearDownFile(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo->file != NULL)
        fclose(fileInfo->file);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageSize * pageNum, SEEK_SET);
    return fread(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageNum * pageSize, SEEK_SET);
    return fwrite(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_CLOSE(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fclose(fileInfo->file);
    fileInfo->file = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    return fflush(fileInfo->file) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "w+b");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "r+b");
    } else {
        return 0;
    }

    if (fileInfo->file == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getFileInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}
