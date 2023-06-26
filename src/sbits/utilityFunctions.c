/**
 * @file utilityFunctions.h
 * @author Ramon Lawernce
 * @brief This file contains some utility functions to be used with sbits.
 * These include functions required to use the bitmap option, and a
 * comparator for comparing keys. They can be modified or implemented
 * differently depending on the application.
 */

#include "utilityFunctions.h"

#include <string.h>

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
 * @param	state	SBITS state structure
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
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
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
 * @param	state	SBITS state structure
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
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint64_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
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

typedef struct {
    char *filename;
    SD_FILE *sdFile;
} SD_FILE_INFO;

void *setupSDFile(char *filename) {
    SD_FILE_INFO *fileInfo = malloc(sizeof(SD_FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->sdFile = NULL;
    return fileInfo;
}

void tearDownSDFile(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo != NULL)
        sd_fclose(fileInfo->sdFile);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageSize * pageNum, SEEK_SET);
    return sd_fread(buffer, pageSize, 1, fileInfo->sdFile);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageNum * pageSize, SEEK_SET);
    return sd_fwrite(buffer, pageSize, 1, fileInfo->sdFile) == pageSize;
}

int8_t FILE_CLOSE(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fclose(fileInfo->sdFile);
    fileInfo->sdFile = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    return sd_fflush(fileInfo->sdFile) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;

    if (mode == SBITS_FILE_MODE_W_PLUS_B) {
        fileInfo->sdFile = sd_fopen(fileInfo->filename, "w+");
    } else if (mode == SBITS_FILE_MODE_R_PLUS_B) {
        fileInfo->sdFile = sd_fopen(fileInfo->filename, "r+");
    } else {
        return 0;
    }

    if (fileInfo->sdFile == NULL) {
        return 0;
    } else {
        return 1;
    }
}

typedef struct {
    uint32_t startPage;
    uint32_t numPages;
} DF_FILE_INFO;

void *setupDataflashFile(uint32_t startPage, uint32_t numPages) {
    DF_FILE_INFO *fileInfo = malloc(sizeof(DF_FILE_INFO));
    fileInfo->startPage = startPage;
    fileInfo->numPages = numPages;
    return fileInfo;
}

void tearDownDataflashFile(void *file) {
    free(file);
}

int8_t DF_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    DF_FILE_INFO *fileInfo = (DF_FILE_INFO *)file;
    if (pageNum >= fileInfo->numPages) {
        return 0;
    } else {
        uint32_t physicalPage = fileInfo->startPage + pageNum;
        uint32_t val = dfread(physicalPage, buffer, pageSize);
        if (val == pageSize) {
            return 1;
        } else {
            return 0;
        }
    }
}

int8_t DF_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    DF_FILE_INFO *fileInfo = (DF_FILE_INFO *)file;
    if (pageNum >= fileInfo->numPages) {
        return 0;
    } else {
        uint32_t physicalPage = fileInfo->startPage + pageNum;
        uint32_t val = dfwrite(physicalPage, buffer, pageSize);
        if (val == pageSize) {
            return 1;
        } else {
            return 0;
        }
    }
}

int8_t DF_CLOSE(void *file) {
    return 1;
}

int8_t DF_OPEN(void *file, uint8_t mode) {
    return 1;
}

int8_t DF_FLUSH(void *file) {
    return 1;
}

sbitsFileInterface *getSDInterface() {
    sbitsFileInterface *fileInterface = malloc(sizeof(sbitsFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}

sbitsFileInterface *getDataflashInterface() {
    sbitsFileInterface *fileInterface = malloc(sizeof(sbitsFileInterface));
    fileInterface->close = DF_CLOSE;
    fileInterface->read = DF_READ;
    fileInterface->write = DF_WRITE;
    fileInterface->open = DF_OPEN;
    fileInterface->flush = DF_FLUSH;
    return fileInterface;
}
