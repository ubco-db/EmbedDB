/**
 * @file utilityFunctions.h
 * @author Ramon Lawernce
 * @brief This file contains some utility functions to be used with sbits.
 * These include functions required to use the bitmap option, and a
 * comparator for comparing keys. They can be modified or implemented
 * differently depending on the application.
 */

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "sbits.h"

void updateBitmapInt8(void *data, void *bm);
void buildBitmapInt8FromRange(void *min, void *max, void *bm);
int8_t inBitmapInt8(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void buildBitmapInt16FromRange(void *min, void *max, void *bm);
void updateBitmapInt64(void *data, void *bm);
void buildBitmapInt64FromRange(void *min, void *max, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
int8_t int32Comparator(void *a, void *b);

sbitsFileInterface *getSDInterface();
sbitsFileInterface *getDataflashInterface();
void *setupDataflashFile(uint32_t startPage, uint32_t numPages);
void tearDownDataflashFile(void *file);
void *setupSDFile(char *filename);
void tearDownSDFile(void *file);

#if defined(__cplusplus)
}
#endif
