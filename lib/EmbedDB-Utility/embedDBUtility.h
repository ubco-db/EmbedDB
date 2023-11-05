/**
 * @file embedDBUtility.h
 * @author Ramon Lawernce
 * @brief This file contains some utility functions to be used with embedDB.
 * These include functions required to use the bitmap option, and a
 * comparator for comparing keys. They can be modified or implemented
 * differently depending on the application.
 */

#if !defined(EMBEDDB_UTILITY)
#define EMBEDDB_UTILITY

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "../../src/embedDB/embedDB.h"
#include "SDFileInterface.h"

/* Constructors */
embedDBState *defaultInitializedState();

/* Bitmap Functions */
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

#ifdef __cplusplus
}
#endif

#endif
