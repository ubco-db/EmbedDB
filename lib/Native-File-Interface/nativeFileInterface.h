#if !defined(NATIVE_FILE_INTERFACE)
#define NATIVE_FILE_INTERFACE

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#if defined(DIST)
#include "embedDB.h"
#else
#include "../../src/embedDB/embedDB.h"
#endif

/* File functions */
embedDBFileInterface *getFileInterface();
void *setupFile(char *filename);
void tearDownFile(void *file);

#ifdef __cplusplus
}
#endif

#endif
