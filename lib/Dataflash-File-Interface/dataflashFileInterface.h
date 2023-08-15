#if !defined(DATAFLASH_FILE_INTERFACE)
#define DATAFLASH_FILE_INTERFACE

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "../../src/sbits/sbits.h"

sbitsFileInterface *getDataflashInterface();
void *setupDataflashFile(uint32_t startPage, uint32_t numPages);
void tearDownDataflashFile(void *file);

#ifdef __cplusplus
}
#endif

#endif
