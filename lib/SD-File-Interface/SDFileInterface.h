#if !defined(SD_FILE_INTERFACE)
#define SD_FILE_INTERFACE

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "../../src/embedDB/embedDB.h"
#include "sdcard_c_iface.h"

embedDBFileInterface *getSDInterface();
void *setupSDFile(char *filename);
void tearDownSDFile(void *file);

#ifdef __cplusplus
}
#endif

#endif
