#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "../../src/sbits/sbits.h"
#include "sdcard_c_iface.h"

sbitsFileInterface *getSDInterface();
void *setupSDFile(char *filename);
void tearDownSDFile(void *file);

#ifdef __cplusplus
}
#endif
