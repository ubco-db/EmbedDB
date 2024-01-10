#ifndef PIO_UNIT_TESTING

#include <errno.h>
#include <string.h>
#include <time.h>

#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "sdcard_c_iface.h"

#if defined(MEGA)
#include "SDFileInterface.h"
#endif

#if defined(DUE)
#include "SDFileInterface.h"
#endif

#if defined(MEMBOARD)
#include "SDFileInterface.h"
#include "dataflashFileInterface.h"
#endif

int main() {
	embedDBState* state = (embedDBState*)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->numSplinePoints = 30;
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->buildBitmapFromRange = buildBitmapInt16FromRange;
    embedDBInit(state, 1);

	// Insert dummy data
	for (int n = 0; n < 20; n++) {
		uint32_t key = n;
		int32_t data[] = {n, n*2, n*3};
		
	}	

	return 0;
}

#endif
