/******************************************************************************/
/**
 * @file		embedDBExample.h
 * @author		EmbedDB Team (See Authors.md)
 * @brief		This file includes and example for insterting and retrieving sequential records for EmbeDB.
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#ifndef PIO_UNIT_TESTING

#include <errno.h>
#include <string.h>
#include <time.h>

#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/advancedQueries.h"
#endif

/**
 * 0 = SD Card
 * 1 = Dataflash
 */
#define STORAGE_TYPE 0

#define SUCCESS 0

#ifdef ARDUINO

#if defined(MEMBOARD) && STORAGE_TYPE == 1

#include "dataflashFileInterface.h"

#endif

#include "SDFileInterface.h"
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile

#define DATA_FILE_PATH "dataFile.bin"
#define INDEX_FILE_PATH "indexFile.bin"

#else

#include "desktopFileInterface.h"
#include "query-interface/activeRules.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"

#endif

embedDBState* init_state();
embedDBSchema* createSchema();
int32_t randomInt(int min, int max) {
    int randomIntInRange = (rand() % (max - min + 1)) + min;
    return randomIntInRange;
}

void GTcallback(void* aggregateValue, void* currentValue, void* context) {
    printf("avg temperature is greater than 25: avg: %f, Current: %i\n", *(float*)aggregateValue, *(int32_t*)currentValue);
}

void LTcallback(void* aggregateValue, void* currentValue, void* context) {
    printf("Max temperature is less than 25: Max: %i, Current: %i\n", *(int32_t*)aggregateValue, *(int32_t*)currentValue);
}

uint32_t embedDBExample() {
    embedDBState* state;
    state = init_state();
    embedDBPrintInit(state);

    embedDBSchema* schema = createSchema();
    int numLast = 5;
    activeRule *activeRuleGT = createActiveRule(schema, NULL);
    activeRuleGT->IF(activeRuleGT, 1, GET_AVG)
                    ->ofLast(activeRuleGT, (void*)&numLast)
                    ->is(activeRuleGT, GreaterThan, (void*)&(float){10.5})
                    ->then(activeRuleGT, GTcallback);

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = activeRuleGT;
    state->numRules = 1;
    srand(time(NULL));

    for (int i = 0; i < 100; i++) {
        uint64_t timestamp = 202411040000 + i; // Example timestamp
        
        // calloc dataPtr in the heap
        void* dataPtr = calloc(1, state->dataSize);

        // set value to be inserted
        *((uint32_t*)dataPtr) = randomInt(15, 30);
        int8_t result = embedDBPut(state, &timestamp, dataPtr);      
        if(result != SUCCESS) {
            printf("Error inserting record\n");
        }
        free(dataPtr);
        int32_t data = 0;
        int32_t success = embedDBGet(state, (void*)&timestamp, (void*)&data);
        if (success != SUCCESS) {
            printf("Error getting record\n");
        }
        printf("from db: Timestamp: %llu, Temperature: %d\n", timestamp, data);
    }

    printf("Example completed!\n");
    return 0;
}

embedDBSchema* createSchema() {
    uint8_t numCols = 2;
    int8_t colSizes[] = {8, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED};
    ColumnType colTypes[] = {embedDB_COLUMN_UINT64, embedDB_COLUMN_INT32};
    embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness, colTypes);
    return schema;
}


embedDBState* init_state() {
    embedDBState* state = (embedDBState*)malloc(sizeof(embedDBState));

    // ensure successful malloc
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(-1);
    }
    /* configure EmbedDB state variables */
    // for fixed-length records
    state->keySize = 8;
    state->dataSize = 4;

    // for buffer(s)
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // ensure successful malloc
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(-1);
    }

    // for learned indexing and bitmap
    state->numSplinePoints = 300;
    state->bitmapSize = 1;

    // address storage characteristics
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->eraseSizeInPages = 4;

    if (STORAGE_TYPE == 1) {
        printf("Dataflash storage is not currently supported in this example. Proceeding using SD storage.\n");
    }

    char dataPath[] = DATA_FILE_PATH, indexPath[] = INDEX_FILE_PATH;
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    // enable parameters
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX  | EMBEDDB_RESET_DATA;

    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int64Comparator;
    state->compareData = int32Comparator;


    // init embedDB
    size_t splineMaxError = 1;
    if (embedDBInit(state, splineMaxError) != 0) {
        printf("Initialization error");
        exit(-1);
    }

    embedDBResetStats(state);
    return state;
}

#endif
