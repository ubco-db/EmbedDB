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
#define VAR_DATA_FILE_PATH "varFile.bin"

#else

#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"
#define VAR_DATA_FILE_PATH "build/artifacts/varFile.bin"

#endif

embedDBState* init_state();
embedDBSchema* createSchema();
void execOperator(embedDBState* state);
int8_t floatComparator(void* a, void* b);
int32_t randomInt(int min, int max) {
    int randomIntInRange = (rand() % (max - min + 1)) + min;
    return randomIntInRange;
}

uint32_t embedDBExample() {
    embedDBState* state;
    state = init_state();
    embedDBPrintInit(state);
    srand(time(NULL));

    for (int i = 0; i < 100; i++) {
        uint32_t timestamp = 10220000 + i; // Example timestamp
        
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
        printf("from db: Timestamp: %d, Temperature: %d\n", timestamp, data);
    }


    execOperator(state);

    printf("Example completed!\n");
    return 0;
}

embedDBSchema* createSchema() {
    uint8_t numCols = 2;
    int8_t colSizes[] = {4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness);
    return schema;
}

int8_t floatComparator(void* a, void* b) {
    float* fa = (float*) a;
    float* fb = (float*) b;
    if (*fa < *fb) {
        return -1;
    } else if (*fa > *fb) {
        return 1;
    } else {
        return 0;
    }
}

int8_t groupFunction(const void* lastRecord, const void* record) {
    return 1;
}

embedDBOperator* createOperator(embedDBState* state, void*** allocatedValues) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    uint32_t minKeyVal = 10220099-9;
    it->minKey = &minKeyVal;
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    embedDBSchema* schema = createSchema();

    embedDBOperator* scanOp = createTableScanOperator(state, it, schema);
    embedDBAggregateFunc* avg0 = createAvgAggregate(1, 4);
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1*sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *avg0;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 1);
    aggOp->init(aggOp);

    embedDBFreeSchema(&schema);
    free(avg0);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;

    return aggOp;
}

void execOperator(embedDBState* state) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(state, &allocatedValues);
    void* recordBuffer = op->recordBuffer;
    float* C1 = (float*)((int8_t*)recordBuffer + 0);
    // Print as csv
    while (exec(op)) {
        printf("average temperature in the last 10 seconds: %f\n", *C1);
    }
    printf("\n");
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
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
    state->keySize = 4;
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
    state->numVarPages = 76;
    state->eraseSizeInPages = 4;

    if (STORAGE_TYPE == 1) {
        printf("Dataflash storage is not currently supported in this example. Proceeding using SD storage.\n");
    }

    char dataPath[] = DATA_FILE_PATH, indexPath[] = INDEX_FILE_PATH, varPath[] = VAR_DATA_FILE_PATH;
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->varFile = setupFile(varPath);

    // enable parameters
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;

    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
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
