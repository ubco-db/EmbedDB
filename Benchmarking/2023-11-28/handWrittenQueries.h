/******************************************************************************/
/**
 * @file		advancedQueryInterfaceExample.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief		This file includes an example of querying EmbedDB using the
 *              advanced query interface.
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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../../src/embedDB/embedDB.h"
#include "../../src/query-interface/advancedQueries.h"
#include "embedDbUtility.h"

#if defined(MEGA)
#include "SDFileInterface.h"
#endif

#if defined(DUE)
#include "SDFileInterface.h"
#endif

#if defined(MEMBOARD)
#include "SDFileInterface.h"
#include "dataflashFileInterface.h"
#include "dataflash_c_iface.h"
#endif

#define STORAGE_TYPE 0  // 0 = SD Card, 1 = Dataflash

int8_t oneGroup(const void* lastRecord, const void* record) {
    return 1;
}

uint32_t watchGroup(const void* record) {
    // Break up into 100 groups
    return (*(uint32_t*)record) / (7060000 / 10);
}

int8_t sameWatchGroup(const void* lastRecord, const void* record) {
    return watchGroup(lastRecord) == watchGroup(record);
}

void writeWatchGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = watchGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
}

uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
}

void customShiftInit(embedDBOperator* op) {
    op->input->init(op->input);
    op->schema = copySchema(op->input->schema);
    op->recordBuffer = malloc(16);
}

int8_t customShiftNext(embedDBOperator* op) {
    if (op->input->next(op->input)) {
        memcpy(op->recordBuffer, op->input->recordBuffer, 16);
        *(uint32_t*)op->recordBuffer += 473385600;  // Add the number of seconds between 2000 and 2015
        return 1;
    }
    return 0;
}

void customShiftClose(embedDBOperator* op) {
    op->input->close(op->input);
    embedDBFreeSchema(&op->schema);
    free(op->recordBuffer);
    op->recordBuffer = NULL;
}

int8_t customInt32Comparator(void* a, void* b) {
    int32_t i1, i2;
    memcpy(&i1, (int8_t*)a + 8, sizeof(int32_t));
    memcpy(&i2, (int8_t*)b + 8, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

void insertData(embedDBState* state, char* filename);
void updateCustomEthBitmap(void* data, void* bm);
int8_t inCustomEthBitmap(void* data, void* bm);
void buildCustomEthBitmapFromRange(void* min, void* max, void* bm);
int8_t customCol2Int32Comparator(void* a, void* b);
void updateCustomWatchBitmap(void* data, void* bm);
int8_t inCustomWatchBitmap(void* data, void* bm);
void buildCustomWatchBitmapFromRange(void* min, void* max, void* bm);

void runAllBenchmarks() {
#define numRuns 5
    int32_t times[numRuns];
    int32_t numReads[numRuns];
    int32_t numIdxReads[numRuns];
    int32_t numRecords[numRuns];

    /////////////////
    // UWA Dataset //
    /////////////////
    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB
        embedDBState* stateUWA = (embedDBState*)calloc(1, sizeof(embedDBState));
        stateUWA->keySize = 4;
        stateUWA->dataSize = 12;
        stateUWA->compareKey = int32Comparator;
        stateUWA->compareData = int32Comparator;
        stateUWA->pageSize = 512;
        stateUWA->eraseSizeInPages = 4;
        stateUWA->numDataPages = 20000;
        stateUWA->numIndexPages = 1000;
        stateUWA->numSplinePoints = 300;
#if STORAGE_TYPE == 0
        stateUWA->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateUWA->dataFile = setupSDFile(dataPath);
        stateUWA->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        stateUWA->fileInterface = getDataflashInterface();
        stateUWA->dataFile = setupDataflashFile(0, 20000);
        stateUWA->indexFile = setupDataflashFile(21000, 100);
#endif
        stateUWA->bufferSizeInBlocks = 4;
        stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
        stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateUWA->bitmapSize = 2;
        stateUWA->inBitmap = inBitmapInt16;
        stateUWA->updateBitmap = updateBitmapInt16;
        stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
        embedDBInit(stateUWA, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateUWA, "data/uwa500K_only_100K.bin");

        // Start benchmark
        numReads[run] = stateUWA->numReads;
        numIdxReads[run] = stateUWA->numIdxReads;
        times[run] = millis();

        /* Aggregate Query: min, max, and avg temperature for each day */
        embedDBIterator it;
        it.minKey = NULL;
        it.maxKey = NULL;
        it.minData = NULL;
        it.maxData = NULL;
        embedDBInitIterator(stateUWA, &it);

        embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
        embedDBAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
        embedDBAggregateFunc* minTemp = createMinAggregate(1, -4);
        embedDBAggregateFunc* maxTemp = createMaxAggregate(1, -4);
        embedDBAggregateFunc* avgTemp = createAvgAggregate(1, 4);
        embedDBAggregateFunc aggFunctions[] = {groupName, *minTemp, *maxTemp, *avgTemp};
        uint32_t functionsLength = 4;
        embedDBOperator* aggOp = createAggregateOperator(scanOp, sameDayGroup, aggFunctions, functionsLength);
        aggOp->init(aggOp);

        int32_t recordsReturned = 0;
        int32_t printLimit = 10;
        int32_t* recordBuffer = (int32_t*)aggOp->recordBuffer;
        // printf("\nAggregate Query: min, max, and avg temperature for each day\n");
        // printf("Day   | Min Temp | Max Temp | Avg Temp\n");
        // printf("------+----------+----------+----------\n");
        while (exec(aggOp)) {
            recordsReturned++;
            // if (recordsReturned < printLimit) {
            //     printf("%5lu | %8.1f | %8.1f | %8.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0, *((float*)recordBuffer + 3) / 10.0);
            // }
        }
        // if (recordsReturned > printLimit) {
        //     printf("...\n");
        //     printf("[Total records returned: %d]\n", recordsReturned);
        // }

        // End benchmark
        times[run] = millis() - times[run];
        numReads[run] = stateUWA->numReads - numReads[run];
        numIdxReads[run] = stateUWA->numIdxReads - numIdxReads[run];
        numRecords[run] = recordsReturned;

        // Free states
        for (int i = 0; i < functionsLength; i++) {
            if (aggFunctions[i].state != NULL) {
                free(aggFunctions[i].state);
            }
        }

        aggOp->close(aggOp);
        embedDBFreeOperatorRecursive(&aggOp);

        // Close embedDB
        embedDBClose(stateUWA);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateUWA->dataFile);
        tearDownSDFile(stateUWA->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateUWA->dataFile);
        tearDownDataflashFile(stateUWA->indexFile);
#endif
        free(stateUWA->fileInterface);
        free(stateUWA->buffer);
        free(stateUWA);
    }

    // Print results
    int sum = 0;
    printf("\nAggregate Query: min, max, and avg temperature for each day\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num records returned: %d\n", numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);

    /////////////////
    // UWA Dataset //
    /////////////////
    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB
        embedDBState* stateUWA = (embedDBState*)calloc(1, sizeof(embedDBState));
        stateUWA->keySize = 4;
        stateUWA->dataSize = 12;
        stateUWA->compareKey = int32Comparator;
        stateUWA->compareData = int32Comparator;
        stateUWA->pageSize = 512;
        stateUWA->eraseSizeInPages = 4;
        stateUWA->numDataPages = 20000;
        stateUWA->numIndexPages = 1000;
        stateUWA->numSplinePoints = 300;
#if STORAGE_TYPE == 0
        stateUWA->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateUWA->dataFile = setupSDFile(dataPath);
        stateUWA->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        stateUWA->fileInterface = getDataflashInterface();
        stateUWA->dataFile = setupDataflashFile(0, 20000);
        stateUWA->indexFile = setupDataflashFile(21000, 100);
#endif
        stateUWA->bufferSizeInBlocks = 4;
        stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
        stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateUWA->bitmapSize = 2;
        stateUWA->inBitmap = inBitmapInt16;
        stateUWA->updateBitmap = updateBitmapInt16;
        stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
        embedDBInit(stateUWA, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateUWA, "data/uwa500K_only_100K.bin");

        // Start benchmark
        numReads[run] = stateUWA->numReads;
        numIdxReads[run] = stateUWA->numIdxReads;
        times[run] = millis();

        /* Aggregate Query: Average temperature on days where the max wind speed was above 15 */
        /* SELECT floor(time / 86400), AVG(airTemp) / 10.0, MAX(windSpeed) / 10.0
         * FROM uwa GROUP BY floor(time / 86400)
         * HAVING MAX(windSpeed) > 150 */
        embedDBIterator it;
        it.minKey = NULL;
        it.maxKey = NULL;
        it.minData = NULL;
        it.maxData = NULL;
        embedDBInitIterator(stateUWA, &it);

        embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
        embedDBAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
        embedDBAggregateFunc* avgTemp = createAvgAggregate(1, 4);
        embedDBAggregateFunc* maxWind = createMaxAggregate(3, -4);
        embedDBAggregateFunc aggFunctions[] = {groupName, *avgTemp, *maxWind};
        uint32_t functionsLength = 3;
        embedDBOperator* aggOp = createAggregateOperator(scanOp, sameDayGroup, aggFunctions, functionsLength);
        int32_t selVal = 150;
        embedDBOperator* selectOp = createSelectionOperator(aggOp, 2, SELECT_GT, &selVal);
        selectOp->init(selectOp);

        int32_t recordsReturned = 0;
        int32_t printLimit = 10;
        int32_t* recordBuffer = (int32_t*)selectOp->recordBuffer;
        // printf("\nAggregate Query: Average temperature on days where the max wind speed was above 15\n");
        // printf("Day   | Avg Temp | Max Wind\n");
        // printf("------+----------+----------\n");
        while (exec(selectOp)) {
            recordsReturned++;
            // if (recordsReturned < printLimit) {
            //     printf("%5lu | %8.1f | %8.1f\n", recordBuffer[0], *((float*)recordBuffer + 1) / 10.0, recordBuffer[2] / 10.0);
            // }
        }
        // if (recordsReturned > printLimit) {
        //     printf("...\n");
        //     printf("[Total records returned: %d]\n", recordsReturned);
        // }

        // End benchmark
        times[run] = millis() - times[run];
        numReads[run] = stateUWA->numReads - numReads[run];
        numIdxReads[run] = stateUWA->numIdxReads - numIdxReads[run];
        numRecords[run] = recordsReturned;

        // Free states
        for (int i = 0; i < functionsLength; i++) {
            if (aggFunctions[i].state != NULL) {
                free(aggFunctions[i].state);
            }
        }

        selectOp->close(selectOp);
        embedDBFreeOperatorRecursive(&selectOp);

        // Close embedDB
        embedDBClose(stateUWA);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateUWA->dataFile);
        tearDownSDFile(stateUWA->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateUWA->dataFile);
        tearDownDataflashFile(stateUWA->indexFile);
#endif
        free(stateUWA->fileInterface);
        free(stateUWA->buffer);
        free(stateUWA);
    }

    // Print results
    sum = 0;
    printf("\nAggregate Query: Average temperature on days where the max wind speed was above 15\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num records returned: %d\n", numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);

    //////////////////////
    // Ethylene Dataset //
    //////////////////////
    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB
        embedDBState* stateEth = (embedDBState*)malloc(sizeof(embedDBState));
        stateEth->keySize = 4;
        stateEth->dataSize = 12;
        stateEth->compareKey = int32Comparator;
        stateEth->compareData = customCol2Int32Comparator;
        stateEth->pageSize = 512;
        stateEth->eraseSizeInPages = 4;
        stateEth->numSplinePoints = 300;
        stateEth->numDataPages = 20000;
        stateEth->numIndexPages = 100;
#if STORAGE_TYPE == 0
        stateEth->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateEth->dataFile = setupSDFile(dataPath);
        stateEth->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        stateEth->fileInterface = getDataflashInterface();
        stateEth->dataFile = setupDataflashFile(0, 20000);
        stateEth->indexFile = setupDataflashFile(21000, 100);
#endif
        stateEth->bufferSizeInBlocks = 4;
        stateEth->buffer = calloc(stateEth->bufferSizeInBlocks, stateEth->pageSize);
        stateEth->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateEth->bitmapSize = 1;
        stateEth->inBitmap = inCustomEthBitmap;
        stateEth->updateBitmap = updateCustomEthBitmap;
        stateEth->buildBitmapFromRange = buildCustomEthBitmapFromRange;
        embedDBInit(stateEth, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateEth, "data/ethylene_CO_only_100K.bin");

        // Start benchmark
        numReads[run] = stateEth->numReads;
        numIdxReads[run] = stateEth->numIdxReads;
        times[run] = millis();

        /* Aggregate Query: Percent of records with ethylene concentration > 0 */
        /* SELECT COUNT(*) / 1000.0 FROM eth WHERE ethConc > 0 */
        embedDBIterator it;
        int32_t minData = 0;
        it.minKey = NULL;
        it.maxKey = NULL;
        it.minData = &minData;
        it.maxData = NULL;
        embedDBInitIterator(stateEth, &it);

        embedDBOperator* scanOp = createTableScanOperator(stateEth, &it, baseSchema);
        int32_t selVal = 0;
        embedDBOperator* selectOp = createSelectionOperator(scanOp, 2, SELECT_GT, &selVal);
        embedDBAggregateFunc* countFunc = createCountAggregate();
        embedDBAggregateFunc aggFunctions[] = {*countFunc};
        uint32_t functionsLength = 1;
        embedDBOperator* aggOp = createAggregateOperator(selectOp, oneGroup, aggFunctions, functionsLength);
        aggOp->init(aggOp);

        int32_t recordsReturned = 0;
        int32_t* recordBuffer = (int32_t*)aggOp->recordBuffer;
        uint32_t result = 0;
        while (exec(aggOp)) {
            recordsReturned++;
            result = recordBuffer[0];
            // printf("Result: %2.1f%%\n", recordBuffer[0] / 1000.0);
        }

        // End benchmark
        times[run] = millis() - times[run];
        numReads[run] = stateEth->numReads - numReads[run];
        numIdxReads[run] = stateEth->numIdxReads - numIdxReads[run];
        if (recordsReturned == 1) {
            numRecords[run] = result;
        } else {
            numRecords[run] = 0;
        }

        // Free states
        for (int i = 0; i < functionsLength; i++) {
            if (aggFunctions[i].state != NULL) {
                free(aggFunctions[i].state);
            }
        }

        aggOp->close(aggOp);
        embedDBFreeOperatorRecursive(&aggOp);

        // Close embedDB
        embedDBClose(stateEth);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateEth->dataFile);
        tearDownSDFile(stateEth->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateEth->dataFile);
        tearDownDataflashFile(stateEth->indexFile);
#endif
        free(stateEth->fileInterface);
        free(stateEth->buffer);
        free(stateEth);
    }

    // Print results
    sum = 0;
    printf("\nAggregate Query: Percent of records with ethylene concentration > 0\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Percent records returned: %2.1f%% (%d/100000)\n", numRecords[0] / 1000.0, numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);

    ///////////////////////
    // UWA & SEA Dataset //
    ///////////////////////
    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB for UWA
        embedDBState* stateUWA = (embedDBState*)calloc(1, sizeof(embedDBState));
        stateUWA->keySize = 4;
        stateUWA->dataSize = 12;
        stateUWA->compareKey = int32Comparator;
        stateUWA->compareData = int32Comparator;
        stateUWA->pageSize = 512;
        stateUWA->eraseSizeInPages = 4;
        stateUWA->numDataPages = 20000;
        stateUWA->numIndexPages = 1000;
        stateUWA->numSplinePoints = 300;
#if STORAGE_TYPE == 0
        stateUWA->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateUWA->dataFile = setupSDFile(dataPath);
        stateUWA->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        stateUWA->fileInterface = getDataflashInterface();
        stateUWA->dataFile = setupDataflashFile(0, 20000);
        stateUWA->indexFile = setupDataflashFile(21000, 100);
#endif
        stateUWA->bufferSizeInBlocks = 4;
        stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
        stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateUWA->bitmapSize = 2;
        stateUWA->inBitmap = inBitmapInt16;
        stateUWA->updateBitmap = updateBitmapInt16;
        stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
        embedDBInit(stateUWA, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateUWA, "data/uwa500K.bin");

        // Setup embedDB for SEA
        embedDBState* stateSEA = (embedDBState*)malloc(sizeof(embedDBState));
        stateSEA->keySize = 4;
        stateSEA->dataSize = 12;
        stateSEA->compareKey = int32Comparator;
        stateSEA->compareData = int32Comparator;
        stateSEA->pageSize = 512;
        stateSEA->eraseSizeInPages = 4;
        stateSEA->numDataPages = 20000;
        stateSEA->numIndexPages = 1000;
        stateSEA->numSplinePoints = 300;
#if STORAGE_TYPE == 0
        stateSEA->fileInterface = getSDInterface();
        char dataPath2[] = "dataFile2.bin", indexPath2[] = "indexFile2.bin";
        stateSEA->dataFile = setupSDFile(dataPath2);
        stateSEA->indexFile = setupSDFile(indexPath2);
#endif
#if STORAGE_TYPE == 1
        stateSEA->fileInterface = getDataflashInterface();
        stateSEA->dataFile = setupDataflashFile(0, 20000);
        UWA
            stateSEA->indexFile = setupDataflashFile(21000, 100);
#endif
        stateSEA->bufferSizeInBlocks = 4;
        stateSEA->buffer = malloc(stateSEA->bufferSizeInBlocks * stateSEA->pageSize);
        stateSEA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateSEA->bitmapSize = 2;
        stateSEA->inBitmap = inBitmapInt16;
        stateSEA->updateBitmap = updateBitmapInt16;
        stateSEA->buildBitmapFromRange = buildBitmapInt16FromRange;
        embedDBInit(stateSEA, 1);

        insertData(stateSEA, "data/sea100K.bin");

        // Start benchmark
        numReads[run] = stateUWA->numReads + stateSEA->numReads;
        numIdxReads[run] = stateUWA->numIdxReads + stateSEA->numIdxReads;
        times[run] = millis();

        /* Join Query: Join SEA and UWA dataset to compare temperatures on the same day of the year */
        embedDBIterator itUWA;
        itUWA.minKey = NULL;
        itUWA.maxKey = NULL;
        itUWA.minData = NULL;
        itUWA.maxData = NULL;
        embedDBInitIterator(stateUWA, &itUWA);  // Iterator on uwa
        embedDBIterator itSEA;
        uint32_t year2015 = 1420099200;      // 2015-01-01
        uint32_t year2016 = 1451635200 - 1;  // 2016-01-01
        itSEA.minKey = &year2015;
        itSEA.maxKey = &year2016;
        itSEA.minData = NULL;
        itSEA.maxData = NULL;
        embedDBInitIterator(stateSEA, &itSEA);  // Iterator on sea to select only 2015

        // Prepare uwa table
        embedDBOperator* scanUWA = createTableScanOperator(stateUWA, &itUWA, baseSchema);
        embedDBOperator* shiftUWA = (embedDBOperator*)malloc(sizeof(embedDBOperator));  // Custom op to shift the year 2000 to 2015 to make the join work
        shiftUWA->input = scanUWA;
        shiftUWA->init = customShiftInit;
        shiftUWA->next = customShiftNext;
        shiftUWA->close = customShiftClose;

        // Prepare sea table
        embedDBOperator* scanSEA = createTableScanOperator(stateSEA, &itSEA, baseSchema);
        scanSEA->init(scanSEA);

        // Join tables
        embedDBOperator* joinOp = createKeyJoinOperator(shiftUWA, scanSEA);

        // Project the timestamp and the two temp columns
        uint8_t projCols[] = {0, 1, 5};
        embedDBOperator* projection = createProjectionOperator(joinOp, 3, projCols);

        projection->init(projection);

        int32_t recordsReturned = 0;
        int32_t printLimit = 10;
        int32_t* recordBuffer = (int32_t*)projection->recordBuffer;
        // printf("\nJoin Query: Join SEA and UWA dataset to compare temperatures on the same day of the year\n");
        // printf("Timestamp  | Temp SEA | Temp UWA\n");
        // printf("-----------+----------+----------\n");
        while (exec(projection)) {
            recordsReturned++;
            // if (recordsReturned < printLimit) {
            //     printf("%10lu | %8.1f | %8.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
            // }
        }
        // if (recordsReturned > printLimit) {
        //     printf("...\n");
        //     printf("[Total records returned: %d]\n", recordsReturned);
        // }

        projection->close(projection);
        free(scanUWA);
        free(shiftUWA);
        free(scanSEA);
        free(joinOp);
        free(projection);

        // End benchmark
        times[run] = millis() - times[run];
        numReads[run] = stateUWA->numReads + stateSEA->numReads - numReads[run];
        numIdxReads[run] = stateUWA->numIdxReads + stateSEA->numIdxReads - numIdxReads[run];
        numRecords[run] = recordsReturned;

        // Close embedDB
        embedDBClose(stateUWA);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateUWA->dataFile);
        tearDownSDFile(stateUWA->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateUWA->dataFile);
        tearDownDataflashFile(stateUWA->indexFile);
#endif
        free(stateUWA->fileInterface);
        free(stateUWA->buffer);
        free(stateUWA);
        embedDBClose(stateSEA);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateSEA->dataFile);
        tearDownSDFile(stateSEA->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateSEA->dataFile);
        tearDownDataflashFile(stateSEA->indexFile);
#endif
        free(stateSEA->fileInterface);
        free(stateSEA->buffer);
        free(stateSEA);
        embedDBFreeSchema(&baseSchema);
    }

    // Print results
    sum = 0;
    printf("\nJoin Query: Join SEA and UWA dataset to compare temperatures on the same day of the year\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num records returned: %d\n", numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);

    ///////////////////
    // Watch Dataset //
    ///////////////////
    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB
        embedDBState* stateWatch = (embedDBState*)calloc(1, sizeof(embedDBState));
        stateWatch->keySize = 4;
        stateWatch->dataSize = 12;
        stateWatch->compareKey = int32Comparator;
        stateWatch->compareData = int32Comparator;
        stateWatch->pageSize = 512;
        stateWatch->eraseSizeInPages = 4;
        stateWatch->numDataPages = 20000;
        stateWatch->numIndexPages = 1000;
        stateWatch->numSplinePoints = 300;
#if STORAGE_TYPE == 0
        stateWatch->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateWatch->dataFile = setupSDFile(dataPath);
        stateWatch->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        stateWatch->fileInterface = getDataflashInterface();
        stateWatch->dataFile = setupDataflashFile(0, 20000);
        stateWatch->indexFile = setupDataflashFile(21000, 100);
#endif
        stateWatch->bufferSizeInBlocks = 4;
        stateWatch->buffer = malloc(stateWatch->bufferSizeInBlocks * stateWatch->pageSize);
        stateWatch->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateWatch->bitmapSize = 2;
        stateWatch->inBitmap = inCustomWatchBitmap;
        stateWatch->updateBitmap = updateCustomWatchBitmap;
        stateWatch->buildBitmapFromRange = buildCustomWatchBitmapFromRange;
        embedDBInit(stateWatch, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateWatch, "data/watch_only_100K.bin");

        // Start benchmark
        numReads[run] = stateWatch->numReads;
        numIdxReads[run] = stateWatch->numIdxReads;
        times[run] = millis();

        /* Aggregate Query: Count the number of records with a magnitude of motion above 5e8 on the X axis for each of 10 time windows */
        embedDBIterator it;
        int32_t minData = (int32_t)5e8;
        it.minKey = NULL;
        it.maxKey = NULL;
        it.minData = &minData;
        it.maxData = NULL;
        embedDBInitIterator(stateWatch, &it);

        embedDBOperator* scanOp = createTableScanOperator(stateWatch, &it, baseSchema);
        embedDBAggregateFunc groupName = {NULL, NULL, writeWatchGroup, NULL, 4};
        embedDBAggregateFunc* countFunc = createCountAggregate();
        embedDBAggregateFunc aggFunctions[] = {groupName, *countFunc};
        uint32_t functionsLength = 2;
        embedDBOperator* aggOp = createAggregateOperator(scanOp, sameWatchGroup, aggFunctions, functionsLength);
        aggOp->init(aggOp);

        int32_t recordsReturned = 0;
        int32_t printLimit = 10;
        int32_t* recordBuffer = (int32_t*)aggOp->recordBuffer;
        // printf("\nAggregate Query: Count the number of records with a magnitude of motion above 5e8 on the X axis for each of 10 time windows\n");
        // printf("Bucket | Count\n");
        // printf("-------+-------\n");
        while (exec(aggOp)) {
            recordsReturned += recordBuffer[1];
            // printf("%6d | %5d\n", recordBuffer[0], recordBuffer[1]);
        }

        // End benchmark
        times[run] = millis() - times[run];
        numReads[run] = stateWatch->numReads - numReads[run];
        numIdxReads[run] = stateWatch->numIdxReads - numIdxReads[run];
        numRecords[run] = recordsReturned;

        // Free states
        for (int i = 0; i < functionsLength; i++) {
            if (aggFunctions[i].state != NULL) {
                free(aggFunctions[i].state);
            }
        }

        aggOp->close(aggOp);
        embedDBFreeOperatorRecursive(&aggOp);

        // Close embedDB
        embedDBClose(stateWatch);
#if STORAGE_TYPE == 0
        tearDownSDFile(stateWatch->dataFile);
        tearDownSDFile(stateWatch->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(stateWatch->dataFile);
        tearDownDataflashFile(stateWatch->indexFile);
#endif
        free(stateWatch->fileInterface);
        free(stateWatch->buffer);
        free(stateWatch);
    }

    // Print results
    sum = 0;
    printf("\nAggregate Query: Count the number of records with a magnitude of motion above 5e8 on the X axis for each of 10 time windows\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num records returned: %d\n", numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);
}

void insertData(embedDBState* state, char* filename) {
    SD_FILE* fp = fopen(filename, "rb");
    char fileBuffer[512];
    int numRecords = 0;
    while (fread(fileBuffer, state->pageSize, 1, fp)) {
        uint16_t count = EMBEDDB_GET_COUNT(fileBuffer);
        for (int i = 1; i <= count; i++) {
            embedDBPut(state, fileBuffer + i * state->recordSize, fileBuffer + i * state->recordSize + state->keySize);
            numRecords++;
        }
    }
    fclose(fp);
    embedDBFlush(state);
}

void updateCustomEthBitmap(void* data, void* bm) {
    int32_t temp = *(int32_t*)((int8_t*)data + 4);

    uint16_t mask = 1;

    if (temp < 0) {
        mask <<= 0;
    } else if (temp == 0) {
        mask <<= 1;
    } else {
        mask <<= 2;
    }

    *(uint8_t*)bm |= mask;
}

int8_t inCustomEthBitmap(void* data, void* bm) {
    uint8_t* bmval = (uint8_t*)bm;

    uint8_t tmpbm = 0;
    updateCustomEthBitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomEthBitmapFromRange(void* min, void* max, void* bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t*)bm = 255; /* Everything */
        return;
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomEthBitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint8_t*)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomEthBitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint8_t*)bm = maxMap;
                return;
            }
        }
        *(uint8_t*)bm = minMap & maxMap;
    }
}

void updateCustomWatchBitmap(void* data, void* bm) {
    int32_t temp = *(int32_t*)(data);
    float norm = abs(temp) / 1e9;

    uint16_t mask = 1;

    // Divide the 0-1 range in 16 buckets
    if (norm < 0.0625) {
        mask <<= 0;
    } else if (norm < 0.125) {
        mask <<= 1;
    } else if (norm < 0.1875) {
        mask <<= 2;
    } else if (norm < 0.25) {
        mask <<= 3;
    } else if (norm < 0.3125) {
        mask <<= 4;
    } else if (norm < 0.375) {
        mask <<= 5;
    } else if (norm < 0.4375) {
        mask <<= 6;
    } else if (norm < 0.5) {
        mask <<= 7;
    } else if (norm < 0.5625) {
        mask <<= 8;
    } else if (norm < 0.625) {
        mask <<= 9;
    } else if (norm < 0.6875) {
        mask <<= 10;
    } else if (norm < 0.75) {
        mask <<= 11;
    } else if (norm < 0.8125) {
        mask <<= 12;
    } else if (norm < 0.875) {
        mask <<= 13;
    } else if (norm < 0.9375) {
        mask <<= 14;
    } else {
        mask <<= 15;
    }

    *(uint16_t*)bm |= mask;
}

int8_t inCustomWatchBitmap(void* data, void* bm) {
    uint16_t* bmval = (uint16_t*)bm;

    uint16_t tmpbm = 0;
    updateCustomWatchBitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomWatchBitmapFromRange(void* min, void* max, void* bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t*)bm = 255; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomWatchBitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t*)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomWatchBitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint16_t*)bm = maxMap;
                return;
            }
        }
        *(uint16_t*)bm = minMap & maxMap;
    }
}

int8_t customCol2Int32Comparator(void* a, void* b) {
    int32_t i1, i2;
    memcpy(&i1, (int8_t*)a + 4, sizeof(int32_t));
    memcpy(&i2, (int8_t*)b + 4, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}
