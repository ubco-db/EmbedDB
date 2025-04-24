#ifndef PIO_UNIT_TESTING

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/advancedQueries.h"

/**
 * 0 = SD Card
 * 1 = Dataflash
 */
#define STORAGE_TYPE 0

#ifdef ARDUINO

#if defined(MEMBOARD) && STORAGE_TYPE == 1
#include "dataflashFileInterface.h"
#endif

#include "SDFileInterface.h"
#define FILE_TYPE SD_FILE
#define fopen sd_fopen
#define fread sd_fread
#define fclose sd_fclose
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile

#define clock millis
#define DATA_FILE_PATH_UWA "dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "indexFileSEA.bin"

#else

#include "desktopFileInterface.h"
#define FILE_TYPE FILE
#define DATA_FILE_PATH_UWA "build/artifacts/dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "build/artifacts/indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "build/artifacts/dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "build/artifacts/indexFileSEA.bin"

#endif

void insertData(embedDBState* state, const char* filename);
void sort_order_last(int32_t numValues, embedDBState* stateUWA, embedDBSchema* baseSchema);
void sort_order_first(int32_t numValues, embedDBState* stateUWA, embedDBSchema* baseSchema);

double time_diff_ms(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) * 1000.0 +
           (end.tv_nsec - start.tv_nsec) / 1000000.0;
}

int sortQueryBenchmark() {
    printf("Sort Query Benchmark.\n");
    embedDBState* stateUWA = (embedDBState*)malloc(sizeof(embedDBState));
    stateUWA->keySize = 4;
    stateUWA->dataSize = 12;
    stateUWA->compareKey = int32Comparator;
    stateUWA->compareData = int32Comparator;
    stateUWA->pageSize = 512;
    stateUWA->eraseSizeInPages = 4;
    stateUWA->numDataPages = 20000;
    stateUWA->numIndexPages = 1000;
    stateUWA->numSplinePoints = 30;

    if (STORAGE_TYPE == 1) {
        printf("Dataflash is not currently supported. Defaulting to SD card interface.");
    }

    /* Setup files */
    char dataPath[] = DATA_FILE_PATH_UWA, indexPath[] = INDEX_FILE_PATH_UWA;
    stateUWA->fileInterface = getFileInterface();
    stateUWA->dataFile = setupFile(dataPath);
    stateUWA->indexFile = setupFile(indexPath);

    stateUWA->bufferSizeInBlocks = 4;
    stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
    stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    stateUWA->bitmapSize = 2;
    stateUWA->inBitmap = inBitmapInt16;
    stateUWA->updateBitmap = updateBitmapInt16;
    stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
    int8_t initResult = embedDBInit(stateUWA, 1);
    if (initResult != 0) {
        printf("There was an error setting up the state of the UWA dataset.");
        return -1;
    }

    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

    // Insert data
    const char datafileName[] = "data/uwa500K.bin";
    insertData(stateUWA, datafileName);

    struct timespec start_time, end_time;
    int32_t num_values[] = {100, 1000, 10000, 100000, 500000};
    
    printf("\nProjection followed by Sort\n");
    // Perform sort runs with different numbers of values
    for (int i = 0; i < 5; i++) {
        printf("%d values:\n", num_values[i]);

        // Repeat each run for consistency
        for (int j = 0; j < 1; j++) {
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            sort_order_last(num_values[i],stateUWA,baseSchema);

            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double elapsed_ms = time_diff_ms(start_time, end_time);
            printf("\tElapsed time: %.3f ms\n", elapsed_ms);
        }
    }

    printf("\nSort followed by Projection\n");
    // Perform sort runs with different numbers of values
    for (int i = 0; i < 5; i++) {
        printf("%d values:\n", num_values[i]);

        // Repeat each run for consistency
        for (int j = 0; j < 1; j++) {
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            sort_order_first(num_values[i],stateUWA,baseSchema);

            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double elapsed_ms = time_diff_ms(start_time, end_time);
            printf("\tElapsed time: %.3f ms\n", elapsed_ms);
        }
    }

    // Close embedDB
    embedDBClose(stateUWA);
    tearDownFile(stateUWA->dataFile);
    tearDownFile(stateUWA->indexFile);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);

    embedDBFreeSchema(&baseSchema);
    return 0;
}

void sort_order_last(int32_t numValues, embedDBState* stateUWA, embedDBSchema* baseSchema) {

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projColsOB[] = {0,1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, projColsOrderBy, 1, numValues, merge_sort_int32_comparator);
    orderByOp->init(orderByOp);
    int32_t* recordBuffer = orderByOp->recordBuffer;
    
    for (uint32_t i = 0; i < 10; i++) {
        if (!exec(orderByOp)) {
            break;
        }
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);
}

void sort_order_first(int32_t numValues, embedDBState* stateUWA, embedDBSchema* baseSchema) {

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(stateUWA, &it, baseSchema);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, scanOpOrderBy, 1, numValues, merge_sort_int32_comparator);
    uint8_t projColsOB[] = {0,1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(orderByOp, 2, projColsOB);
    projColsOrderBy->init(projColsOrderBy);
    int32_t* recordBuffer = projColsOrderBy->recordBuffer;
    
    for (uint32_t i = 0; i < 10; i++) {
        if (!exec(projColsOrderBy)) {
            break;
        }
    }

    projColsOrderBy->close(projColsOrderBy);
    embedDBFreeOperatorRecursive(&projColsOrderBy);
}

void insertData(embedDBState* state, const char* filename) {
    FILE_TYPE* fp = fopen(filename, "rb");
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

    printf("\nInserted %d Records\n", numRecords);
}

#endif
