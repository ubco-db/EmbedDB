#include <errno.h>
#include <stdint.h>
#include <stdio.h>
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

#include "Arduino.h"

#if defined(MEMBOARD) && STORAGE_TYPE == 1

#include "dataflashFileInterface.h"

#endif

#include "SDFileInterface.h"
#include "query-interface/activeRules.h"
#define FILE_TYPE SD_FILE
#define fopen sd_fopen
#define fread sd_fread
#define fclose sd_fclose
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile

#define DATA_PATH "dataFile.bin"
#define INDEX_PATH "indexFile.bin"

#else
#define FILE_TYPE FILE
#include "desktopFileInterface.h"
#include "query-interface/activeRules.h"
#define DATA_PATH "build/artifacts/dataFile.bin"
#define INDEX_PATH "build/artifacts/indexFile.bin"

#endif

#define NUM_INSERTIONS 1000

embedDBState* init_state();
embedDBSchema* createSchema();
void GTcallback(void* aggregateValue, void* currentValue, void* context);

int callbacks = 0;

// Get current time in nanoseconds
uint64_t get_nanoseconds() {
#ifdef ARDUINO
    return (uint64_t)micros() * 1000ULL;
#else
#if defined(TIME_UTC)
    struct timespec ts;
    if (timespec_get(&ts, TIME_UTC) == TIME_UTC) {
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    }
#endif
    return (uint64_t)((double)clock() * (1000000000.0 / CLOCKS_PER_SEC));
#endif
}

// Callback function for active rule
void GTcallback(void* aggregateValue, void* currentValue, void* context) {
    // FILE_TYPE* perfLog = (FILE_TYPE*)context;
    uint64_t callbackTime = get_nanoseconds();
    // fprintf(perfLog, "%llu,CALLBACK,%f\n", (unsigned long long)callbackTime, *(float*)aggregateValue);
    // printf("%llu,CALLBACK,%f\n", (unsigned long long)callbackTime, *(float*)aggregateValue);
    callbacks++;
}

int8_t groupFunctionLocal(const void* lastRecord, const void* record) {
    return 1;
}

embedDBOperator* createOperatorLocal(embedDBState* state, embedDBSchema* schema, void*** allocatedValues, uint32_t key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    uint32_t numRecords = 1;
    uint32_t minKeyVal = key - (numRecords - 1);
    uint32_t* minKeyPtr = (uint32_t*)malloc(sizeof(uint32_t));
    *minKeyPtr = minKeyVal;
    it->minKey = minKeyPtr;

    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    embedDBOperator* scanOp = createTableScanOperator(state, it, schema);

    embedDBAggregateFunc* aggFunc = NULL;
    aggFunc = createAvgAggregate(1, 4);

    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *aggFunc;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunctionLocal, aggFuncs, 1);
    aggOp->init(aggOp);

    free(aggFunc);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;
    ((void**)*allocatedValues)[2] = minKeyPtr;

    return aggOp;
}

void GetAvgLocal(embedDBState* state, embedDBSchema* schema, uint32_t key, float currentVal, void* context) {
    void** allocatedValues;
    embedDBOperator* op = createOperatorLocal(state, schema, &allocatedValues, key);

    void* recordBuffer = op->recordBuffer;
    float* C1 = (float*)((int8_t*)recordBuffer + 0);
    // Print as csv
    exec(op);
    float avg = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 3; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    if (avg < 0) {
        GTcallback(&avg, &currentVal, context);
    }
}

uint32_t activeRulesBenchmark() {
    printf("Active Rules Benchmark\n");
    embedDBState* state = init_state();
    embedDBPrintInit(state);
    embedDBSchema* schema = createSchema();

    // Create active rule
    activeRule* activeRuleGT = createActiveRule(schema, NULL);
    uint32_t numRecords = 10;
    float minVal = 0.0f;
    activeRuleGT->IF(activeRuleGT, 1, GET_AVG)
        ->ofLast(activeRuleGT, (void*)&numRecords)
        //    ->is(activeRuleGT, GreaterThan, (void*)&minVal)
        ->is(activeRuleGT, LessThan, (void*)&minVal)
        ->then(activeRuleGT, GTcallback);

    printf("Window size: %u\n", numRecords);
    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = activeRuleGT;
    state->numRules = 1;
    state->rules[0]->enabled = false;  // Disable the rule for initial insertions
    srand(12345);                      // Fixed seed for reproducibility

    // Open performance log file
    // FILE_TYPE perfLog = fopen("C:/tmp/EmbedDB-Vs-InfluxDB/embeddb_perf_new.csv", "w");
    // FILE* advancedPerfLog = fopen("C:/Users/richa/OneDrive/Documents/influxdb/embeddb_advanced_perf.csv", "w");

    // fprintf(advancedPerfLog, "timestamp,event,temperature,latency\n");
    // fprintf(perfLog, "timestamp,event,temperature,latency\n");
    printf("timestamp,event,temperature,latency\n");

    // Set callback context to the log file
    // state->rules[0]->context = perfLog;

    uint32_t j = 0;
    // Insert without active query first
    printf("Initial inserts\n");
    void* dataPtr = malloc(state->dataSize);
    for (int i = 0; i < 1000; i++) {
        uint64_t timestamp = get_nanoseconds();
        float temperature = -5 + (float)rand() / RAND_MAX * 10;  // Random temperature between -5°C and 5°C

        uint64_t start = get_nanoseconds();

        *((float*)dataPtr) = temperature;
        int8_t result = embedDBPut(state, &j, dataPtr);
        (void)result;

        uint64_t end = get_nanoseconds();
        uint64_t insertTime = end - start;

        // Log insertion event
        // fprintf(advancedPerfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);
        // fprintf(perfLog, "%llu,INSERT,%f,%llu\n", (unsigned long long)timestamp, temperature, (unsigned long long)insertTime);
        if (i % 100 == 0)
            printf("%llu,INSERT,%f,%llu\n", (unsigned long long)timestamp, temperature, (unsigned long long)insertTime);

        j++;
    }

    printf("Test inserts\n");
    // state->rules[0]->enabled = true;  // Enable the rule for subsequent insertions
    uint64_t startTime = get_nanoseconds();
    for (int i = 0; i < NUM_INSERTIONS; i++) {
        uint64_t timestamp = get_nanoseconds();
        float temperature = -5 + (float)rand() / RAND_MAX * 10;  // Random temperature between -5°C and 5°C

        uint64_t start = get_nanoseconds();
        // void* dataPtr = malloc(state->dataSize);
        *((float*)dataPtr) = temperature;
        // using j instead of timestamp ensures same number of records queried each time independent of changing insert speed

        int8_t result = embedDBPut(state, &j, dataPtr);
        // Uncomment this if want to test performance of advanced query without active rule callback
        GetAvgLocal(state, schema, j, temperature, NULL);

        uint64_t end = get_nanoseconds();
        uint64_t insertTime = end - start;

        // Log insertion event
        // fprintf(advancedPerfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);
        // fprintf(perfLog, "%llu,INSERT,%f,%llu\n", (unsigned long long)timestamp, temperature, (unsigned long long)insertTime);
        if (i % 100000 == 0)
            printf("%llu,INSERT,%f,%llu\n", (unsigned long long)timestamp, temperature, (unsigned long long)insertTime);

        j++;
    }
    free(dataPtr);
    uint64_t endTime = get_nanoseconds();

    // Calculate throughput
    double totalTime = (double)(endTime - startTime) / 1e9;  // Convert to seconds
    double throughput = NUM_INSERTIONS / totalTime;
    printf("Throughput: %f insertions/second  Time: %f Records: %d  Callbacks: %d\n", throughput, totalTime, NUM_INSERTIONS, callbacks);

    // Clean up
    // fclose(perfLog);
    // fclose(advancedPerfLog);
    return 0;
}

embedDBSchema* createSchema() {
    uint8_t numCols = 2;
    int8_t colSizes[] = {4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED};
    ColumnType colTypes[] = {embedDB_COLUMN_UINT32, embedDB_COLUMN_FLOAT};
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

    char dataPath[] = DATA_PATH, indexPath[] = INDEX_PATH;
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    // enable parameters
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;

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
