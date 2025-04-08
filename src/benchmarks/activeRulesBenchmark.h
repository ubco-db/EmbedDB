#include <errno.h>
#include <string.h>
#include <time.h>
#include <windows.h>
#include <stdio.h>
#include <mmsystem.h>  // Required for timeBeginPeriod

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

#define DATA_PATH "dataFile.bin"
#define INDEX_PATH "indexFile.bin"

#else

#include "desktopFileInterface.h"
#include "query-interface/activeRules.h"
#define DATA_PATH "build/artifacts/dataFile.bin"
#define INDEX_PATH "build/artifacts/indexFile.bin"

#endif

#define NUM_INSERTIONS 2000

embedDBState* init_state();
embedDBSchema* createSchema();
void GTcallback(void* aggregateValue, void* currentValue, void* context);

// Get current time in nanoseconds
uint64_t get_nanoseconds() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1e9 + ts.tv_nsec;
}

// Callback function for active rule
void GTcallback(void* aggregateValue, void* currentValue, void* context) {
    FILE* perfLog = (FILE*)context;
    uint64_t callbackTime = get_nanoseconds();
    fprintf(perfLog, "%llu,CALLBACK,%f\n", callbackTime, *(float*)aggregateValue);
}



int8_t groupFunctionLocal(const void* lastRecord, const void* record) {
    return 1;
}

embedDBOperator* createOperatorLocal(embedDBState* state, embedDBSchema* schema, void*** allocatedValues, uint32_t key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    uint32_t minKeyVal = key - (1000 - 1);
    uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
    *minKeyPtr = minKeyVal;
    it->minKey = minKeyPtr;
    
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    embedDBOperator* scanOp = createTableScanOperator(state, it, schema);

    embedDBAggregateFunc* aggFunc = NULL;    
    aggFunc = createAvgAggregate(1, 4);

    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1*sizeof(embedDBAggregateFunc));
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
    if(avg > 0){
        GTcallback(&avg, &currentVal, context);
    }
}

uint32_t activeRulesBenchmark() {
    embedDBState* state = init_state();
    embedDBPrintInit(state);
    embedDBSchema* schema = createSchema();

    // Create active rule
    activeRule *activeRuleGT = createActiveRule(schema, NULL);
    activeRuleGT->IF(activeRuleGT, 1, GET_AVG)
                    ->ofLast(activeRuleGT, (void*)&(uint32_t){1000}) 
                    ->is(activeRuleGT, GreaterThan, (void*)&(float){0})
                    ->then(activeRuleGT, GTcallback);

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = activeRuleGT;
    state->numRules = 1;
    state->rules[0]->enabled = false; // Disable the rule for initial insertions
    srand(12345);  // Fixed seed for reproducibility

    // Open performance log file
    FILE* perfLog = fopen("C:/Users/richa/OneDrive/Documents/influxdb/embeddb_perf_new.csv", "w");
    //FILE* advancedPerfLog = fopen("C:/Users/richa/OneDrive/Documents/influxdb/embeddb_advanced_perf.csv", "w");

    //fprintf(advancedPerfLog, "timestamp,event,temperature,latency\n");
    fprintf(perfLog, "timestamp,event,temperature,latency\n");

    // Set callback context to the log file
    state->rules[0]->context = perfLog;
    timeBeginPeriod(1);

    uint32_t j = 0;
    // Insert without active query first
    for (int i = 0; i < NUM_INSERTIONS; i++) {
        uint64_t timestamp = get_nanoseconds();
        float temperature = 15 + (float)rand() / RAND_MAX * 15;  // Random temperature between 15°C and 30°C

        LARGE_INTEGER start, end, freq;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);

        void* dataPtr = malloc(state->dataSize);
        *((float*)dataPtr) = temperature;
        int8_t result = embedDBPut(state, &j, dataPtr);
        
        QueryPerformanceCounter(&end);
        int insertTime = (double)(end.QuadPart - start.QuadPart) / freq.QuadPart * 1e9;  // Convert to nanoseconds

        // Log insertion event
        //fprintf(advancedPerfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);
        fprintf(perfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);

        free(dataPtr);
        j++;
    }


    state->rules[0]->enabled = true; // Enable the rule for subsequent insertions
    uint64_t startTime = get_nanoseconds();
    for (int i = 0; i < NUM_INSERTIONS; i++) {
        uint64_t timestamp = get_nanoseconds();
        float temperature = 15 + (float)rand() / RAND_MAX * 15;  // Random temperature between 15°C and 30°C

        LARGE_INTEGER start, end, freq;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);

        void* dataPtr = malloc(state->dataSize);
        *((float*)dataPtr) = temperature;
        //using j instead of timestamp ensures same number of records queried each time independent of changing insert speed

        int8_t result = embedDBPut(state, &j, dataPtr); 
        
        // QueryPerformanceFrequency(&freq);
        // QueryPerformanceCounter(&start);
        // GetAvgLocal(state, schema, j, temperature, advancedPerfLog);

        
        QueryPerformanceCounter(&end);
        int insertTime = (double)(end.QuadPart - start.QuadPart) / freq.QuadPart * 1e9;  // Convert to nanoseconds

        // Log insertion event
        //fprintf(advancedPerfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);
        fprintf(perfLog, "%llu,INSERT,%f,%i\n", timestamp, temperature, insertTime);

        free(dataPtr);
        j++;
    }
    timeEndPeriod(1);
    uint64_t endTime = get_nanoseconds();

    // Calculate throughput
    double totalTime = (double)(endTime - startTime) / 1e9;  // Convert to seconds
    double throughput = NUM_INSERTIONS / totalTime;
    printf("Throughput: %f insertions/second\n", throughput);

    // Clean up
    fclose(perfLog);
    //fclose(advancedPerfLog);
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
