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
#include "query-interface/streamingQueries.h"
#define DATA_PATH "build/artifacts/dataFile.bin"
#define INDEX_PATH "build/artifacts/indexFile.bin"

#endif

#define NUM_INSERTIONS 10000 // Run for 10 seconds
#define INTERVAL 1 //Insert every 1 ms

embedDBState* init_state();
embedDBSchema* createSchema();
void GTcallback(void* aggregateValue, void* currentValue, void* context);

// Get current time in nanoseconds
uint64_t get_nanoseconds() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1e9 + ts.tv_nsec;
}

int32_t randomInt(int min, int max) {
    int randomIntInRange = (rand() % (max - min + 1)) + min;
    return randomIntInRange;
}

// Callback function for streaming query
void GTcallback(void* aggregateValue, void* currentValue, void* context) {
    FILE* perfLog = (FILE*)context;
    uint64_t callbackTime = get_nanoseconds();
    fprintf(perfLog, "%llu,CALLBACK,%f\n", callbackTime, *(float*)aggregateValue);
}

uint32_t streamingQueryBenchmark() {
    embedDBState* state = init_state();
    embedDBPrintInit(state);
    embedDBSchema* schema = createSchema();

    // Create streaming query
    StreamingQuery *streamingQueryGT = createStreamingQuery(state, schema, NULL);
    streamingQueryGT->IF(streamingQueryGT, 1, GET_AVG)
                    ->ofLast(streamingQueryGT, (void*)&(uint32_t){1000})  // 1-second window
                    ->is(streamingQueryGT, GreaterThan, (void*)&(float){25.07})
                    ->then(streamingQueryGT, GTcallback);

    StreamingQuery **queries = (StreamingQuery**)malloc(sizeof(StreamingQuery*));
    queries[0] = streamingQueryGT;
    srand(12345);  // Fixed seed for reproducibility

    // Open performance log file
    FILE* perfLog = fopen("C:/Users/richa/OneDrive/Documents/influxdb/embeddb_perf.csv", "w");
    fprintf(perfLog, "timestamp,event,temperature,insert_time\n");

    // Set callback context to the log file
    streamingQueryGT->context = perfLog;
    timeBeginPeriod(1);

    uint32_t j = 0;
    // Insert without active query first
    for (int i = 0; i < NUM_INSERTIONS; i++) {
        uint64_t timestamp = get_nanoseconds();
        int32_t temperature = randomInt(15, 35);  // Random temperature between 15°C and 30°C

        LARGE_INTEGER start, end, freq;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);

        void* dataPtr = malloc(state->dataSize);
        *((int32_t*)dataPtr) = temperature;
        int8_t result = embedDBPut(state, &j, dataPtr);
        
        QueryPerformanceCounter(&end);
        int insertTime = (double)(end.QuadPart - start.QuadPart) / freq.QuadPart * 1e9;  // Convert to nanoseconds

        // Log insertion event
        //fprintf(perfLog, "%llu,INSERT,%i,%i\n", timestamp, temperature, insertTime);
        free(dataPtr);
        //Sleep(INTERVAL);  // Insert every 1 ms
        j++;
    }


    uint64_t startTime = get_nanoseconds();
    for (int i = 0; i < NUM_INSERTIONS/5; i++) {
        uint64_t timestamp = get_nanoseconds();
        int32_t temperature = randomInt(15, 35);  // Random temperature between 15°C and 30°C

        LARGE_INTEGER start, end, freq;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&start);

        void* dataPtr = malloc(state->dataSize);
        *((int32_t*)dataPtr) = temperature;
        int8_t result = streamingQueryPut(queries, 1, &j, dataPtr);
        
        QueryPerformanceCounter(&end);
        int insertTime = (double)(end.QuadPart - start.QuadPart) / freq.QuadPart * 1e9;  // Convert to nanoseconds

        // Log insertion event
        fprintf(perfLog, "%llu,INSERT,%i,%i\n", timestamp, temperature, insertTime);
        free(dataPtr);
        j++;
        //Sleep(INTERVAL);  // Insert every 1 ms
    }
    timeEndPeriod(1);
    uint64_t endTime = get_nanoseconds();

    // Calculate throughput
    double totalTime = (double)(endTime - startTime) / 1e9;  // Convert to seconds
    double throughput = (NUM_INSERTIONS/5) / totalTime;
    printf("Throughput: %f insertions/second\n", throughput);

    // Clean up
    fclose(perfLog);
    free(queries);
    return 0;
}

embedDBSchema* createSchema() {
    uint8_t numCols = 2;
    int8_t colSizes[] = {4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness);
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
