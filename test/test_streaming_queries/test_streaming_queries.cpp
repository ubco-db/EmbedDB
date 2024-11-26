#include <math.h>
#include <string.h>
#include <iostream>

#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/streamingQueries.h"
#endif

#if defined(MEMBOARD)
#include "memboardTestSetup.h"
#endif

#if defined(MEGA)
#include "megaTestSetup.h"
#endif

#if defined(DUE)
#include "dueTestSetup.h"
#endif

#include "unity.h"

#ifdef ARDUINO
#include "SDFileInterface.h"
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
#include "desktopFileInterface.h"
#define FILE_TYPE FILE
#define DATA_PATH "build/artifacts/dataFile.bin"
#define INDEX_PATH "build/artifacts/indexFile.bin"
#endif

embedDBState *state;
embedDBSchema *schema;

void setUpEmbedDB(void) {

    // Initialize the embedDB state
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBstate.");

    state->keySize = 4;
    state->dataSize = 4;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->numSplinePoints = 8;

    // Setup file IO
    char dataPath[] = DATA_PATH, indexPath[] = INDEX_PATH;
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);

    state->bufferSizeInBlocks = 4;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->buildBitmapFromRange = buildBitmapInt16FromRange;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    // Initialize schema
    int8_t colSizes[] = {4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED};
    schema = embedDBCreateSchema(2, colSizes, colSignedness);
    TEST_ASSERT_NOT_NULL_MESSAGE(schema, "Failed to create schema.");
}

void tearDown(void) {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);

    embedDBFreeSchema(&schema);

}

void test_streamingQueryPutWMaxEqual(void) {
    std::cout << "Running test_streamingQueryPut..." << std::endl;
    StreamingQuery *query = createStreamingQuery(state, schema);

    int value = 5;
    int counter = 0;
    query->IF(query, 1, GET_MAX)
        ->is(query, Equal, (void*)&value)
        ->forLast(query, 10)
        ->then(query, [](void* val) {
            TEST_ASSERT_EQUAL_FLOAT_MESSAGE(5, *(int*)val, "Callback did not return correct value.");            
    });


    int32_t data[] = {4,3,3,5,4,5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < 3; i++) {

        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(query, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }



    free(query);
    std::cout << "test_streamingQueryPut complete" << std::endl;
}



int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(test_streamingQueryPutWMaxEqual);
    return UNITY_END();
}

void setUp() {
    setUpEmbedDB();
}

#ifdef ARDUINO

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

#else

int main() {
    return runUnityTests();
}

#endif