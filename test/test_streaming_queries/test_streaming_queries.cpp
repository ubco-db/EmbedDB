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

typedef struct {
    int counter1;
    int counter2;
} CallbackContext;

void test_MaxEqual(void) {
    std::cout << "Running test_MaxEqual..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->counter1 = 0;
    context->counter2 = 0;

    StreamingQuery **queries = (StreamingQuery**)malloc(sizeof(StreamingQuery*));
    queries[0] = createStreamingQuery(state, schema, context);

    int value = 5;
    queries[0]->IF(queries[0], 1, GET_MAX)
            ->ofLast(queries[0], 5)
            ->is(queries[0], Equal, (void*)&value)
            ->then(queries[0], [](void* maximum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                context->counter2 += 2;
                TEST_ASSERT_EQUAL_INT_MESSAGE(5, *(int*)maximum, "Callback did not return correct value.");            
    });


    int32_t data[] = {4,3,3,5,4,5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {

        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(queries, 1, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(3, context->counter1);
    TEST_ASSERT_EQUAL_INT32(6, context->counter2);


    free(queries);
    free(context);
    std::cout << "test_MaxEqual complete" << std::endl;
}

void test_MinGreaterThan(void) {
    std::cout << "Running test_MinGreaterThan..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->counter1 = 0;

    StreamingQuery **queries = (StreamingQuery**)malloc(sizeof(StreamingQuery*));
    queries[0] = createStreamingQuery(state, schema, context);

    int value = 2;
    queries[0]->IF(queries[0], 1, GET_MIN)
            ->ofLast(queries[0], 3)
            ->is(queries[0], GreaterThan, (void*)&value)
            ->then(queries[0], [](void* minimum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                TEST_ASSERT_TRUE(*(int*)minimum > 2);
    });

    int32_t data[] = {1, 2, 3, 4, 5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(queries, 1, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(1, context->counter1);

    free(queries);
    free(context);
    std::cout << "test_MinGreaterThan complete" << std::endl;
}

void test_AvgLessThanOrEqual(void) {
    std::cout << "Running test_AvgLessThanOrEqual..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->counter1 = 0;

    StreamingQuery **queries = (StreamingQuery**)malloc(sizeof(StreamingQuery*));
    queries[0] = createStreamingQuery(state, schema, context);

    float value = 3.5;
    queries[0]->IF(queries[0], 1, GET_AVG)
            ->ofLast(queries[0], 4)
            ->is(queries[0], LessThanOrEqual, (void*)&value)
            ->then(queries[0], [](void* average, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                TEST_ASSERT_TRUE(*(float*)average <= 3.5);
    });

    int32_t data[] = {2, 3, 4, 5, 6};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(queries, 1, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(4, context->counter1);

    free(queries);
    free(context);
    std::cout << "test_AvgLessThanOrEqual complete" << std::endl;
}

void test_MultipleQueries(void) {
    std::cout << "Running test_MultipleQueries..." << std::endl;
    CallbackContext* context1 = (CallbackContext*)malloc(sizeof(CallbackContext));
    context1->counter1 = 0;
    CallbackContext* context2 = (CallbackContext*)malloc(sizeof(CallbackContext));
    context2->counter1 = 0;

    StreamingQuery **queries = (StreamingQuery**)malloc(2 * sizeof(StreamingQuery*));
    queries[0] = createStreamingQuery(state, schema, context1);
    queries[1] = createStreamingQuery(state, schema, context2);

    int value1 = 5;
    queries[0]->IF(queries[0], 1, GET_MAX)
            ->ofLast(queries[0], 5)
            ->is(queries[0], Equal, (void*)&value1)
            ->then(queries[0], [](void* maximum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                TEST_ASSERT_EQUAL_INT_MESSAGE(5, *(int*)maximum, "Callback did not return correct value.");
    });

    int value2 = 2;
    queries[1]->IF(queries[1], 1, GET_MIN)
            ->ofLast(queries[1], 3)
            ->is(queries[1], GreaterThan, (void*)&value2)
            ->then(queries[1], [](void* minimum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                TEST_ASSERT_TRUE(*(int*)minimum > 2);
    });

    int32_t data[] = {1, -1, 2, 5, 4, 5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(queries, 2, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(3, context1->counter1);
    TEST_ASSERT_EQUAL_INT32(1, context2->counter1);

    free(queries);
    free(context1);
    free(context2);
    std::cout << "test_MultipleQueries complete" << std::endl;
}


embedDBOperator* createMinOperator(StreamingQuery *query, void*** allocatedValues, void *key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    uint32_t minKeyVal = *(uint32_t*)key - (query->numLastEntries-1);
    uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
    if (minKeyPtr != NULL) {
        *minKeyPtr = minKeyVal;
        it->minKey = minKeyPtr;
    }
    
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(query->state, it);

    embedDBOperator* scanOp = createTableScanOperator(query->state, it, query->schema);

    embedDBAggregateFunc* aggFunc = NULL;    
    aggFunc = createMinAggregate(query->colNum, query->schema->columnSizes[query->colNum]);

    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1*sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *aggFunc;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 1);
    aggOp->init(aggOp);

    free(aggFunc);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;

    return aggOp;

}

void* GetMin(StreamingQuery *query, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createMinOperator(query, &allocatedValues, key);

    void* recordBuffer = op->recordBuffer;
    exec(op);
    int32_t* minmax = (int32_t*)malloc(sizeof(int32_t));
    *minmax = *(int32_t*)((int8_t*)recordBuffer + 0);
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return (void*)minmax;
}


 void test_CustomQuery(void) {
    std::cout << "Running test_CustomQuery..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->counter1 = 0;

    StreamingQuery **queries = (StreamingQuery**)malloc(sizeof(StreamingQuery*));
    queries[0] = createStreamingQuery(state, schema, context);
    
    int value = 10;
    queries[0]->IFCustom(queries[0], 1, GetMin, INT32)
            ->ofLast(queries[0], 1)
            ->is(queries[0], GreaterThanOrEqual, (void*)&value)
            ->then(queries[0], [](void* result, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->counter1++;
                TEST_ASSERT_TRUE(*(int*)result >= 10);
                TEST_ASSERT_TRUE(*(int*)current >= 10);
    });

    int32_t data[] = {8, 9, 10, 11, 12};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        streamingQueryPut(queries, 1, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(3, context->counter1);

    free(queries);
    free(context);
    std::cout << "test_CustomQuery complete" << std::endl;
}



int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(test_MaxEqual);
    RUN_TEST(test_MinGreaterThan);
    RUN_TEST(test_AvgLessThanOrEqual);
    RUN_TEST(test_MultipleQueries);
    RUN_TEST(test_CustomQuery);
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