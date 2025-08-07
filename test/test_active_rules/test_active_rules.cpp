#include <cmath>
#include <cstring>
#include <iostream>

#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/activeRules.h"
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

// This function sets up the EmbedDB state and schema for testing.
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
    ColumnType colTypes[] = {embedDB_COLUMN_UINT32, embedDB_COLUMN_INT32};
    schema = embedDBCreateSchema(2, colSizes, colSignedness, colTypes);
    TEST_ASSERT_NOT_NULL_MESSAGE(schema, "Failed to create schema.");
}

// The tearDown function is used to clean up resources and reset the state after each test.
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
    int int1;
    int int2;
    float array[10];
    float float1;
} CallbackContext;

void test_MaxEqual(void) {
    std::cout << "Running test_MaxEqual..." << std::endl;
    
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->int1 = 0;
    context->int2 = 0;
    
    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);

    int value = 5;
    int numLast = 5;
    state->rules[0]->IF(state->rules[0], 1, GET_MAX)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->is(state->rules[0], Equal, (void*)&value)
            ->then(state->rules[0], [](void* maximum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                context->int2 += 2;
                TEST_ASSERT_EQUAL_INT_MESSAGE(5, *(int*)maximum, "Callback did not return correct value.");            
    });
    state->numRules = 1;

    int32_t data[] = {4,3,3,5,4,5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {

        *((uint32_t*)dataPtr) = data[i];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(3, context->int1);
    TEST_ASSERT_EQUAL_INT32(6, context->int2);


    
    free(context);
    std::cout << "test_MaxEqual complete" << std::endl;
}

void test_MinGreaterThan(void) {
    std::cout << "Running test_MinGreaterThan..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->int1 = 0;
    context->int2 = 0;

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);

    int value = 2;
    int numLast = 3;
    state->rules[0]->IF(state->rules[0], 1, GET_MIN)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->is(state->rules[0], GreaterThan, (void*)&value)
            ->then(state->rules[0], [](void* minimum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                TEST_ASSERT_TRUE(*(int*)minimum > 2);
    });
    state->numRules = 1;

    int32_t data[] = {1, 2, 3, 4, 5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(1, context->int1);

    
    free(context);
    std::cout << "test_MinGreaterThan complete" << std::endl;
}

void test_AvgLessThanOrEqual(void) {
    std::cout << "Running test_AvgLessThanOrEqual..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->int1 = 0;

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);

    float value = 3.5;
    int numLast = 4;
    state->rules[0]->IF(state->rules[0], 1, GET_AVG)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->is(state->rules[0], LessThanOrEqual, (void*)&value)
            ->then(state->rules[0], [](void* average, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                TEST_ASSERT_TRUE(*(float*)average <= 3.5);
                std::cout << "Average: " << *(float*)average << std::endl;
    });
    state->numRules = 1;

    int32_t data[] = {2, 3, 4, 5, 6};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(4, context->int1);

    
    free(context);
    std::cout << "test_AvgLessThanOrEqual complete" << std::endl;
}

void test_AvgLessThanOrEqual_Float(void) {
    std::cout << "Running test_AvgLessThanOrEqual_Float..." << std::endl;

    schema->columnTypes[1] = embedDB_COLUMN_FLOAT;

    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->int1 = 0;

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);

    float value = 3.75f; // Comparison threshold
    int numLast = 4; // Number of last values to calculate AVG
    state->rules[0]->IF(state->rules[0], 1, GET_AVG)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->is(state->rules[0], LessThanOrEqual, (void*)&value)
            ->then(state->rules[0], [](void* average, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                float avg = *(float*)average;
                TEST_ASSERT_TRUE(avg <= 3.75f);
                std::cout << "Computed Average: " << avg << std::endl;
    });
    state->numRules = 1;

    // Using float values with decimals
    float data[] = {2.1f, 3.3f, 4.7f, 3.8f, 5.5f, 2.9f};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((float*)dataPtr) = data[i];  // Store float values
        embedDBPut(state, &i, dataPtr);

        float dataRetrieved = 0.0f;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_FLOAT(data[i], dataRetrieved);
    }

    // Verify that the callback was triggered the expected number of times
    TEST_ASSERT_EQUAL_INT32(4, context->int1);

    
    free(context);
    free(dataPtr);
    std::cout << "test_AvgLessThanOrEqual_Float complete" << std::endl;
}

// This function tests the execution of multiple active rules in a row.
// It verifies that each rule correctly processes the data and triggers the appropriate callbacks.
void test_MultipleQueries(void) {
    std::cout << "Running test_MultipleQueries..." << std::endl;
    CallbackContext* context1 = (CallbackContext*)malloc(sizeof(CallbackContext));
    context1->int1 = 0;
    CallbackContext* context2 = (CallbackContext*)malloc(sizeof(CallbackContext));
    context2->int1 = 0;

    state->rules = (activeRule**)malloc(2 * sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context1);
    state->rules[1] = createActiveRule(schema, context2);

    int value1 = 5;
    int numLast = 5;
    state->rules[0]->IF(state->rules[0], 1, GET_MAX)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->is(state->rules[0], Equal, (void*)&value1)
            ->then(state->rules[0], [](void* maximum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                TEST_ASSERT_EQUAL_INT_MESSAGE(5, *(int*)maximum, "Callback did not return correct value.");
    });

    int value2 = 2;
    int numLast2 = 3;
    state->rules[1]->IF(state->rules[1], 1, GET_MIN)
            ->ofLast(state->rules[1], (void*)&numLast2)
            ->is(state->rules[1], GreaterThan, (void*)&value2)
            ->then(state->rules[1], [](void* minimum, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1++;
                TEST_ASSERT_TRUE(*(int*)minimum > 2);
    });
    state->numRules = 2;

    int32_t data[] = {1, -1, 2, 5, 4, 5};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(3, context1->int1);
    TEST_ASSERT_EQUAL_INT32(1, context2->int1);

    
    free(context1);
    free(context2);
    std::cout << "test_MultipleQueries complete" << std::endl;
}

void* GetWeightedAverage(activeRule *rule, void *key) {
    int currentKey = *(int*)key;
    int slidingWindowStart = currentKey - (*(uint32_t*)rule->numLastEntries - 1);

    float totalWeight = 0;
    float weightedSum = 0;

    for (int key = slidingWindowStart; key <= currentKey; key++) {
        int32_t record = 0;
        int8_t result = embedDBGet(state, (void*)&key, (void*)&record);
        if(result != 0) { 
            continue;
        }
        TEST_ASSERT_EQUAL_INT32(key%2,0); //test data is inserted every 2 seconds i.e. timestamp (key) is even
        int timeDifference = currentKey - key;
        float weight = (*(uint32_t*)rule->numLastEntries - 1) - timeDifference; // Linear decay 
        if (weight < 0) weight = 0;
        
        weightedSum += record * weight;
        totalWeight += weight;
    }

    float* weightedAverage = (float*)malloc(sizeof(float));
    *weightedAverage = weightedSum / totalWeight;
    printf("Weighted Average at %is: %f\n", *(int*)key, *weightedAverage);
    return (void*)weightedAverage;
}
// The test_CustomQuery function tests a custom active rule that calculates a weighted average
// over a sliding window of the last 10 seconds and verifies the results against expected values.
// It also stores the weighted average in a context variable and uses it in a subsequent rule to compare
// the average of the last 10 seconds with the weighted average.
 void test_CustomQuery(void) {
    std::cout << "Running test_CustomQuery..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext) + 10 * sizeof(float));
    context->int1 = 0;

    int32_t data[] = {21,20,22,23,24,23,25,26,27,26};
    float weighted_averages[] = {
        21.00,  // at 2 seconds
        ((7*21) + (9*20)) / (7.0 + 9),  // at 4 seconds
        ((5*21)+(7*20)+(9*22))/(5.0+7+9),  // at 6 seconds
        ((3*21)+(5*20)+(7*22)+(9*23))/(3.0+5+7+9),  // at 8 seconds
        ((1*21)+(3*20)+(5*22)+(7*23)+(9*24))/(1.0+3+5+7+9),  // at 10 seconds
        ((1*20)+(3*22)+(5*23)+(7*24)+(9*23))/(1.0+3+5+7+9),  // at 12 seconds
        ((1*22)+(3*23)+(5*24)+(7*23)+(9*25))/(1.0+3+5+7+9),  // at 14 seconds
        ((1*23)+(3*24)+(5*23)+(7*25)+(9*26))/(1.0+3+5+7+9),  // at 16 seconds
        ((1*24)+(3*23)+(5*25)+(7*26)+(9*27))/(1.0+3+5+7+9),  // at 18 seconds
        ((1*23)+(3*25)+(5*26)+(7*27)+(9*26))/(1.0+3+5+7+9),  // at 20 seconds
    };

    for (int i = 0; i < 10; i++) {
        context->array[i] = weighted_averages[i];
    }

    state->rules = (activeRule**)malloc(2*sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);
    
    int value = 0; //ensure callback is called everytime
    int numLast = 10;
    state->rules[0]->IFCustom(state->rules[0], 1, GetWeightedAverage, DBFLOAT)
            ->ofLast(state->rules[0], (void*)&numLast) // last 10 seconds
            ->is(state->rules[0], GreaterThanOrEqual, (void*)&value)
            ->then(state->rules[0], [](void* result, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                TEST_ASSERT_EQUAL_FLOAT(context->array[context->int1], *(float*)result);
                context->int1++;
                context->float1 = *(float*)result; //store weighted average for next rule
    });

    // Second rule to compare the average of the last 10 seconds with the weighted average
    state->rules[1] = createActiveRule(schema, context);
    state->rules[1]->IF(state->rules[1], 1, GET_AVG)
            ->ofLast(state->rules[1], (void*)&numLast)
            ->is(state->rules[1], LessThanOrEqual, (void*)&(context->float1))
            ->then(state->rules[1], [](void* result, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                TEST_ASSERT_TRUE(*(float*)result <= context->array[context->int1 - 1]);
                printf("Average of last 10 seconds at %is: %f, weighted average is: %f\n", context->int1*2, *(float*)result, (context->float1));

    });
    state->numRules = 2;

    void* dataPtr = calloc(1, state->dataSize);
    int j = 0;
    for (int32_t i = 2; i < 22; i+=2) {
        *((uint32_t*)dataPtr) = data[j++];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[j-1], dataRetrieved);
    }

    TEST_ASSERT_EQUAL_INT32(10, context->int1);

    
    free(context);
    std::cout << "test_CustomQuery complete" << std::endl;
}

void test_whereClause(void){
    std::cout << "Running test_whereClause..." << std::endl;
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    context->int1 = 0;

    state->rules = (activeRule**)malloc(sizeof(activeRule*));
    state->rules[0] = createActiveRule(schema, context);

    int value = 0;
    int minData = 3;
    int numLast = 4;
    state->rules[0]->IF(state->rules[0], 1, GET_MIN)
            ->ofLast(state->rules[0], (void*)&numLast)
            ->where(state->rules[0], (void*)&minData, NULL)
            ->is(state->rules[0], GreaterThan, (void*)&value)
            ->then(state->rules[0], [](void* mini, void* current, void* ctx) {
                CallbackContext* context = (CallbackContext*)ctx;
                context->int1 = *(int*)mini;
                TEST_ASSERT_TRUE(*(int*)mini > 2);
    });
    state->numRules = 1;
    int32_t data[] = {2, 3, 4, 5, 6};
    void* dataPtr = calloc(1, state->dataSize);
    for (int32_t i = 0; i < sizeof(data)/sizeof(data[0]); i++) {
        *((uint32_t*)dataPtr) = data[i];
        embedDBPut(state, &i, dataPtr);

        int32_t dataRetrieved = 0;
        embedDBGet(state, (void*)&i, (void*)&dataRetrieved);
        TEST_ASSERT_EQUAL_INT32(data[i], dataRetrieved);
    }
}

int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(test_MaxEqual);
    RUN_TEST(test_MinGreaterThan);
    RUN_TEST(test_AvgLessThanOrEqual);
    RUN_TEST(test_AvgLessThanOrEqual_Float);
    RUN_TEST(test_MultipleQueries);
    RUN_TEST(test_CustomQuery);
    RUN_TEST(test_whereClause);
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