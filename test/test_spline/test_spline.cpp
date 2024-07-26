#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
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

#ifdef ARDUINO
#include "SDFileInterface.h"
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile
#define DATA_FILE_PATH "dataFile.bin"
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#endif

#include "unity.h"

embedDBState *state;

void setupEmbedDB(int8_t parameters) {
    /* The setup below will result in having 42 records per page */
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBState.");
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

/* configure EmbedDB storage */
#ifdef MOCK_ERASE_INTERFACE
    state->fileInterface = getMockEraseFileInterface();
#else
    state->fileInterface = getFileInterface();
#endif

    state->dataFile = setupFile(DATA_FILE_PATH);

    state->numDataPages = 32;
    state->eraseSizeInPages = 4;
    state->parameters = parameters;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp() {
    int8_t setupParamaters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_RESET_DATA;
    setupEmbedDB(setupParamaters);
}

void tearDown() {
    free(state->buffer);
    embedDBClose(state);
    tearDownFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void should_erase_previous_spline_points_when_full() {
    uint32_t startingKey = 97855;
    uint64_t startingData = 98413;
    int8_t insertResult = 0;
    /* Insert 80 records with one increment at a time*/
    for (size_t i = 0; i < 80; i++) {
        insertResult = embedDBPut(state, &startingKey, &startingData);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        startingKey++;
        startingData++;
    }

    splinePrint(state->spl);

    /* Insert 170 records with one increment 15 at a time*/
    for (size_t i = 0; i < 170; i++) {
        insertResult = embedDBPut(state, &startingKey, &startingData);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        startingKey += 15;
        startingData++;
    }

    splinePrint(state->spl);
    TEST_ASSERT_LESS_OR_EQUAL_size_t(4, state->spl->count);

    /* Insert 170 records with one increment 2 at a time*/
    for (size_t i = 0; i < 170; i++) {
        insertResult = embedDBPut(state, &startingKey, &startingData);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        startingKey += 2;
        startingData++;
    }
    TEST_ASSERT_LESS_OR_EQUAL_size_t(4, state->spl->count);

    /* Insert 170 records with one increment 45 at a time*/
    for (size_t i = 0; i < 170; i++) {
        insertResult = embedDBPut(state, &startingKey, &startingData);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        startingKey += 45;
        startingData++;
    }
    TEST_ASSERT_LESS_OR_EQUAL_size_t(4, state->spl->count);

    /* Insert 200 records with one increment 55 at a time*/
    for (size_t i = 0; i < 300; i++) {
        insertResult = embedDBPut(state, &startingKey, &startingData);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        startingKey += 128;
        startingData++;
    }
    TEST_ASSERT_LESS_OR_EQUAL_size_t(4, state->spl->count);

    /* test querrying key before minimum spline point */
    uint32_t keyToQuery = 97856;
    uint64_t actualData = 0;
    uint64_t expectedData = 98414;
    int8_t getResult = embedDBGet(state, &keyToQuery, &actualData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDBGet unable to query key 97856.");
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), "embedDBGet retrieved incorrect data for key 97856.");
}

void should_clean_spline_when_data_overwritten() {
    uint32_t key = 27693354;
    uint64_t data = 53097707;
    int8_t insertResult = 0;

    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->spl->count, "embedDB should not initialize with any spline points.");
    /* Insert 300 records with one increment at a time*/
    for (size_t i = 0; i < 300; i++) {
        insertResult = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        key++;
        data++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2, state->spl->count, "embedDB spline point count should be 2 after inserting records at a linear rate.");

    /* Insert 400 records with one increment of 50 at a time */
    for (size_t i = 0; i < 400; i++) {
        insertResult = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        key += 50;
        data++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->spl->count, "embedDB spline point count should be three after inserting more records with a different pace of insertion.");

    /* Insert 200 records with one increment of 10 at a time */
    for (size_t i = 0; i < 1000; i++) {
        insertResult = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPut was unable to insert records into the database.");
        key += 10;
        data++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2, state->spl->count, "embedDB spline point count should be two after erasing an earlier spline point that is not needed.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(should_erase_previous_spline_points_when_full);
    RUN_TEST(should_clean_spline_when_data_overwritten);
    return UNITY_END();
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
