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

void setupEmbedDB() {
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
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(DATA_FILE_PATH);

    state->numDataPages = 32;
    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp() {
    setupEmbedDB();
}

void tearDown() {
    free(state->buffer);
    embedDBClose(state);
    tearDownFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void insertRecords(uint32_t startingKey, uint64_t startingData, uint32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((uint32_t *)data) = startingKey;
    *((uint64_t *)(data + 4)) = startingData;
    for (uint32_t i = 0; i < numRecords; i++) {
        *((uint32_t *)data) += 1;
        *((uint64_t *)(data + 4)) += 1;
        int8_t result = embedDBPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDBPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void embedDBInit_should_initialize_with_correct_values_for_record_level_consistency() {
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "embedDBInit did not reserve two blocks of pages for record-level consistency temporary pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->rlcPhysicalStartingPage, "embedDBInit did not initialize with the correct record-level consistency physical starting page.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextRLCPhysicalPageLocation, "embedDBInit did not initialize with the correct record-level consistency next physical page location.");
}

void writeTemporaryPage_places_pages_in_correct_location() {
    /* insert a single record and checked that we have updated values correctly */
    insertRecords(400, 204021, 1);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "Inserting one record should not have decresed the count of available pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(5, state->nextRLCPhysicalPageLocation, "embedDBInit did not initialize with the correct record-level consistency next physical page location.");

    /* Check that record was written to storage */
    int8_t readResult = readPage(state, 4);

    /* insert 41 more records but check that we did not write yet */
    insertRecords(401, 204022, 41);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "Inserting one record should not have decresed the count of available pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(6, state->nextRLCPhysicalPageLocation, "embedDBInit did not initialize with the correct record-level consistency next physical page location.");

    /* insert one more record to trigger page write */
    insertRecords(442, 204001, 1);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, state->numAvailDataPages, "Inserting one record should not have decresed the count of available pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->nextRLCPhysicalPageLocation, "embedDBInit did not initialize with the correct record-level consistency next physical page location.");

    /* TODO: shifting is not yet implemented */
    // insertRecords(443, 223945, 126);
    // TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, state->numAvailDataPages, "Inserting one record should not have decresed the count of available pages.");
    // TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->nextRLCPhysicalPageLocation, "embedDBInit did not initialize with the correct record-level consistency next physical page location.");
}

void record_level_consistency_blocks_should_move_when_write_block_is_full() {
    /* insert four pages of records to check that the record-level consitency block moves at the right time */
    insertRecords(1000, 384617, 168);
    /* should still be in initial location */
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBInit_should_initialize_with_correct_values_for_record_level_consistency);
    RUN_TEST(writeTemporaryPage_places_pages_in_correct_location);
    RUN_TEST(record_level_consistency_blocks_should_move_when_write_block_is_full);
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
