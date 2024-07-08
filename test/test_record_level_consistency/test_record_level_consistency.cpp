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
    /* The setup below will result in having 42 records per page */
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBState.");
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 8;
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
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(5, state->nextRLCPhysicalPageLocation, "Inserting one record should have caused the location for the next record-level consistency page to increase.");

    /* Check that record was written to storage */
    int8_t readResult = readPage(state, 4);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, readResult, "Unable to read page four when it should have been written to storage.");
    uint32_t key;
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_READ_BUFFER);
    memcpy(&key, buffer + state->headerSize, sizeof(uint32_t));
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(401, key, "Unable to get the correct key after writing out a record-level consistency temporary page to storage.");

    /* insert 41 more records but check that we did not write yet */
    insertRecords(401, 204022, 41);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "Inserting 42 records should not have decreased the number of available pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(6, state->nextRLCPhysicalPageLocation, "The next record-level consistancy page was not in the correct location after inserting 42 records.");

    /* insert one more record to trigger page write */
    insertRecords(442, 204001, 1);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, state->numAvailDataPages, "Insertion of 43 records should have caused one page to be written to storage.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->nextRLCPhysicalPageLocation, "After writing a page to storage, the location of the next record-level consistency page did not increment.");
}

void record_level_consistency_blocks_should_move_when_write_block_is_full() {
    /* insert four pages of records to check that the record-level consitency block moves at the right time */
    insertRecords(1000, 384617, 168);

    /* should still be in initial location */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(21, state->numAvailDataPages, "After inserting 168 records there should still be 21 available data pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->rlcPhysicalStartingPage, "The rlcPhysicalStartingPage was moved before 4 pages of data were written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, state->nextRLCPhysicalPageLocation, "After inserting 168 records, the nextRLCPhysicalPageLocation was not correct.");

    /* Insert one more record and check if the block moves */
    insertRecords(2000, 8217243, 1);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(20, state->numAvailDataPages, "After inserting 169 records there should be 20 available data pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->rlcPhysicalStartingPage, "The rlcPhysicalStartingPage was not moved after one block of records was written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(9, state->nextRLCPhysicalPageLocation, "The nextRLCPhysicalPageLocation was not moved after moving the starting page and inserting a record.");

    /* insert several more pages of records */
    insertRecords(2001, 431229, 68);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(19, state->numAvailDataPages, "After inserting 68 more records there should still be 19 available data pages.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->rlcPhysicalStartingPage, "The rlcPhysicalStartingPage should not be shifter until the previous block is full.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, state->nextRLCPhysicalPageLocation, "After inserting 68 more records, the nextRLCPhysicalPageLocation is incorrect.");
}

void record_level_consistency_blocks_should_wrap_when_storage_is_full() {
    /* insert records so storage is almost completely full */
    insertRecords(20240708, 334521, 1008);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->numAvailDataPages, "After inserting 1008 records, one data page should still be available.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->rlcPhysicalStartingPage, "After inserting 1008 records, the rlcPhysicalStartingPage should be page 24.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(32, state->nextRLCPhysicalPageLocation, "After inserting 1008 records, the nextRLCPhysicalPageLocation is incorrect.");

    /* insert one more record to cause wrap*/
    insertRecords(20250101, 234125, 1);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "After wrapping record-level consistency, the number of available data pages is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->minDataPageId, "After wrapping the record-level consistency blocks, the minDataPageId is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->nextDataPageId, "After wrapping the record-level consistency blocks, the nextDataPageId is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->rlcPhysicalStartingPage, "After wrapping the second record-level consistency block to the start of storage, the rlcPhysicalStartingPage is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(29, state->nextRLCPhysicalPageLocation, "After wrapping the second record-level consistency block to the start of storage, the nextRLCPhysicalPageLocation is incorrect.");

    /* insert 4 more records to check that the record-level consistency block wraps around properly */
    insertRecords(20250102, 244121, 4);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->rlcPhysicalStartingPage, "After wrapping in the record-level consistency blocks, the rlcPhysicalStartingPage is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextRLCPhysicalPageLocation, "After wrapping in the record-level consistency blocks, the nextRLCPhysicalPageLocation is incorrect.");

    /* insert 4 more records to check that we wrap back to the start of the record-level consistency block */
    insertRecords(20250110, 244121, 4);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->rlcPhysicalStartingPage, "After wrapping in the record-level consistency blocks, the rlcPhysicalStartingPage is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(29, state->nextRLCPhysicalPageLocation, "After wrapping in the record-level consistency blocks, the nextRLCPhysicalPageLocation is incorrect.");

    /* insert enough records to cause the rest of the record-level consistency block to wrap to the start */
    insertRecords(20250201, 121213, 160);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "After wrapping the second record-level consistency block, the number of available data pages is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->minDataPageId, "After wrapping the second record-level consistency block, the minDataPageId is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->nextDataPageId, "After wrapping the second record-level consistency block, the nextDataPageId is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->rlcPhysicalStartingPage, "After wrapping the second record-level consistency block, the rlcPhysicalStartingPage is incorrect.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextRLCPhysicalPageLocation, "After wrapping the second record-level consistency block, the nextRLCPhysicalPageLocation is incorrect.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBInit_should_initialize_with_correct_values_for_record_level_consistency);
    RUN_TEST(writeTemporaryPage_places_pages_in_correct_location);
    RUN_TEST(record_level_consistency_blocks_should_move_when_write_block_is_full);
    RUN_TEST(record_level_consistency_blocks_should_wrap_when_storage_is_full);
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
