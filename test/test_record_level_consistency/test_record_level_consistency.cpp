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
    state->numSplinePoints = 8;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

    /* configure EmbedDB storage */
    state->fileInterface = getFileInterface();
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

void embedDBInit_should_detect_when_no_records_written_with_record_level_consistency() {
    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* test that we recovered correctly to the default state */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->minKey, "embedDBInit did not set the correct minKey after recovering with no records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages after recovering with no records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId after recovering with no records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation after recovering with no records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage after recovering with no records written.");
}

void embedDBInit_should_recover_record_level_consistency_records_when_no_permanent_pages_written() {
    /* insert records */
    insertRecords(202020, 101010, 12);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* test that we recovered correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* test that we can query all records written before reset */
    uint32_t key = 202021;
    uint64_t expectedData = 101011;
    uint64_t actualData = 0;
    char message[100];
    for (uint32_t i = 0; i < 12; i++) {
        int8_t getResult = embedDBGet(state, &key, &actualData);
        snprintf(message, 100, "embedDBGet was unable to fetch the data for key %u.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "embedDBGet returned the wrong data for key %u.", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        key++;
        expectedData++;
    }

    /* Check that if we try to query one more it should return an error */
    int8_t getResult = embedDBGet(state, &key, &actualData);
    snprintf(message, 100, "embedDBGet fetched data for a record that should not exist %u.", key);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

void embedDBInit_should_recover_record_level_consistency_records_when_one_permanent_page_is_written() {
    /* insert records */
    insertRecords(12344, 11, 42);
    embedDBFlush(state);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12345, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that there is nothing in the buffer */
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_WRITE_BUFFER);
    int8_t count = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, count, "embedDBInit did not correctly initialize the write buffer after recovery.");
}

void embedDBInit_should_recover_record_level_consistency_records_when_four_permanent_pages_are_written() {
    /* insert records */
    insertRecords(1032, 243718, 168);
    embedDBFlush(state);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1033, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(20, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that there is nothing in the buffer */
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_WRITE_BUFFER);
    int8_t count = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, count, "embedDBInit did not correctly initialize the write buffer after recovery.");

    /* Should be able to write records to record-level consistency pages */
    insertRecords(1400, 231427, 34);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBPut incremented minDataPageId before a page should have been written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextDataPageId, "embedDBPut incremented nextDataPageId before a page should have been written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1033, state->minKey, "embedDBPut changed minKey after inserting keys.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(20, state->numAvailDataPages, "embedDBPut changed numAvailDataPages before a page should have been written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(10, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that we can query these new records */
    uint32_t key = 1401;
    uint64_t expectedData = 231428;
    uint64_t actualData = 0;
    char message[100];
    for (uint32_t i = 0; i < 34; i++) {
        int8_t getResult = embedDBGet(state, &key, &actualData);
        snprintf(message, 100, "embedDBGet was unable to fetch the data for key %u.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "embedDBGet returned the wrong data for key %u.", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        key++;
        expectedData++;
    }
}

void embedDBInit_should_recover_record_level_consistency_records_when_eight_permanent_pages_are_written() {
    /* insert 8 pages of records and 39 individual records */
    insertRecords(544479, 651844, 375);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(544480, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(19, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that buffer was initialised correctly */
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_WRITE_BUFFER);
    int8_t count = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(39, count, "embedDBInit did not correctly initialize the write buffer after recovery.");

    /* insert four more records to trigger page write */
    insertRecords(552242, 2431549, 4);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(9, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(544480, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");
}

void embedDBInit_should_recover_record_level_consistency_records_when_twenty_one_permanent_pages_are_written() {
    /* insert 21 pages of records and 13 individual records */
    insertRecords(20241017, 370701, 895);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(21, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(20241018, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(31, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that buffer was initialised correctly */
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_WRITE_BUFFER);
    int8_t count = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(13, count, "embedDBInit did not correctly initialize the write buffer after recovery.");
}

void embedDBInit_should_recover_record_level_consistency_records_when_twenty_three_permanent_pages_are_written() {
    /* insert 23 pages of records and 42 individual records, which is the max before we need to wrap */
    insertRecords(2803579, 7902382, 1008);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2803580, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");

    /* Check that buffer was initialised correctly */
    int8_t *buffer = (int8_t *)(state->buffer) + (state->pageSize * EMBEDDB_DATA_WRITE_BUFFER);
    int8_t count = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(42, count, "embedDBInit did not correctly initialize the write buffer after recovery.");

    /* insert one more records to check that we wrap properly after recovery */
    insertRecords(2903579, 0, 1);

    /* Check values */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(29, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");
}

void embedDBInit_should_recover_correctly_with_wraped_record_level_consistency_block() {
    /* insert 24 pages of records and 15 record-level consistency records*/
    insertRecords(240559, 459870, 1023);

    /* close embedDB and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY;
    setupEmbedDB(setupParameters);

    /* Check that data was initialised correctly */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->minDataPageId, "embedDBInit did not set the correct minDataPageId after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, state->nextDataPageId, "embedDBInit did not set the correct value of nextDataPageId with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(240560, state->minKey, "embedDBInit did not set the correct minKey after recovering with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "embedDBInit did not set the correct value of numAvailDataPages with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28, state->rlcPhysicalStartingPage, "embedDBInit did not set the correct value of rlcPhysicalStartingPage with no permanent records written.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->nextRLCPhysicalPageLocation, "embedDBInit did not set the correct value of nextRLCPhysicalPageLocation with no permanent records written.");
}

/* TODO: Add cases for wrap around */

int runUnityTests() {
    UNITY_BEGIN();
    // RUN_TEST(embedDBInit_should_initialize_with_correct_values_for_record_level_consistency);
    // RUN_TEST(writeTemporaryPage_places_pages_in_correct_location);
    // RUN_TEST(record_level_consistency_blocks_should_move_when_write_block_is_full);
    // RUN_TEST(record_level_consistency_blocks_should_wrap_when_storage_is_full);
    // RUN_TEST(embedDBInit_should_detect_when_no_records_written_with_record_level_consistency);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_no_permanent_pages_written);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_one_permanent_page_is_written);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_four_permanent_pages_are_written);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_eight_permanent_pages_are_written);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_twenty_one_permanent_pages_are_written);
    // RUN_TEST(embedDBInit_should_recover_record_level_consistency_records_when_twenty_three_permanent_pages_are_written);
    RUN_TEST(embedDBInit_should_recover_correctly_with_wraped_record_level_consistency_block);
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
