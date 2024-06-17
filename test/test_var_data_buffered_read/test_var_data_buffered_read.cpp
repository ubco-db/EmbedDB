/******************************************************************************/
/**
 * @file        Test_var_data_buffered_read.cpp
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Test EmbedDB variable length data feature.
 * @copyright   Copyright 2024
 *              EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *  may be used to endorse or promote products derived from this software without
 *  specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

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
#define INDEX_FILE_PATH "indexFile.bin"
#define VAR_DATA_FILE_PATH "varFile.bin"
#else
#include "nativeFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"
#define VAR_DATA_FILE_PATH "build/artifacts/varFile.bin"
#endif

#include "unity.h"

int8_t insertRecords(uint32_t numberOfRecordsToInsert, uint32_t startingKey);
embedDBState *init_state();
embedDBState *state;

void setUp(void) {
    state = init_state();
}

void tearDown() {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    tearDownFile(state->varFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
    state = NULL;
}

void embedDBGetVar_should_retrieve_record_from_write_budder(void) {
    /* data for insert */
    uint32_t key = 121;
    uint32_t data[] = {12345, 6789, 101112};
    char varData[] = "Hello world";

    /* insert record */
    embedDBPutVar(state, &key, &data, varData, 12);

    /* setup variables for get */
    uint32_t actualFixedLengthData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;

    /* size of variable data inserted */
    uint32_t varBufSize = 12;
    char varDataBuffer[12];

    /* retrieve inserted record */
    int8_t r = embedDBGetVar(state, &key, actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(r, 0, "embedDBGetVar was unable to retrieve a record located in the write buffer");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(actualFixedLengthData, data, 3, "embedDBGetVar did not return the correct fixed length data");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 12, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

void embedDBGetVar_should_query_from_buffer_after_page_write(void) {
    /* insert just over one page of data */
    int8_t insertResult = insertRecords(27, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* setup for querying records */
    uint32_t key = 26;
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 20;
    char varDataBuffer[20];

    /* qeury databse for record */
    int8_t getStatusResult = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(getStatusResult, 0, "embedDBGetVar was unable to retrieve a record in the buffer after writing out data");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 026...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(14, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 14, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

void embedDBGetVar_should_return_variable_data_after_reading_records_and_inserting_more_records(void) {
    /* insert records into database */
    int8_t insertResult = insertRecords(3, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    uint32_t key = 2;
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 20;
    char varDataBuffer[20];

    /* query and test results */
    int8_t getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(getResultStatus, 0, "embedDBGetVar was unable to retrieve a record with variable data located in the write buffer");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char expectedVariableData[] = "Testing 002...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(14, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, varDataBuffer, 14, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;

    /* insert more records in to the database */
    insertResult = insertRecords(58, 3);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* fetch another of the records */
    key = 55;
    char expectedVariableData2[] = "Testing 055...";
    getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResultStatus, "embedDBGetVar was unable to retrieve a record with variable data after writing records to storage");
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(14, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData2, varDataBuffer, 14, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

void embedDBIterator_should_query_variable_lenth_data_for_fixed_length_records_located_in_the_write_buffer(void) {
    /* insert records in to database */
    int8_t insertResult = insertRecords(5, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* setup iterator to retrieve records */
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t fixedLengthData[] = {0, 0, 0};
    uint32_t minKey = 0;
    uint32_t maxKey = 3;

    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    /* variables for query results */
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 20;
    char varDataBuffer[20];
    uint32_t expectedDataSize = 14;

    /* initialize iterator */
    embedDBInitIterator(state, &it);

    /* variables for test */
    char varData[] = "Testing 000...";
    uint32_t numberOfRecordsRetrieved = 0;
    uint32_t expectedData[] = {1024, 0, 0};
    char message[100];

    while (embedDBNextVar(state, &it, &itKey, &fixedLengthData, &varStream)) {
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfRecordsRetrieved, itKey, "embedDBNextVar did not return the correct key value");
        snprintf(message, 100, "embedDBNextVar did not return the correct data for key %li.", numberOfRecordsRetrieved);
        TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, fixedLengthData, 3, message);
        if (varStream != NULL) {
            uint32_t numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataSize, numBytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
            snprintf(message, 100, "embedDBVarDataStreamRead did not return the correct variable length data for key %li.", numberOfRecordsRetrieved);
            varData[10] = '0' + numberOfRecordsRetrieved;
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, expectedDataSize, "embedDBGetVar did not return the correct vardata");
            free(varStream);
            varStream = NULL;
        }
        numberOfRecordsRetrieved += 1;
        expectedData[0] += 1;
    }

    /* test that the iterator fetched the correct number of records */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, numberOfRecordsRetrieved, "embedDBIterator did not return the correct number of records based on the applied filters");

    /* tear down */
    embedDBCloseIterator(&it);
}

void embedDBGetVar_should_fetch_records_in_write_buffer_after_flushing_data_to_storage(void) {
    /* insert some initial records */
    int8_t insertResult = insertRecords(3, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* setup for fetching records from database */
    uint32_t key = 2;
    uint32_t actualFixedLengthData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 20;
    char varDataBuffer[20];
    uint32_t expectedDataSize = 14;

    /* expected values for record */
    char expectedVariableData[] = "Testing 002...";
    uint32_t expectedFixedLengthData[] = {1026, 0, 0};

    /* query the database */
    int8_t getResultStatus = embedDBGetVar(state, &key, &actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(getResultStatus, 0, "embedDBGetVar was unable to fetch a record located in the write buffer");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);

    /* test returned data is correct */
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not return the correct fixed length data");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataSize, bytesRead, "embedDbGetVar dit not return the correct amound of variable length data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, varDataBuffer, expectedDataSize, "embedDBGetVar did not return the correct variable length data");

    /* tear down */
    free(varStream);
    varStream = NULL;

    /* flush records to storage */
    embedDBFlush(state);

    // insert more records
    insertRecords(58, 3);

    /* check that we can query new records */
    key = 55;
    char varData_2[] = "Testing 055...";
    expectedFixedLengthData[0] = 1079;

    getResultStatus = embedDBGetVar(state, &key, &actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, getResultStatus, "embedDBGetVar was unable to fetch a record located in the write buffer after flushing data to storage");
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);

    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not return the correct fixed length data");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataSize, bytesRead, "embedDbVarDataStreamRead did not return the correct length of variable data for a record fetched after flushing to storage");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData_2, varDataBuffer, expectedDataSize, "embedDbVarDataStreamRead did not return the correct variable data after flushing records to storage");

    /* tear down */
    free(varStream);
}

void embedDBGetVar_should_fetch_record_before_and_after_flush_to_storage(void) {
    // insert 3 records
    int8_t insertResult = insertRecords(3, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* variables for query */
    int key = 2;
    uint32_t actualFixedLengthData[] = {0, 0, 0};
    uint32_t varBufSize = 20;
    char varDataBuffer[20];
    uint32_t expectedDataSize = 14;
    uint32_t expectedFixedLengthData[] = {1026, 0, 0};
    embedDBVarDataStream *varStream = NULL;

    /* check that record can be fetched from buffer */
    int8_t r = embedDBGetVar(state, &key, &actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(r, 0, "embedDBGetVar was unable to retrieve the record with key 2");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not retrieve the correct fixed length data for the record with key 2");

    /* retrieve variable length portion of record */
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 002...";

    /* test that the returned values are correct */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataSize, bytesRead, "embedDBGetVar did not return the right length of variable length data for the record with key 2");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, expectedDataSize, "embedDBGetVar did not return the correct variable length data for the record with key 2");

    /* tear down */
    free(varStream);
    varStream = NULL;

    /* flush data to storage */
    embedDBFlush(state);

    /* insert more records in to our database */
    insertRecords(55, 3);

    /* query database for the same record that should now be located in storage */
    r = embedDBGetVar(state, &key, &actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, r, "embedDBGetVar was unable to retrieve the record with key 2 after flushing it to storage");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not retrieve the correct fixed length data for the record with key 2 after it was flushed to storage");

    /* check that the vardata is correct */
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataSize, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, expectedDataSize, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

void embedDBGetVar_should_fetch_record_from_buffer_and_storage_with_no_variable_length_data() {
    /* insert records */
    int8_t insertResult = insertRecords(64, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* insert record without variable length data */
    uint32_t key = 65;
    uint32_t fixedLengthData[] = {251, 2938, 55092};
    insertResult = embedDBPutVar(state, &key, fixedLengthData, NULL, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting a record without variable length data in to the database");

    /* query record without fixed length data */
    embedDBVarDataStream *varStream = NULL;
    uint32_t actualFixedLengthData[] = {0, 0, 0};
    uint32_t expectedFixedLengthData[] = {251, 2938, 55092};
    int8_t queryResult = embedDBGetVar(state, &key, actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGetVar was unable to retrieve the record with key 65");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not return the correct fixed length data for the record with key 65");
    TEST_ASSERT_NULL_MESSAGE(varStream, "embedDBGetVar should have returned NULL for varDataStream");
    varStream = NULL;

    /* query record with no fixed length data using regular get */
    memset(actualFixedLengthData, 0, sizeof(actualFixedLengthData));
    queryResult = 0;
    queryResult = embedDBGet(state, &key, actualFixedLengthData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGet was unable to retrieve the record with key 65");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGet did not return the correct fixed length data for the record with key 65");

    /* flush and insert more records */
    embedDBFlush(state);
    queryResult = 0;
    memset(actualFixedLengthData, 0, sizeof(actualFixedLengthData));
    insertResult = insertRecords(312, 241);

    /* query for record again */
    key = 65;
    queryResult = embedDBGetVar(state, &key, actualFixedLengthData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGetVar was unable to retrieve the record with key 65 after flushing and inserting more records");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGetVar did not return the correct fixed length data for the record with key 65 after flushing and inserting more records");
    TEST_ASSERT_NULL_MESSAGE(varStream, "embedDBGetVar should have returned NULL for varDataStream after flushing and inserting more records");

    queryResult = embedDBGet(state, &key, actualFixedLengthData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGet was unable to retrieve the record with key 65 after flushing and inserting more records");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGet did not return the correct fixed length data for the record with key 65 after flushing and inserting more records");
}

void embedDBGet_should_fetch_records_with_that_have_variable_length_data() {
    /* insert records */
    int8_t insertResult = insertRecords(16, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDBPutVar encountered an error inserting records in to the database");

    /* query one of the records */
    uint32_t key = 15;
    uint32_t actualFixedLengthData[] = {0, 0, 0};
    uint32_t expectedFixedLengthData[] = {1039, 0, 0};
    int8_t queryResult = embedDBGet(state, &key, actualFixedLengthData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGet was unable to retrieve the record for key 15");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGet did not return the correct fixed length data for the record with key 15");

    embedDBFlush(state);
    queryResult = 0;
    memset(actualFixedLengthData, 0, sizeof(actualFixedLengthData));

    /* try to query the record after flushing */
    key = 15;
    queryResult = embedDBGet(state, &key, actualFixedLengthData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, queryResult, "embedDBGet was unable to retrieve the record for key 15");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedFixedLengthData, actualFixedLengthData, 3, "embedDBGet did not return the correct fixed length data for the record with key 15");
}

int8_t insertRecords(uint32_t numberOfRecordsToInsert, uint32_t startingKey) {
    uint32_t fixedData[] = {0, 0, 0};
    char varData[] = "Testing 000...";
    uint32_t length = 14;
    int8_t insertResult = 0;
    uint32_t key = startingKey;
    for (uint32_t i = 0; i < numberOfRecordsToInsert; i++) {
        /* compute data value for record insert */
        fixedData[0] = 1024 + key;
        varData[10] = (char)(key % 10) + '0';
        varData[9] = (char)((key / 10) % 10) + '0';
        varData[8] = (char)((key / 100) % 10) + '0';

        insertResult = embedDBPutVar(state, &key, fixedData, varData, length);
        if (insertResult != 0) {
            return -1;
        }
        key += 1;
    }
    return 0;
}

embedDBState *init_state() {
    embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }

    // configure state variables
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 2;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 6;

    // allocate buffer
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);

    // check that buffer was allocated
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate EmbedDB buffer.");

    // address level parameters
    state->numDataPages = 30;
    state->numIndexPages = 8;
    state->numVarPages = 12;
    state->eraseSizeInPages = 4;

    // configure file interface
    char dataPath[] = DATA_FILE_PATH, indexPath[] = INDEX_FILE_PATH, varPath[] = VAR_DATA_FILE_PATH;
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->varFile = setupFile(varPath);

    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;  // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    embedDBResetStats(state);

    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    return state;
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBGetVar_should_retrieve_record_from_write_budder);
    RUN_TEST(embedDBGetVar_should_query_from_buffer_after_page_write);
    RUN_TEST(embedDBGetVar_should_return_variable_data_after_reading_records_and_inserting_more_records);
    RUN_TEST(embedDBIterator_should_query_variable_lenth_data_for_fixed_length_records_located_in_the_write_buffer);
    RUN_TEST(embedDBGetVar_should_fetch_records_in_write_buffer_after_flushing_data_to_storage);
    RUN_TEST(embedDBGetVar_should_fetch_record_before_and_after_flush_to_storage);
    RUN_TEST(embedDBGetVar_should_fetch_record_from_buffer_and_storage_with_no_variable_length_data);
    RUN_TEST(embedDBGet_should_fetch_records_with_that_have_variable_length_data);
    return UNITY_END();
}

int main() {
    return runUnityTests();
}

#ifdef ARDUINO

void setup() {
    delay(2000);
    setupBoard();
    main();
}

void loop() {}

#endif
