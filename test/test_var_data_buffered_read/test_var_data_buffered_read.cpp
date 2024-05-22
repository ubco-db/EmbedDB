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

#include "../src/embedDB/embedDB.h"
#include "embedDBUtility.h"

#if defined(MEMBOARD)
#include "memboardTestSetup.h"
#endif

#if defined(MEGA)
#include "megaTestSetup.h"
#endif

#if defined(DUE)
#include "dueTestSetup.h"
#endif

#include "SDFileInterface.h"
#include "unity.h"

int insertRecords(uint32_t n);
embedDBState *init_state();
embedDBState *state;
uint32_t inserted;

void setUp(void) {
    state = init_state();
}

void tearDown() {
    embedDBClose(state);
    tearDownSDFile(state->dataFile);
    tearDownSDFile(state->indexFile);
    tearDownSDFile(state->varFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
    state = NULL;
    inserted = 0;
}

void embedDBGetVar_should_retrieve_record_from_write_budder(void) {
    /* data for insert */
    uint32_t key = 121;
    uint32_t data = 12345;
    char varData[] = "Hello world";

    /* insert record */
    embedDBPutVar(state, &key, &data, varData, 12);

    /* setup variables for get */
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;

    /* size of variable data inserted */
    uint32_t varBufSize = 12;
    char varDataBuffer[12];

    /* retrieve inserted record */
    int8_t r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(r, 0, "embedDBGetVar was unable to retrieve a record located in the write buffer");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expData[0], data, "embedDBGetVar did not return the correct fixed length data");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 12, "embedDBGetVar did not return the correct vardata");
}

void embedDBGetVar_should_query_from_buffer_after_page_write(void) {
    /* insert just over one page of data */
    insertRecords(27);

    /* setup for querying records */
    uint32_t key = 26;
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;
    char varDataBuffer[15];

    /* qeury databse for record */
    int8_t getStatusResult = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(getStatusResult, 0, "embedDBGetVar was unable to retrieve a record in the buffer after writing out data");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 026...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

void embedDBGetVar_should_return_variable_data_after_reading_records_and_inserting_more_records(void) {
    /* insert records into database */
    insertRecords(3);

    uint32_t key = 2;
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;
    char varDataBuffer[15];

    /* query and test results */
    int8_t getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(getResultStatus, 0, "embedDBGetVar was unable to retrieve a record with variable data located in the write buffer");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char expectedVariableData[] = "Testing 002...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");

    /* insert more records in to the database */
    insertRecords(58);

    /* fetch another of the records */
    key = 55;
    char expectedVariableData2[] = "Testing 055...";
    getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResultStatus, "embedDBGetVar was unable to retrieve a record with variable data after writing records to storage");
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData2, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");

    /* tear down */
    free(varStream);
    varStream = NULL;
}

/* TODO: seems to be an issue with either this test or filtering records by key when using variable data */
void embedDBIterator_should_query_variable_lenth_data_for_fixed_length_records_located_in_the_write_buffer(void) {
    /* insert records in to database */
    insertRecords(5);

    /* setup iterator to retrieve records */
    embedDBIterator it;
    uint32_t *itKey;
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

    /* initialize iterator */
    embedDBInitIterator(state, &it);

    char varData[] = "Testing 000...";
    uint32_t temp = 0;
    uint32_t numberOfRecordsRetrieved = 0;

    while (embedDBNextVar(state, &it, &itKey, (void **)&itKey, &varStream)) {
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfRecordsRetrieved, itKey, "Unexpected item in bagging area");
        numberOfRecordsRetrieved += 1;
        if (varStream != NULL) {
            uint32_t numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, numBytesRead, "embedDBGetVar returned a var data stream which did not read the correct length of variable data");
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");

            // modify varData according to eaach record.
            temp += 1;
            char whichRec = temp + '0';
            varData[10] = whichRec;

            free(varStream);
            varStream = NULL;
        }
    }

    /* test that the iterator fetched the correct number of records */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, numberOfRecordsRetrieved, "embedDBIterator did not return the correct number of records based on the applied filters");

    /* tear down */
    embedDBCloseIterator(&it);
}

void embedDBGetVar_should_fetch_records_in_write_buffer_after_flushing_data_to_storage(void) {
    /* insert some initial records */
    insertRecords(3);

    /* setup for fetching records from database */
    uint32_t key = 2;
    uint32_t expData[] = {0, 0, 0};
    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;
    char varDataBuffer[15];

    // query embedDB
    int8_t getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(getResultStatus, 0, "embedDBGetVar was unable to fetch a record located in the write buffer");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);

    // create string to compaare to
    char expectedVariableData[] = "Testing 002...";

    /* test returned data is correct */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "embedDbGetVar dit not return the correct amound of variable length data");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, varDataBuffer, 15, "embedDBGetVar did not return the correct variable length data");

    /* flush records to storage */
    embedDBFlush(state);

    // insert more records
    insertRecords(58);

    /* check that we can query new records */
    key = 55;
    char varData_2[] = "Testing 055...";

    getResultStatus = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, getResultStatus, "embedDBGetVar was unable to fetch a record located in the write buffer after flushing data to storage");

    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "embedDbVarDataStreamRead did not return the correct length of variable data for a record fetched after flushing to storage");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData_2, varDataBuffer, 15, "embedDbVarDataStreamRead did not return the correct variable data after flushing records to storage");

    /* tear down */
    free(varStream);
}

void test_insert_retrieve_flush_insert_retrieve_single_record_again(void) {
    // insert 3 records
    insertRecords(3);
    // retrieve record
    int key = 2;
    uint32_t expData[] = {0, 0, 0};
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 15;
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    // test that records are found
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    // retrieve variable record
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    // create string to compaare to
    char varData[] = "Testing 002...";
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
    // free and flush
    embedDBFlush(state);
    // insert more records
    insertRecords(55);
    // create buffer for input
    r = embedDBGetVar(state, &key, &expData, &varStream);
    // test that records are found
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, r, "Records should have been found");
    // retrieve variable record
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");

    free(varDataBuffer);
}

/* TODO: add test to make sure regular get and get var work together */

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBGetVar_should_retrieve_record_from_write_budder);
    RUN_TEST(embedDBGetVar_should_query_from_buffer_after_page_write);
    RUN_TEST(embedDBGetVar_should_return_variable_data_after_reading_records_and_inserting_more_records);
    RUN_TEST(embedDBIterator_should_query_variable_lenth_data_for_fixed_length_records_located_in_the_write_buffer);
    RUN_TEST(embedDBGetVar_should_fetch_records_in_write_buffer_after_flushing_data_to_storage);
    RUN_TEST(test_insert_retrieve_flush_insert_retrieve_single_record_again);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

int insertRecords(uint32_t n) {
    char varData[] = "Testing 000...";
    uint32_t targetNum = n + inserted;
    for (uint32_t i = inserted; i < targetNum; i++) {
        varData[10] = (char)(i % 10) + '0';
        varData[9] = (char)((i / 10) % 10) + '0';
        varData[8] = (char)((i / 100) % 10) + '0';

        uint32_t data = i % 100;
        int result = embedDBPutVar(state, &i, &data, varData, 15);
        if (result != 0) {
            return result;
        }
        inserted++;
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
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->varFile = setupSDFile(varPath);

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
