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
int inserted;

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

void test_insert_single_record_and_retrieval_from_buffer_no_flush(void) {
    uint32_t key = 121;
    uint32_t data = 12345;
    char varData[] = "Hello world";  // size 12
    embedDBPutVar(state, &key, &data, varData, 12);
    //  reset data
    uint32_t expData[] = {0, 0, 0};
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 12;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    TEST_ASSERT_EQUAL_INT(*expData, data);
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 12, "embedDBGetVar did not return the correct vardata");
}

void test_single_variable_page_insert_and_retrieve_from_buffer(void) {
    // insert just over a page worth's of data
    insertRecords(27);
    int key = 26;
    uint32_t expData[] = {0, 0, 0};
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 026...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
}

void test_insert_retrieve_insert_and_retrieve_again(void) {
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
    free(varDataBuffer);
    // insert more records
    insertRecords(58);
    key = 55;
    // create buffer for input
    varDataBuffer = malloc(varBufSize);
    r = embedDBGetVar(state, &key, &expData, &varStream);
    // test that records are found
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, r, "Records should have been found. 2");
    char varData_2[] = "Testing 055...";
    // retrieve variable record
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData_2, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
}

void test_var_read_iterator_buffer(void) {
    insertRecords(5);
    embedDBIterator it;
    uint32_t *itKey;
    // uint32_t itData[] = {0, 0, 0};
    uint32_t minKey = 0, maxKey = 3;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);

    embedDBInitIterator(state, &it);

    char varData[] = "Testing 000...";
    int temp = 0;

    while (embedDBNextVar(state, &it, &itKey, (void **)&itKey, &varStream)) {
        if (varStream != NULL) {
            uint32_t numBytesRead = 0;
            while ((numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
            }
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
            // modify varData according to eaach record.
            temp += 1;
            char whichRec = temp + '0';
            varData[10] = whichRec;

            free(varStream);
            varStream = NULL;
        }
    }
    free(varDataBuffer);
    embedDBCloseIterator(&it);
}

void test_insert_retrieve_flush_insert_retrieve_again(void) {
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
    free(varDataBuffer);
    embedDBFlush(state);
    // insert more records
    insertRecords(58);
    key = 55;
    // create buffer for input
    varDataBuffer = malloc(varBufSize);
    r = embedDBGetVar(state, &key, &expData, &varStream);
    // test that records are found
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, r, "Records should have been found. 2");
    char varData_2[] = "Testing 055...";
    // retrieve variable record
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData_2, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
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
    free(varDataBuffer);
    embedDBFlush(state);
    // insert more records
    insertRecords(55);
    // create buffer for input
    varDataBuffer = malloc(varBufSize);
    r = embedDBGetVar(state, &key, &expData, &varStream);
    // test that records are found
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, r, "Records should have been found");
    // retrieve variable record
    bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(test_insert_single_record_and_retrieval_from_buffer_no_flush);
    RUN_TEST(test_single_variable_page_insert_and_retrieve_from_buffer);
    RUN_TEST(test_insert_retrieve_insert_and_retrieve_again);
    RUN_TEST(test_var_read_iterator_buffer);
    RUN_TEST(test_insert_retrieve_flush_insert_retrieve_again);
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
    uint64_t targetNum = n + inserted;
    for (uint64_t i = inserted; i < targetNum; i++) {
        varData[10] = (char)(i % 10) + '0';
        varData[9] = (char)((i / 10) % 10) + '0';
        varData[8] = (char)((i / 100) % 10) + '0';

        uint64_t data = i % 100;
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
    state->recordSize = 16;  // size of record in bytes
    state->keySize = 4;      // size of key in bytes
    state->dataSize = 12;    // size of data in bytes
    state->pageSize = 512;   // page size (I am sure this is in bytes)
    state->numSplinePoints = 2;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 6;
    // allocate buffer
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    // check
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate EmbedDB buffer.");
    // address level parameters
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->numVarPages = 1000;
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
    // init
    // size_t splineMaxError = 1;

    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    return state;
}
