/******************************************************************************/
/**
 * @file        test_buffered_read.cpp
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
/*****************************************************************************/

#include <math.h>
#include <string.h>

#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#endif

#ifdef ARDUINO
#include "SDFileInterface.h"
#else
#include "nativeFileInterface.h"
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

int insertStaticRecord(embedDBState* state, uint32_t key, uint32_t data);
embedDBState* init_state();

embedDBState* state;

void setUp(void) {
    state = init_state();
}

void tearDown(void) {
    free(state->buffer);
    embedDBClose(state);
    tearDownSDFile(state->dataFile);
    free(state->fileInterface);
    free(state);
    state = NULL;
}

void embedDBGet_should_return_data_when_single_record_inserted_and_flushed_to_storage(void) {
    /* key to insert */
    uint32_t key = 1;

    /* insert in to database */
    insertStaticRecord(state, key, 123);

    /* flush to storage */
    embedDBFlush(state);

    /* query record */
    uint32_t actualData[] = {0, 0, 0};
    int8_t getResult = embedDBGet(state, &key, actualData);

    /* test query was successful and returned data is corrrect */
    uint32_t expectedData[] = {123, 0, 0};
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(0, getResult, "embedDBGet was unable to return the record with key 1");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not return the expected data for the record with key 1");
}

void embedDBGet_should_return_data_when_multiple_records_inserted_and_flushed_to_storage(void) {
    /* insert records */
    uint32_t numInserts = 100;
    for (uint32_t i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }
    /* flush records to storage */
    embedDBFlush(state);

    /* query inserted record */
    uint32_t key = 93;
    uint32_t actualData[] = {0, 0, 0};
    int8_t getResult = embedDBGet(state, &key, actualData);

    /* test that record is returned with correct data */
    uint32_t expectedData[] = {193, 0, 0};
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDBGet was unable to return the record with key 93");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not return the correct data for the record with key 93");
}

void embedDBGet_should_return_data_for_record_in_write_buffer(void) {
    /* insert record in to database */
    uint32_t key = 1;
    insertStaticRecord(state, key, 245);

    /* query record */
    uint32_t actualData[] = {0, 0, 0};
    int8_t getResult = embedDBGet(state, &key, actualData);

    /* test get was successful */
    uint32_t expectedData[] = {245, 0, 0};
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDBGet was unable to return the record with key 100");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not retrieve the correct data for the record with key 1");
}

void embedDBGet_should_return_data_for_record_when_multiple_records_are_inserted_in_write_buffer(void) {
    /* insert records */
    uint32_t numInserts = 31;
    for (uint32_t i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }

    /* query records */
    uint32_t key = 30;
    uint32_t return_data[] = {0, 0, 0};
    int8_t getResult = embedDBGet(state, &key, return_data);

    /* test that get was successful */
    uint32_t expectedData[] = {130, 0, 0};
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(0, getResult, "embedDBGat was unable to retrieve the record for key 30");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, return_data, 3, "embedDBGet did not return the correct data for the record with key 30");
}

void embedDBGet_should_return_data_for_records_in_file_storage_and_write_buffer(void) {
    /* insert record */
    uint32_t key = 1;
    insertStaticRecord(state, key, 154);

    /* query record */
    uint32_t actualData[] = {0, 0, 0};
    int8_t getResult = embedDBGet(state, &key, actualData);

    /* test get was successful */
    uint32_t expectedData[] = {154, 0, 0};
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDBGet was unable to retrieve the record for key 1");
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not return the correct data for the record with key 1");

    /* flush record to storage */
    embedDBFlush(state);

    /* insert another record */
    key = 2;
    insertStaticRecord(state, key, 12345);

    /* query new record */
    getResult = embedDBGet(state, &key, actualData);

    /* test that get for second record was successful */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDBGet was unable to retrieve the record for key 2");
    expectedData[0] = 12345;
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not return the correct data for the record with key 2");

    /* test that the first record can still be retrieved correctly */
    key = 1;
    getResult = embedDBGet(state, &key, actualData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "embedDbGet was unable to retrieve the record for key 1 after flushing it to storage and inserting another record in to the write buffer");
    expectedData[0] = 154;
    TEST_ASSERT_EQUAL_UINT32_ARRAY_MESSAGE(expectedData, actualData, 3, "embedDBGet did not return the correct data for the record with key 1 after flushing it to storage and inserting another record in to the write buffer");
}

void embedDBGet_should_return_no_data_when_requested_key_greater_than_max_buffer_key(void) {
    /* flush database so nextDataPageId > 0*/
    embedDBFlush(state);

    /* insert records */
    uint32_t numInserts = 8;
    for (uint32_t i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }

    /* query for key greater than max key in database */
    uint32_t key = 55;
    u_int32_t return_data[] = {0, 0, 0};
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, embedDBGet(state, &key, return_data), "embedDBGet returned data for a key greater than the maximum key in the database");

    key = 8;
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, embedDBGet(state, &key, return_data), "embedDBGet returned data for a key greater than the maximum key in the database");
}

void embedDBGet_should_return_not_found_when_key_is_less_then_min_key(void) {
    /* flush database so nextDataPageId > 0*/
    embedDBFlush(state);

    /* insert some records */
    uint32_t numInserts = 8;
    for (uint32_t i = 1; i <= numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }

    /* query for key lower then the min key in the database */
    uint32_t key = 0;
    u_int32_t actualData[] = {0, 0, 0};
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, embedDBGet(state, &key, actualData), "embedDBGet returned data for a key that is less than the minimum key in the database");
}

void embedDBGet_should_return_no_data_found_when_database_and_buffer_are_empty(void) {
    /* query for key when database is empty */
    uint32_t key = 1;
    u_int32_t actualData[] = {0, 0, 0};
    int8_t status = embedDBGet(state, &key, actualData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, status, "embedDBGet returned data when there were no keys in the database or write buffer");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBGet_should_return_data_when_single_record_inserted_and_flushed_to_storage);
    RUN_TEST(embedDBGet_should_return_data_when_multiple_records_inserted_and_flushed_to_storage);
    RUN_TEST(embedDBGet_should_return_data_for_record_in_write_buffer);
    RUN_TEST(embedDBGet_should_return_data_for_record_when_multiple_records_are_inserted_in_write_buffer);
    RUN_TEST(embedDBGet_should_return_data_for_records_in_file_storage_and_write_buffer);
    RUN_TEST(embedDBGet_should_return_no_data_when_requested_key_greater_than_max_buffer_key);
    RUN_TEST(embedDBGet_should_return_not_found_when_key_is_less_then_min_key);
    RUN_TEST(embedDBGet_should_return_no_data_found_when_database_and_buffer_are_empty);
    return UNITY_END();
}

/* function puts a static record into buffer without flushing. Creates and frees record allocation in the heap.*/
int insertStaticRecord(embedDBState* state, uint32_t key, uint32_t data) {
    // calloc dataSize bytes in heap.
    void* dataPtr = calloc(1, state->dataSize);
    // set dataPtr[0] to data
    ((uint32_t*)dataPtr)[0] = data;
    // insert into buffer, save result
    char result = embedDBPut(state, (void*)&key, (void*)dataPtr);
    // free dataPtr
    free(dataPtr);
    // return based on success
    return (result == 0) ? 0 : -1;
}

embedDBState* init_state() {
    embedDBState* state = (embedDBState*)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }

    // configure state variables
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->numSplinePoints = 20;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;

    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

    // check buffer was alloacted
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }

    // address level parameters
    state->numDataPages = 256;
    state->numIndexPages = 8;
    state->eraseSizeInPages = 4;

    // configure file interface
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);

    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;

    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    return state;
}

int main() {
    runUnityTests();
}

#ifdef ARDUINO

void setup() {
    delay(2000);
    setupBoard();
    main();
}

void loop() {}

#endif
