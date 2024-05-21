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
    // create a key
    uint32_t key = 1;
    // save to buffer
    insertStaticRecord(state, key, 123);
    // flush to file storage
    embedDBFlush(state);
    // query data
    uint32_t return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(123, *return_data, "embedDBGet did not return the expected data for the provided key");
}

void embedDBGet_should_return_data_when_multiple_records_inserted_and_flushed_to_storage(void) {
    int numInserts = 100;
    for (int i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }
    embedDBFlush(state);
    uint32_t key = 93;
    uint32_t return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL_MESSAGE(193, *return_data, "Unable to retrieve data which was written to storage");
}

void embedDBGet_should_return_data_for_record_in_write_buffer(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insertStaticRecord(state, key, 123);
    // query data
    uint32_t return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(123, *return_data, "embedDBGet was unable to retrieve data still located in the write buffer");
}

void embedDBGet_should_return_data_for_record_when_multiple_records_are_inserted_in_write_buffer(void) {
    uint32_t numInserts = 31;
    for (uint32_t i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }
    uint32_t key = 30;
    uint32_t return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(130, *return_data, "embedDBGet was unable to retrieve the data for one of the records located in the write buffer");
}

void embedDBGet_should_return_data_for_records_in_file_storage_and_write_buffer(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insertStaticRecord(state, key, 154);
    // query data
    uint32_t return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test record is in buffer
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(154, *return_data, "embedDBGet was unable to retrieve the data for a record located in the write buffer");
    // flush
    embedDBFlush(state);
    // insert another record
    key = 2;
    insertStaticRecord(state, key, 12345);
    embedDBGet(state, &key, return_data);
    // test second record is retrieved from buffer
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12345, *return_data, "embedDBGet was unable to retrieve the data for a record located in the write buffer");
    // check if first record is retrieved from file storage
    key = 1;
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(154, *return_data, "embedDBGet was unable to retrieve the data for a record written to file storage after being queried in the buffer");
}

void embedDBGet_should_return_no_data_when_requested_key_greater_than_max_buffer_key(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // insert random records
    uint32_t numInserts = 8;
    for (uint32_t i = 0; i < numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }
    // query for max key not in database
    uint32_t key = 55;
    u_int32_t return_data[] = {0, 0, 0};
    // test if embedDBGet can't retrieve data
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, embedDBGet(state, &key, return_data), "embedDBGet returned data for a key that should not exist in the database");
}

void embedDBGet_should_return_not_found_when_key_is_less_then_min_key(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // insert random records
    uint32_t numInserts = 8;
    for (uint32_t i = 1; i <= numInserts; ++i) {
        insertStaticRecord(state, i, (i + 100));
    }
    // query for max key not in database
    uint32_t key = 0;
    u_int32_t return_data[] = {0, 0, 0};
    // test if embedDBGet can't retrieve data
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, embedDBGet(state, &key, return_data), "embedDBGet returned data for a key that is less than the min key in the database");
}

void embedDBGet_should_return_no_data_found_when_database_and_buffer_are_empty(void) {
    // create a key
    uint32_t key = 1;
    // allocate dataSize record in heap
    void* temp = calloc(1, state->dataSize);
    // query embedDB and returun pointer
    int8_t status = embedDBGet(state, &key, (void*)temp);
    // test
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, status, "embedDBGet returned data when there were no keys in the database or write buffer");
    free(temp);
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

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

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
    state->recordSize = 16;  // size of record in bytes
    state->keySize = 4;      // size of key in bytes
    state->dataSize = 12;    // size of data in bytes
    state->pageSize = 512;   // page size (I am sure this is in bytes)
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;  // size of the buffer in blocks (where I am assuming that a block is the same as a page size)
    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // check
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }
    // address level parameters
    state->numDataPages = 1000;
    state->numIndexPages = 48;
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
