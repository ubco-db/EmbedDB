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

#include <iostream>

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
int8_t insertRecordFloatData(embedDBState* state, uint32_t key, float data);
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

void embedDBIterator_should_return_records_in_storage_and_in_write_buffer(void) {
    /* create key and data to be inserted */
    uint32_t key = 1;
    uint32_t data = 111;
    uint32_t numberOfRecordsToInsert = 36;

    /* insert records into database */
    for (uint32_t i = 0; i < numberOfRecordsToInsert; ++i) {
        insertStaticRecord(state, key, data);
        key += 1;
        data += 5;
    }

    /* initalize iterator */
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    uint32_t minKey = 1, maxKey = 36;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    uint32_t expectedDataValue = 111;

    embedDBInitIterator(state, &it);
    char message[100];

    /* test if correct data values returned */
    uint32_t numRecordsReturned = 0;
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        uint32_t actualDataValue;
        memcpy(&actualDataValue, itData, sizeof(int));
        snprintf(message, 100, "embedDBIterator returned the wrong data value for key %li.", key);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataValue, actualDataValue, message);
        expectedDataValue += 5;
        numRecordsReturned += 1;
    }

    /* test that the correct number of records was returned by the iterator */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(36, numRecordsReturned, "embedDBIterator did not return the expected number of records");

    /* close the iterator */
    embedDBCloseIterator(&it);
}

void embedDBIterator_should_return_records_in_storage_and_in_write_buffer_with_float_data(void) {
    /* create key and data for record insertion */
    uint32_t key = 2024;
    float data = -15.24;
    uint32_t numberOfRecordsToInsert = 72;

    /* insert records into database */
    for (uint32_t i = 0; i < numberOfRecordsToInsert; ++i) {
        insertRecordFloatData(state, key, data);
        key += 3;
        data += 5.18;
    }

    /* initialize iterator */
    embedDBIterator it;
    uint32_t actualKeyValue = 0;
    float returnedDataValue[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(state, &it);

    /* initialize variables for expected test values */
    float expectedDataValue = -15.24;
    uint32_t expectedKeyValue = 2024;
    uint32_t numRecordsRetrieved = 0;
    char message[100];
    float actualDataValue = 0;

    /* test data and keys are returned correctly */
    while (embedDBNext(state, &it, (void**)&actualKeyValue, (void**)&returnedDataValue)) {
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedKeyValue, actualKeyValue, "embedDBIterator returned an unexpected key value");
        snprintf(message, 100, "embedDBIterator did not return the correct data for key %li).", expectedKeyValue);
        memcpy(&actualDataValue, returnedDataValue, sizeof(float));
        TEST_ASSERT_EQUAL_FLOAT_MESSAGE(expectedDataValue, actualDataValue, message);
        expectedKeyValue += 3;
        expectedDataValue += 5.18;
        numRecordsRetrieved += 1;
    }

    /* test that the correct number of records was returned by the iterator */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(72, numRecordsRetrieved, "embedDBIterator did not return the expected number of records");

    /* tear down iterator */
    embedDBCloseIterator(&it);
}

void embedDBIterator_should_return_keys_in_write_buffer_when_no_data_has_been_flushed_to_storage(void) {
    /* initialize records */
    uint32_t key = 1000;
    uint32_t data = 2048;
    uint32_t numberOfRecordsToInsert = 16;
    for (uint32_t i = 0; i < numberOfRecordsToInsert; ++i) {
        insertStaticRecord(state, key, data);
        key += 1;
        data += 15;
    }

    /* initialize iterator */
    embedDBIterator it;
    uint32_t* actualKeyValue;
    uint32_t minKey = 1000, maxKey = 1015;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(state, &it);

    /* expected values for test */
    key = 1000;
    data = 2048;
    uint32_t numberOfRecordsRetrieved = 0;
    int8_t* dataBuffer = (int8_t*)malloc(state->dataSize);
    uint32_t acutalDataValue = 0;
    char message[100];

    while (embedDBNext(state, &it, &actualKeyValue, dataBuffer)) {
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key, actualKeyValue, "embedDBIterator returned an unexpected key value");
        memcpy(&acutalDataValue, dataBuffer, sizeof(uint32_t));
        snprintf(message, 100, "embedDBIterator did not return the correct data for key %li).", key);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(data, acutalDataValue, message);
        data += 15;
        key += 1;
        numberOfRecordsRetrieved += 1;
    }

    /* check that the correct number of records is returned */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, numberOfRecordsRetrieved, "embedDBIterator did not return the expected number of records");

    /* tear down */
    embedDBCloseIterator(&it);
    free(dataBuffer);
}

void embedDBIterator_should_filter_and_rechieve_records_by_data_value(void) {
    /* insert records in database */
    uint32_t key = 1;
    uint32_t data = 111;
    uint32_t numberOfRecordsToInsert = 48;
    for (uint32_t i = 0; i < numberOfRecordsToInsert; ++i) {
        insertStaticRecord(state, key, data);
        key += 1;
        data += 5;
    }

    /* initialize iterator */
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 227, maxData = 310;
    it.minData = &minData;
    it.maxData = &maxData;
    embedDBInitIterator(state, &it);

    /* expected values for tests */
    uint32_t expectedKeyValue = 25;
    uint32_t expectedDataValue = 231;
    uint32_t numberOfRecordsRetrieved = 0;
    char message[100];

    /* assert returned records have correct values */
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedKeyValue, itKey, "embedDBIterator returned a key value which should have been filtered out");
        snprintf(message, 100, "embedDBIterator did not return the correct data for key %li).", expectedKeyValue);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedDataValue, itData[0], message);
        expectedKeyValue += 1;
        expectedDataValue += 5;
        numberOfRecordsRetrieved += 1;
    }

    /* assert that the correct number of records was returned by the iterator */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, numberOfRecordsRetrieved, "embedDBIterator did not return the correct number of records based on the filters applied");

    /* tear down iterator */
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using data without flushing to storage
void test_iterator_no_flush_on_data(void) {
    /* insert records */
    uint32_t key = 1;
    uint32_t data = 111;
    uint32_t numberOfRecordsToInsert = 15;
    for (uint32_t i = 0; i < numberOfRecordsToInsert; ++i) {
        insertStaticRecord(state, key, data);
        key += 1;
        data += 5;
    }

    /* initialize iterator */
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 111, maxData = 186;
    it.minData = &minData;
    it.maxData = &maxData;

    uint32_t key_comparison = 1;
    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(key_comparison, itKey);
        key_comparison += 1;
    }

    // close
    embedDBCloseIterator(&it);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBIterator_should_return_records_in_storage_and_in_write_buffer);
    RUN_TEST(embedDBIterator_should_return_records_in_storage_and_in_write_buffer_with_float_data);
    RUN_TEST(embedDBIterator_should_return_keys_in_write_buffer_when_no_data_has_been_flushed_to_storage);
    RUN_TEST(embedDBIterator_should_filter_and_rechieve_records_by_data_value);
    RUN_TEST(test_iterator_no_flush_on_data);
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

int8_t insertRecordFloatData(embedDBState* state, uint32_t key, float data) {
    // calloc dataSize bytes in heap.
    void* dataPtr = calloc(1, state->dataSize);

    // set dataPtr[0] to data
    ((float*)dataPtr)[0] = data;

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
