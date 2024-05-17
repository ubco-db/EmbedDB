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

int insert_static_record(embedDBState* state, uint32_t key, uint32_t data);
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

// test ensures iterator checks written pages using keys after writing.
void test_iterator_flush_on_keys_int(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    uint32_t minKey = 1, maxKey = 36;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    int data_comparison = 111;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        int testData;
        memcpy(&testData, itData, sizeof(int));
        TEST_ASSERT_EQUAL(data_comparison, testData);
        data_comparison += 5;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using keys after writing.
void test_iterator_flush_on_keys_float(void) {
    // create key and data
    float key = 1;
    float data = 111.00;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    float itKey = 0;
    float itData[] = {0, 0, 0};
    uint32_t minKey = 1, maxKey = 36;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    float data_comparison = 111.00;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        int testData;
        memcpy(&testData, itData, sizeof(int));
        TEST_ASSERT_EQUAL(data_comparison, testData);
        data_comparison += 5;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks write buffer using keys without writing to file storage.
void test_iterator_no_flush_on_keys(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    // records all to be contained in buffer
    int recNum = 16;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t *itKey, *itData;
    key = 1;
    data = 111;

    uint32_t minKey = 1, maxKey = 15;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBInitIterator(state, &it);
    // test data
    while (embedDBNext(state, &it, &itKey, &itData)) {
        TEST_ASSERT_EQUAL(data, itData);
        data += 5;
    }
    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using data after writing.
void test_iterator_flush_on_data(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 111, maxData = 286;
    it.minData = &minData;
    it.maxData = &maxData;

    int key_comparison = 1;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(key_comparison, itKey);
        key_comparison += 1;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using data without flushing to storage
void test_iterator_no_flush_on_data(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 15;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 111, maxData = 186;
    it.minData = &minData;
    it.maxData = &maxData;

    int key_comparison = 1;

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
    RUN_TEST(test_iterator_flush_on_keys_int);
    RUN_TEST(test_iterator_flush_on_keys_float);
    RUN_TEST(test_iterator_no_flush_on_keys);
    RUN_TEST(test_iterator_flush_on_data);
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
int insert_static_record(embedDBState* state, uint32_t key, uint32_t data) {
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
    // init
    // size_t splineMaxError = 1;

    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    return state;
}
