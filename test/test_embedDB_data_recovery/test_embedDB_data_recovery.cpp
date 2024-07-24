/******************************************************************************/
/**
 * @file        test_embedDB_data_recovery.cpp
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Test for EmbedDB data recovery.
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
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
/* On the desktop platform, there is a file interface which simulates "erasing" by writing out all 1's to the location in the file ot be erased */
#define MOCK_ERASE_INTERFACE
#endif

#include "unity.h"

#define UNITY_SUPPORT_64

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
#ifdef MOCK_ERASE_INTERFACE
    state->fileInterface = getMockEraseFileInterface();
#else
    state->fileInterface = getFileInterface();
#endif
    state->dataFile = setupFile(DATA_FILE_PATH);

    state->numDataPages = 92;
    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void initalizeEmbedDBFromFile(void) {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate EmbedDB state.");
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

/* configure EmbedDB storage */
#ifdef MOCK_ERASE_INTERFACE
    state->fileInterface = getMockEraseFileInterface();
#else
    state->fileInterface = getFileInterface();
#endif
    state->dataFile = setupFile(DATA_FILE_PATH);

    state->numDataPages = 92;
    state->eraseSizeInPages = 4;
    state->parameters = 0;
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

void insertRecordsLinearly(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = embedDBPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDBPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void insertRecordsParabolic(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += i;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = embedDBPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDBPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void embedDB_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly() {
    insertRecordsLinearly(9, 20230614, 1135);
    tearDown();
    initalizeEmbedDBFromFile();
    uint64_t expectedMinKey = 10;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinKey, &state->minKey, sizeof(uint64_t), "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(65, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
}

/* The setup function allocates 93 pages, so check to make sure it initalizes correctly when it is full */
void embedDB_parameters_initializes_from_data_file_with_ninety_two_pages_correctly() {
    insertRecordsLinearly(3456, 2548, 3865);
    tearDown();
    initalizeEmbedDBFromFile();
    uint64_t expectedMinKey = 3457;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinKey, &state->minKey, sizeof(uint64_t), "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(92, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
}

void embedDB_parameters_initializes_from_data_file_with_ninety_three_pages_correctly() {
    insertRecordsLinearly(1645, 2548, 3907);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 1814;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
}

void embedDB_parameters_initializes_correctly_from_data_file_with_four_hundred_sixteen_previous_page_inserts() {
    insertRecordsLinearly(2000, 11205, 17473);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 15777;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(416, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(328, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
}

void embedDB_parameters_initializes_correctly_from_data_file_with_no_data() {
    tearDown();
    initalizeEmbedDBFromFile();
    uint64_t expectedMinKey = UINT32_MAX;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinKey, &state->minKey, sizeof(uint64_t), "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(92, state->numAvailDataPages, "EmbedDB numAvailDataPages is not initialized correctly.");
}

void embedDB_inserts_correctly_into_data_file_after_reload() {
    insertRecordsLinearly(1000, 5600, 3655);
    tearDown();
    initalizeEmbedDBFromFile();
    insertRecordsLinearly(4654, 10, 43);
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 1001;
    int64_t data = 5601;
    char message[100];
    /* Records inserted before reload */
    for (int i = 0; i < 3654; i++) {
        int8_t getResult = embedDBGet(state, &key, recordBuffer);
        snprintf(message, 100, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, ((int64_t *)recordBuffer), state->dataSize, message);
        key++;
        data++;
    }
    /* Records inserted after reload */
    data = 11;
    for (int i = 0; i < 42; i++) {
        int8_t getResult = embedDBGet(state, &key, recordBuffer);
        snprintf(message, 100, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "EmbedDB get did not return correct data for a record inserted after reloading (key %li).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, ((int64_t *)recordBuffer), state->dataSize, message);
        key++;
        data++;
    }
    free(recordBuffer);
}

void embedDB_correctly_gets_records_after_reload_with_wrapped_data() {
    insertRecordsLinearly(0, 0, 13758);
    embedDBFlush(state);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 10081;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 10081;
    int64_t data = 10081;
    char message[100];
    int8_t getResult = 0;
    /* Records inserted before reload */
    for (int i = 0; i < 3678; i++) {
        getResult = embedDBGet(state, &key, recordBuffer);
        snprintf(message, 100, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, ((int64_t *)recordBuffer), state->dataSize, message);
        key++;
        data++;
    }
    getResult = embedDBGet(state, &key, recordBuffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, "embedDBGet returned a record that does not exist.");
    free(recordBuffer);
}

void embedDB_prevents_duplicate_inserts_after_reload() {
    insertRecordsLinearly(0, 8751, 1975);
    tearDown();
    initalizeEmbedDBFromFile();
    int32_t key = 1974;
    int64_t data = 1974;
    int8_t insertResult = embedDBPut(state, &key, &data);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, insertResult, "EmbedDB inserted a duplicate key.");
}

void embedDB_queries_correctly_with_non_liner_data_after_reload() {
    insertRecordsParabolic(1000, 367, 4495);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 227128;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 227128;
    int64_t data = 1040;
    char message[100];
    /* Records inserted before reload */
    int32_t increment = 673;
    uint32_t i;
    for (i = 0; i < 3822; i++) {
        int8_t getResult = embedDBGet(state, &key, recordBuffer);
        snprintf(message, 80, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 100, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, recordBuffer, sizeof(int64_t), message);
        key += increment;
        data += 1;
        increment++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3822, i, "Loop to return records did not run the correct number of times.");
    free(recordBuffer);
}

void embedDB_recovery_algorithm_wraps_when_skipping_to_next_block() {
    insertRecordsLinearly(0, 0, 7560);
    embedDBFlush(state);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 3865;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(180, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(92, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
}

void embedDB_recovery_algorithm_functions_correctly_when_have_wrapped_but_at_the_end_of_storage() {
    insertRecordsLinearly(0, 0, 7728);
    embedDBFlush(state);
    tearDown();
    initalizeEmbedDBFromFile();
    uint32_t expectedMinKey = 3865;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(184, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(92, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");

    /* Check that we can still insert records properly */
    int32_t key = 10001;
    int64_t actualData = 0;
    int64_t expectedData = 123457;
    insertRecordsLinearly(10000, 123456, 42);
    embedDBFlush(state);

    /* Check that database incremented correctly and we can fetch data */
    expectedMinKey = 4033;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedMinKey, state->minKey, "EmbedDB minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(185, state->nextDataPageId, "EmbedDB nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(96, state->minDataPageId, "EmbedDB minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->numAvailDataPages, "EmbedDB numAvailDataPages is not correctly initialized.");
    int8_t result = embedDBGet(state, &key, &actualData);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBGet did not return the data for a key inserted after recovery.");
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(int64_t), "embedDBGet did not return the correct data for a key inserted after recovery.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDB_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly);
    RUN_TEST(embedDB_parameters_initializes_from_data_file_with_ninety_two_pages_correctly);
    RUN_TEST(embedDB_parameters_initializes_from_data_file_with_ninety_three_pages_correctly);
    RUN_TEST(embedDB_parameters_initializes_correctly_from_data_file_with_four_hundred_sixteen_previous_page_inserts);
    RUN_TEST(embedDB_inserts_correctly_into_data_file_after_reload);
    RUN_TEST(embedDB_correctly_gets_records_after_reload_with_wrapped_data);
    RUN_TEST(embedDB_prevents_duplicate_inserts_after_reload);
    RUN_TEST(embedDB_queries_correctly_with_non_liner_data_after_reload);
    RUN_TEST(embedDB_parameters_initializes_correctly_from_data_file_with_no_data);
    RUN_TEST(embedDB_recovery_algorithm_wraps_when_skipping_to_next_block);
    RUN_TEST(embedDB_recovery_algorithm_functions_correctly_when_have_wrapped_but_at_the_end_of_storage);
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
