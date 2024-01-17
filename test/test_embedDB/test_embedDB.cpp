/******************************************************************************/
/**
 * @file        test_embedDB.cpp
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Test EmbedDB initialization, insertion, and querying.
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

embedDBState *state;

void setupEmbedDB() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBstate.");
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->numSplinePoints = 2;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");
    state->numDataPages = 1000;
    state->parameters = EMBEDDB_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp(void) {
    setupEmbedDB();
}

void tearDown(void) {
    free(state->buffer);
    embedDBClose(state);
    tearDownSDFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void embedDB_initial_configuration_is_correct() {
    TEST_ASSERT_NOT_NULL_MESSAGE(state->dataFile, "EmbedDB file was not initialized correctly.");
    TEST_ASSERT_NULL_MESSAGE(state->varFile, "EmbedDB varFile was intialized for non-variable data.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "EmbedDB nextDataPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(6, state->headerSize, "EmbedDB headerSize was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->minKey, "EmbedDB minKey was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedPageId, "EmbedDB bufferedPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedIndexPageId, "EmbedDB bufferedIndexPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedVarPage, "EmbedDB bufferedVarPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT16_MESSAGE(63, state->maxRecordsPerPage, "EmbedDB maxRecordsPerPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(63, state->maxError, "EmbedDB maxError was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1000, state->numDataPages, "EmbedDB numDataPages was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "EmbedDB minDataPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->avgKeyDiff, "EmbedDB avgKeyDiff was not initialized correctly.");
    TEST_ASSERT_NOT_NULL_MESSAGE(state->spl, "EmbedDB spline was not initialized correctly.");
}

void embedDB_put_inserts_single_record_correctly() {
    uint32_t key = 15648;
    int32_t data = 27335;
    int8_t result = embedDBPut(state, &key, &data);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPut did not correctly insert data (returned non-zero code)");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15648, state->minKey, "embedDBPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "embedDBPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, EMBEDDB_GET_COUNT(state->buffer), "embedDBPut did not increment count in buffer correctly.");
    uint32_t embedDBPutResultKey = 0;
    int32_t embedDBPutResultData = 0;
    memcpy(&embedDBPutResultKey, (int8_t *)state->buffer + 6, 4);
    memcpy(&embedDBPutResultData, (int8_t *)state->buffer + 10, 4);
    TEST_ASSERT_EQUAL_INT32_MESSAGE(15648, embedDBPutResultKey, "embedDBPut did not put correct key value in buffer.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(27335, embedDBPutResultData, "embedDBPut did not put correct data value in buffer.");
}

void embedDB_put_inserts_eleven_records_correctly() {
    uint32_t embedDBPutResultKey = 0;
    int32_t embedDBPutResultData = 0;
    uint32_t key = 16321;
    int32_t data = 28361;
    for (int i = 0; i < 11; i++) {
        key += i;
        data %= (i + 1);
        int8_t result = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPut did not correctly insert data (returned non-zero code)");
        memcpy(&embedDBPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(&embedDBPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key, embedDBPutResultKey, "embedDBPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, embedDBPutResultData, "embedDBPut did not put correct data value in buffer.");
    }
    uint64_t expectedMinimumKey = 16321;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinimumKey, &state->minKey, sizeof(uint64_t), "embedDBPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "embedDBPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(11, EMBEDDB_GET_COUNT(state->buffer), "embedDBPut did not increment count in buffer correctly.");
}

void embedDB_put_inserts_one_page_of_records_correctly() {
    uint32_t embedDBPutResultKey = 0;
    int32_t embedDBPutResultData = 0;
    uint32_t key = 100;
    int32_t data = 724;
    for (int i = 0; i < 63; i++) {
        key += i;
        data %= (i + 1);
        int8_t result = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPut did not correctly insert data (returned non-zero code)");
        memcpy(&embedDBPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(&embedDBPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key, embedDBPutResultKey, "embedDBPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, embedDBPutResultData, "embedDBPut did not put correct data value in buffer.");
    }
    uint64_t expectedMinKey = 100;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinKey, &state->minKey, sizeof(uint64_t), "embedDBPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "embedDBPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(63, EMBEDDB_GET_COUNT(state->buffer), "embedDBPut did not increment count in buffer correctly.");
}

void embedDB_put_inserts_one_more_than_one_page_of_records_correctly() {
    uint32_t embedDBPutResultKey = 0;
    int32_t embedDBPutResultData = 0;
    uint32_t key = 4444444;
    int32_t data = 96875;
    for (int32_t i = 0; i < 64; i++) {
        key += i;
        data %= (i + 1);
        int8_t result = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPut did not correctly insert data (returned non-zero code)");
    }
    uint64_t expectedMinKey = 4444444;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedMinKey, &state->minKey, sizeof(uint64_t), "embedDBPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextDataPageId, "embedDBPut did not move to next page after writing the first page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, EMBEDDB_GET_COUNT(state->buffer), "embedDBPut did not reset buffer count to correct value after writing the page");
}

void iteratorReturnsCorrectRecords(void) {
    int32_t numRecordsToInsert = 1000;
    for (uint32_t key = 0; key < numRecordsToInsert; key++) {
        uint32_t data = key % 100;
        embedDBPut(state, &key, &data);
    }
    embedDBFlush(state);

    // Query records using an iterator
    embedDBIterator it;
    uint32_t minData = 23, maxData = 38, minKey = 32;
    it.minData = &minData;
    it.maxData = &maxData;
    it.minKey = &minKey;
    it.maxKey = NULL;
    embedDBInitIterator(state, &it);

    uint32_t numRecordsRead = 0;
    uint32_t key, data;
    while (embedDBNext(state, &it, &key, &data)) {
        numRecordsRead++;
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minKey, key, "Key is not in range of query");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key % 100, data, "Record contains the wrong data");
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minData, data, "Data is not in range of query");
        TEST_ASSERT_LESS_OR_EQUAL_UINT32_MESSAGE(maxData, data, "Data is not in range of query");
    }

    // Check that the correct number of records were read
    uint32_t expectedNum = 0;
    for (uint32_t i = 0; i < numRecordsToInsert; i++) {
        if (it.minKey != NULL && i < *(uint32_t *)it.minKey) continue;
        if (it.maxKey != NULL && i > *(uint32_t *)it.maxKey) continue;
        if (it.minData != NULL && i % 100 < *(uint32_t *)it.minData) continue;
        if (it.maxData != NULL && i % 100 > *(uint32_t *)it.maxData) continue;
        expectedNum++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedNum, numRecordsRead, "Iterator did not read the correct number of records");
    embedDBCloseIterator(&it);
}

int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(embedDB_initial_configuration_is_correct);
    RUN_TEST(embedDB_put_inserts_single_record_correctly);
    RUN_TEST(embedDB_put_inserts_eleven_records_correctly);
    RUN_TEST(embedDB_put_inserts_one_page_of_records_correctly);
    RUN_TEST(embedDB_put_inserts_one_more_than_one_page_of_records_correctly);
    RUN_TEST(iteratorReturnsCorrectRecords);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}
