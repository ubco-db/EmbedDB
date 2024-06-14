/******************************************************************************/
/**
 * @file        test_embedDB_index_recovery.cpp
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Test for EmbedDB index recovery.
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

embedDBState *state;

void setupEmbedDB() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->numDataPages = 10000;
    state->eraseSizeInPages = 2;
    state->numIndexPages = 4;
    state->bitmapSize = 1;
    state->parameters = EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void initalizeEmbedDBFromFile() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate EmbedDB buffer.");
    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->numDataPages = 10000;
    state->numIndexPages = 4;
    state->eraseSizeInPages = 2;
    state->bitmapSize = 1;
    state->parameters = EMBEDDB_USE_INDEX;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp() {
    setupEmbedDB();
}

void tearDown() {
    free(state->buffer);
    embedDBClose(state);
    tearDownSDFile(state->dataFile);
    tearDownSDFile(state->indexFile);
    free(state->fileInterface);
    free(state);
}

void insertRecordsLinearly(int32_t startingKey, int32_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int32_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = embedDBPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB Put did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void embedDB_index_file_correctly_reloads_with_no_data() {
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(496, state->maxIdxRecordsPerPage, "EmbedDB maxIdxRecordsPerPage was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when no data was present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_one_page_of_data() {
    insertRecordsLinearly(100, 100, 31312);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when one index page was present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_four_pages_of_data() {
    insertRecordsLinearly(100, 100, 125056);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_eleven_pages_of_data() {
    insertRecordsLinearly(100, 100, 343792);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(11, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDB_index_file_correctly_reloads_with_no_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_one_page_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_four_pages_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_eleven_pages_of_data);
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
