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
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"
#endif

#include "unity.h"

embedDBState *state;
void *indexWriteBufferBeforeTearDown = NULL;
const int16_t RECOVERY_PARAMETERS = EMBEDDB_USE_INDEX | EMBEDDB_USE_BMAP;

void setupEmbedDB(int16_t parameters) {
    /* This setup results in having 63 records per page */
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 8;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

    /* setup EmbedDB storage */
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(DATA_FILE_PATH);
    state->indexFile = setupFile(INDEX_FILE_PATH);

    state->numDataPages = 10000;
    state->eraseSizeInPages = 2;
    state->numIndexPages = 16;
    state->bitmapSize = 1;
    state->parameters = parameters;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp() {
    /* Setup extra buffer for testing */
    indexWriteBufferBeforeTearDown = malloc(state->pageSize);

    int16_t parameters = EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA | EMBEDDB_USE_BMAP;
    setupEmbedDB(parameters);
}

void tearDownEmbedDB() {
    free(state->buffer);
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state);
}

void tearDown() {
    tearDownEmbedDB();
    free(indexWriteBufferBeforeTearDown);
}

void insertRecordsLinearly(int32_t startingKey, uint32_t numRecords) {
    int32_t key = startingKey;
    int32_t data = 0;
    for (uint32_t i = 0; i < numRecords; i++) {
        int8_t result = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB Put did not correctly insert data (returned non-zero code)");
        key++;
        if (i % 2 == 0) {
            data++;
        }
        if (data % 110 == 0) {
            data = 0;
        }
    }
}

void embedDB_index_file_correctly_reloads_with_no_data() {
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(496, state->maxIdxRecordsPerPage, "EmbedDB maxIdxRecordsPerPage was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when no data was present in the index file.");

    /* Check that index buffer also has no records. */
    void *indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    count_t numIndices = EMBEDDB_GET_COUNT(indexWriteBuffer);
    TEST_ASSERT_EQUAL_UINT16(0, numIndices);
}

void embedDBFlush_should_not_flush_index_pages() {
    /* Check that there is the correct number of indicies in buffer before flushing */
    insertRecordsLinearly(100, 24948);
    void *buffer = (int8_t *)state->buffer + (state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER);
    count_t recordCount = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(395, recordCount, "Count of data indicies was incorrect before flushing to storage.");

    /* Flush to storage */
    embedDBFlush(state);

    /* Check that we only added one for the new page */
    recordCount = EMBEDDB_GET_COUNT(buffer);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(396, recordCount, "Count of data indicies was incorrect before flushing to storage.");
}

void embedDB_index_file_correctly_reloads_with_one_page_of_data() {
    insertRecordsLinearly(100, 31312);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when one index page was present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_four_pages_of_data() {
    insertRecordsLinearly(100, 125056);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_eight_pages_of_data() {
    insertRecordsLinearly(100, 250111);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_sixteen_pages_of_data() {
    insertRecordsLinearly(100, 500222);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void embedDB_index_file_correctly_reloads_with_seventeen_pages_of_data() {
    insertRecordsLinearly(100, 532288);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(17, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void embedDBIndexRecovery_should_recover_indicies_in_buffer_with_no_index_pages_written() {
    /* Inerst records into embedDB */
    insertRecordsLinearly(100, 11907);
    embedDBFlush(state);

    /* Copy current buffer into another place to compare with after teardown */
    void *indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    memcpy(indexWriteBufferBeforeTearDown, indexWriteBuffer, state->pageSize);

    /* Tear down and recover embedDB */
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);

    /* Check that the index parameters are what is expected when no pages have been written to storage yet */
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");

    /* Check that index buffer is recovered correctly */
    indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    count_t numIndices = EMBEDDB_GET_COUNT(indexWriteBuffer);
    TEST_ASSERT_EQUAL_UINT16(189, numIndices);

    /* Check that the bitmap is correct */
    uint8_t expectedBitmap = 128 | 64 | 32 | 16;
    uint8_t actualBitmap = 0;
    memcpy(&actualBitmap, (uint8_t *)indexWriteBuffer + EMBEDDB_IDX_HEADER_SIZE, state->bitmapSize);
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(expectedBitmap, actualBitmap, "embedDBIndexRecovery did not correctly recover the bitmap for the first data index.");

    /* Compare with buffer before tearDown */
    TEST_ASSERT_EQUAL_MEMORY(indexWriteBufferBeforeTearDown, indexWriteBuffer, state->pageSize);
}

void embedDBIndexRecovery_should_recover_indicies_in_buffer_with_with_seven_pages_written() {
    /* This number of inserts results in 7 full index pages being written and then three data pages whose indicies are only in the buffer before tearDown */
    insertRecordsLinearly(100, 218925);
    embedDBFlush(state);

    /* Copy current buffer into another place to compare with after teardown */
    void *indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    memcpy(indexWriteBufferBeforeTearDown, indexWriteBuffer, state->pageSize);

    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(9, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");

    /* Check that index buffer also has correct data. */
    indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    count_t numIndices = EMBEDDB_GET_COUNT(indexWriteBuffer);
    TEST_ASSERT_EQUAL_UINT16(3, numIndices);

    /* Check that the bitmap is correct */
    uint8_t expectedBitmap = 32 | 16 | 8 | 4;
    uint8_t actualBitmap = 0;
    memcpy(&actualBitmap, (uint8_t *)indexWriteBuffer + EMBEDDB_IDX_HEADER_SIZE, state->bitmapSize);
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(expectedBitmap, actualBitmap, "embedDBIndexRecovery did not correctly recover the bitmap for the first data index.");

    /* Compare with buffer from before recovery */
    TEST_ASSERT_EQUAL_MEMORY(indexWriteBufferBeforeTearDown, indexWriteBuffer, state->pageSize);
}

void embedDBIndexRecovery_should_recover_indicies_in_buffer_with_sixteen_pages_of_data_written() {
    /* Write out 16 index pages and have 289 on the datafile but not buffered */
    insertRecordsLinearly(4000, 518175);
    embedDBFlush(state);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");

    void *indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    count_t numIndices = EMBEDDB_GET_COUNT(indexWriteBuffer);
    TEST_ASSERT_EQUAL_UINT16(289, numIndices);

    /* Check that the bitmap is correct */
    uint8_t expectedBitmap = 2;
    uint8_t actualBitmap = 0;
    memcpy(&actualBitmap, (uint8_t *)indexWriteBuffer + EMBEDDB_IDX_HEADER_SIZE, state->bitmapSize);
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(expectedBitmap, actualBitmap, "embedDBIndexRecovery did not correctly recover the bitmap for the first data index.");
}

void embedDBIndexRecovery_should_recover_indicies_in_buffer_with_21_pages_of_data_written() {
    /* 21 pages of index written and then five extra indicies */
    insertRecordsLinearly(4000, 656523);
    embedDBFlush(state);
    tearDownEmbedDB();
    setupEmbedDB(RECOVERY_PARAMETERS);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(21, state->nextIdxPageId, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->numAvailIndexPages, "EmbedDB nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(6, state->minIndexPageId, "EmbedDB minIndexPageId was initialized incorrectly when four index pages were present in the index file.");

    void *indexWriteBuffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_INDEX_WRITE_BUFFER;
    count_t numIndices = EMBEDDB_GET_COUNT(indexWriteBuffer);
    TEST_ASSERT_EQUAL_UINT16(5, numIndices);

    uint8_t expectedBitmap = 1 | 128 | 2;
    uint8_t actualBitmap = 0;
    memcpy(&actualBitmap, (uint8_t *)indexWriteBuffer + EMBEDDB_IDX_HEADER_SIZE + state->bitmapSize * (numIndices - 1), state->bitmapSize);
    TEST_ASSERT_EQUAL_UINT8_MESSAGE(expectedBitmap, actualBitmap, "embedDBIndexRecovery did not correctly recover the bitmap for the first data index.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDB_index_file_correctly_reloads_with_no_data);
    RUN_TEST(embedDBFlush_should_not_flush_index_pages);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_one_page_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_four_pages_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_eight_pages_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_sixteen_pages_of_data);
    RUN_TEST(embedDB_index_file_correctly_reloads_with_seventeen_pages_of_data);
    RUN_TEST(embedDBIndexRecovery_should_recover_indicies_in_buffer_with_no_index_pages_written);
    RUN_TEST(embedDBIndexRecovery_should_recover_indicies_in_buffer_with_with_seven_pages_written);
    RUN_TEST(embedDBIndexRecovery_should_recover_indicies_in_buffer_with_sixteen_pages_of_data_written);
    RUN_TEST(embedDBIndexRecovery_should_recover_indicies_in_buffer_with_21_pages_of_data_written);
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
