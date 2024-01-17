/******************************************************************************/
/**
 * @file        test_embedDB_var_data.cpp
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

embedDBState *state;
uint32_t numRecords = 1000;
uint32_t inserted = 0;

uint32_t i = 0;
uint32_t dataSizes[] = {4, 6, 8};

void test_init() {
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, embedDBInit(state, 0), "embedDBInit did not return 0");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->keySize, "Key size was changed during embedDBInit");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(dataSizes[i], state->dataSize, "Data size was changed during embedDBInit");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(state->keySize + state->dataSize + 4, state->recordSize, "State's record size is not correct");
}

void initState(uint32_t dataSize) {
    // Initialize EmbedDB State
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = dataSize;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate EmbedDB buffer.");
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->numVarPages = 1000;
    state->eraseSizeInPages = 4;
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->varFile = setupSDFile(varPath);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    embedDBResetStats(state);
}

void resetState() {
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

int insertRecords(uint32_t n) {
    char varData[] = "Testing 000...";
    uint64_t targetNum = inserted + n;
    for (uint64_t j = inserted; j < targetNum; j++) {
        varData[10] = (char)(j % 10) + '0';
        varData[9] = (char)((j / 10) % 10) + '0';
        varData[8] = (char)((j / 100) % 10) + '0';

        uint64_t data = j % 100;

        int result = embedDBPutVar(state, &j, &data, varData, 15);
        if (result != 0) {
            return result;
        }
        inserted++;
    }

    return 0;
}

void test_get_when_empty() {
    uint32_t key = 1, data;
    embedDBVarDataStream *varStream = NULL;
    int8_t result = embedDBGetVar(state, &key, &data, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBGetVar did not return -1 when the key was not found");
    if (varStream != NULL) {
        free(varStream);
    }
}

void test_get_when_1() {
    // Check that the record is correctly in the buffer
    uint32_t expectedKey = 0;
    uint64_t expectedData = 0;
    uint32_t expectedVarDataSize = 15;
    char expectedVarData[] = "Testing 000...";
    void *key = (int8_t *)state->buffer + EMBEDDB_DATA_WRITE_BUFFER * state->pageSize + state->headerSize;
    void *data = (int8_t *)key + state->keySize;
    uint32_t *varDataSize = (uint32_t *)((int8_t *)state->buffer + EMBEDDB_VAR_WRITE_BUFFER(state->parameters) * state->pageSize + state->variableDataHeaderSize);
    void *varData = (int8_t *)state->buffer + EMBEDDB_VAR_WRITE_BUFFER(state->parameters) * state->pageSize + state->variableDataHeaderSize + sizeof(uint32_t);

    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedKey, key, state->keySize, "Key was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, data, state->dataSize, "Data was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedVarDataSize, *varDataSize, "Vardata size was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedVarData, varData, 15, "Vardata was not correct with 1 record inserted");
}

void test_get_when_almost_almost_full_page() {
    // Check that page gasn't been written
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "EmbedDB should not have written a page yet");
    // Check that there is still space for another record
    TEST_ASSERT_EACH_EQUAL_CHAR_MESSAGE(0, (int8_t *)state->buffer + EMBEDDB_DATA_WRITE_BUFFER * state->pageSize + (state->pageSize - state->recordSize), state->recordSize, "There isn't space for another record in the buffer");
}

void test_get_when_almost_full_page() {
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "EmbedDB should not have written a page yet");
}

void test_get_when_full_page() {
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextDataPageId, "EmbedDB should have written a page by now");

    uint32_t key = 23;
    uint64_t expectedData = 23, data = 0;
    embedDBVarDataStream *varStream = NULL;
    int8_t result = embedDBGetVar(state, &key, &data, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB was unable to find data for a given key.");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, &data, state->dataSize, "embedDBGetVar did not return the correct fixed data");
    TEST_ASSERT_NOT_NULL_MESSAGE(varStream, "embedDBGetVar did not return vardata");
    char buf[20];
    uint32_t length = embedDBVarDataStreamRead(state, varStream, buf, 20);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, length, "Returned vardata was not the right length");
    char expected[] = "Testing 023...";
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expected, buf, 15, "embedDBGetVar did not return the correct vardata");

    if (varStream != NULL) {
        free(varStream);
    }
}

void test_get_when_all() {
    char expectedVarData[] = "Testing 000...";
    char buf[20];
    embedDBVarDataStream *varStream = NULL;
    for (uint32_t key = 0; key < numRecords; key++) {
        expectedVarData[10] = (char)(key % 10) + '0';
        expectedVarData[9] = (char)((key / 10) % 10) + '0';
        expectedVarData[8] = (char)((key / 100) % 10) + '0';
        uint64_t data = 0, expectedData = key % 100;

        int result = embedDBGetVar(state, &key, &data, &varStream);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, &data, state->dataSize, "embedDBGetVar did not return the correct fixed data");
        TEST_ASSERT_NOT_NULL_MESSAGE(varStream, "embedDBGetVar did not return vardata");
        uint32_t length = embedDBVarDataStreamRead(state, varStream, buf, 20);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, length, "Returned vardata was not the right length");
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVarData, buf, 15, "embedDBGetVar did not return the correct vardata");
        if (varStream != NULL) {
            free(varStream);
            varStream = NULL;
        }
    }
}

void test_insert_1() {
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertRecords(1), "embedDBPutVar was not successful when inserting a record");
}

void test_insert_lt_page() {
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, insertRecords(state->maxRecordsPerPage - inserted - 1), "Error while inserting records");
}

void test_insert_rest() {
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, insertRecords(numRecords - inserted), "Error while inserting records");
}

int runUnityTests() {
    UNITY_BEGIN();

    for (i = 0; i < sizeof(dataSizes) / sizeof(dataSizes[i]); i++) {
        // Setup state
        initState(dataSizes[i]);
        RUN_TEST(test_init);

        // Run tests
        RUN_TEST(test_get_when_empty);
        RUN_TEST(test_insert_1);
        RUN_TEST(test_get_when_1);
        RUN_TEST(test_insert_lt_page);
        RUN_TEST(test_get_when_almost_almost_full_page);
        RUN_TEST(test_insert_1);
        RUN_TEST(test_get_when_almost_full_page);
        RUN_TEST(test_insert_1);
        RUN_TEST(test_get_when_full_page);
        RUN_TEST(test_insert_rest);
        embedDBFlush(state);
        RUN_TEST(test_get_when_all);

        // Clean up state
        resetState();
    }

    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}
