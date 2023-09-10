/******************************************************************************/
/**
 * @file		test_embedDB_var_data_recovery.cpp
 * @author		EmbedDB Team (See Authors.md)
 * @brief		Test EmbedDB variable length data recovery.
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
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

void setupEmbedDB() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void initalizeEmbedDBFromFile(void) {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_USE_VDATA;
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
    tearDownSDFile(state->varFile);
    free(state->fileInterface);
    free(state);
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    char variableData[13] = "Hello World!";
    for (int32_t i = 0; i < numberOfRecords; i++) {
        key += 1;
        data += 1;
        int8_t insertResult = embedDBPutVar(state, &key, &data, variableData, 13);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "EmbedDB failed to insert data.");
    }
}

void embedDB_variable_data_page_numbers_are_correct() {
    insertRecords(1429, 1444, 64);
    /* Number of records * average data size % page size */
    uint32_t numberOfPagesExpected = 69;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfPagesExpected - 1, state->nextVarPageId, "EmbedDB next variable data logical page number is incorrect.");
    uint32_t pageNumber;
    void *buffer = (int8_t *)state->buffer + state->pageSize * EMBEDDB_VAR_READ_BUFFER(state->parameters);
    for (uint32_t i = 0; i < numberOfPagesExpected - 1; i++) {
        readVariablePage(state, i);
        memcpy(&pageNumber, buffer, sizeof(id_t));
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(i, pageNumber, "EmbedDB variable data did not have the correct page number.");
    }
}

void embedDB_variable_data_reloads_with_no_data_correctly() {
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "EmbedDB variableDataHeaderSize did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->currentVarLoc, "EmbedDB currentVarLoc did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "EmbedDB minVarRecordId did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(75, state->numAvailVarPages, "EmbedDB numAvailVarPages did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextVarPageId, "EmbedDB nextVarPageId did not have the correct value after initializing variable data from a file with no records.");
}

void embedDB_variable_data_reloads_with_one_page_of_data_correctly() {
    insertRecords(30, 100, 10);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(520, state->currentVarLoc, "EmbedDB currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "EmbedDB minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(74, state->numAvailVarPages, "EmbedDB numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextVarPageId, "EmbedDB nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void embedDB_variable_data_reloads_with_sixteen_pages_of_data_correctly() {
    insertRecords(337, 1648, 10);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8200, state->currentVarLoc, "EmbedDB currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "EmbedDB minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(59, state->numAvailVarPages, "EmbedDB numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextVarPageId, "EmbedDB nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void embedDB_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly() {
    insertRecords(2227, 100, 10);
    tearDown();
    initalizeEmbedDBFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15880, state->currentVarLoc, "EmbedDB currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(773, state->minVarRecordId, "EmbedDB minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailVarPages, "EmbedDB numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(106, state->nextVarPageId, "EmbedDB nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void embedDB_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly() {
    int32_t key = 1000;
    int32_t data = 10;
    insertRecords(651, key, data);
    embedDBFlush(state);
    tearDown();
    initalizeEmbedDBFromFile();
    int32_t recordData = 0;
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    char message[100];
    embedDBVarDataStream *stream = NULL;
    key = 1001;
    data = 11;
    /* Records inserted before reload */
    for (int i = 0; i < 650; i++) {
        int8_t getResult = embedDBGetVar(state, &key, &recordData, &stream);
        snprintf(message, 100, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 100, "EmbedDB get var returned null stream for key %li.", key);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, variableDataBuffer, 13);
        snprintf(message, 100, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, recordData, message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(message, 100, "EmbedDB get var did not return the correct variable data for key %li.", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(variableData, variableDataBuffer, 13, message);
        key++;
        data++;
        free(stream);
    }
}

void embedDB_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly() {
    int32_t key = 6798;
    int32_t data = 13467895;
    insertRecords(5187, key, data);
    embedDBFlush(state);
    tearDown();
    initalizeEmbedDBFromFile();
    int32_t recordData = 0;
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    char message[120];
    embedDBVarDataStream *stream = NULL;
    key = 9277;
    data = 13470374;
    /* Records inserted before reload */
    for (int i = 0; i < 2708; i++) {
        int8_t getResult = embedDBGetVar(state, &key, &recordData, &stream);
        if (i > 1163) {
            snprintf(message, 120, "EmbedDB get encountered an error fetching the data for key %li.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 120, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, recordData, message);
            snprintf(message, 120, "EmbedDB get var returned null stream for key %li.", key);
            TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
            uint32_t streamBytesRead = embedDBVarDataStreamRead(state, stream, variableDataBuffer, 13);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
            snprintf(message, 120, "EmbedDB get var did not return the correct variable data for key %li.", key);
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(variableData, variableDataBuffer, 13, message);
            free(stream);
        } else {
            snprintf(message, 120, "EmbedDB get encountered an error fetching the data for key %li. The var data was not detected as being overwritten.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(1, getResult, message);
            snprintf(message, 120, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, recordData, message);
            snprintf(message, 120, "EmbedDB get var did not return null stream for key %li when it should have no variable data.", key);
            TEST_ASSERT_NULL_MESSAGE(stream, message);
        }
        key++;
        data++;
    }
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDB_variable_data_page_numbers_are_correct);
    RUN_TEST(embedDB_variable_data_reloads_with_no_data_correctly);
    RUN_TEST(embedDB_variable_data_reloads_with_one_page_of_data_correctly);
    RUN_TEST(embedDB_variable_data_reloads_with_sixteen_pages_of_data_correctly);
    RUN_TEST(embedDB_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly);
    RUN_TEST(embedDB_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly);
    RUN_TEST(embedDB_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}
