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

sbitsState *state;

void setupSbits() {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    state->parameters = SBITS_USE_VDATA | SBITS_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    resetStats(state);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void initalizeSbitsFromFile() {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;

    state->parameters = SBITS_USE_VDATA;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    resetStats(state);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void setUp() {
    setupSbits();
}

void tearDown() {
    free(state->buffer);
    sbitsClose(state);
    tearDownSDFile(state->dataFile);
    tearDownSDFile(state->varFile);
    free(state->fileInterface);
    free(state);
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *((int32_t *)recordBuffer) = key;
    *((int32_t *)(recordBuffer + state->keySize)) = startingData;
    char variableData[13] = "Hello World!";
    for (int32_t i = 0; i < numberOfRecords; i++) {
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPutVar(state, recordBuffer, (int8_t *)recordBuffer + state->keySize, variableData, 13);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
    }
    free(recordBuffer);
}

void sbits_variable_data_page_numbers_are_correct() {
    insertRecords(1429, 1444, 64);
    /* Number of records * average data size % page size */
    uint32_t numberOfPagesExpected = 69;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfPagesExpected - 1, state->nextVarPageId, "SBITS next variable data logical page number is incorrect.");
    uint32_t pageNumber;
    printf("Number of pages expected: %li\n", numberOfPagesExpected);
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
    for (uint32_t i = 0; i < numberOfPagesExpected - 1; i++) {
        readVariablePage(state, i);
        memcpy(&pageNumber, buffer, sizeof(id_t));
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(i, pageNumber, "SBITS variable data did not have the correct page number.");
    }
}

void sbits_variable_data_reloads_with_no_data_correctly() {
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "SBITS variableDataHeaderSize did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with no records.");
    uint64_t minVarRecordId = 0;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&minVarRecordId, &state->minVarRecordId, 8, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(75, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with no records.");
}

void sbits_variable_data_reloads_with_one_page_of_data_correctly() {
    insertRecords(30, 100, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(520, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    uint64_t minVarRecordId = 0;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&minVarRecordId, &state->minVarRecordId, 8, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(74, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_with_sixteen_pages_of_data_correctly() {
    insertRecords(337, 1648, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8200, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    uint64_t minVarRecordId = 0;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&minVarRecordId, &state->minVarRecordId, 8, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(59, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly() {
    insertRecords(2227, 100, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15880, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    uint64_t minVarRecordId = 773;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&minVarRecordId, &state->minVarRecordId, 8, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(106, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly() {
    int32_t key = 1000;
    int32_t data = 10;
    insertRecords(651, key, data);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    char message[100];
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    sbitsVarDataStream *stream = NULL;
    key = 1001;
    data = 11;
    /* Records inserted before reload */
    for (int i = 0; i < 650; i++) {
        int8_t getResult = sbitsGetVar(state, &key, recordBuffer, &stream);
        snprintf(message, 100, "SBITS get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 100, "SBITS get var returned null stream for key %li.", key);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = sbitsVarDataStreamRead(state, stream, variableDataBuffer, 13);
        snprintf(message, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "SBITS var data stream did not read the correct number of bytes.");
        snprintf(message, 100, "SBITS get var did not return the correct variable data for key %li.", key);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(variableData, variableDataBuffer, 13, message);
        free(stream);
        key++;
        data++;
    }
    free(recordBuffer);
}

void sbits_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly() {
    int32_t key = 6798;
    int32_t data = 13467895;
    insertRecords(5187, key, data);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    char messageBuffer[120];
    char variableData[] = "Hello World!";
    char *variableDataBuffer = (char *)calloc(13, sizeof(char));
    sbitsVarDataStream *stream = NULL;
    key = 9277;
    data = 13470374;
    /* Records inserted before reload */
    for (int i = 0; i < 2708; i++) {
        int8_t getResult = sbitsGetVar(state, &key, recordBuffer, &stream);
        if (i > 1163) {
            snprintf(messageBuffer, 120, "SBITS get encountered an error fetching the data for key %li.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, messageBuffer);
            snprintf(messageBuffer, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), messageBuffer);
            snprintf(messageBuffer, 80, "SBITS get var did not return the correct variable data for key %li.", key);
            TEST_ASSERT_NOT_NULL_MESSAGE(stream, messageBuffer);
            uint32_t streamBytesRead = sbitsVarDataStreamRead(state, stream, variableDataBuffer, 13);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "SBITS var data stream did not read the correct number of bytes.");
            snprintf(messageBuffer, 100, "SBITS get var returned null stream for key %li.", key);
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(variableData, variableDataBuffer, 13, messageBuffer);
            free(stream);
        } else {
            snprintf(messageBuffer, 120, "SBITS get encountered an error fetching the data for key %li. The var data was not detected as being overwritten.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(1, getResult, messageBuffer);
            snprintf(messageBuffer, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), messageBuffer);
            snprintf(messageBuffer, 100, "SBITS get var did not return null stream for key %li when it should have no variable data.", key);
            TEST_ASSERT_NULL_MESSAGE(stream, messageBuffer);
        }
        key++;
        data++;
    }
    free(recordBuffer);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(sbits_variable_data_page_numbers_are_correct);
    RUN_TEST(sbits_variable_data_reloads_with_no_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_page_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_sixteen_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}
