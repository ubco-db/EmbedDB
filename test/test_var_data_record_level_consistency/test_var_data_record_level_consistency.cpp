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
#define VAR_DATA_FILE_PATH "varFile.bin"
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define VAR_DATA_FILE_PATH "build/artifacts/varFile.bin"
/* On the desktop platform, there is a file interface which simulates "erasing" by writing out all 1's to the location in the file ot be erased */
#define MOCK_ERASE_INTERFACE
#endif

#define LONG_VARIABLE_DATA "I thought not. It's not a story the Jedi would tell you. It's a Sith legend. Darth Plagueis was a Dark Lord of the Sith, so powerful and so wise he could use the Force to influence the midichlorians to create life... He had such a knowledge of the dark side that he could even keep the ones he cared about from dying. The dark side of the Force is a pathway to many abilities some consider to be unnatural. He became so powerful... the only thing he was afraid of was losing his power, which eventually, of course, he did. Unfortunately, he taught his apprentice everything he knew, then his apprentice killed him in his sleep. It's ironic he could save others from death, but not himself."

#include "unity.h"

embedDBState *state;

void setupEmbedDB(int16_t parameters) {
    /* The setup below will result in having 42 records per page */
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBState.");
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 8;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

/* configure EmbedDB storage */
#ifdef MOCK_ERASE_INTERFACE
    state->fileInterface = getMockEraseFileInterface();
#else
    state->fileInterface = getFileInterface();
#endif

    char dataFilePath[] = DATA_FILE_PATH;
    char varDataFilePath[] = VAR_DATA_FILE_PATH;
    state->dataFile = setupFile(dataFilePath);
    state->varFile = setupFile(varDataFilePath);

    state->numDataPages = 128;
    state->numVarPages = 64;
    state->eraseSizeInPages = 4;
    state->parameters = parameters;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");
}

void setUp() {
    int16_t setupParamaters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_RESET_DATA | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParamaters);
}

void tearDown() {
    free(state->buffer);
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->varFile);
    free(state->fileInterface);
    free(state);
}

void insertRecords(uint32_t startingKey, uint64_t startingData, void *variableData, uint32_t length, uint32_t numRecords) {
    uint32_t key = startingKey;
    uint64_t data = startingData;
    for (uint32_t i = 0; i < numRecords; i++) {
        int8_t result = embedDBPutVar(state, &key, &data, variableData, length);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPutVar did not correctly insert data (returned non-zero code)");
        key++;
        data++;
    }
}

void insertRecordsCustomVarData(uint32_t startingKey, uint64_t startingData, uint32_t numRecords, uint32_t length) {
    uint32_t key = startingKey;
    uint64_t data = startingData;
    char variableData[length];
    for (uint32_t i = 0; i < numRecords; i++) {
        snprintf(variableData, length, "Variable Data %u", key);
        int8_t result = embedDBPutVar(state, &key, &data, variableData, length);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPutVar did not correctly insert data (returned non-zero code)");
        key++;
        data++;
    }
}

void variable_data_record_level_consistency_records_should_be_readable() {
    /* insert 16 records into database with record-level consistency */
    char variableData[] = "Database COD";
    uint32_t key = 955666;
    uint64_t data = 960651;
    insertRecords(key, data, variableData, 12, 16);

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that the state was correctly setup again */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(955666, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(48, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8200, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can still query the remaining records */
    uint64_t recordData = 0;
    char variableDataBuffer[15];
    char expectedVariableData[] = "Database COD";
    char message[120];
    key = 955666;
    data = 960651;
    embedDBVarDataStream *stream = NULL;

    /* Query records inserted before reload */
    for (int i = 0; i < 16; i++) {
        int8_t getResult = embedDBGetVar(state, &key, &recordData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", key);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, variableDataBuffer, 15);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, &recordData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(expectedVariableData, variableDataBuffer, 12, message);
        key++;
        data++;
        free(stream);
        stream = NULL;
    }
}

void variable_data_record_level_consistency_should_recover_64_records_correctly() {
    uint32_t key = 157557064;
    uint64_t data = 449130689;
    insertRecordsCustomVarData(key, data, 64, 24);

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that state was initialised correctly */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(157557064, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(64, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can still query the records inserted before recovery */
    uint64_t actualData = 0;
    char actualVariableData[25];
    char expectedVariableData[24];
    char message[120];
    uint32_t expectedKey = 157557064;
    uint64_t expectedData = 449130689;
    embedDBVarDataStream *stream = NULL;

    for (uint32_t i = 0; i < 64; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", expectedKey);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, actualVariableData, 25);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(expectedVariableData, 24, "Variable Data %u", expectedKey);
        snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, actualVariableData, 24, message);
        expectedKey++;
        expectedData++;
        free(stream);
        stream = NULL;
    }

    /* Check that we can't query anymore records */
    int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
    snprintf(message, 120, "embedDBGetVar should not have retrieved any data for key %u.", expectedKey);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

void variable_data_record_level_consistency_should_recover_four_pages_data_records_correctly() {
    uint32_t key = 571933978;
    uint64_t data = 691272876;
    insertRecordsCustomVarData(key, data, 124, 24);
    embedDBFlush(state);

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that state was initialised correctly */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(571934043, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(124, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(30728, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can still query the records inserted before recovery */
    uint64_t actualData = 0;
    char actualVariableData[25];
    char expectedVariableData[24];
    char message[120];
    uint32_t expectedKey = 571933978;
    uint64_t expectedData = 691272876;
    embedDBVarDataStream *stream = NULL;

    /* Query records inserted before reload with vardata that was overwritten */
    for (uint32_t i = 0; i < 65; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar expected variable data to be overwritten for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(1, getResult, message);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        expectedKey++;
        expectedData++;
    }

    /* Query records with variable data */
    for (uint32_t i = 0; i < 59; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", expectedKey);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, actualVariableData, 25);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(24, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(expectedVariableData, 24, "Variable Data %u", expectedKey);
        snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, actualVariableData, 24, message);
        expectedKey++;
        expectedData++;
        free(stream);
        stream = NULL;
    }

    /* Check that we can't query anymore records */
    int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
    snprintf(message, 120, "embedDBGetVar should not have retrieved any data for key %u.", expectedKey);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

void variable_data_record_level_consistency_should_recover_71_pages_data_and_19_record_level_consistency_records() {
    uint32_t key = 85824389;
    uint64_t data = 46212944;
    insertRecordsCustomVarData(key, data, 2218, 23);

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that state was initialised correctly */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(85826546, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2218, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(2, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(21512, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can query some record-level consistency and regular records */
    uint64_t actualData = 0;
    char actualVariableData[25];
    char expectedVariableData[23];
    char message[120];
    uint32_t expectedKey = 85826577;
    uint64_t expectedData = 46215132;
    embedDBVarDataStream *stream = NULL;

    /* Query records with variable data */
    for (uint32_t i = 0; i < 30; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", expectedKey);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, actualVariableData, 25);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(expectedVariableData, 23, "Variable Data %u", expectedKey);
        snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, actualVariableData, 23, message);
        free(stream);
        stream = NULL;
        expectedKey++;
        expectedData++;
    }

    /* Check that we can't query anymore records */
    int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
    snprintf(message, 120, "embedDBGetVar should not have retrieved any data for key %u.", expectedKey);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

void variable_data_record_level_consistency_should_recover_variable_data_longer_than_one_page() {
    /* Insert Records */
    uint32_t key = 98208683;
    uint64_t data = 95361655;
    char variableData[] = LONG_VARIABLE_DATA;
    insertRecords(key, data, variableData, 690, 24);

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that state was initialised correctly */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(98208683, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(48, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(24584, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can query records */
    uint64_t actualData = 0;
    char actualVariableData[700];
    char expectedVariableData[] = LONG_VARIABLE_DATA;
    char message[120];
    uint32_t expectedKey = 98208683;
    uint64_t expectedData = 95361655;
    embedDBVarDataStream *stream = NULL;

    /* Query records with variable data */
    for (uint32_t i = 0; i < 24; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", expectedKey);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, actualVariableData, 700);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(690, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, actualVariableData, 690, message);
        free(stream);
        stream = NULL;
        expectedKey++;
        expectedData++;
    }

    /* Check that we can't query anymore records */
    int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
    snprintf(message, 120, "embedDBGetVar should not have retrieved any data for key %u.", expectedKey);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

void variable_data_record_level_consistency_should_recover_after_inserting_131_pages_data() {
    /* 131 pages of data and 6 indidivudal records*/
    uint32_t key = 64454095;
    uint64_t data = 29636444;
    char variableData[23];
    /* 1/4 of records have variable data */
    for (uint32_t i = 0; i < 4067; i++) {
        int8_t result = 0;
        if (i % 4 == 3) {
            snprintf(variableData, 23, "Variable Data %u", key);
            result = embedDBPutVar(state, &key, &data, variableData, 23);
        } else {
            embedDBPutVar(state, &key, &data, NULL, 23);
        }
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBPutVar did not correctly insert data (returned non-zero code)");
        key++;
        data++;
    }

    /* tear down state and recover */
    tearDown();
    int16_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that state was initialised correctly */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(64457923, state->minVarRecordId, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1016, state->nextVarPageId, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailVarPages, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(28680, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can still query the records inserted before recovery */
    uint64_t actualData = 0;
    char actualVariableData[25];
    char expectedVariableData[23];
    char message[120];
    uint32_t expectedKey = 64457843;
    uint64_t expectedData = 29640192;
    embedDBVarDataStream *stream = NULL;

    /* Query records inserted before reload with vardata that was overwritten */
    for (uint32_t i = 0; i < 80; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        if (expectedKey % 4 == 2) {
            snprintf(message, 120, "embedDBGetVar expected variable data to be overwritten for key %u.", expectedKey);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(1, getResult, message);
        } else {
            snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        }
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        expectedKey++;
        expectedData++;
    }

    /* Query records with variable data */
    for (uint32_t i = 0; i < 239; i++) {
        int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
        snprintf(message, 120, "embedDBGetVar encountered an error fetching the data for key %u.", expectedKey);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 120, "embedDBGetVar did not return correct data for a record inserted before reloading (key %u).", expectedKey);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedData, &actualData, sizeof(uint64_t), message);
        if (expectedKey % 4 == 2) {
            uint32_t streamBytesRead = 0;
            snprintf(message, 120, "embedDBGetVar returned null stream for key %u.", expectedKey);
            TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
            streamBytesRead = embedDBVarDataStreamRead(state, stream, actualVariableData, 25);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(23, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
            snprintf(expectedVariableData, 23, "Variable Data %u", expectedKey);
            snprintf(message, 120, "embedDBGetVar did not return the correct variable data for key %u.", expectedKey);
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVariableData, actualVariableData, 23, message);
            free(stream);
            stream = NULL;
        }
        expectedKey++;
        expectedData++;
    }

    /* Check that we can't query anymore records */
    int8_t getResult = embedDBGetVar(state, &expectedKey, &actualData, &stream);
    snprintf(message, 120, "embedDBGetVar should not have retrieved any data for key %u.", expectedKey);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, getResult, message);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(variable_data_record_level_consistency_records_should_be_readable);
    RUN_TEST(variable_data_record_level_consistency_should_recover_64_records_correctly);
    RUN_TEST(variable_data_record_level_consistency_should_recover_four_pages_data_records_correctly);
    RUN_TEST(variable_data_record_level_consistency_should_recover_71_pages_data_and_19_record_level_consistency_records);
    RUN_TEST(variable_data_record_level_consistency_should_recover_variable_data_longer_than_one_page);
    RUN_TEST(variable_data_record_level_consistency_should_recover_after_inserting_131_pages_data);
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
