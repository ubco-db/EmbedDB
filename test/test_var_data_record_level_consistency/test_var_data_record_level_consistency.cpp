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
#endif

#include "unity.h"

embedDBState *state;

void setupEmbedDB(int8_t parameters) {
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
    state->fileInterface = getFileInterface();
    char *dataFilePath = DATA_FILE_PATH;
    char *varDataFilePath = VAR_DATA_FILE_PATH;
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
    int8_t setupParamaters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_RESET_DATA | EMBEDDB_USE_VDATA;
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

void variable_data_for_record_level_consistency_records_should_be_readable() {
    /* insert 16 records into database with record-level consistency */
    char variableData[] = "Database COD";
    uint32_t key = 955666;
    uint64_t data = 960651;
    insertRecords(key, data, variableData, 12, 16);

    /* tear down state and recover */
    tearDown();
    int8_t setupParameters = EMBEDDB_RECORD_LEVEL_CONSISTENCY | EMBEDDB_USE_VDATA;
    setupEmbedDB(setupParameters);

    /* Check that the state was correctly setup again */
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "embedDBInit did not set the correct variableDataHeaderSize when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(state->minVarRecordId, 955666, "embedDBInit did not set the correct minVarRecordID when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(state->nextVarPageId, 16, "embedDBInit did not set the correct nextVarPageId when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(state->numAvailVarPages, 48, "embedDBInit did not set the correct numAvailVarPages when recovering with variable data and record-level consistency.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8200, state->currentVarLoc, "embedDBInit did not set the correct currentVarLoc when recovering with variable data and record-level consistency.");

    /* Check that we can still query the remaining records */
    uint64_t recordData = 0;
    char variableDataBuffer[13];
    char expectedVariableData[] = "Database COD";
    char message[120];
    key = 955666;
    data = 960651;
    embedDBVarDataStream *stream = NULL;
    /* Records inserted before reload */
    for (int i = 0; i < 16; i++) {
        int8_t getResult = embedDBGetVar(state, &key, &recordData, &stream);
        snprintf(message, 120, "EmbedDB get encountered an error fetching the data for key %li.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        uint32_t streamBytesRead = 0;
        snprintf(message, 120, "EmbedDB get var returned null stream for key %li.", key);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, message);
        streamBytesRead = embedDBVarDataStreamRead(state, stream, variableDataBuffer, 12);
        snprintf(message, 120, "EmbedDB get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&data, &recordData, sizeof(uint64_t), message);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, streamBytesRead, "EmbedDB var data stream did not read the correct number of bytes.");
        snprintf(message, 120, "EmbedDB get var did not return the correct variable data for key %li.", key);
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(expectedVariableData, variableDataBuffer, 12, message);
        key++;
        data++;
        free(stream);
    }
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(variable_data_for_record_level_consistency_records_should_be_readable);
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
