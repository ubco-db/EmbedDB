#include "../src/sbits/sbits.h"
#include "sbitsUtility.h"

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
#include "sdcard_c_iface.h"
#include "unity.h"

sbitsState **states;

void setupSbitsInstanceKeySize4DataSize4(sbitsState **stateArray, int number) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");
    state->numDataPages = 2000;
    state->parameters = SBITS_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getSDInterface();
    char dataPath[40];
    snprintf(dataPath, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupSDFile(dataPath);
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void setUp() {}

void tearDown() {}

void insertRecords(sbitsState *state, int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t insertResult = sbitsPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
        key++;
        data++;
    }
    sbitsFlush(state);
}

void queryRecords(sbitsState *state, int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t dataBuffer;
    int32_t key = startingKey;
    int32_t data = startingData;
    char message[120];
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t getResult = sbitsGet(state, &key, &dataBuffer);
        snprintf(message, 120, "sbitsGet returned a non-zero value when getting key %i from state %i", key, i);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 120, "sbitsGet did not return the correct data for key %i from state %i", key, i);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, dataBuffer, message);
        key++;
        data++;
    }
}

void insertRecordsFromFile(sbitsState *state, char *fileName, int32_t numRecords) {
    SD_FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numInserted = 0;
    char message[100];
    while (numInserted < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * state->recordSize);
            int8_t putResult = sbitsPut(state, buf, (void *)((int8_t *)buf + 4));
            snprintf(message, 100, "sbitsPut returned non-zero value for insert of key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, putResult, message);
            numInserted++;
            if (numInserted >= numRecords) {
                break;
            }
        }
    }
    sbitsFlush(state);
    fclose(infile);
}

void insertRecordsFromFileWithVarData(sbitsState *state, char *fileName, int32_t numRecords) {
    SD_FILE *infile;
    infile = fopen(fileName, "r+b");
    if (infile == NULL) {
        printf("Error!!!\n");
    }

    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numInserted = 0;
    char message[100];
    char *varData = (char *)calloc(30, sizeof(char));
    while (numInserted < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * (state->keySize + state->dataSize));
            snprintf(varData, 30, "Hello world %i", *((uint32_t *)buf));
            int8_t putResult = sbitsPutVar(state, buf, (void *)((int8_t *)buf + 4), varData, strlen(varData));
            snprintf(message, 100, "sbitsPut returned non-zero value for insert of key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, putResult, message);
            numInserted++;
            if (numInserted >= numRecords) {
                break;
            }
        }
    }
    free(varData);
    sbitsFlush(state);
    fclose(infile);
}

void queryRecordsFromFile(sbitsState *state, char *fileName, int32_t numRecords) {
    SD_FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numRead = 0;
    int8_t *dataBuffer = (int8_t *)malloc(state->dataSize * sizeof(int8_t));
    char message[100];
    while (numRead < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * state->recordSize);
            int8_t getResult = sbitsGet(state, buf, dataBuffer);
            snprintf(message, 100, "sbitsGet was not able to find the data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 100, "sbitsGet did not return the correct data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(buf + 4, dataBuffer, state->dataSize, message);
            numRead++;
            if (numRead >= numRecords)
                break;
        }
    }
    TEST_ASSERT_EQUAL_INT32_MESSAGE(numRecords, numRead, "The number of records read was not equal to the number of records inserted.");
    fclose(infile);
}

void queryRecordsFromFileWithVarData(sbitsState *state, char *fileName, int32_t numRecords) {
    SD_FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numRead = 0;
    int8_t *dataBuffer = (int8_t *)malloc(state->dataSize * sizeof(int8_t));
    char *varDataBuffer = (char *)calloc(30, sizeof(char));
    char *varDataExpected = (char *)calloc(30, sizeof(char));
    char message[100];
    while (numRead < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * (state->keySize + state->dataSize));
            snprintf(varDataExpected, 30, "Hello world %i", *((uint32_t *)buf));
            sbitsVarDataStream *stream = NULL;
            int8_t getResult = sbitsGetVar(state, buf, dataBuffer, &stream);
            snprintf(message, 100, "sbitsGetVar was not able to find the data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 100, "sbitsGetBar did not return the correct data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(buf + 4, dataBuffer, state->dataSize, message);
            if (946714800 == *((uint32_t *)buf)) {
                printf("Expected data: %s, actual data: %s \n", varDataExpected, varDataBuffer);
                printf("Length of data expected: %i \n", strlen(varDataExpected));
                printf("Length of data actually: %i \n", stream->totalBytes);
                printf("Data offset: %i \n", stream->dataStart);
            }
            uint32_t streamBytesRead = sbitsVarDataStreamRead(state, stream, varDataBuffer, strlen(varDataExpected));
            snprintf(message, 100, "sbitsGetVar did not return the correct variable data for key %i", *((uint32_t *)buf));

            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(varDataExpected, varDataBuffer, strlen(varDataExpected), message);
            numRead++;
            free(stream);
            if (numRead >= numRecords)
                break;
        }
    }
    TEST_ASSERT_EQUAL_INT32_MESSAGE(numRecords, numRead, "The number of records read was not equal to the number of records inserted.");
    fclose(infile);
    free(dataBuffer);
    free(varDataBuffer);
    free(varDataExpected);
}

void setupSbitsInstanceKeySize4DataSize12(sbitsState **stateArray, int number) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->parameters = SBITS_RESET_DATA | SBITS_USE_INDEX;
    state->eraseSizeInPages = 4;
    state->fileInterface = getSDInterface();
    char path[40];
    snprintf(path, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupSDFile(path);
    snprintf(path, 40, "build/artifacts/indexFile%i.bin", number);
    state->indexFile = setupSDFile(path);
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void setupSbitsInstanceKeySize4DataSize12WithVarData(sbitsState **stateArray, int number) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate SBITS buffer.");
    state->numDataPages = 22000;
    state->numIndexPages = 1000;
    state->numVarPages = 44000;
    state->parameters = SBITS_RESET_DATA | SBITS_USE_INDEX | SBITS_USE_VDATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getSDInterface();
    char path[40];
    snprintf(path, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupSDFile(path);
    snprintf(path, 40, "build/artifacts/indexFile%i.bin", number);
    state->indexFile = setupSDFile(path);
    snprintf(path, 40, "build/artifacts/varFile%i.bin", number);
    state->varFile = setupSDFile(path);
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void test_insert_on_multiple_sbits_states() {
    int numStates = 3;
    states = (sbitsState **) malloc(numStates * sizeof(sbitsState *));
    for (int i = 0; i < numStates; i++)
        setupSbitsInstanceKeySize4DataSize4(states, i);
    int32_t key = 100;
    int32_t data = 1000;
    int32_t numRecords = 100000;

    // Insert records
    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        insertRecords(state, numRecords, key, data);
    }

    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        queryRecords(state, numRecords, key, data);
    }

    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        sbitsClose(state);
        tearDownSDFile(state->dataFile);
        free(state->buffer);
        free(state->fileInterface);
    }
    free(states);
}

void test_insert_from_files_with_index_multiple_states() {
    int numStates = 3;
    states = (sbitsState **)malloc(numStates * sizeof(sbitsState *));
    for (int i = 0; i < numStates; i++)
        setupSbitsInstanceKeySize4DataSize12(states, i);

    insertRecordsFromFile(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFile(*(states + 1), "data/ethylene_CO.bin", 400000);
    queryRecordsFromFile(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFile(*(states + 2), "data/PRSA_Data_Hongxin.bin", 33311);
    queryRecordsFromFile(*(states + 1), "data/ethylene_CO.bin", 400000);
    queryRecordsFromFile(*(states + 2), "data/PRSA_Data_Hongxin.bin", 33311);

    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        sbitsClose(state);
        tearDownSDFile(state->dataFile);
        tearDownSDFile(state->indexFile);
        free(state->buffer);
        free(state->fileInterface);
    }
    free(states);
}

void test_insert_from_files_with_vardata_multiple_states() {
    int numStates = 4;
    states = (sbitsState **)malloc(numStates * sizeof(sbitsState *));
    for (int i = 0; i < numStates; i++)
        setupSbitsInstanceKeySize4DataSize12WithVarData(states, i);

    insertRecordsFromFileWithVarData(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFileWithVarData(*(states + 1), "data/measure1_smartphone_sens.bin", 18354);
    queryRecordsFromFileWithVarData(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFileWithVarData(*(states + 2), "data/ethylene_CO.bin", 185589);
    insertRecordsFromFileWithVarData(*(states + 3), "data/position.bin", 1518);
    queryRecordsFromFileWithVarData(*(states + 2), "data/ethylene_CO.bin", 185589);
    queryRecordsFromFileWithVarData(*(states + 3), "data/position.bin", 1518);
    queryRecordsFromFileWithVarData(*(states + 1), "data/measure1_smartphone_sens.bin", 18354);

    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        sbitsClose(state);
        tearDownSDFile(state->dataFile);
        tearDownSDFile(state->indexFile);
        tearDownSDFile(state->varFile);
        free(state->buffer);
        free(state->fileInterface);
    }
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_insert_on_multiple_sbits_states);
    RUN_TEST(test_insert_from_files_with_index_multiple_states);
    RUN_TEST(test_insert_from_files_with_vardata_multiple_states);
    return UNITY_END();
}
