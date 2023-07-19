#include <math.h>
#include <string.h>

#include "Arduino.h"
#include "SPI.h"
#include "dataflash_c_iface.h"
#include "sdcard_c_iface.h"
/**
 * SPI configurations for memory */
#include "mem_spi.h"

/*
Includes for DataFlash memory
*/
// #include "at45db32_test.h"
#include "dataflash.h"

/**
 * Includes for SD card
 */
/** @TODO optimize for clock speed */
#include "sdios.h"
static ArduinoOutStream cout(Serial);

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "SdFat.h"
#include "sd_test.h"
#include "unity.h"

#define ENABLE_DEDICATED_SPI 1
#define SPI_DRIVER_SELECT 1
#define SD_FAT_TYPE 1
#define SD_CONFIG SdSpiConfig(CS_SD, DEDICATED_SPI, SD_SCK_MHZ(12), &spi_0)

sbitsState *state;

bool test_sd_card();

SdFat32 sd;
File32 file;

void setupBoard() {
    // Setup Board
    Serial.begin(115200);
    while (!Serial) {
        delay(1);
    }

    delay(1000);
    Serial.println("Skeleton startup");

    pinMode(CHK_LED, OUTPUT);
    pinMode(PULSE_LED, OUTPUT);

    /* Setup for SD card */
    Serial.print("\nInitializing SD card...");
    if (test_sd_card()) {
        file = sd.open("/");
        cout << F("\nList of files on the SD.\n");
        sd.ls("/", LS_R);
    }

    init_sdcard((void *)&sd);

    /* Setup for data flash memory (DB32 512 byte pages) */
    pinMode(CS_DB32, OUTPUT);
    digitalWrite(CS_DB32, HIGH);
    at45db32_m.spi->begin();

    df_initialize(&at45db32_m);
    cout << "AT45DF32"
         << "\n";
    cout << "page size: " << (at45db32_m.actual_page_size = get_page_size(&at45db32_m)) << "\n";
    cout << "status: " << get_ready_status(&at45db32_m) << "\n";
    cout << "page size: " << (at45db32_m.actual_page_size) << "\n";
    at45db32_m.bits_per_page = (uint8_t)ceil(log2(at45db32_m.actual_page_size));
    cout << "bits per page: " << (unsigned int)at45db32_m.bits_per_page << "\n";

    init_df((void *)&at45db32_m);
}

bool test_sd_card() {
    if (!sd.cardBegin(SD_CONFIG)) {
        Serial.println(F(
            "\nSD initialization failed.\n"
            "Do not reformat the card!\n"
            "Is the card correctly inserted?\n"
            "Is there a wiring/soldering problem?\n"));
        if (isSpi(SD_CONFIG)) {
            Serial.println(F(
                "Is SD_CS_PIN set to the correct value?\n"
                "Does another SPI device need to be disabled?\n"));
        }
        errorPrint(sd);
        return false;
    }

    if (!sd.card()->readCID(&m_cid) ||
        !sd.card()->readCSD(&m_csd) ||
        !sd.card()->readOCR(&m_ocr)) {
        cout << F("readInfo failed\n");
        errorPrint(sd);
    }
    printCardType(sd);
    cidDmp();
    csdDmp();
    cout << F("\nOCR: ") << uppercase << showbase;
    cout << hex << m_ocr << dec << endl;
    if (!mbrDmp(sd)) {
        return false;
    }
    if (!sd.volumeBegin()) {
        cout << F("\nvolumeBegin failed. Is the card formatted?\n");
        errorPrint(sd);
        return false;
    }
    dmpVol(sd);
    return true;
}

void setupSbits() {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = SBITS_USE_VDATA | SBITS_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
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
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);

    state->fileInterface = getSDInterface();
    char dataPath[] = "dataFile.bin", varPath[] = "varFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->varFile = setupSDFile(varPath);

    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;

    state->bitmapSize = 0;
    state->parameters = SBITS_USE_VDATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
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

void loop() {}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}
