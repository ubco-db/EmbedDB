#include <math.h>
#include <string.h>

#include "Arduino.h"
#include "SPI.h"
#include "dataflash_c_iface.h"
#include "sdcard_c_iface.h"
#define UNITY_SUPPORT_64
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

void setupSbits() {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting.\n");
        return;
    }

    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exiting.\n");
        return;
    }

    /* Address level parameters */
    state->numDataPages = 93;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = SBITS_RESET_DATA;

    char dataPath[] = "dataFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;

    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void setUp() {
    setupSbits();
}

void tearDown() {
    free(state->buffer);
    sbitsClose(state);
    tearDownSDFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void insertRecordsLinearly(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void insertRecordsParabolic(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += i;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

int queryRecordsLinearly(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int64_t startingData) {
    int64_t *result = (int64_t *)malloc(state->recordSize);
    int32_t key = startingKey;
    int64_t data = startingData;
    for (uint32_t i = 0; i < numberOfRecords; i++) {
        key = startingKey += 1;
        data += 1;
        int8_t getStatus = sbitsGet(state, &key, (void *)result);
        if (getStatus != 0) {
            printf("ERROR: Failed to find: %lu\n", key);
            return 1;
        }
        if (*((int64_t *)result) != data) {
            printf("ERROR: Wrong data for: %lu\n", key);
            printf("Key: %lu Data: %lu\n", key, *((int64_t *)result));
            free(result);
            return 1;
        }
    }
    free(result);
    return 0;
}

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

void initalizeSbitsFromFile(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting.\n");
        return;
    }

    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exiting.\n");
        return;
    }

    char dataPath[] = "dataFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);

    state->numDataPages = 93;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = 0;

    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void loop() {}

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

void sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly() {
    insertRecordsLinearly(9, 20230614, 1135);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(10, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(66, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

/* The setup function allocates 93 pages, so check to make sure it initalizes correctly when it is full */
void sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly() {
    insertRecordsLinearly(3456, 2548, 3907);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(3457, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_from_data_file_with_ninety_four_pages_correctly() {
    insertRecordsLinearly(1645, 2548, 3949);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(1688, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(94, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts() {
    insertRecordsLinearly(2000, 11205, 17515);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(15609, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(417, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(324, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_correctly_from_data_file_with_no_data() {
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(UINT32_MAX, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->numAvailDataPages, "SBITS numAvailDataPages is not ");
}

void sbits_inserts_correctly_into_data_file_after_reload() {
    insertRecordsLinearly(1000, 5600, 3655);
    tearDown();
    initalizeSbitsFromFile();
    insertRecordsLinearly(4654, 10, 43);
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 1001;
    int64_t data = 5601;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 0; i < 3654; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %li.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    /* Records inserted after reload */
    data = 11;
    for (int i = 0; i < 42; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %li.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted after reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    free(recordBuffer);
}

void sbits_correctly_gets_records_after_reload_with_wrapped_data() {
    insertRecordsLinearly(0, 0, 13758);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(9871, state->minKey, "SBITS minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 9871;
    int64_t data = 9871;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 0; i < 3888; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %li.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    free(recordBuffer);
}

void sbits_prevents_duplicate_inserts_after_reload() {
    insertRecordsLinearly(0, 8751, 1975);
    tearDown();
    initalizeSbitsFromFile();
    int32_t key = 1974;
    int64_t data = 1974;
    int8_t insertResult = sbitsPut(state, &key, &data);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, insertResult, "SBITS inserted a duplicate key.");
}

void sbits_queries_correctly_with_non_liner_data_after_reload() {
    insertRecordsParabolic(1000, 367, 4495);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(174166, state->minKey, "SBITS minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 174166;
    int64_t data = 956;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 174166; i < 4494; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %li.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %li).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key += i;
        data += i;
    }
    free(recordBuffer);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_four_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts);
    RUN_TEST(sbits_inserts_correctly_into_data_file_after_reload);
    RUN_TEST(sbits_correctly_gets_records_after_reload_with_wrapped_data);
    RUN_TEST(sbits_prevents_duplicate_inserts_after_reload);
    RUN_TEST(sbits_queries_correctly_with_non_liner_data_after_reload);
    RUN_TEST(sbits_parameters_initializes_correctly_from_data_file_with_no_data);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}
