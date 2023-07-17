
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

void setupSbits() {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    int8_t M = 6;
    if (state == NULL) {
        printf("Unable to allocate state. Exiting.\n");
        return;
    }

    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bitmapSize = 0;
    state->bufferSizeInBlocks = M;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exiting.\n");
        return;
    }
    int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);
    if (recordBuffer == NULL) {
        printf("Unable to allocate record buffer. Exiting.\n");
        return;
    }

    /* Address level parameters */
    state->numDataPages = 1000;
    state->eraseSizeInPages = 4;

    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->varFile = setupSDFile(varPath);

    state->parameters = SBITS_RESET_DATA;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void setUp(void) {
    setupSbits();
}

void tearDown(void) {
    free(state->buffer);
    sbitsClose(state);
    tearDownSDFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void sbits_initial_configuration_is_correct() {
    TEST_ASSERT_NOT_NULL_MESSAGE(state->dataFile, "SBITS file was not initialized correctly.");
    TEST_ASSERT_NULL_MESSAGE(state->varFile, "SBITS varFile was intialized for non-variable data.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "SBITS nextDataPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(6, state->headerSize, "SBITS headerSize was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->minKey, "SBITS minKey was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedPageId, "SBITS bufferedPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedIndexPageId, "SBITS bufferedIndexPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedVarPage, "SBITS bufferedVarPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT16_MESSAGE(63, state->maxRecordsPerPage, "SBITS maxRecordsPerPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(63, state->maxError, "SBITS maxError was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1000, state->numDataPages, "SBITS numDataPages was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->avgKeyDiff, "SBITS avgKeyDiff was not initialized correctly.");
    TEST_ASSERT_NOT_NULL_MESSAGE(state->spl, "SBITS spline was not initialized correctly.");
}

void sbits_put_inserts_single_record_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = 15648;
    *((int32_t *)(data + 4)) = 27335;
    uint64_t minKey = 15648;
    int8_t result = sbitsPut(state, data, (void *)(data + 4));
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&minKey, &state->minKey, 8, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    int32_t *sbitsPutResultKey = (int32_t *)malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = (int32_t *)malloc(sizeof(int32_t));
    memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6, 4);
    memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10, 4);
    TEST_ASSERT_EQUAL_INT32_MESSAGE(15648, *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(27335, *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_eleven_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = (int32_t *)malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = (int32_t *)malloc(sizeof(int32_t));
    *((int32_t *)data) = 16321;
    *((int32_t *)(data + 4)) = 28361;
    for (int i = 0; i < 11; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
        memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)data), *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)(data + 4)), *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16321, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(11, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_one_page_of_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = (int32_t *)malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = (int32_t *)malloc(sizeof(int32_t));
    *((int32_t *)data) = 100;
    *((int32_t *)(data + 4)) = 724;
    for (int i = 0; i < 63; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
        memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)data), *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)(data + 4)), *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(100, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(63, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_one_more_than_one_page_of_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = (int32_t *)malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = (int32_t *)malloc(sizeof(int32_t));
    *((int32_t *)data) = 4444444;
    *((int32_t *)(data + 4)) = 96875;
    for (int i = 0; i < 64; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(4444444, state->minKey, 4, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextDataPageId, "sbitsPut did not move to next page after writing the first page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, SBITS_GET_COUNT(state->buffer), "sbitsPut did not reset buffer count to correct value after writing the page");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void iteratorReturnsCorrectRecords(void) {
    uint32_t numRecordsToInsert = 1000;
    for (uint32_t key = 0; key < numRecordsToInsert; key++) {
        uint32_t data = key % 100;
        sbitsPut(state, &key, &data);
    }
    sbitsFlush(state);

    // Query records using an iterator
    sbitsIterator it;
    uint32_t minData = 23, maxData = 38, minKey = 32;
    it.minData = &minData;
    it.maxData = &maxData;
    it.minKey = &minKey;
    it.maxKey = NULL;
    sbitsInitIterator(state, &it);

    uint32_t numRecordsRead = 0;
    uint32_t key, data;
    while (sbitsNext(state, &it, &key, &data)) {
        numRecordsRead++;
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minKey, key, "Key is not in range of query");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key % 100, data, "Record contains the wrong data");
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minData, data, "Data is not in range of query");
        TEST_ASSERT_LESS_OR_EQUAL_UINT32_MESSAGE(maxData, data, "Data is not in range of query");
    }

    // Check that the correct number of records were read
    uint32_t expectedNum = 0;
    for (uint32_t i = 0; i < numRecordsToInsert; i++) {
        if (it.minKey != NULL && i < *(uint32_t *)it.minKey) continue;
        if (it.maxKey != NULL && i > *(uint32_t *)it.maxKey) continue;
        if (it.minData != NULL && i % 100 < *(uint32_t *)it.minData) continue;
        if (it.maxData != NULL && i % 100 > *(uint32_t *)it.maxData) continue;
        expectedNum++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedNum, numRecordsRead, "Iterator did not read the correct number of records");
}

int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_initial_configuration_is_correct);
    RUN_TEST(sbits_put_inserts_single_record_correctly);
    RUN_TEST(sbits_put_inserts_eleven_records_correctly);
    RUN_TEST(sbits_put_inserts_one_page_of_records_correctly);
    RUN_TEST(sbits_put_inserts_one_more_than_one_page_of_records_correctly);
    RUN_TEST(iteratorReturnsCorrectRecords);
    return UNITY_END();
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

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
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
