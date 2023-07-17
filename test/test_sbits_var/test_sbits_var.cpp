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
uint32_t numRecords = 1000;
uint32_t inserted = 0;

int i = 0;
uint32_t dataSizes[] = {4, 6, 8};

bool test_sd_card();

SdFat32 sd;
File32 file;

void test_init() {
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, sbitsInit(state, 0), "sbitsInit did not return 0");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->keySize, "Key size was changed during sbitsInit");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(dataSizes[i], state->dataSize, "Data size was changed during sbitsInit");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(state->keySize + state->dataSize + 4, state->recordSize, "State's record size is not correct");
}

void initState(uint32_t dataSize) {
    // Initialize sbits State
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = dataSize;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->numVarPages = 1000;
    state->eraseSizeInPages = 4;

    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    state->varFile = setupSDFile(varPath);

    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_USE_VDATA | SBITS_RESET_DATA;
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    resetStats(state);
}

void resetState() {
    sbitsClose(state);
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

        int result = sbitsPutVar(state, &j, &data, varData, 15);
        if (result != 0) {
            return result;
        }
        inserted++;
    }

    return 0;
}

void test_get_when_empty() {
    uint32_t key = 1, data;
    sbitsVarDataStream *varStream = NULL;
    int8_t result = sbitsGetVar(state, &key, &data, &varStream);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "sbitsGetVar did not return -1 when the key was not found");
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
    void *key = (int8_t *)state->buffer + SBITS_DATA_WRITE_BUFFER * state->pageSize + state->headerSize;
    void *data = (int8_t *)key + state->keySize;
    uint32_t *varDataSize = (uint32_t *)((int8_t *)state->buffer + SBITS_VAR_WRITE_BUFFER(state->parameters) * state->pageSize + state->variableDataHeaderSize);
    void *varData = (int8_t *)state->buffer + SBITS_VAR_WRITE_BUFFER(state->parameters) * state->pageSize + state->variableDataHeaderSize + sizeof(uint32_t);

    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedKey, key, state->keySize, "Key was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, data, state->dataSize, "Data was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedVarDataSize, *varDataSize, "Vardata size was not correct with 1 record inserted");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedVarData, varData, 15, "Vardata was not correct with 1 record inserted");
}

void test_get_when_almost_almost_full_page() {
    // Check that page gasn't been written
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "sbits should not have written a page yet");
    // Check that there is still space for another record
    TEST_ASSERT_EACH_EQUAL_CHAR_MESSAGE(0, (int8_t *)state->buffer + SBITS_DATA_WRITE_BUFFER * state->pageSize + (state->pageSize - state->recordSize), state->recordSize, "There isn't space for another record in the buffer");
}

void test_get_when_almost_full_page() {
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "sbits should not have written a page yet");
}

void test_get_when_full_page() {
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextDataPageId, "sbits should have written a page by now");

    uint32_t key = 23;
    uint64_t expectedData = 23, data = 0;
    sbitsVarDataStream *varStream = NULL;
    sbitsGetVar(state, &key, &data, &varStream);
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, &data, state->dataSize, "sbitsGetVar did not return the correct fixed data");
    TEST_ASSERT_NOT_NULL_MESSAGE(varStream, "sbitsGetVar did not return vardata");
    char buf[20];
    uint32_t length = sbitsVarDataStreamRead(state, varStream, buf, 20);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, length, "Returned vardata was not the right length");
    char expected[] = "Testing 023...";
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expected, buf, 15, "sbitsGetVar did not return the correct vardata");

    if (varStream != NULL) {
        free(varStream);
    }
}

void test_get_when_all() {
    char expectedVarData[] = "Testing 000...";
    char buf[20];
    sbitsVarDataStream *varStream = NULL;
    for (uint32_t key = 0; key < numRecords; key++) {
        expectedVarData[10] = (char)(key % 10) + '0';
        expectedVarData[9] = (char)((key / 10) % 10) + '0';
        expectedVarData[8] = (char)((key / 100) % 10) + '0';
        uint64_t data = 0, expectedData = key % 100;

        int result = sbitsGetVar(state, &key, &data, &varStream);
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(&expectedData, &data, state->dataSize, "sbitsGetVar did not return the correct fixed data");
        TEST_ASSERT_NOT_NULL_MESSAGE(varStream, "sbitsGetVar did not return vardata");
        uint32_t length = sbitsVarDataStreamRead(state, varStream, buf, 20);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, length, "Returned vardata was not the right length");
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expectedVarData, buf, 15, "sbitsGetVar did not return the correct vardata");
        if (varStream != NULL) {
            free(varStream);
            varStream = NULL;
        }
    }
}

void test_insert_1() {
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertRecords(1), "sbitsPutVar was not successful when inserting a record");
}

void test_insert_lt_page() {
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, insertRecords(state->maxRecordsPerPage - inserted - 1), "Error while inserting records");
}

void test_insert_rest() {
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, insertRecords(numRecords - inserted), "Error while inserting records");
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
        sbitsFlush(state);
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
