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
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);

    state->numDataPages = 10000;
    state->eraseSizeInPages = 2;
    state->numIndexPages = 4;
    state->bitmapSize = 1;
    state->parameters = SBITS_USE_INDEX | SBITS_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
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
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);

    state->numDataPages = 10000;
    state->numIndexPages = 4;
    state->eraseSizeInPages = 2;
    state->bitmapSize = 1;
    state->parameters = SBITS_USE_INDEX;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
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
    tearDownSDFile(state->indexFile);
    free(state->fileInterface);
    free(state);
}

void insertRecordsLinearly(int32_t startingKey, int32_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int32_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void sbits_index_file_correctly_reloads_with_no_data() {
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(496, state->maxIdxRecordsPerPage, "SBITS maxIdxRecordsPerPage was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextIdxPageId, "SBITS nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->numAvailIndexPages, "SBITS nextIdxPageId was initialized incorrectly when no data was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "SBITS minIndexPageId was initialized incorrectly when no data was present in the index file.");
}

void sbits_index_file_correctly_reloads_with_one_page_of_data() {
    insertRecordsLinearly(100, 100, 31312);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextIdxPageId, "SBITS nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(3, state->numAvailIndexPages, "SBITS nextIdxPageId was initialized incorrectly when one index page was present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "SBITS minIndexPageId was initialized incorrectly when one index page was present in the index file.");
}

void sbits_index_file_correctly_reloads_with_four_pages_of_data() {
    insertRecordsLinearly(100, 100, 125056);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(4, state->nextIdxPageId, "SBITS nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "SBITS nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minIndexPageId, "SBITS minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

void sbits_index_file_correctly_reloads_with_eleven_pages_of_data() {
    insertRecordsLinearly(100, 100, 343792);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(11, state->nextIdxPageId, "SBITS nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailIndexPages, "SBITS nextIdxPageId was initialized incorrectly when four index pages were present in the index file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7, state->minIndexPageId, "SBITS minIndexPageId was initialized incorrectly when four index pages were present in the index file.");
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(sbits_index_file_correctly_reloads_with_no_data);
    RUN_TEST(sbits_index_file_correctly_reloads_with_one_page_of_data);
    RUN_TEST(sbits_index_file_correctly_reloads_with_four_pages_of_data);
    return UNITY_END();
}

void loop() {}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}
