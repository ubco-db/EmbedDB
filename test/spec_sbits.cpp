
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

void updateBitmapInt8Bucket(void *data, void *bm);
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm);
int8_t inBitmapInt8Bucket(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void updateBitmapInt64(void *data, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
int8_t int32Comparator(void *a, void *b);
bool test_sd_card();

SdFat32 sd;
File32 file;

void setUp(void) {
}

void tearDown(void) {
}

void test_init(void) {
    int result = sbitsInit(state, 0);
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, result, "SBITS init failed.");
}

int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(test_init);
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

void setupSbits(void *storage) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    int8_t M = 6;
    if (state == NULL) {
        printf("Unable to allocate state. Exiting.\n");
        return;
    }

    state->recordSize = 16;
    state->keySize = 4;
    state->dataSize = 12;
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
    state->storageType = FILE_STORAGE;
    state->storage = storage;
    state->startAddress = 0;
    state->endAddress = 6000 * state->pageSize;  // state->pageSize * numRecords / 10; /* Modify this value lower to test wrap around */
    state->eraseSizeInPages = 4;
    // state->parameters = SBITS_USE_MAX_MIN | SBITS_USE_BMAP |
    // SBITS_USE_INDEX;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
    // state->parameters =  0;
    if (SBITS_USING_INDEX(state->parameters) == 1)
        state->endAddress += state->pageSize * (state->eraseSizeInPages * 2);
    if (SBITS_USING_BMAP(state->parameters))
        state->bitmapSize = 8;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt64;
    state->updateBitmap = updateBitmapInt64;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
}

void setup() {
    setupBoard();
    setupSbits(&at45db32_m);
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
