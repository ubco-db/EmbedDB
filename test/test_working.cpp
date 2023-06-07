
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

#include "SdFat.h"
#include "sbits.h"
#include "sd_test.h"

#include "sbits.h"
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
    state->endAddress = 6000 * state->pageSize; // state->pageSize * numRecords / 10; /* Modify this value lower to test wrap around */
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

void loop() {
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8Bucket(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and
 * max value. */
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm) {
    /* Note: Assuming int key is right at the start of the data record */
    uint8_t *bmval = (uint8_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = 255; /* Everything */
    } else {
        int8_t i = 0;
        uint8_t val = 128;
        if (min != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(min, bm);

            /* Assume here that bits are set in increasing order based on
             * smallest value */
            /* Find first set bit */
            while ((val & *bmval) == 0 && i < 8) {
                i++;
                val = val / 2;
            }
            val = val / 2;
            i++;
        }
        if (max != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(max, bm);

            while ((val & *bmval) == 0 && i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        } else {
            while (i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        }
    }
}

int8_t inBitmapInt8Bucket(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8Bucket(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15; // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10; // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
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
