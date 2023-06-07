
#include "Arduino.h"
#include "SPI.h"

#include "../lib/file/dataflash_c_iface.h"
#include "../lib/file/sdcard_c_iface.h"

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

#include "../src/sbits.h"
#include "SdFat.h"
#include "sd_test.h"

#include "unity.h"

#define ENABLE_DEDICATED_SPI 1
#define SPI_DRIVER_SELECT 1
#define SD_FAT_TYPE 1
#define SD_CONFIG SdSpiConfig(CS_SD, DEDICATED_SPI, SD_SCK_MHZ(12), &spi_0)

SdFat32 sd;
File32 file;

void setUp(void) {
}

void tearDown(void) {
}

void test_test(void) {
    TEST_ASSERT_EQUAL_INT(1, 0);
}

int runUnityTests(void) {
    UNITY_BEGIN();
    RUN_TEST(test_test);
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

void setupSbits() {
}

void setup() {
    setupBoard();
    setupSbits();
    runUnityTests();
}

void loop() {
}
