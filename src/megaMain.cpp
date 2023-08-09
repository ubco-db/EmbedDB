#ifndef PIO_UNIT_TESTING

#include "Arduino.h"
#include "SPI.h"

/**
 * Includes for SD card
 */
/** @TODO optimize for clock speed */
#include "sdios.h"
static ArduinoOutStream cout(Serial);

#include "SdFat.h"
#include "sd_test.h"
#include "sdcard_c_iface.h"
#include "serial_c_iface.h"

#define TEST 0
#if TEST == 0
#include "test_sbits.h"
#elif TEST == 1
#include "varTest.h"
#endif

#define ENABLE_DEDICATED_SPI 1
#define SPI_DRIVER_SELECT 1
// SD_FAT_TYPE = 0 for SdFat/File as defined in SdFatConfig.h,
// 1 for FAT16/FAT32, 2 for exFAT, 3 for FAT16/FAT32 and exFAT.
#define SD_FAT_TYPE 1
/** @TODO Update max SPI speed for SD card */
#define SD_CONFIG SdSpiConfig(CS_SD, DEDICATED_SPI, SD_SCK_MHZ(12), &(SPISettings()))

SdFat32 sd;
File32 file;

// Headers
bool test_sd_card();

void setup() {
    Serial.begin(115200);
    while (!Serial) {
        delay(1);
    }

    delay(1000);
    Serial.println("Skeleton startup");

    pinMode(LED_BUILTIN, OUTPUT);

    /* Setup for SD card */
    Serial.print("\nInitializing SD card...");
    if (test_sd_card()) {
        file = sd.open("/");
        cout << F("\nList of files on the SD.\n");
        sd.ls("/", LS_R);
    }

    init_sdcard((void *)&sd);
#if TEST == 0
    runalltests_sbits();
#elif TEST == 1
    test_vardata();
#endif
}

void loop() {
    digitalWrite(LED_BUILTIN, HIGH);  // turn the LED on (HIGH is the voltage level)
    delay(1000);                      // wait for a second
    digitalWrite(LED_BUILTIN, LOW);   // turn the LED off by making the voltage LOW
    delay(1000);                      // wait for a second
}

/**
 * Testing for SD card -> Can be removed as needed */
bool test_sd_card() {
    if (!sd.cardBegin(SD_CHIP_SELECT, SD_SCK_MHZ(50))) {
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

#endif
