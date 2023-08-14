#if !defined(DUE_TEST_SETUP)
#define DUE_TEST_SETUP

#include "Arduino.h"
#include "SPI.h"

/**
 * Includes for SD card
 */
/** @TODO optimize for clock speed */
#include "sdios.h"

#include "SdFat.h"
#include "sd_test.h"
#include "sdcard_c_iface.h"
#include "serial_c_iface.h"

const uint8_t SD_CS_PIN = 4;
#define SD_CONFIG SdSpiConfig(SD_CS_PIN, SHARED_SPI, SD_SCK_MHZ(12))

bool test_sd_card();
void setupBoard();

#endif
