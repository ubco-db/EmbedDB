#if !defined(MEMBOARD_TEST_SETUP)
#define MEMBOARD_TEST_SETUP

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
#include "SdFat.h"
#include "sd_test.h"
#include "sdios.h"

bool test_sd_card();
void setupBoard();

#endif
