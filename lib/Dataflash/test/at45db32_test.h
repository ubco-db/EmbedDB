#ifndef _AT45DB32_TEST_H_
#define _AT45DB32_TEST_H_

#include "SPI.h"
#include "mem_spi.h"

#include "minuint.h"
#include "dataflash.h"

static char * check_device_id();
static char * test_address_translation();
static char * test_buffer_read_write_buffer_1();
static char * test_buffer_read_write_buffer_2();
static char * test_write_to_MM_page_w_erase_buffer_1();
static char * test_write_to_MM_page_w_erase_buffer_2();
static char * test_write_to_MM_page_wo_erase_buffer_1();
static char * test_write_to_MM_page_wo_erase_buffer_2();
static char * test_MM_direct_read();
static char * test_MM_direct_read_partial();
static char * test_MM_memory_comparison();
static char * test_page_access();
static char * test_continious_read_lf();

//runner
char * at45db32_all_tests();
#endif