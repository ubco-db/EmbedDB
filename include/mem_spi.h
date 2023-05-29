/**
 * mem_spi.cpp
 * 
 * SPI drivers for the different memory modules on the MemBoard.
 * 
 * Do not change this configuration.
 *
 */

#ifndef _MEM_SPI_H_
#define _MEM_SPI_H_

#include "SPI.h"


#define status_t               unsigned char
#define pin_number_t           unsigned char

extern SPIClass spi_0;
extern SPIClass spi_1;
extern SPIClass spi_2;
extern SPIClass spi_4;
extern SPIClass spi_5;

/**
 * Maintains information regarding device
 */

typedef struct Memory
{
    SPIClass*           spi;
    SPISettings         spi_settings;
    pin_number_t        cs_pin; 
    uint8_t             device_id[9];
    uint16_t            page_size;
    volatile uint8_t    bits_per_page;                
    uint16_t            actual_page_size;               
} memory_t;

/**
 * Sercom 2 configuration for Adesto parts
 **/
#define MOSI_2 6ul
#define SCLK_2 7ul
#define MISO_2 8ul
#define CS_DB64 9ul
#define CS_DB32 10ul
#define CS_AT25 11ul

/**
 * Sercom 1 configuration for large capacity NOR
 **/
#define MOSI_1 17ul
#define SCLK_1 18ul
#define MISO_1 19ul
#define CS_MT25 20ul
#define CS_GD25 21ul

/**
 * Sercom 4 configuration for Special memory
 **/
#define MOSI_4 12ul
#define SCLK_4 13ul
#define MISO_4 14ul
#define CS_M3008 15ul
#define CS_CY15 16ul

/**
 * Sercom 5 configuration for SD card
 **/
#define MOSI_5 22ul
#define SCLK_5 23ul
#define MISO_5 24ul
#define CS_GD5F 25ul
#define CS_W25 26ul

extern memory_t                at45db32_m;
extern memory_t                at45db641_m;
extern memory_t                at25_m;
extern memory_t                mt25_m;
extern memory_t                gd25_m;
extern memory_t                m3008_m;
extern memory_t                cy15_m;
extern memory_t                gd5f_m;
extern memory_t                w25_m;

void
spi_write(
    memory_t*           memory, 
    unsigned char       cmd, 
    unsigned char *     data, 
    uint16_t            length
    );
    
void
spi_write_data(
    memory_t*                   memory, 
    unsigned char               cmd, 
    unsigned char*              address,
    unsigned char               address_length,
    unsigned char*              data, 
    uint16_t                    data_length
    );

void
spi_read_data(
    memory_t*                   memory, 
    unsigned char               cmd, 
    unsigned char *             data_in, 
    unsigned char               length_in, 
    unsigned char *             data_out, 
    uint16_t                    length_out
    );
#endif