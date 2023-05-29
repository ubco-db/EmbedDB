/**
 * mem_spi.cpp
 * 
 * SPI drivers for the different memory modules on the MemBoard.
 * 
 * Do not change this configuration.
 *
 */
 
#include "mem_spi.h"

/** @TODO better name? **/
SPIClass spi_0(&sercom0, MISO_SD, SCLK_SD, MOSI_SD, SPI_PAD_0_SCK_1, SERCOM_RX_PAD_2);

SPIClass spi_2(&sercom2, MISO_2, SCLK_2, MOSI_2, SPI_PAD_0_SCK_1, SERCOM_RX_PAD_2);

SPIClass spi_1(&sercom1, MISO_1, SCLK_1, MOSI_1, SPI_PAD_0_SCK_1, SERCOM_RX_PAD_2);

SPIClass spi_4(&sercom4, MISO_4, SCLK_4, MOSI_4, SPI_PAD_0_SCK_1, SERCOM_RX_PAD_2);

SPIClass spi_5(&sercom5, MISO_5, SCLK_5, MOSI_5, SPI_PAD_0_SCK_1, SERCOM_RX_PAD_2);

memory_t                at45db32_m{
                            &spi_2,
                            SPISettings(12000000, MSBFIRST, SPI_MODE3),
                            CS_DB32,
                            {0x1F,0x27,0x01,0x01,0x00},
                            512
                            };


memory_t                at45db641_m{
                            &spi_2,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_DB64,
                            {0x1F,0x28,0x00,0x01,0x00},
                            256
                            };

memory_t                at25_m{
                            &spi_2,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_AT25,
                            {0x1F,0x89,0x01}
                            };    

//3 byte ID                            
memory_t                mt25_m{
                            &spi_1,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_MT25,
                            {0x20,0xBA,0x20}
                            };

memory_t                gd25_m{
                            &spi_1,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_GD25,
                            {0xCA,0x40,0x19}
                            };                                                                         

memory_t                m3008_m{
                            &spi_4,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_M3008,
                            {0xCA,0x40,0x19}  //udpate
                            };                                                                         
          

memory_t                cy15_m{
                            &spi_4,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_CY15,
                            {0x03,0x2E,0xC2,0x7F,0x7F,0x7F,0x7F,0x7F,0x7F}
                            };                                                                         


memory_t                gd5f_m{
                            &spi_5,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_GD5F,
                            {0xC8,0xB1,0x48}
                            };   


memory_t                w25_m{
                            &spi_5,
                            SPISettings(8000000, MSBFIRST, SPI_MODE0),
                            CS_W25,
                            {0xFF,0xEF,0xAA,0x21}
                            };  

void
spi_write(
    memory_t*                   memory, 
    unsigned char               cmd, 
    unsigned char*              data, 
    uint16_t                    length
    )
{
        memory->spi->beginTransaction(memory->spi_settings);
        digitalWrite(memory->cs_pin,LOW);
        memory->spi->transfer(cmd);
        memory->spi->transfer(data,length);
        digitalWrite(memory->cs_pin,HIGH);
        memory->spi->endTransaction();
}

void

spi_write_data(
    memory_t*                   memory, 
    unsigned char               cmd, 
    unsigned char*              address,
    unsigned char               address_length,
    unsigned char*              data, 
    uint16_t                    data_length
    )
{
        memory->spi->beginTransaction(memory->spi_settings);
        digitalWrite(memory->cs_pin,LOW);
        memory->spi->transfer(cmd);
        //Critical: set rxbuf to NULL to prevent data from being overwritten in the buffer
        //(const void *txbuf, void *rxbuf, size_t count,bool block) 
        memory->spi->transfer(address,address_length);        
        memory->spi->transfer(data,NULL,data_length);
        digitalWrite(memory->cs_pin,HIGH);
        memory->spi->endTransaction();
}

void
spi_read_data(
    memory_t*                   memory, 
    unsigned char               cmd, 
    unsigned char *             data_in, 
    unsigned char               length_in, 
    unsigned char *             data_out, 
    uint16_t                    length_out
    )
{
        memory->spi->beginTransaction(memory->spi_settings);
        digitalWrite(memory->cs_pin,LOW);
        memory->spi->transfer(cmd);
        memory->spi->transfer(data_in,length_in);
        memory->spi->transfer(data_out,length_out);
        digitalWrite(memory->cs_pin,HIGH);
        memory->spi->endTransaction();
}