/**
 * Methods for use across all dataflash devices.
 */


#ifndef _DATAFLASH_H_
#define _DATAFLASH_H_

#include "mem_spi.h"

#define     PAGE_SIZE                   528

//READ
#define     CONTINIOUS_ARRAY_READ_HF                0x1B
#define     CONTINIOUS_ARRAY_READ_LF                0x03
#define     CONTINIOUS_ARRAY_READ_LP                0x01
#define     MAIN_MEMORY_PAGE_READ                   0xD2
#define     BUFFER_1_READ_LF                        0xD1
#define     BUFFER_2_READ_LF                        0xD3
#define     BUFFER_1_READ_HF                        0xD4
#define     BUFFER_2_READ_HF                        0xD8

//PROGRAM & ERASE

#define     BUFFER_1_WRITE                          0x84
#define     BUFFER_2_WRITE                          0x87
#define     BUFFER_1_TO_MM_W_ERASE                  0x83
#define     BUFFER_2_TO_MM_W_ERASE                  0x86
#define     BUFFER_1_TO_MM_NO_ERASE                 0x88
#define     BUFFER_2_TO_MM_NO_ERASE                 0x89
#define     MM_THROUGH_BUFFER_1_W_ERASE             0x82
#define     MM_THROUGH_BUFFER_2_W_ERASE             0x85
#define     MM_BYTE_THROUGH_BUFFER_1_NO_ERASE       0x02
#define     PAGE_ERASE                              0x81
#define     BLOCK_ERASE                             0x50
#define     SECTOR_ERASE                            0x7C
#define     READ_MODIDY_WRITE_THORUGH_BUFFER_1      0x58
#define     READ_MODIDY_WRITE_THORUGH_BUFFER_2      0x58

#define     MM_PAGE_TO_BUFFER_1                     0x53
#define     MM_PAGE_TO_BUFFER_2                     0x55
#define     MM_PAGE_TO_BUFFER_1_COMPARE             0x60
#define     MM_PAGE_TO_BUFFER_2_COMPARE             0x61
#define     AUTO_PAGE_REWRITE_BUFFER_1              0x58
#define     AUTO_PAGE_REWRITE_BUFFER_2              0x59

#define     STATUS_REGISTER_READ                    0xD7

#define     CHIP_ERASE                              0xC7

//JEDEC COMPLIANT
#define     MFGR_DEVICE_ID                          0x9F

#define     PAGE_SIZE_MASK                          0x01        // b0000 0001
#define     DENSITY_MASK                            0x3C        // b0011 1100
#define     READY_MASK                              0x80        // b1000 0000
#define     COMPARE_MASK                            0x40        // b0100 0000

#define     CONFIGURE_PAGE_SIZE                     0x3D

typedef enum DATAFLASH_STATUS
{
    DATAFLASH_READY,                                //  dataflash is free
    DATAFLASH_BUSY,                                 //  dataflash is busy with internal operation
    DATAFLASH_BUFFERS_MATCH,                        
    DATAFLASH_BUFFERS_DO_NOT_MATCH,
}  df_dataflash_status_e;

/** Page address **/
typedef     uint16_t                                df_page_addr_t;            
typedef     uint8_t                                 df_status_t;
typedef     uint16_t                                df_page_size_t;
typedef     uint16_t                                df_byte_offset_t;
typedef     uint8_t                                 erase_mode_t;
typedef     uint8_t                                 buffer_t;

/**
 * Read status register.
 * @return The content of the status register.
* **/
df_status_t 
get_status(
    memory_t*                       memory
);

/**
 * Returns the current configuration of the device's page size
 * as these devices can be a power of 2 or have extra bits/page.
 * @return The content of the status register.
* **/
df_page_size_t
get_page_size(   
    memory_t*                       memory
); 

df_dataflash_status_e
get_ready_status(
    memory_t*                       memory
);

df_dataflash_status_e
get_page_memory_comparision(
    memory_t*                       memory
);

/**
* Erase a page in the main memory array.
* @param page Page number to erase.
**/
void 
df_page_erase(
    memory_t*                       memory,
    df_page_addr_t                  page
    );

void
df_erase_chip(
    memory_t*                       memory
);

void 
df_buffer_to_MM(
    memory_t*                       memory,
    erase_mode_t                    erase_mode,
    df_page_addr_t                  page
);

void
df_MM_to_buffer(
    memory_t*                       memory,
    buffer_t                        buffer_command,
    df_page_addr_t                  page
);

void
df_MM_to_buffer_1
(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_MM_to_buffer_2
(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_buffer_1_to_MM_erase(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_buffer_2_to_MM_erase(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_buffer_1_to_MM_no_erase(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_buffer_2_to_MM_no_erase(
    memory_t*                       memory,
    df_page_addr_t                  page 
);

void
df_buffer_operation(
    memory_t*                       memory,
    buffer_t                        buffer_command,
    df_byte_offset_t                byte_offset,
    uint8_t*                        data,
    uint16_t                        length
);

void 
df_buffer_1_read(
    memory_t*                       memory,
    df_byte_offset_t                byte_offset,
    uint8_t*                        data,
    uint16_t                        length  
);

void 
df_buffer_2_read(
    memory_t*                       memory,
    df_byte_offset_t                byte_offset,
    uint8_t*                        data,
    uint16_t                        length  
);

void 
df_buffer_1_write(
    memory_t*                       memory,
    df_byte_offset_t                byte_offset,
    uint8_t*                        data,
    uint16_t                        length  
);

void 
df_buffer_2_write(
    memory_t*                       memory,
    df_byte_offset_t                byte_offset,
    uint8_t*                        data,
    uint16_t                        length  
);

/** Compares buffer to memory location
 * IMPORTANT: need to use status check to determine status
 */
void
df_compare_buffer_to_MM(
    memory_t*                       memory,
    buffer_t                        buffer_command,
    df_page_addr_t                  page
);

void
df_compare_buffer_1_to_MM(
    memory_t*                       memory,
    df_page_addr_t                  page
);

void
df_compare_buffer_2_to_MM(
    memory_t*                       memory,
    df_page_addr_t                  page
);


/**
 *  Given a page address, transforms into the correct memory format for
 *  dataflash memory.
 *  @param memory       the specific memory device
 *  @param page         number page address to convert  
 *  @param address      correctly formatted address (assumes that it is 3 bytes) - caller
 *                      must allocate.
 */
inline void
df_compute_address_inline(
    memory_t*                       memory,
    df_page_addr_t                  page,
    uint8_t*                        address
);

/** exposes for external testing
 */
void
df_compute_address(
    memory_t*                       memory,
    df_page_addr_t                  page,
    uint8_t*                        address
);

/**
 * Allows for the memory to be read in one contigious block.
 */
void
df_continious_array_read_lf(
    memory_t*                       memory,
    df_page_addr_t                  page,
    df_byte_offset_t                byte_offet,
    uint8_t*                        data,
    uint16_t                        ength
);

void
df_main_memory_read(
    memory_t*                       memory,
    df_page_addr_t                  page,
    df_byte_offset_t                byte_offet,
    uint8_t*                        data,
    uint16_t                        length
);

void
df_get_device_id(
    memory_t*                       memory,
    uint8_t*                        data
);

void
df_set_page_size(
    memory_t*                       memory,
    bool                            extended
);


/**
 * Initializes the memory device in memory for the number 
 * of bits per page and page size.  
 * 
 * This function needs to be called before using the device. 
 * 
 * @param memory memory device
 */
void
df_initialize(
    memory_t*                       memory
);

#endif