#include "dataflash.h"

/**
 * @todo - Make sure that the status is checked on operations for
 *              -writing
 *              -erasing
 *          otherwise you will get bad results (you can read a buffer while the chip is busy!)
 */


df_status_t 
get_status(
    memory_t*                   memory
    )
{
    df_status_t     status[2];

    //read one byte for device status -> there are two status bytes but the second currently doesn't contain useful information
    spi_write(memory, STATUS_REGISTER_READ, status, 2);   
    return status[0];
}

df_page_size_t
get_page_size(   
    memory_t*                   memory
    )
{
    df_status_t status = get_status(memory);
    if ((status & PAGE_SIZE_MASK) == PAGE_SIZE_MASK)     //then the device is a power of 2 page size
    {
        return memory->page_size;
    } 
    else
    {
        // add on extra OOB area
        return (memory->page_size>>5)+memory->page_size;
    }
}

df_dataflash_status_e
get_ready_status(
    memory_t*                   memory
    )
{
    df_status_t status = get_status(memory);
    if ((status & READY_MASK) == READY_MASK)     //then the device is a power of 2 page size
    {
        return DATAFLASH_READY;
    } 
    else
    {
        // add on extra OOB area
        return DATAFLASH_BUSY;
    } 
}

df_dataflash_status_e
get_page_memory_comparision(
    memory_t*                   memory
    )
{
    df_status_t status = get_status(memory);
    if ((status & COMPARE_MASK ) == COMPARE_MASK)     
    {
        return DATAFLASH_BUFFERS_DO_NOT_MATCH;
    } 
    else
    {
        return DATAFLASH_BUFFERS_MATCH;
    } 
}

void 
df_page_erase(
    memory_t*                   memory,
    df_page_addr_t              page
    )
{
    uint8_t address[3];
    df_compute_address_inline(memory,page,address);
    spi_write(memory, PAGE_ERASE, address, 3);   

}

void
df_erase_chip(
    memory_t*                   memory
)
{
    unsigned char data[3] = {0x94, 0x80, 0x9a};
    spi_write(memory, CHIP_ERASE, data, 3);   
}



/**
 * @param erase_moded selects the buffer to use and the erase mode
*/
void 
df_buffer_to_MM(
    memory_t*                   memory,
    erase_mode_t                erase_mode,
    df_page_addr_t              page
    )
{
    uint8_t address[3];
    df_compute_address_inline(memory,page,address);
    spi_write(
        memory,
        erase_mode,
        address,
        3);
}

/** @todo add protection for incorrect commands */
void
 df_MM_to_buffer(
    memory_t*                   memory,
    buffer_t                    buffer_command,
    df_page_addr_t              page
    )
{
    uint8_t address[3];
    df_compute_address_inline(memory,page,address);
    spi_write(memory,buffer_command,address,3);
}

void
df_MM_to_buffer_1(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_MM_to_buffer(memory,MM_PAGE_TO_BUFFER_1,page);
}


void
df_MM_to_buffer_2(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_MM_to_buffer(memory,MM_PAGE_TO_BUFFER_2,page);
}

void
df_buffer_1_to_MM_erase(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_buffer_to_MM(memory,BUFFER_1_TO_MM_W_ERASE,page);
}

void
df_buffer_2_to_MM_erase(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_buffer_to_MM(memory,BUFFER_2_TO_MM_W_ERASE,page);
}

void
df_buffer_1_to_MM_no_erase(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_buffer_to_MM(memory,BUFFER_1_TO_MM_NO_ERASE,page);
}

void
df_buffer_2_to_MM_no_erase(
    memory_t*                   memory,
    df_page_addr_t              page 
    )
{
    df_buffer_to_MM(memory,BUFFER_2_TO_MM_NO_ERASE,page);
}

/** @todo add page check boundaries when writting to address
 * to ensure that the data will be within bounds
*/
void
df_buffer_operation(
    memory_t*                   memory,
    buffer_t                    buffer_command,
    df_byte_offset_t            byte_offset,
    uint8_t*                    data,
    uint16_t                    length
    )
{
    uint8_t address[3];

    //address[0] contains dummy data
    address[2] = (uint8_t)(byte_offset & 0xff);        
    address[1] = (uint8_t)((byte_offset >> 8) & 0xff);
    //Depending on the type of operation the correct function needs to be called
    //to prevent the data from being overwritten
    if ((BUFFER_1_WRITE == buffer_command) || (BUFFER_2_WRITE == buffer_command))
        spi_write_data(memory,buffer_command,address,3,data,length);
    else if ((BUFFER_1_READ_LF == buffer_command) || (BUFFER_2_READ_LF == buffer_command))
        spi_read_data(memory,buffer_command,address,3,data,length);

}

void df_buffer_1_read(
    memory_t*                   memory,
    df_byte_offset_t            byte_offset,
    uint8_t*                    data,
    uint16_t                    length  
    )
{
    df_buffer_operation(memory,BUFFER_1_READ_LF,byte_offset,data,length);
}

void df_buffer_2_read(
    memory_t*                   memory,
    df_byte_offset_t            byte_offset,
    uint8_t*                    data,
    uint16_t                    length  
    )
{
    df_buffer_operation(memory,BUFFER_2_READ_LF,byte_offset,data,length);
}

void df_buffer_1_write(
    memory_t*                   memory,
    df_byte_offset_t            byte_offset,
    uint8_t*                    data,
    uint16_t                    length  
    )
{
    df_buffer_operation(memory,BUFFER_1_WRITE,byte_offset,data,length);
}

void df_buffer_2_write(
    memory_t*                   memory,
    df_byte_offset_t            byte_offset,
    uint8_t*                    data,
    uint16_t                    length  
    )
{
    df_buffer_operation(memory,BUFFER_2_WRITE,byte_offset,data,length);
}

void
df_compare_buffer_to_MM(
    memory_t*                   memory,
    buffer_t                    buffer_command,
    df_page_addr_t              page
    )
{
    uint8_t address[3];    
    df_compute_address_inline(memory, page, address);
    spi_write(memory,buffer_command,address,3);
}


void
df_compare_buffer_1_to_MM(
    memory_t*                   memory,
    df_page_addr_t              page
    )
{
    df_compare_buffer_to_MM(memory, MM_PAGE_TO_BUFFER_1_COMPARE, page);
}


void
df_compare_buffer_2_to_MM(
    memory_t*                   memory,
    df_page_addr_t              page
    )
{
    df_compare_buffer_to_MM(memory, MM_PAGE_TO_BUFFER_2_COMPARE, page);
}

inline void
df_compute_address_inline(
    memory_t*                   memory,
    df_page_addr_t              page,
    uint8_t*                    address
    ) 
{

    // //shift for the correct positions
    df_page_addr_t temp = (page << (memory->bits_per_page - 8));
    
    //resolve endianness issue (big endian)
    address[2] = 0;                                 //Dummy
    address[1] = (uint8_t)(temp & 0xff);            //LSB
    address[0] = (uint8_t)((temp >> 8) & 0xff);     //MSB
   
    // Serial.println("Address in fn - 1");
   
    // Serial.print("bits/page: ");
    // Serial.println(memory->bits_per_page);
    // Serial.println(memory->actual_page_size);
    // Serial.print(page);
    // Serial.print(" ");
    // Serial.print(temp);
    // Serial.print(" ");
    // Serial.print(" converted address: ");
    // Serial.print(*((uint16_t *)address),HEX);
    // Serial.print(" out order -> (MSB->LSB): ");
    // Serial.print(address[0],HEX);
    // Serial.print(":");
    // Serial.print(address[1],HEX);
    // Serial.print(":");
    // Serial.print(address[2],HEX);
    // Serial.println();

    
}

void
df_compute_address(
    memory_t*                   memory,
    df_page_addr_t              page,
    uint8_t*                    address
    ) 
{
  df_compute_address_inline(memory,page,address); 
}

void
df_continious_array_read_lf(
    memory_t*                   memory,
    df_page_addr_t              page,
    df_byte_offset_t            byte_offet,
    uint8_t*                    data,
    uint16_t                    length
    )
{
    uint8_t address[3];

    df_compute_address_inline(memory, page, address);
    // add if byte offset
    address[2] = (uint8_t)(0x00ff & byte_offet);
    //figure out the upper bits that need to get brought in 
    uint8_t temp = (( (1 << (memory->bits_per_page - 8)) - 1)  &  (byte_offet >> 8));
    address[1] |= temp;

    spi_read_data(memory,CONTINIOUS_ARRAY_READ_LF, address, 3, data, length);

}

/** 
* memory needs to be preallocated.  Will only ready single page
*/
void
df_main_memory_read(
    memory_t*                   memory,
    df_page_addr_t              page,
    df_byte_offset_t            byte_offet,
    uint8_t*                    data,
    uint16_t                    length
    )
{   
    uint8_t address[3];

    df_compute_address_inline(memory, page, address);
    
    //add in byte offset
    address[2] = (uint8_t)(0x00ff & byte_offet);
    
    //figure out the upper bits that need to get brought in 
    uint8_t temp = (( (1 << (memory->bits_per_page - 8)) - 1)  &  (byte_offet >> 8));
    address[1] |= temp;

    // four dummy bytes -> just read anything in
    spi_read_data(memory,MAIN_MEMORY_PAGE_READ, address, 7, data, length);
}

void
df_get_device_id(
    memory_t*                  memory,
    uint8_t*                   data
)
{
    spi_write(memory, MFGR_DEVICE_ID, data, 5);
}

void
df_set_page_size(
    memory_t*                   memory,
    bool                        extended
    )
{
    uint8_t data[3] = {0x2A,0x80,0xA6};
    if (extended)  data[2] = 0xA7;

    spi_write(memory, CONFIGURE_PAGE_SIZE, data, 3);
}

void
df_initialize(
    memory_t*                   memory
    )
{
    memory->spi->begin();
    memory->actual_page_size = get_page_size(memory);
    memory->bits_per_page = (uint8_t)ceil(log2(memory->actual_page_size));

}