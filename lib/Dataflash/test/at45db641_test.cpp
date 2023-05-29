#include "at45db641_test.h"

/**
 * 
 *      Tests:
 *              -buffer read and write for both buffer 1 and buffer 1
 *              -page erase and read
 *              -writing with erase and without erase for both buffer 1 and 2
 *              -Direct reads
 */

//#define SHOW_OUTPUT 1

#define PAGE_SIZE               264

static uint8_t data[264];

static char *test_buffer_read_write_buffer_1()
{
    Serial.println(__func__);
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //erase page 0
    df_page_erase(&at45db641_m, 1);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //read into buffer 1
    df_MM_to_buffer_1(&at45db641_m, 1);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //check for 0
    uint8_t temp_data[PAGE_SIZE];
    df_buffer_1_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
#ifdef SHOW_OUTOUT
        Serial.print(temp_data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on erase", temp_data[i] == 0xff);
    }
    //write data to buffer
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //read back and compare
    df_buffer_1_read(&at45db641_m, 0, temp_data, PAGE_SIZE);

#ifdef SHOW_OUTPUT
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
        //mu_assert("error, bad data on readback", temp_data[i] == data[i]);
    }
    Serial.println("\nActual Data");
#endif
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(temp_data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on readback", temp_data[i] == data[i]);
    }

    //write offsets in buffer
    uint8_t cell = 0xff;
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        //knockout cell
        df_buffer_1_write(&at45db641_m, i, &cell, 1);
        //read back and compare
        data[i] = cell;

        df_buffer_1_read(&at45db641_m, 0, temp_data, PAGE_SIZE);

        for (uint16_t j = 0; j < PAGE_SIZE; j++)
        {
#ifdef SHOW_OUTPUT
            Serial.print(temp_data[j]);
            Serial.print(" ");
            if ((j + 1) % 32 == 0)
                Serial.println();
#endif
            mu_assert("error, bad data on readback", temp_data[j] == data[j]);
        }
    }

    return 0;
}

static char *test_buffer_read_write_buffer_2()
{
    Serial.println(__func__);
    //Serial.println("Input dataset");
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //erase page 0
    df_page_erase(&at45db641_m, 1);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //read into buffer 1
    df_MM_to_buffer_2(&at45db641_m, 1);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //check for 0
    uint8_t temp_data[PAGE_SIZE];
    df_buffer_2_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    //Serial.println("\nErase data");
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
#ifdef SHOW_OUTOUT
        Serial.print(temp_data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on erase", temp_data[i] == 0xff);
    }
    //write data to buffer
    df_buffer_2_write(&at45db641_m, 0, data, PAGE_SIZE);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //read back and compare
    df_buffer_2_read(&at45db641_m, 0, temp_data, PAGE_SIZE);

//Serial.println("\nRead back");
#ifdef SHOW_OUTPUT
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
        //mu_assert("error, bad data on readback", temp_data[i] == data[i]);
    }
    Serial.println("\nActual Data");
#endif
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(temp_data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on readback", temp_data[i] == data[i]);
    }

    //write offsets in buffer
    uint8_t cell = 0xff;
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        //knockout cell
        df_buffer_2_write(&at45db641_m, i, &cell, 1);
        //read back and compare
        data[i] = cell;

        df_buffer_2_read(&at45db641_m, 0, temp_data, PAGE_SIZE);

        for (uint16_t j = 0; j < PAGE_SIZE; j++)
        {
#ifdef SHOW_OUTPUT
            Serial.print(temp_data[j]);
            Serial.print(" ");
            if ((j + 1) % 32 == 0)
                Serial.println();
#endif
            mu_assert("error, bad data on readback", temp_data[j] == data[j]);
        }
    }

    return 0;
}

static char *test_write_to_MM_page_w_erase_buffer_1()
{
    Serial.println(__func__);
    //Serial.println("\nInput dataset");
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }

    uint8_t temp_data[PAGE_SIZE];

    //write to buffer 1 and store in page
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    df_buffer_1_to_MM_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };
    //read back to buffer 2 and compare
    df_MM_to_buffer_2(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_2_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    //check buffer compare method
    for (uint16_t j = 0; j < PAGE_SIZE; j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(" ");
        if ((j + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == data[j]);
    }
    return 0;
}

static char *test_write_to_MM_page_w_erase_buffer_2()
{
    Serial.println(__func__);
    //Serial.println("\nInput dataset");
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }

    uint8_t temp_data[PAGE_SIZE];

    //write to buffer 1 and store in page
    df_buffer_2_write(&at45db641_m, 0, data, PAGE_SIZE);
    df_buffer_2_to_MM_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };
    //read back to buffer 2 and compare
    df_MM_to_buffer_1(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_1_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    //check buffer compare method
    for (uint16_t j = 0; j < PAGE_SIZE; j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(" ");
        if ((j + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == data[j]);
    }
    return 0;
}

/**
 * Write to main memory page without erasing the page first
 * and reads back
 */
static char *test_write_to_MM_page_wo_erase_buffer_1()
{
    Serial.println(__func__);
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }

    uint8_t temp_data[PAGE_SIZE];

    //erase the page
    df_page_erase(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //write to buffer 1 and store in page
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    df_buffer_1_to_MM_no_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };
    //read back to buffer 2 and compare
    df_MM_to_buffer_2(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_2_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    //check buffer compare method
    for (uint16_t j = 0; j < PAGE_SIZE; j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(" ");
        if ((j + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == data[j]);
    }
    return 0;
}

/**
 * Write to main memory page without erasing the page first
 * and reads back
 */
static char *test_write_to_MM_page_wo_erase_buffer_2()
{
    Serial.println(__func__);
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }

    uint8_t temp_data[PAGE_SIZE];

    //erase the page
    df_page_erase(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //write to buffer 1 and store in page
    df_buffer_2_write(&at45db641_m, 0, data, PAGE_SIZE);
    df_buffer_2_to_MM_no_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };
    //read back to buffer 2 and compare
    df_MM_to_buffer_1(&at45db641_m, 0);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_1_read(&at45db641_m, 0, temp_data, PAGE_SIZE);
    //check buffer compare method
    for (uint16_t j = 0; j < PAGE_SIZE; j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(" ");
        if ((j + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == data[j]);
    }
    return 0;
}

/**
 * Read back data directly from a memor page */
static char *test_MM_direct_read()
{
    Serial.println(__func__);
    //input dataset
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //write to buffer 1 and store in page
    df_buffer_2_write(&at45db641_m, 0, data, PAGE_SIZE);
    df_buffer_2_to_MM_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    uint8_t temp_data[PAGE_SIZE];
    df_main_memory_read(&at45db641_m, 0, 0, temp_data, PAGE_SIZE);
    //check buffer compare method
    for (uint16_t j = 0; j < PAGE_SIZE; j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(" ");
        if ((j + 1) % 32 == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == data[j]);
    }
    return 0;
}

/**
 * Read back data directly from a memory page
 *  Also tests partial buffer writes 
 */
static char *test_MM_direct_read_partial()
{
    Serial.println(__func__);

    uint8_t small_data[] = {0x01, 0x02, 0x03, 0x04};

    //erase the buffer
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = 0xff;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //write to
    //write to buffer 1 and store in page
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    //update partial record - watch out for wrapping - checks only the PAGE_SIZE byte page
    //Data should wrap around
    for (uint16_t position = 0; position < PAGE_SIZE; position++)
    {
        df_buffer_1_write(&at45db641_m, position, small_data, 4);
        df_buffer_1_to_MM_erase(&at45db641_m, 0);
        //need to have a wait state here!
        while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
        {
        };
        uint8_t temp_data[] = {0xff, 0xff, 0xff, 0xff};

        df_main_memory_read(&at45db641_m, 0, position, temp_data, 4);
        //check buffer compare method
        for (uint16_t j = 0; j < 4; j++)
        {
#ifdef SHOW_OUTPUT
            Serial.print(j);
            Serial.print(":");
            Serial.print(temp_data[j]);
            Serial.print(":");
            Serial.print(small_data[j]);
            Serial.print(" ");
            if ((position + 1) % 32 == 0)
                Serial.println();
#endif
            mu_assert("error, bad data on compare", temp_data[j] == small_data[j]);
        }
    }
    return 0;
}

/**
 *  Compares data directly from page
 *  Also tests partial buffer writes 
 */
static char *test_MM_memory_comparison()
{
    Serial.println(__func__);

    uint8_t small_data[] = {0x01, 0x02, 0x03, 0x04};

    //erase the buffer
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[i] = 0xff;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //write to
    //write to buffer 1 and store in page
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    //update partial record - watch out for wrapping - checks only the PAGE_SIZE byte page
    //Data should wrap around
    for (uint16_t position = 0; position < PAGE_SIZE; position++)
    {
        df_buffer_1_write(&at45db641_m, position, small_data, 4);

        df_buffer_1_to_MM_erase(&at45db641_m, 0);
        //need to have a wait state here!
        while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
        {
        };

        df_compare_buffer_1_to_MM(&at45db641_m, 0);
        while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
        {
        };

        mu_assert("error, bad data on compare", DATAFLASH_BUFFERS_MATCH == get_page_memory_comparision(&at45db641_m));
    }
    return 0;
}

/**
 * Writes across all pages with test data 
 */
static char *test_page_access()
{
    Serial.println(__func__);

    //8192 pages for this device

    df_erase_chip(&at45db641_m);

    uint16_t offset = 0;

    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //erase the buffer
    for (uint16_t i = 0; i < PAGE_SIZE; i++)
    {
        data[(i + offset) % PAGE_SIZE] = i;
#ifdef SHOW_OUTOUT
        Serial.print(data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //write to buffer 1 and store in page
    df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
    //update partial record - watch out for wrapping - checks only the PAGE_SIZE byte page
    //Data should wrap around
    for (uint16_t page = 0; page < 8192; page++)
    {
        for (uint16_t i = 0; i < PAGE_SIZE; i++)
        {
            data[(i + offset) % PAGE_SIZE] = i;
#ifdef SHOW_OUTOUT
            Serial.print(data[i]);
            Serial.print(" ");
            if ((i + 1) % 32 == 0)
                Serial.println();
#endif
        }
        df_buffer_1_write(&at45db641_m, 0, data, PAGE_SIZE);
        df_buffer_1_to_MM_no_erase(&at45db641_m, page);
        //need to have a wait state here!
        while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
        {
        };

        //read directly and compare
        uint8_t temp_data[PAGE_SIZE];

        df_main_memory_read(&at45db641_m, page, 0, temp_data, PAGE_SIZE);

        for (uint16_t j = 0; j < PAGE_SIZE; j++)
        {
#ifdef SHOW_OUTPUT
            Serial.print(j);
            Serial.print(":");
            Serial.print(temp_data[j]);
            Serial.print(" ");
            if ((j + 1) % 32 == 0)
                Serial.println();
#endif
            mu_assert("error, bad data on compare", temp_data[j] == data[j]);
        }
    }
    return 0;
}

static char *test_address_translation()
{
    Serial.println(__func__);

    //32768 pages for this device

    /** 
    * For a 264 byte page, the address is composed of:
    *       -15 page address bits (MSB -> LSB)
    *       -9 dummy bits
    *
    * For a 265 byte page, the address is composed of:
    *       -one dummy bit
    *       -15 page address bits (MSB -> LSB)
    *       -8 dummy bits
    */

    // uint16_t            page_size;
    // uint8_t             bits_per_page;
    // uint16_t            actual_page_size;
    memory_t test_memory; //just need to

    test_memory.page_size = 256;
    test_memory.bits_per_page = 9;
    test_memory.actual_page_size = 264;

    u_int16_t test_address = 0;

    for (test_address = 0; test_address < 8192; test_address++)
    {
        uint8_t address_array[3];

        //watch out for endianness!
        address_array[1] = (uint8_t)(test_address & 0xff);
        address_array[0] = (uint8_t)((test_address >> 8) & 0xff);

        *((uint16_t *)address_array) = (*((uint16_t *)address_array) << 1); //(byte 0 and 1 contain address shifted by 2)

        uint8_t converted_address[3];

        df_compute_address(&test_memory, test_address, converted_address);

        mu_assert("error, bad first byte", (0x7F & address_array[0]) == (0x7F & converted_address[0]));  //mask off upper bit
        mu_assert("error, bad second byte", (0xFC & address_array[1]) == (0xFC & converted_address[1])); //mask off two lower bits (d/c)
    }

    test_memory.page_size = 256;
    test_memory.bits_per_page = 8;
    test_memory.actual_page_size = 256;

    for (test_address = 0; test_address < 8192; test_address++)
    {
        uint8_t address_array[3];

        //watch out for endianness!
        address_array[1] = (uint8_t)(test_address & 0xff);
        address_array[0] = (uint8_t)((test_address >> 8) & 0xff);

        //*((uint16_t *)address_array) = (*((uint16_t *)address_array) << 1); //(byte 0 and 1 contain address shifted by 1 for 512 byte buffer)

        uint8_t converted_address[3];

        df_compute_address(&test_memory, test_address, converted_address);

        mu_assert("error, bad first byte", (0x7F & address_array[0]) == (0x7F & converted_address[0]));  //mask off upper bit
        mu_assert("error, bad second byte", (0xFC & address_array[1]) == (0xFC & converted_address[1])); //mask off two lower bits (d/c)
    }

    return 0;
}

static char *test_continious_read_lf()
{
    Serial.println(__func__);
    //input dataset
    uint8_t large_data[PAGE_SIZE * 4];

    for (uint16_t i = 0; i < PAGE_SIZE * 4; i++)
    {
        large_data[i] = (uint8_t)i;
#ifdef SHOW_OUTOUT
        Serial.print(large_data[i]);
        Serial.print(" ");
        if ((i + 1) % 32 == 0)
            Serial.println();
#endif
    }
    //write to buffer 2 and store in page 0
    df_buffer_2_write(&at45db641_m, 0, large_data, PAGE_SIZE);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_2_to_MM_erase(&at45db641_m, 0);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    // //write to buffer 2 and store in page 1
    df_buffer_2_write(&at45db641_m, 0, (large_data + PAGE_SIZE), PAGE_SIZE);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    df_buffer_2_to_MM_erase(&at45db641_m, 1);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //write to buffer 2 and store in page 3
    df_buffer_2_write(&at45db641_m, 0, (large_data + (2 * PAGE_SIZE)), PAGE_SIZE);
    df_buffer_2_to_MM_erase(&at45db641_m, 2);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //write to buffer 2 and store in page 4
    df_buffer_2_write(&at45db641_m, 0, (large_data + (3 * PAGE_SIZE)), PAGE_SIZE);
    df_buffer_2_to_MM_erase(&at45db641_m, 3);
    //need to have a wait state here!
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    uint8_t temp_data[4 * PAGE_SIZE];
    df_continious_array_read_lf(&at45db641_m, 0, 0, temp_data, 4 * PAGE_SIZE);
    //df_main_memory_read(&at45db641_m,0,0,temp_data,PAGE_SIZE);
    while (DATAFLASH_BUSY == get_ready_status(&at45db641_m))
    {
    };

    //df_main_memory_read(&at45db641_m,1,0,(temp_data+PAGE_SIZE),PAGE_SIZE);
    //while (DATAFLASH_BUSY == get_ready_status(&at45db641_m)){};

    //check buffer compare method
    for (uint16_t j = 0; j < (4 * PAGE_SIZE); j++)
    {
#ifdef SHOW_OUTPUT
        Serial.print(j);
        Serial.print(":");
        Serial.print(temp_data[j]);
        Serial.print(":");
        Serial.print(large_data[j]);
        Serial.print(" ");
        if ((j + 1) % 16 == 0)
            Serial.println();
        if ((j + 1) % PAGE_SIZE == 0)
            Serial.println();
#endif
        mu_assert("error, bad data on compare", temp_data[j] == large_data[j]);
    }
    
    return 0;
}
static char *check_device_id()
{
    Serial.println(__func__);

    df_get_device_id(&at45db641_m, data);

    for (uint8_t i = 0; i < 5; i++) //5 bytes for id
    {
#ifdef SHOW_OUTPUT
        Serial.print(i);
        Serial.print(":");
        Serial.print(at45db641_m.device_id[i]);
        Serial.print(":");
        Serial.print(data[i]);
        Serial.print(" ");
#endif
        mu_assert("error, bad data on compare", at45db641_m.device_id[i] == data[i]);
    }

    return 0;
}

char *at45db641_all_tests()
{
    mu_run_test(check_device_id);
    mu_run_test(test_address_translation);
    mu_run_test(test_buffer_read_write_buffer_1);
    mu_run_test(test_buffer_read_write_buffer_2);
    mu_run_test(test_write_to_MM_page_w_erase_buffer_1);
    mu_run_test(test_write_to_MM_page_w_erase_buffer_2);
    mu_run_test(test_write_to_MM_page_wo_erase_buffer_1);
    mu_run_test(test_write_to_MM_page_wo_erase_buffer_2);
    mu_run_test(test_MM_direct_read);
    mu_run_test(test_MM_direct_read_partial);
    mu_run_test(test_MM_memory_comparison);
    mu_run_test(test_page_access);
    mu_run_test(test_continious_read_lf);
    return 0;
}
