/******************************************************************************/
/**
@file		sdcard_c_iface.cpp
@author		Ramon Lawrence
@brief		This code contains C wrapper for Sdfat library.
@details	Since the Arduino library doesn't have definitions for
                        standard I/O file functions, we have to write our own.
@copyright	Copyright 2021
                        The University of British Columbia,
                        IonDB Project Contributors (see AUTHORS.md)
@par Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
        this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
        this list of conditions and the following disclaimer in the documentation
        and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
        may be used to endorse or promote products derived from this software without
        specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
        AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
        IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
        ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
        LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
        CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
        SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
        INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
        CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
        ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
        POSSIBILITY OF SUCH DAMAGE.
*/
/******************************************************************************/

#include "sdcard_c_iface.h"

#include "SPI.h"
#include "SdFat.h"

#if defined(ARDUINO)
#include "serial_c_iface.h"
#endif

/**
@brief		A structure that translates a Fat32 file object to a C-compatible
                        struct.
*/
struct _SD_File {
    File32 f;   /**< The Arduino SD File object we to use. */
    int8_t eof; /**< A position telling us where the end of the
                                         file currently is. */
};

SdFat32 *sdcard;

void init_sdcard(void *sd) {
    sdcard = (SdFat32 *)sd;
}

int sd_fclose(SD_FILE *stream) {
    if (stream) {
        stream->f.close();
    }

    delete stream;
    return 0;
}

int sd_fflush(SD_FILE *stream) {
    stream->f.flush();
    return 0;
}

SD_FILE *sd_fopen(const char *filename, const char *mode) {
    File32 f;
    if (mode[0] == 'w') {
        if (mode[1] == '+') {
            f = sdcard->open(filename, O_RDWR | O_CREAT);
        } else {
            f = sdcard->open(filename, O_WRITE | O_CREAT);
        }
        if(f.isOpen() == false)
            return NULL;
        f.truncate(0);
    } else if (mode[0] == 'r') {
        if (mode[1] == '+') {
            f = sdcard->open(filename, O_RDWR);
        } else {
            f = sdcard->open(filename, O_READ);
        }
    }

    if (f.isOpen() == false)
        return NULL;

    _SD_File *file = new struct _SD_File();
    file->f = f;
    return file;
}

size_t sd_fread(void *ptr, size_t size, size_t nmemb, SD_FILE *stream) {
    /* read is the size of bytes * num of size-bytes */
    int16_t num_bytes = stream->f.read((char *)ptr, size * nmemb);

    if (num_bytes < 0)
        return 0;
    return num_bytes / size;
}

int sd_fseek(SD_FILE *stream, unsigned long int offset, int whence) {
    if (NULL == stream)
        return -1;

    bool result = stream->f.seek(offset);
    if (!result)
        return -1;
    return 0;
}

size_t sd_fwrite(void *ptr, size_t size, size_t nmemb, SD_FILE *stream) {
    size_t total_count = size * nmemb;
    size_t bytes_written = stream->f.write(ptr, total_count);

    if (total_count != bytes_written)
        return 0;
    return total_count;
}
