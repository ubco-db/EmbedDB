/******************************************************************************/
/**
@file		sdcard_c_iface.h
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

#if !defined(SDCARD_C_IFACE_H_)
#define SDCARD_C_IFACE_H_

#if defined(ARDUINO)

#include <Arduino.h>

#if defined(__cplusplus)
extern "C" {
#endif

/* Redefine stdio.h functions for operation on Arduino SD card. */
#define fopen(x, y) sd_fopen(x, y)
#define fclose(x) sd_fclose(x)
#define fwrite(w, x, y, z) sd_fwrite(w, x, y, z)
#define fflush(x) sd_fflush(x)
#define fseek(x, y, z) sd_fseek(x, y, z)
#define fread(w, x, y, z) sd_fread(w, x, y, z)

/**
@brief		Wrapper around Arduino File type (a C++ object).
*/
typedef struct _SD_File SD_FILE;

/**
@brief		Wrapper around Arduino SD file close method.
@param		stream
                                A pointer to the C file struct type associated with an SD
                                file object.
@returns	@c 0, always.
*/
int sd_fclose(
    SD_FILE *stream);

/**
@brief		Flush the output buffer of a stream to the file.
@param		stream
                                A pointer to the C file struct type associated with an SD
                                file object representing the file to flush.
@returns	@c 0, always.
*/
int sd_fflush(
    SD_FILE *stream);

/**
@brief		Open a reference to an Arduino SD file given it's name.
@details	Wrapper around Arduino SD file open method. This function
                        will allocate memory, that must be freed by using @ref sd_fclose().
@param		filepath
                                String containing the path to file (basic filename).
@param		mode
                                Which mode to open the file under.
@returns	A pointer to a file struct representing a file for reading,
                        or @c NULL if an error occurred.
*/
SD_FILE *
sd_fopen(
    const char *filename,
    const char *mode);

/**
@brief		Read data from an Arduino SD file.
@details	A wrapper around Arduino SD file read method.

                        A total of @p size * @p nmemb bytes will be read into @p ptr
                        on a success.
@param		ptr
                                A pointer to the memory segment to be read into.
@param		size
                                The number of bytes to be read (per @p nmemb items).
@param		nmemb
                                The total number of items to read (each of size @p size).
@param		stream
                                A pointer to C file struct type associated with an SD
                                file object.
@returns	The number of items that have been read.
*/
size_t
sd_fread(
    void *ptr,
    size_t size,
    size_t nmemb,
    SD_FILE *stream);

/**
@brief		Wrapper around Arduino SD file read method.
@param		stream
                                A pointer to a C file struct type associated with an SD
                                file object.
@param		offset
                                The number of bytes to move from @p whence.
@param		whence
                                Where, in the file, to move from. Valid options are
                                        - SEEK_SET:	From the beginning of the file.
                                        - SEEK_CUR: From the current position in the file.
                                        - SEEK_END: From the end of the file, moving backwards.
@returns	@c 0 for success, a non-zero integer otherwise.
*/
int sd_fseek(
    SD_FILE *stream,
    unsigned long int offset,
    int whence);

/**
@brief		Write data to an Arduino SD file.
@details	Wrapper around Arduino SD file write method.
@param		ptr
                                A pointer to the data that is to be written.
@param		size
                                The number of bytes to be written (for each of the @p nmemb
                                items).
@param		nmemb
                                The number of items to be written (each @p size bytes long).
@param		stream
                                A pointer to a C file struct type associated with an SD
                                file object.
@returns	The number of items successfully written.
*/
size_t
sd_fwrite(
    void *ptr,
    size_t size,
    size_t nmemb,
    SD_FILE *stream);

void init_sdcard(void *sd);

#if defined(__cplusplus)
}
#endif

#endif /* Clause ARDUINO */

#endif
