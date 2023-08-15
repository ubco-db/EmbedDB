/******************************************************************************/
/**
@file		dataflash_c_iface.h
@author		Ramon Lawrence
@brief		This code contains C wrapper for data flash library.
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

#if !defined(DATAFLASH_C_IFACE_H_)
#define DATAFLASH_C_IFACE_H_

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/**
@brief 		Initializes local copy of data flash memory structure.
@param		df
                                data flash memory object
*/
void init_df(void *df);

/**
@brief 		Erases entire data flash chip.
*/
void dferase_chip();

/**
@brief		Read data from memory at given address.
@param		pagenum
                                Page number
@param		ptr
                                A pointer to the memory to be read into.
@param		size
                                The number of bytes to be read
@returns	The number of bytes that have been read.
*/
int32_t dfread(int32_t pagenum, void *ptr, int32_t size);

/**
@brief		Write data page to data flash.
@param		pagenum
                                Page number
@param		ptr
                                A pointer to the memory containing data to be written
@param		size
                                The number of bytes to be write
@returns	The number of bytes written
*/
int32_t dfwrite(int32_t pagenum, void *ptr, int32_t size);

#if defined(__cplusplus)
}
#endif

#endif
