/******************************************************************************/
/**
 * @file        dataflashFileInterface.c
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code file for the dataflash file interface. Dataflash
 *              is a custom type of flash memory.
 * @copyright   Copyright 2024
 *              EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *  may be used to endorse or promote products derived from this software without
 *  specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include "dataflashFileInterface.h"

#include "dataflash_c_iface.h"

typedef struct {
    uint32_t startPage;
    uint32_t numPages;
} DF_FILE_INFO;

void *setupDataflashFile(uint32_t startPage, uint32_t numPages) {
    DF_FILE_INFO *fileInfo = malloc(sizeof(DF_FILE_INFO));
    fileInfo->startPage = startPage;
    fileInfo->numPages = numPages;
    return fileInfo;
}

void tearDownDataflashFile(void *file) {
    free(file);
}

int8_t DF_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    DF_FILE_INFO *fileInfo = (DF_FILE_INFO *)file;
    if (pageNum >= fileInfo->numPages) {
        return 0;
    } else {
        uint32_t physicalPage = fileInfo->startPage + pageNum;
        uint32_t val = dfread(physicalPage, buffer, pageSize);
        if (val == pageSize) {
            return 1;
        } else {
            return 0;
        }
    }
}

int8_t DF_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    DF_FILE_INFO *fileInfo = (DF_FILE_INFO *)file;
    if (pageNum >= fileInfo->numPages) {
        return 0;
    } else {
        uint32_t physicalPage = fileInfo->startPage + pageNum;
        uint32_t val = dfwrite(physicalPage, buffer, pageSize);
        if (val == pageSize) {
            return 1;
        } else {
            return 0;
        }
    }
}

int8_t DF_CLOSE(void *file) {
    return 1;
}

int8_t DF_OPEN(void *file, uint8_t mode) {
    return 1;
}

int8_t DF_FLUSH(void *file) {
    return 1;
}

embedDBFileInterface *getDataflashInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = DF_CLOSE;
    fileInterface->read = DF_READ;
    fileInterface->write = DF_WRITE;
    fileInterface->open = DF_OPEN;
    fileInterface->flush = DF_FLUSH;
    return fileInterface;
}
