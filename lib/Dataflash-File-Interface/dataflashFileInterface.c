#include "dataflashFileInterface.h"

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

sbitsFileInterface *getDataflashInterface() {
    sbitsFileInterface *fileInterface = malloc(sizeof(sbitsFileInterface));
    fileInterface->close = DF_CLOSE;
    fileInterface->read = DF_READ;
    fileInterface->write = DF_WRITE;
    fileInterface->open = DF_OPEN;
    fileInterface->flush = DF_FLUSH;
    return fileInterface;
}
