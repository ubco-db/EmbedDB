#include "SDFileInterface.h"

typedef struct {
    char *filename;
    SD_FILE *sdFile;
} SD_FILE_INFO;

void *setupSDFile(char *filename) {
    SD_FILE_INFO *fileInfo = malloc(sizeof(SD_FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->sdFile = NULL;
    return fileInfo;
}

void tearDownSDFile(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo != NULL)
        sd_fclose(fileInfo->sdFile);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageSize * pageNum, SEEK_SET);
    return sd_fread(buffer, pageSize, 1, fileInfo->sdFile);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageNum * pageSize, SEEK_SET);
    return sd_fwrite(buffer, pageSize, 1, fileInfo->sdFile) == pageSize;
}

int8_t FILE_CLOSE(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fclose(fileInfo->sdFile);
    fileInfo->sdFile = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    return sd_fflush(fileInfo->sdFile) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->sdFile = sd_fopen(fileInfo->filename, "w+");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->sdFile = sd_fopen(fileInfo->filename, "r+");
    } else {
        return 0;
    }

    if (fileInfo->sdFile == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getSDInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}
