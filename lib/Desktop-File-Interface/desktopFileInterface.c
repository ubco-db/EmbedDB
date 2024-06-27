#include "desktopFileInterface.h"

typedef struct {
    char *filename;
    FILE *file;
} FILE_INFO;

void *setupFile(char *filename) {
    FILE_INFO *fileInfo = malloc(sizeof(FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->file = NULL;
    return fileInfo;
}

void tearDownFile(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo->file != NULL)
        fclose(fileInfo->file);
    free(file);
}

int8_t FILE_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageSize * pageNum, SEEK_SET);
    return fread(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fseek(fileInfo->file, pageNum * pageSize, SEEK_SET);
    return fwrite(buffer, pageSize, 1, fileInfo->file);
}

int8_t FILE_ERASE(uint32_t startPage, uint32_t endPage, void *file) {
    return 1;
}

int8_t FILE_CLOSE(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    fclose(fileInfo->file);
    fileInfo->file = NULL;
    return 1;
}

int8_t FILE_FLUSH(void *file) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;
    return fflush(fileInfo->file) == 0;
}

int8_t FILE_OPEN(void *file, uint8_t mode) {
    FILE_INFO *fileInfo = (FILE_INFO *)file;

    if (mode == EMBEDDB_FILE_MODE_W_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "w+b");
    } else if (mode == EMBEDDB_FILE_MODE_R_PLUS_B) {
        fileInfo->file = fopen(fileInfo->filename, "r+b");
    } else {
        return 0;
    }

    if (fileInfo->file == NULL) {
        return 0;
    } else {
        return 1;
    }
}

embedDBFileInterface *getFileInterface() {
    embedDBFileInterface *fileInterface = malloc(sizeof(embedDBFileInterface));
    fileInterface->close = FILE_CLOSE;
    fileInterface->read = FILE_READ;
    fileInterface->write = FILE_WRITE;
    fileInterface->erase = FILE_ERASE;
    fileInterface->open = FILE_OPEN;
    fileInterface->flush = FILE_FLUSH;
    return fileInterface;
}
