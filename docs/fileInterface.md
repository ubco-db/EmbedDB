# Setting up a Storage Interface

## What is it?

SBITS uses an interface with basic file system functions like open, close, read, write, and flush. Reading and writing is only done at exactly one page per function call to simplify the interface implementation. The implementation of these functions is up to the user due to the wide array of storage technologies that can be found on embedded systems. This allows SBITS to support any storage device.

## How to use it

The basic idea is to create a struct containing whatever file object you would normally use to interact with the file as well as any information necessary for opening the file. This struct is then given to SBITS. When SBITS needs to use the file it is able to make a call to open the file and read/write to it in the manner in which it requires.

## Examples

For a full code example see [sbits.h](../src/sbits/sbits.h) for the definition of `sbitsFileInterface` struct and [utilityFunctions.c](../src/sbits/utilityFunctions.c) for implementations of the interface.

Below is a step-by-step for two differnet storage devices.

### Micro SD card

This example is for using the sd card library used by this project.

The first reccommended step is to define a struct that will be given to SBITS. For an sd card, this is simple. Just the filename, which we will need to implementing the `open` function, and the actual `SD_FILE` object.

```c
typedef struct {
    char *filename;
    SD_FILE *sdFile;
} SD_FILE_INFO;
```

Next, we should create a function to quickly initialize a new file. The return of this function is to be put into SBITS with something like `state->dataFile = setupFile("filename.txt")`. This data is what will always provided to every function call in the `sbitsFileInterface` as `void *file`.

```c
void *setupSDFile(char *filename) {
    SD_FILE_INFO *fileInfo = malloc(sizeof(SD_FILE_INFO));
    int nameLen = strlen(filename);
    fileInfo->filename = calloc(1, nameLen + 1);
    memcpy(fileInfo->filename, filename, nameLen);
    fileInfo->sdFile = NULL;
    return fileInfo;
}
```

Along with that we need to provide a tearDown function since we calloc'd something

```c
 void tearDownSDFile(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    free(fileInfo->filename);
    if (fileInfo != NULL)
        sd_fclose(fileInfo->sdFile);
    free(file);
}
```

Now, let's implement the `open` function. The `mode` here can be any one of the `SBITS_FILE_MODE` defined macros in sbits.h. Be sure to consult the function docs in sbits.h for the return value.

```c
int8_t SD_OPEN(void *file, uint8_t mode) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;

    if (mode == SBITS_FILE_MODE_W_PLUS_B) {
        fileInfo->sdFile = sd_fopen(fileInfo->filename, "w+");
    } else if (mode == SBITS_FILE_MODE_R_PLUS_B) {
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
```

Closing is pretty simple. Note that we only want to close the file object, not destroy the whole file struct because SBITS may request to re-open the file again after calling `close`.

```c
int8_t SD_CLOSE(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fclose(fileInfo->sdFile);
    fileInfo->sdFile = NULL;
    return 1;
}
```

Both reading and writing will require seeking to the correct location before reading or writing the page. Again, be sure to return the correct return code.

```c
int8_t SD_READ(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageSize * pageNum, SEEK_SET);
    return sd_fread(buffer, pageSize, 1, fileInfo->sdFile);
}

int8_t SD_WRITE(void *buffer, uint32_t pageNum, uint32_t pageSize, void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    sd_fseek(fileInfo->sdFile, pageNum * pageSize, SEEK_SET);
    return sd_fwrite(buffer, pageSize, 1, fileInfo->sdFile) == pageSize;
}
```

And `flush`:

```c
int8_t SD_FLUSH(void *file) {
    SD_FILE_INFO *fileInfo = (SD_FILE_INFO *)file;
    return sd_fflush(fileInfo->sdFile) == 0;
}
```

Now that we've defined all required functions, we might want to create a function to assemble the `sbitsFileInterface` struct.

```c
sbitsFileInterface *getSDInterface() {
    sbitsFileInterface *fileInterface = malloc(sizeof(sbitsFileInterface));
    fileInterface->close = SD_CLOSE;
    fileInterface->read = SD_READ;
    fileInterface->write = SD_WRITE;
    fileInterface->open = SD_OPEN;
    fileInterface->flush = SD_FLUSH;
    return fileInterface;
}
```

### Raw Dataflash Memory

An example on raw memory with no file system.

The struct for this one does not have any kind of file object, only parameters that will help with reading/writing

```c
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
```

Since there is no file object to create when opening the file, the `open` and `close` functions will be doing nothing.

```c
int8_t DF_OPEN(void *file, uint8_t mode) {
    return 1;
}

int8_t DF_CLOSE(void *file) {
    return 1;
}
```

Reading and writing must calculate the physical address to access as well as determine if it is a legal request.

```c
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
```

Flushing is also not something that needs to be done on this storage device, so it will also just return 1

```c
int8_t DF_FLUSH(void *file) {
    return 1;
}
```

Again, we'll combine these functions into the interface for SBITS

```c
sbitsFileInterface *getDataflashInterface() {
    sbitsFileInterface *fileInterface = malloc(sizeof(sbitsFileInterface));
    fileInterface->close = DF_CLOSE;
    fileInterface->read = DF_READ;
    fileInterface->write = DF_WRITE;
    fileInterface->open = DF_OPEN;
    fileInterface->flush = DF_FLUSH;
    return fileInterface;
}
```
