/******************************************************************************/
/**
 * @file        variableDataBenchmark.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       This file includes an example of inserting and querying EmbedDB
 *              with variable length data and tests the inserted data for correctness.
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

#ifndef PIO_UNIT_TESTING

#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "embedDB/embedDB.h"
#include "embedDBUtility.h"

#define NUM_STEPS 10
#define NUM_RUNS 1
#define VALIDATE_VAR_DATA 0

/**
 * 0 = SD Card
 * 1 = Dataflash
 */
#define STORAGE_TYPE 0

/**
 * 0 = Random data
 * 1 = Image data
 * 2 = Set length string
 */
#define TEST_TYPE 2

/*
 * 1: Query each record from original data set.
 * 2: Query random records in the range of original data set.
 * 3: Query range of records using an iterator.
 */
#define QUERY_TYPE 3

/*
 * 0: Use data from one of the data sets
 * 1: Use sequentially generated data
 */
#define SEQUENTIAL_DATA 1

#ifdef ARDUINO

#if defined(MEMBOARD) && STORAGE_TYPE == 1
#include "dataflashFileInterface.h"
#endif

#include "SDFileInterface.h"
#define FILE_TYPE SD_FILE
#define fopen sd_fopen
#define fread sd_fread
#define fclose sd_fclose
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile

#define clock millis
#define DATA_FILE_PATH "dataFile.bin"
#define INDEX_FILE_PATH "indexFile.bin"
#define VAR_DATA_FILE_PATH "varFile.bin"

#else

#include "desktopFileInterface.h"
#define FILE_TYPE FILE
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"
#define VAR_DATA_FILE_PATH "build/artifacts/varFile.bin"

#endif

/* LinkedList for tracking data */
typedef struct Node {
    int32_t key;
    void *data;
    uint32_t length;
    struct Node *next;
} Node;

uint32_t readImageFromFile(void **data, char *filename);
void writeDataToFile(embedDBState *state, embedDBVarDataStream *data, char *filename);
void imageVarData(float chance, char *filename, uint8_t *usingVarData, uint32_t *length, void **varData);
void retrieveImageData(embedDBState *state, embedDBVarDataStream *varStream, int32_t key, char *filename, char *filetype);
uint8_t dataEquals(embedDBState *state, embedDBVarDataStream *varStream, Node *node);
void randomVarData(uint32_t chance, uint32_t sizeLowerBound, uint32_t sizeUpperBound, uint8_t *usingVarData, uint32_t *length, void **varData);

int test_vardata() {
    printf("\nSTARTING EmbedDB VARIABLE DATA TESTS.\n");

    // Two extra bufferes required for variable data
    int8_t M = 6;

    // Initialize to default values
    int32_t numRecords = 600;   // default values
    int32_t testRecords = 600;  // default values
    uint8_t useRandom = 0;      // default values
    size_t splineMaxError = 0;  // default values
    uint32_t stepSize = numRecords / NUM_STEPS;
    count_t r, l;
    uint32_t times[NUM_STEPS][NUM_RUNS];
    uint32_t reads[NUM_STEPS][NUM_RUNS];
    uint32_t writes[NUM_STEPS][NUM_RUNS];
    uint32_t overwrites[NUM_STEPS][NUM_RUNS];
    uint32_t hits[NUM_STEPS][NUM_RUNS];
    uint32_t rtimes[NUM_STEPS][NUM_RUNS];
    uint32_t rreads[NUM_STEPS][NUM_RUNS];
    uint32_t rhits[NUM_STEPS][NUM_RUNS];

    // Files for non-sequentioal data
    FILE_TYPE *infile = NULL, *infileRandom = NULL;
    uint32_t minRange, maxRange;

    if (!SEQUENTIAL_DATA) {
        /* Open file to read input records */

        // measure1_smartphone_sens.bin
        // infile = fopen("data/measure1_smartphone_sens.bin", "r+b");
        // infileRandom = fopen("data/measure1_smartphone_sens_randomized.bin",
        // "r+b"); minRange = 0; maxRange = INT32_MAX; numRecords = 18354;
        // testRecords = 18354;

        // position.bin
        // infile = fopen("data/position.bin", "r+b");
        // infileRandom = fopen("data/position_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 1518;
        // testRecords = 1518;

        // ethylene_CO.bin
        // infile = fopen("data/ethylene_CO.bin", "r+b");
        // infileRandom = fopen("data/ethylene_CO_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 4085589;
        // testRecords = 4085589;

        // Watch_gyroscope.bin
        // infile = fopen("data/Watch_gyroscope.bin", "r+b");
        // infileRandom = fopen("data/Watch_gyroscope_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 2865713;
        // testRecords = 2865713;

        // PRSA_Data_Hongxin.bin
        // infile = fopen("data/PRSA_Data_Hongxin.bin", "r+b");
        // infileRandom = fopen("data/PRSA_Data_Hongxin_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 35064;
        // testRecords = 35064;

        // S7hl500K.bin
        // infile = fopen("data/S7hl500K.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 500000;

        // infile = fopen("data/sea100K.bin", "r+b");
        // minRange = 1314604380;
        // maxRange = 1609487580;
        // numRecords = 100001;
        // testRecords = 100001;

        infile = fopen("data/uwa500K.bin", "r+b");
        // infileRandom =
        // fopen("data/uwa_data_only_2000_500KSorted_randomized.bin", "r+b");
        minRange = 946713600;
        maxRange = 977144040;
        numRecords = 500000;
        testRecords = 500000;

        splineMaxError = 1;
        useRandom = 0;

        stepSize = numRecords / NUM_STEPS;
    }

    for (r = 0; r < NUM_RUNS; r++) {
        embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));

        state->keySize = 4;
        state->dataSize = 12;
        state->pageSize = 512;
        state->bitmapSize = 0;
        state->bufferSizeInBlocks = M;
        state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

        /* Address level parameters */
        state->numDataPages = 1000;
        state->numIndexPages = 48;
        state->numVarPages = 1000;
        state->eraseSizeInPages = 4;
        state->numSplinePoints = 30;

        state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;

#if STORAGE_TYPE == 0
        char dataPath[] = DATA_FILE_PATH, indexPath[] = INDEX_FILE_PATH, varPath[] = VAR_DATA_FILE_PATH;
        state->fileInterface = getFileInterface();
        state->dataFile = setupFile(dataPath);
        state->indexFile = setupFile(indexPath);
        state->varFile = setupFile(varPath);
#elif defined(MEMBOARD) && STORAGE_TYPE == 1
        state->fileInterface = getDataflashInterface();
        state->dataFile = setupDataflashFile(0, state->numDataPages);
        state->indexFile = setupDataflashFile(state->numDataPages, state->numIndexPages);
        state->varFile = setupDataflashFile(state->numDataPages + state->numIndexPages, state->numVarPages);
#else
        printf("Invalid storage configuration. Program terminating.");
        exit(-1);
#endif

        if (EMBEDDB_USING_BMAP(state->parameters))
            state->bitmapSize = 1;

        /* Setup for data and bitmap comparison functions */
        state->inBitmap = inBitmapInt8;
        state->updateBitmap = updateBitmapInt8;
        state->buildBitmapFromRange = buildBitmapInt8FromRange;
        // state->inBitmap = inBitmapInt16;
        // state->updateBitmap = updateBitmapInt16;
        // state->buildBitmapFromRange = buildBitmapInt16FromRange;
        // state->inBitmap = inBitmapInt64;
        // state->updateBitmap = updateBitmapInt64;
        // state->buildBitmapFromRange = buildBitmapInt64FromRange;
        state->compareKey = int32Comparator;
        state->compareData = int32Comparator;

        if (embedDBInit(state, splineMaxError) != 0) {
            printf("Initialization error.\n");
            return -1;
        } else {
            printf("Initialization success.\n");
            embedDBPrintInit(state);
        }

        // Initialize Buffer
        int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);

        // Initialize the data validation struct
#if VALIDATE_VARDATA
        Node *validationHead = (Node *)malloc(sizeof(Node));
        Node *validationTail = validationHead;
        if (validationHead == NULL) {
            printf("Error allocating memory for validation linked list.\n");
            return;
        }
#else
        // Put this here to prevent compiler warning but not have to malloc if not using
        Node *validationHead = NULL, *validationTail = NULL;
#endif

        printf("\n\nINSERT TEST:\n");
        /* Insert records into structure */
        uint32_t start = clock();
        embedDBResetStats(state);

        int32_t i;
        char vardata[15] = "Testing 000...";
        uint32_t numVarData = 0;
        if (SEQUENTIAL_DATA) {
            for (i = 0; i < numRecords; i++) {
                // Key = i, fixed data = i % 100
                memcpy(recordBuffer, &i, sizeof(int32_t));
                int32_t data = i % 100;
                memcpy(recordBuffer + state->keySize, &data, sizeof(int32_t));

                // Generate variable-length data
                void *variableData = NULL;
                uint8_t hasVarData = 0;
                uint32_t length;
                if (TEST_TYPE == 0) {
                    randomVarData(10, 10, 100, &hasVarData, &length, &variableData);
                } else if (TEST_TYPE == 1) {
                    char filename[] = "test.png";
                    imageVarData(0.05, filename, &hasVarData, &length, &variableData);
                } else if (TEST_TYPE == 2) {
                    hasVarData = 1;
                    length = 15;
                    vardata[10] = (char)(i % 10) + '0';
                    vardata[9] = (char)((i / 10) % 10) + '0';
                    vardata[8] = (char)((i / 100) % 10) + '0';
                    variableData = malloc(length);
                    memcpy(variableData, vardata, length);
                }

                // Put variable length data
                embedDBPutVar(state, recordBuffer, (void *)(recordBuffer + state->keySize), hasVarData ? variableData : NULL, length);

                if (hasVarData) {
                    if (VALIDATE_VAR_DATA) {
                        validationTail->key = i;
                        validationTail->data = variableData;
                        validationTail->length = length;
                        validationTail->next = (Node *)malloc(sizeof(Node));
                        if (validationTail->next == NULL) {
                            printf("Error allocating memory for validation linked list.\n");
                            return;
                        }
                        validationTail = validationTail->next;
                        uint32_t z = 0;
                        memcpy(&validationTail->length, &z, sizeof(uint32_t));
                    } else {
                        free(variableData);
                        variableData = NULL;
                    }
                    // printf("Using var data: KEY: %d\n", i);
                }

                if (i % stepSize == 0) {
                    // printf("Num: %lu KEY: %lu\n", i, i);
                    l = i / stepSize - 1;
                    if (l < NUM_STEPS && l >= 0) {
                        times[l][r] = clock() - start;
                        ;
                        reads[l][r] = state->numReads;
                        writes[l][r] = state->numWrites;
                        overwrites[l][r] = 0;
                        hits[l][r] = state->bufferHits;
                    }
                }
            }
        } else {
            /* Read data from a file */

            minRange = UINT32_MAX;
            maxRange = 0;

            char infileBuffer[512];
            int8_t headerSize = 16;
            int32_t i = 0;
            fseek(infile, 0, SEEK_SET);
            // uint32_t readCounter = 0;
            while (1) {
                /* Read page */
                if (0 == fread(infileBuffer, state->pageSize, 1, infile)) {
                    break;
                }

                /* Process all records on page */
                int16_t count = *((int16_t *)(infileBuffer + 4));
                for (int j = 0; j < count; j++) {
                    // Key size is always 4 in the file, but we may want to increase its size by padding with 0s
                    void *buf = (infileBuffer + headerSize + j * (state->keySize + state->dataSize));
                    uint64_t keyBuf = 0;
                    memcpy(&keyBuf, buf, 4);
                    if ((uint32_t)keyBuf < minRange) {
                        minRange = keyBuf;
                    }
                    if ((uint32_t)keyBuf > maxRange) {
                        maxRange = keyBuf;
                    }

                    // Generate variable-length data
                    void *variableData = NULL;
                    uint8_t hasVarData = 0;
                    uint32_t length = 0;
                    if (TEST_TYPE == 0) {
                        randomVarData(10, 10, 100, &hasVarData, &length, &variableData);
                    } else if (TEST_TYPE == 1) {
                        char filename[] = "test.png";
                        imageVarData(0.05, filename, &hasVarData, &length, &variableData);
                    } else if (TEST_TYPE == 2) {
                        hasVarData = 1;
                        length = 15;
                        vardata[10] = (char)(i % 10) + '0';
                        vardata[9] = (char)((i / 10) % 10) + '0';
                        vardata[8] = (char)((i / 100) % 10) + '0';
                        variableData = malloc(length);
                        memcpy(variableData, vardata, length);
                    }

                    if (hasVarData) {
                        numVarData++;
                    }

                    // Put variable length data
                    if (0 != embedDBPutVar(state, buf, (void *)((int8_t *)buf + 4), hasVarData ? variableData : NULL, length)) {
                        printf("ERROR: Failed to insert record\n");
                    }

                    if (hasVarData) {
                        if (VALIDATE_VAR_DATA) {
                            validationTail->key = *((int32_t *)buf);
                            validationTail->data = variableData;
                            validationTail->length = length;
                            validationTail->next = (Node *)malloc(sizeof(Node));
                            if (validationTail->next == NULL) {
                                printf("Error allocating memory for validation linked list.\n");
                                return;
                            }
                            validationTail = validationTail->next;
                            validationTail->length = 0;
                        } else {
                            free(variableData);
                            variableData = NULL;
                        }
                        // printf("Using var data: KEY: %d\n", i);
                    }

                    if (i % stepSize == 0) {
                        printf("Num: %lu KEY: %lu\n", i, *((int32_t *)buf));
                        l = i / stepSize - 1;
                        if (l < NUM_STEPS && l >= 0) {
                            times[l][r] = clock() - start;
                            ;
                            reads[l][r] = state->numReads;
                            writes[l][r] = state->numWrites;
                            overwrites[l][r] = 0;
                            hits[l][r] = state->bufferHits;
                        }
                    }
                    i++;
                    /* Allows stopping at set number of records instead of reading entire file */
                    if (i == numRecords) {
                        maxRange = *((uint32_t *)buf);
                        printf("Num: %lu KEY: %lu\n", i, *((int32_t *)buf));

                        goto doneread;
                    }
                }
            }
            numRecords = i;
        }

    doneread:
        embedDBFlush(state);
        uint32_t end = clock();

        l = NUM_STEPS - 1;
        times[l][r] = end - start;
        reads[l][r] = state->numReads;
        writes[l][r] = state->numWrites;
        overwrites[l][r] = 0;
        hits[l][r] = state->bufferHits;

        printf("Elapsed Time: %lu ms\n", times[l][r]);
        printf("Records inserted: %lu\n", numRecords);
        printf("Records with variable data: %lu\n", numVarData);

        embedDBPrintStats(state);
        embedDBResetStats(state);

        printf("\n\nQUERY TEST:\n");
        /* Verify that all values can be found and test query performance */

        start = clock();

        uint32_t varDataFound = 0, fixedFound = 0, deleted = 0, notFound = 0;

        if (SEQUENTIAL_DATA) {
            if (QUERY_TYPE == 1) {
                void *keyBuf = calloc(1, state->keySize);
                uint32_t varBufSize = 6;
                void *varDataBuf = malloc(varBufSize);
                embedDBVarDataStream *varStream = NULL;
                for (i = 0; i < numRecords; i++) {
                    *((uint32_t *)keyBuf) = i;
                    int8_t result = embedDBGetVar(state, keyBuf, recordBuffer, &varStream);

                    if (result == 0) {
                        fixedFound++;
                    } else if (result == -1) {
                        printf("ERROR: Failed to find: %lu\n", i);
                        notFound++;
                    } else if (result == 1) {
                        printf("WARN: Variable data associated with key %lu was deleted\n", i);
                        deleted++;
                    } else if (*((int32_t *)recordBuffer) != i % 100) {
                        printf("ERROR: Wrong data for: %lu\n", i);
                    } else if (VALIDATE_VAR_DATA && varStream != NULL) {
                        while (validationHead->key != i) {
                            Node *tmp = validationHead;
                            validationHead = validationHead->next;
                            free(tmp->data);
                            free(tmp);
                        }
                        if (validationHead == NULL) {
                            printf("ERROR: No validation data for: %lu\n", i);
                            return;
                        }
                        // Check that the var data is correct
                        if (!dataEquals(state, varStream, validationHead)) {
                            printf("ERROR: Wrong var data for: %lu\n", i);
                        }
                    }

                    if (varStream != NULL) {
                        if (TEST_TYPE == 1) {
                            // Retrieve image if using image test
                            char filename[] = "test";
                            char extension[] = ".png";
                            retrieveImageData(state, varStream, i, filename, extension);
                        } else if (TEST_TYPE == 2) {
                            // Print string if using string test
                            char reconstructed[15];
                            uint32_t bytesRead, total = 0;
                            while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
                                memcpy(reconstructed + total, varDataBuf, bytesRead);
                                total += bytesRead;
                            }
                            // printf("Var data: %s\n", reconstructed);
                        }
                        free(varStream);
                        varStream = NULL;
                        varDataFound++;
                    }

                    if (i % stepSize == 0) {
                        l = i / stepSize - 1;
                        if (l < NUM_STEPS && l >= 0) {
                            rtimes[l][r] = clock() - start;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                }
            } else if (QUERY_TYPE == 3) {
                uint32_t itKey;
                void *itData = calloc(1, state->dataSize);
                embedDBIterator it;
                it.minKey = NULL;
                it.maxKey = NULL;
                int32_t mv = 26;
                int32_t v = 49;
                it.minData = &mv;
                it.maxData = &v;
                int32_t rec, reads;
                embedDBVarDataStream *varStream = NULL;
                uint32_t varBufSize = 8;
                void *varDataBuf = malloc(varBufSize);

                start = clock();
                embedDBInitIterator(state, &it);
                rec = 0;
                reads = state->numReads;
                while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
                    if (*((int32_t *)itData) < *((int32_t *)it.minData) ||
                        *((int32_t *)itData) > *((int32_t *)it.maxData)) {
                        printf("Key: %d Data: %d Error\n", itKey, *(uint32_t *)itData);
                    } else {
                        printf("Key: %d  Data: %d\n", itKey, *(uint32_t *)itData);
                        if (varStream != NULL) {
                            char reconstructed[15];
                            uint32_t bytesRead, total = 0;
                            while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
                                memcpy(reconstructed + total, varDataBuf, bytesRead);
                                total += bytesRead;
                            }
                            // printf("Var data: %s\n", reconstructed);
                            free(varStream);
                            varStream = NULL;
                        }
                    }
                    rec++;
                }
                printf("Read records: %d\n", rec);
                printf("Num: %lu KEY: %lu Perc: %d Records: %d Reads: %d \n", i, mv, ((state->numReads - reads) * 1000 / (state->nextDataPageId - state->minDataPageId + state->nextVarPageId)), rec, (state->numReads - reads));

                embedDBCloseIterator(&it);
                free(varDataBuf);
                free(itData);
            }
        } else {
            /* Data from file */

            char infileBuffer[512];
            int8_t headerSize = 16;
            i = 0;

            if (QUERY_TYPE == 1) {
                /* Query each record from original data set. */
                if (useRandom) {
                    fseek(infileRandom, 0, SEEK_SET);
                } else {
                    fseek(infile, 0, SEEK_SET);
                }
                int32_t readCounter = 0;

                uint32_t varBufSize = 6;
                void *varDataBuf = malloc(varBufSize);
                embedDBVarDataStream *varStream = NULL;

                while (1) {
                    /* Read page */
                    if (useRandom) {
                        if (0 == fread(infileBuffer, state->pageSize, 1, infileRandom))
                            break;
                    } else {
                        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
                            break;
                    }

                    readCounter++;

                    /* Process all records on page */
                    int16_t count = *((int16_t *)(infileBuffer + 4));
                    for (int j = 0; j < count; j++) {
                        void *buf = (infileBuffer + headerSize + j * (state->keySize + state->dataSize));
                        int32_t *key = (int32_t *)buf;

                        int8_t result = embedDBGetVar(state, key, recordBuffer, &varStream);

                        if (result == -1) {
                            printf("ERROR: Failed to find: %lu\n", *key);
                            notFound++;
                        } else if (result == 1) {
                            printf("WARN: Variable data associated with key %lu was deleted\n", *key);
                            deleted++;
                        } else if (*((int32_t *)recordBuffer) != *((int32_t *)((int8_t *)buf + 4))) {
                            printf("ERROR: Wrong data for: %lu\n", *key);
                        } else if (VALIDATE_VAR_DATA && varStream != NULL) {
                            while (validationHead->key != i) {
                                Node *tmp = validationHead;
                                validationHead = validationHead->next;
                                free(tmp->data);
                                free(tmp);
                            }
                            if (validationHead == NULL) {
                                printf("ERROR: No validation data for: %lu\n", i);
                                return;
                            }
                            // Check that the var data is correct
                            if (!dataEquals(state, varStream, validationHead)) {
                                printf("ERROR: Wrong var data for: %lu\n", i);
                            }
                        }

                        if (varStream != NULL) {
                            if (TEST_TYPE == 1) {
                                // Retrieve image if using image test
                                char filename[] = "test";
                                char extension[] = ".png";
                                retrieveImageData(state, varStream, i, filename, extension);
                            } else if (TEST_TYPE == 2) {
                                // Print string if using string test
                                char reconstructed[15];
                                uint32_t bytesRead, total = 0;
                                while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
                                    memcpy(reconstructed + total, varDataBuf, bytesRead);
                                    total += bytesRead;
                                }
                                // printf("Var data: %s\n", reconstructed);
                            }
                            free(varStream);
                            varStream = NULL;
                            varDataFound++;
                        }

                        if (i % stepSize == 0) {
                            l = i / stepSize - 1;
                            printf("Num: %lu KEY: %lu\n", i, *key);
                            if (l < NUM_STEPS && l >= 0) {
                                rtimes[l][r] = clock() - start;
                                rreads[l][r] = state->numReads;
                                rhits[l][r] = state->bufferHits;
                            }
                        }
                        i++;

                        /* Allows ending test after set number of records rather than processing entire file */
                        if (i == numRecords || i == testRecords)
                            goto donetest;
                    }
                }
            donetest:
                numRecords = i;
            } else if (QUERY_TYPE == 2) {
                /* Query random values in range. May not exist in data set. */

                // Only query 10000 records
                int32_t numToQuery = 10000;
                int32_t queryStepSize = numToQuery / NUM_STEPS;

                uint32_t varBufSize = 6;
                void *varDataBuf = malloc(varBufSize);
                embedDBVarDataStream *varStream = NULL;

                i = 0;
                int32_t num = maxRange - minRange;
                printf("Rge: %d Rand max: %d\n", num, RAND_MAX);
                while (i < numToQuery) {
                    // Generate number between minRange and maxRange
                    uint32_t key = (uint32_t)((rand() % num) + minRange);
                    uint64_t sizedKey = 0;
                    memcpy(&sizedKey, &key, sizeof(uint32_t));

                    int8_t result = embedDBGetVar(state, &sizedKey, recordBuffer, &varStream);

                    if (result == -1) {
                        // printf("ERROR: Failed to find: %lu\n", key);
                        notFound++;
                    } else if (result == 1) {
                        printf("WARN: Variable data associated with key %lu was deleted\n", key);
                        deleted++;
                    } else {
                        fixedFound++;
                    }

                    if (varStream != NULL) {
                        if (TEST_TYPE == 1) {
                            // Retrieve image if using image test
                            char filename[] = "test";
                            char extension[] = ".png";
                            retrieveImageData(state, varStream, i, filename, extension);
                        } else if (TEST_TYPE == 2) {
                            // Print string if using string test
                            char reconstructed[15];
                            uint32_t bytesRead, total = 0;
                            while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
                                memcpy(reconstructed + total, varDataBuf, bytesRead);
                                total += bytesRead;
                            }
                            // printf("Var data: %s\n", reconstructed);
                        }
                        free(varStream);
                        varStream = NULL;
                        varDataFound++;
                    }

                    if (i % queryStepSize == 0) {
                        l = i / queryStepSize - 1;
                        printf("Num: %lu KEY: %lu\n", i, key);
                        if (l < NUM_STEPS && l >= 0) {
                            rtimes[l][r] = clock() - start;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                    i++;
                }
            } else {
                uint32_t itKey;
                void *itData = calloc(1, state->dataSize);
                embedDBIterator it;
                it.minKey = NULL;
                it.maxKey = NULL;
                int32_t mv = 26;
                int32_t v = 49;
                it.minData = &mv;
                it.maxData = &v;
                int32_t rec, reads;
                embedDBVarDataStream *varStream = NULL;
                uint32_t varBufSize = 8;
                void *varDataBuf = malloc(varBufSize);

                start = clock();
                embedDBInitIterator(state, &it);
                rec = 0;
                reads = state->numReads;
                while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
                    if (*((int32_t *)itData) < *((int32_t *)it.minData) ||
                        *((int32_t *)itData) > *((int32_t *)it.maxData)) {
                        printf("Key: %d Data: %d Error\n", itKey, *(uint32_t *)itData);
                    } else {
                        printf("Key: %d  Data: %d\n", itKey, *(uint32_t *)itData);
                        if (varStream != NULL) {
                            char reconstructed[15];
                            uint32_t bytesRead, total = 0;
                            while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
                                memcpy(reconstructed + total, varDataBuf, bytesRead);
                                total += bytesRead;
                            }
                            // printf("Var data: %s\n", reconstructed);
                            free(varStream);
                            varStream = NULL;
                        }
                    }
                    rec++;
                }
                printf("Read records: %d\n", rec);
                // embedDBPrintStats(state);
                printf("Num: %lu KEY: %lu Perc: %.1f Records: %d Reads: %d \n", i, mv, ((state->numReads - reads) * 1000 / (state->nextDataPageId - state->minDataPageId + state->nextVarPageId - state->minVarRecordId)) / 10.0, rec, (state->numReads - reads));

                embedDBCloseIterator(&it);
                free(varDataBuf);
                free(itData);
            }
        }

        end = clock();
        l = NUM_STEPS - 1;
        rtimes[l][r] = end - start;
        rreads[l][r] = state->numReads;
        rhits[l][r] = state->bufferHits;
        printf("Elapsed Time: %lu ms\n", rtimes[l][r]);
        printf("Records queried: %lu\n", i);
        printf("Fixed records found: %lu\n", fixedFound);
        printf("Vardata found: %lu\n", varDataFound);
        printf("Vardata deleted: %lu\n", deleted);
        printf("Num records not found: %lu\n", notFound);

        embedDBPrintStats(state);

        printf("Done\n");

        // Optional: Test iterator
        // testIterator(state);
        // embedDBPrintStats(state);

        /* close embedDB */
        embedDBClose(state);

        /* tear down storage */
#if STORAGE_TYPE == 0
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        tearDownFile(state->varFile);
#elif defined(MEMBOARD) && STORAGE_TYPE == 1
        tearDownDataflashFile(state->dataFile);
        tearDownDataflashFile(state->indexFile);
        tearDownDataflashFile(state->varFile);
#endif

        /* free memory */
        free(recordBuffer);
        free(state->buffer);
        free(state->fileInterface);
        free(state);
    }

    // Close files
    if (infile != NULL) {
        fclose(infile);
    }
    if (infileRandom != NULL) {
        fclose(infileRandom);
    }

    // Prints results
    uint32_t sum;
    for (count_t i = 1; i <= NUM_STEPS; i++) {
        printf("Stats for %lu:\n", i * stepSize);

        printf("Reads:   ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += reads[i - 1][r];
            printf("\t%lu", reads[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Writes: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += writes[i - 1][r];
            printf("\t%lu", writes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Overwrites: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += overwrites[i - 1][r];
            printf("\t%lu", overwrites[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Totwrites: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += overwrites[i - 1][r] + writes[i - 1][r];
            printf("\t%lu", overwrites[i - 1][r] + writes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Buffer hits: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += hits[i - 1][r];
            printf("\t%lu", hits[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Write Time: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += times[i - 1][r];
            printf("\t%lu", times[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Time: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += rtimes[i - 1][r];
            printf("\t%lu", rtimes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Reads: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += rreads[i - 1][r];
            printf("\t%lu", rreads[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Buffer hits: ");
        sum = 0;
        for (r = 0; r < NUM_RUNS; r++) {
            sum += rhits[i - 1][r];
            printf("\t%lu", rhits[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);
    }

    return 0;
}

uint32_t randomData(void **data, uint32_t sizeLowerBound, uint32_t sizeUpperBound) {
    uint32_t size;
    if (sizeLowerBound == sizeUpperBound) {
        size = sizeLowerBound;
    } else {
        size = rand() % (sizeUpperBound - sizeLowerBound) + sizeLowerBound;
    }
    *data = malloc(size);
    if (*data == NULL) {
        printf("ERROR: Failed to allocate memory for random data\n");
        exit(-1);
    }
    for (uint32_t i = 0; i < size; i++) {
        *((uint8_t *)(*data) + i) = rand() % UINT8_MAX;
    }
    return size;
}

uint32_t readImageFromFile(void **data, char *filename) {
    printf("Reading image from file is not currently supported\n");
    exit(-1);
    // SD_FILE* file = fopen(filename, "rb");
    // if (file == NULL) {
    //     printf("Failed to open the file\n");
    //     return 0;
    // }

    // // Determine the file size
    // fseek(file, 0, SEEK_END);
    // long file_size = ftell(file);
    // rewind(file);

    // // Allocate memory to store the file data
    // *data = (char*)malloc(file_size);
    // if (*data == NULL) {
    //     printf("Failed to allocate memory\n");
    //     fclose(file);
    //     return 0;
    // }

    // // Read the file data into the buffer
    // size_t bytes_read = fread(*data, 1, file_size, file);
    // if (bytes_read != file_size) {
    //     printf("Failed to read the file\n");
    //     free(*data);
    //     fclose(file);
    //     return 0;
    // }

    // fclose(file);

    // return file_size;
}

void writeDataToFile(embedDBState *state, embedDBVarDataStream *data, char *filename) {
    if (data == NULL) {
        printf("There's no data here bud. Can't write image\n");
        return;
    }

    FILE_TYPE *file = fopen(filename, "w+b");
    if (file == NULL) {
        printf("Failed to open the file\n");
        return;
    }

    // Get data from iterator
    char buf[512];
    uint32_t numBytes;
    while ((numBytes = embedDBVarDataStreamRead(state, data, buf, 512)) > 0) {
        // Write the data to the file
        size_t bytes_written = fwrite(buf, 1, numBytes, file);
        if (bytes_written != numBytes) {
            printf("Failed to write to the file\n");
        }
    }

    fclose(file);
}

void imageVarData(float chance, char *filename, uint8_t *usingVarData, uint32_t *length, void **varData) {
    *usingVarData = (rand() % 100) / 100.0 < chance;
    if (usingVarData) {
        *length = readImageFromFile(varData, filename);
        if (*length == 0) {
            printf("ERROR: Failed to read image '%s'\n", filename);
            exit(-1);
        }
    } else {
        *length = 0;
        *varData = NULL;
    }
}

/**
 * @param chance 1 in `chance` chance of having variable data
 */
void randomVarData(uint32_t chance, uint32_t sizeLowerBound, uint32_t sizeUpperBound, uint8_t *usingVarData, uint32_t *length, void **varData) {
    *usingVarData = (rand() % chance) == 0;
    if (*usingVarData) {
        *length = randomData(varData, sizeLowerBound, sizeUpperBound);
    } else {
        *length = 0;
        *varData = NULL;
    }
}

void retrieveImageData(embedDBState *state, embedDBVarDataStream *varStream, int32_t key, char *filename, char *filetype) {
    int numDigits = log10(key) + 1;
    char *keyAsString = (char *)calloc(numDigits, sizeof(char));
    char destinationFolder[17] = "build/artifacts/";
    sprintf(keyAsString, "%i", key);
    uint32_t destinationFolderLength = strlen(destinationFolder);
    uint32_t filenameLength = strlen(filename);
    uint32_t filetypeLength = strlen(filetype);
    uint32_t totalLength = filenameLength + numDigits + filetypeLength + destinationFolderLength + 1;
    char *file = (char *)calloc(totalLength, sizeof(char));
    strncpy(file, destinationFolder, destinationFolderLength);
    strncpy(file + destinationFolderLength, filename, filenameLength);
    strncpy(file + filenameLength + destinationFolderLength, keyAsString, numDigits);
    strncpy(file + filenameLength + numDigits + destinationFolderLength, filetype, filetypeLength);
    strncpy(file + totalLength, "\0", 1);
    writeDataToFile(state, varStream, file);
}

uint8_t dataEquals(embedDBState *state, embedDBVarDataStream *varStream, Node *node) {
    if (varStream == NULL) {
        return 0;
    } else {
        void *data = malloc(node->length + 1);
        uint32_t length = embedDBVarDataStreamRead(state, varStream, data, node->length + 1);

        // Reset iterator
        varStream->bytesRead = 0;
        varStream->fileOffset = varStream->dataStart;  // Set flag that the next read is the first read

        return length == node->length && memcmp(data, node->data, length) == 0;
    }
}

#endif
