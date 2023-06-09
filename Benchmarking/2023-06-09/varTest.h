#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sbits.h"

#define NUM_STEPS 10
#define NUM_RUNS 3
#define VALIDATE_VAR_DATA 0
/**
 * 0 = Random data
 * 1 = Image data
 * 2 = Set length string
 */
#define TEST_TYPE 0

// Cursed linkedList for tracking data
typedef struct Node {
    int32_t key;
    void *data;
    uint32_t length;
    struct Node *next;
} Node;

void updateBitmapInt8Bucket(void *data, void *bm);
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm);
int8_t inBitmapInt8Bucket(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void updateBitmapInt64(void *data, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
int8_t int32Comparator(void *a, void *b);
uint32_t keyModifier(uint32_t inputKey);
uint32_t readImageFromFile(void **data, char *filename);
void writeDataToFile(void *data, char *filename, uint32_t length);
void imageVarData(float chance, char *filename, uint8_t *usingVarData, uint32_t *length, void **varData);
void retrieveImageData(void **varData, uint32_t length, int32_t key, char *filename, char *filetype);
uint8_t dataEquals(void *varData, uint32_t length, Node *node);
void randomVarData(uint32_t chance, uint32_t sizeLowerBound, uint32_t sizeUpperBound, uint8_t *usingVarData, uint32_t *length, void **varData);

void test_vardata(void *storage) {
    for (int KEY_SIZE = 4; KEY_SIZE <= 8; KEY_SIZE += 2) {
        int varDataSizes[] = {0, 50, 100, 500, 1000};
        for (int vardataSizeIndex = 0; vardataSizeIndex < 5; vardataSizeIndex++) {
            int VAR_DATA_SIZE = varDataSizes[vardataSizeIndex];
            for (int DATASET = 0; DATASET < 3; DATASET++) {
                for (int STORAGE_TYPE = 0; STORAGE_TYPE < 2; STORAGE_TYPE++) {

                    printf("\nSTARTING SBITS VARIABLE DATA TESTS.\n");
                    printf("KEY_SIZE: %d\n", KEY_SIZE);
                    printf("VAR_DATA_SIZE: %d\n", VAR_DATA_SIZE);
                    printf("STORAGE_TYPE: %s\n", STORAGE_TYPE == 0 ? "Dataflash" : "New 32GB SD Card");

                    // Two extra bufferes required for variable data
                    int8_t M = 6;

                    // Initialize to default values
                    int32_t numRecords = 600;  // default values
                    int32_t testRecords = 600; // default values
                    uint8_t useRandom = 0;     // default values
                    size_t splineMaxError = 0; // default values
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

                    /* Determines if generated, sequential data is used, or data from an input file*/
                    int8_t seqdata = 0;

                    // Files for non-sequentioal data
                    SD_FILE *infile = NULL, *infileRandom = NULL;
                    uint32_t minRange, maxRange;

                    if (seqdata != 1) {
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
                        if (DATASET == 0) {
                            infile = fopen("data/ethylene_CO.bin", "r+b");
                            // infileRandom = fopen("data/ethylene_CO_randomized.bin", "r+b");
                            minRange = 0;
                            maxRange = 4208755;
                            numRecords = 100000; // 4085589;
                            testRecords = 4085589;
                            printf("DATASET: ethylene_CO.bin\n");
                        }

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

                        if (DATASET == 1) {
                            infile = fopen("data/sea100K.bin", "r+b");
                            minRange = 1314604380;
                            maxRange = 1609487580;
                            numRecords = 100001;
                            testRecords = 100001;
                            printf("DATASET: sea100K.bin\n");
                        }

                        if (DATASET == 2) {
                            infile = fopen("data/uwa500K.bin", "r+b");
                            // infileRandom =
                            // fopen("data/uwa_data_only_2000_500KSorted_randomized.bin", "r+b");
                            minRange = 946713600;
                            maxRange = 977144040;
                            numRecords = 100000; // 500000;
                            testRecords = 500000;
                            printf("DATASET: uwa500K.bin\n");
                        }

                        splineMaxError = 1;
                        useRandom = 0;

                        stepSize = numRecords / NUM_STEPS;
                    }

                    for (r = 0; r < NUM_RUNS; r++) {
                        sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));

                        state->keySize = KEY_SIZE;
                        state->dataSize = 12;
                        state->pageSize = 512;
                        state->bitmapSize = 0;
                        state->bufferSizeInBlocks = M;
                        state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

                        /* Address level parameters */
                        state->storageType = STORAGE_TYPE == 0 ? DATAFLASH_STORAGE : FILE_STORAGE;
                        state->storage = storage;
                        state->startAddress = 0;
                        state->endAddress = 5500 * state->pageSize; // state->pageSize * numRecords / 10; /* Modify this value lower to test wrap around */
                        state->varAddressStart = 6000 * state->pageSize;
                        state->varAddressEnd = state->varAddressStart + state->pageSize * 4000;
                        state->eraseSizeInPages = 4;

                        state->parameters = SBITS_USE_INDEX | SBITS_USE_VDATA | SBITS_USE_BMAP;

                        if (SBITS_USING_INDEX(state->parameters) == 1)
                            state->endAddress += state->pageSize * (state->eraseSizeInPages * 2);
                        if (SBITS_USING_BMAP(state->parameters))
                            state->bitmapSize = 8;

                        /* Setup for data and bitmap comparison functions */
                        // state->inBitmap = inBitmapInt16;
                        // state->updateBitmap = updateBitmapInt16;
                        state->inBitmap = inBitmapInt64;
                        state->updateBitmap = updateBitmapInt64;
                        state->compareKey = int32Comparator;
                        state->compareData = int32Comparator;

                        if (sbitsInit(state, splineMaxError) != 0) {
                            printf("Initialization error.\n");
                            return;
                        } else {
                            printf("Initialization success.\n");
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
                        uint32_t start = millis();

                        int32_t i;
                        char vardata[15] = "Testing 000...";
                        uint32_t numVarData = 0;
                        if (seqdata == 1) {
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
                                sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + state->keySize), hasVarData ? variableData : NULL, length);

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
                                        times[l][r] = millis() - start;
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
                                    void *buf = (infileBuffer + headerSize + j * (4 + state->dataSize));
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
                                        if (VAR_DATA_SIZE > 0) {
                                            randomVarData(VAR_DATA_SIZE / 10, VAR_DATA_SIZE, VAR_DATA_SIZE, &hasVarData, &length, &variableData);
                                        }
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
                                    if (0 != sbitsPutVar(state, &keyBuf, (void *)((int8_t *)buf + 4), hasVarData ? variableData : NULL, length)) {
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
                                            times[l][r] = millis() - start;
                                            ;
                                            reads[l][r] = state->numReads;
                                            writes[l][r] = state->numWrites;
                                            overwrites[l][r] = 0;
                                            hits[l][r] = state->bufferHits;
                                        }
                                    }
                                    i++;
                                    /* Allows stopping at set number of records instead of
                                     * reading entire file */
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
                        sbitsFlush(state);
                        fflush(state->file);
                        fflush(state->varFile);
                        uint32_t end = millis();

                        l = NUM_STEPS - 1;
                        times[l][r] = end - start;
                        reads[l][r] = state->numReads;
                        writes[l][r] = state->numWrites;
                        overwrites[l][r] = 0;
                        hits[l][r] = state->bufferHits;

                        printf("Elapsed Time: %lu ms\n", times[l][r]);
                        printf("Records inserted: %lu\n", numRecords);
                        printf("Records with variable data: %lu\n", numVarData);

                        printStats(state);
                        resetStats(state);

                        printf("\n\nQUERY TEST:\n");
                        /* Verify that all values can be found and test query performance */

                        start = millis();

                        uint32_t varDataFound = 0, fixedFound = 0, deleted = 0, notFound = 0;

                        if (seqdata == 1) {
                            void *keyBuf = calloc(1, state->keySize);
                            for (i = 0; i < numRecords; i++) {
                                memcpy(keyBuf, &i, sizeof(int32_t));
                                void *varData = NULL;
                                uint32_t length = 0;
                                int8_t result = sbitsGetVar(state, keyBuf, recordBuffer, &varData, &length);
                                int32_t retrievedData;
                                memcpy(&retrievedData, recordBuffer, min(state->dataSize, (int8_t)sizeof(int32_t)));
                                if (result == -1) {
                                    printf("ERROR: Failed to find: %lu\n", i);
                                } else if (result == 1) {
                                    printf("WARN: Variable data associated with key %lu was deleted\n", i);
                                } else if (retrievedData != i % 100) {
                                    printf("ERROR: Wrong data for: %lu: %lu\n", i, retrievedData);
                                } else if (VALIDATE_VAR_DATA && varData != NULL) {
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
                                    if (!dataEquals(varData, length, validationHead)) {
                                        printf("ERROR: Wrong var data for: %lu\n", i);
                                    }
                                }

                                // Retrieve image if using image test
                                if (varData != NULL) {
                                    if (TEST_TYPE == 1) {
                                        char filename[] = "test";
                                        char extension[] = ".png";
                                        retrieveImageData(&varData, length, i, filename, extension);
                                    }
                                    free(varData);
                                    varData = NULL;
                                }

                                if (i % stepSize == 0) {
                                    l = i / stepSize - 1;
                                    if (l < NUM_STEPS && l >= 0) {
                                        rtimes[l][r] = millis() - start;
                                        rreads[l][r] = state->numReads;
                                        rhits[l][r] = state->bufferHits;
                                    }
                                }
                            }
                        } else {
                            /* Data from file */

                            char infileBuffer[512];
                            int8_t headerSize = 16;
                            i = 0;
                            int8_t queryType = 2;

                            if (queryType == 1) {
                                /* Query each record from original data set. */
                                if (useRandom) {
                                    fseek(infileRandom, 0, SEEK_SET);
                                } else {
                                    fseek(infile, 0, SEEK_SET);
                                }
                                int32_t readCounter = 0;
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

                                        void *varData = NULL;
                                        uint32_t length = 0;

                                        int8_t result = sbitsGetVar(state, key, recordBuffer, &varData, &length);

                                        if (result == -1) {
                                            printf("ERROR: Failed to find: %lu\n", *key);
                                        } else if (result == 1) {
                                            printf("WARN: Variable data associated with key %lu was deleted\n", *key);
                                        } else if (*((int32_t *)recordBuffer) != *((int32_t *)((int8_t *)buf + 4))) {
                                            printf("ERROR: Wrong data for: %lu\n", *key);
                                        } else if (VALIDATE_VAR_DATA && length != 0) {
                                            while (validationHead->key != *key) {
                                                Node *tmp = validationHead;
                                                validationHead = validationHead->next;
                                                free(tmp->data);
                                                free(tmp);
                                            }
                                            if (validationHead == NULL) {
                                                printf("ERROR: No validation data for: %lu\n", *key);
                                                return;
                                            }
                                            // Check that the var data is correct
                                            if (!dataEquals(varData, length, validationHead)) {
                                                printf("ERROR: Wrong var data for: %lu\n", *key);
                                            }
                                            Node *tmp = validationHead;
                                            validationHead = validationHead->next;
                                            free(tmp->data);
                                            free(tmp);
                                        }

                                        // Retrieve image
                                        if (varData != NULL) {
                                            if (TEST_TYPE == 1) {
                                                char filename[] = "test";
                                                char extension[] = ".png";
                                                retrieveImageData(&varData, length, *key, filename, extension);
                                            }
                                            free(varData);
                                            varData = NULL;
                                        }

                                        if (i % stepSize == 0) {
                                            l = i / stepSize - 1;
                                            printf("Num: %lu KEY: %lu\n", i, *key);
                                            if (l < NUM_STEPS && l >= 0) {
                                                rtimes[l][r] = millis() - start;
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
                            } else if (queryType == 2) {
                                /* Query random values in range. May not exist in data set. */

                                // Only query 10000 records
                                int32_t numToQuery = 10000;
                                int32_t queryStepSize = numToQuery / NUM_STEPS;
                                i = 0;
                                int32_t num = maxRange - minRange;
                                printf("Rge: %d Rand max: %d\n", num, RAND_MAX);
                                while (i < numToQuery) {
                                    // Generate number between minRange and maxRange
                                    uint32_t key = (uint32_t)((rand() % num) + minRange);
                                    uint64_t sizedKey = 0;
                                    memcpy(&sizedKey, &key, sizeof(uint32_t));

                                    void *varData = NULL;
                                    uint32_t length = 0;
                                    int8_t result = sbitsGetVar(state, &sizedKey, recordBuffer, &varData, &length);

                                    if (result == -1) {
                                        // printf("ERROR: Failed to find: %lu\n", key);
                                        notFound++;
                                    } else if (result == 1) {
                                        printf("WARN: Variable data associated with key %lu was deleted\n", key);
                                        deleted++;
                                    } else {
                                        fixedFound++;
                                    }
                                    // else if (*((int32_t *)recordBuffer) != key % 100) {
                                    //     printf("ERROR: Wrong data for: %lu\n", key);
                                    //     // printf("Key: %lu Data: %lu Var length: %d\n", key, *((int32_t*)recordBuffer), length);
                                    // }

                                    // Retrieve image
                                    if (length != 0 && TEST_TYPE == 1) {
                                        char filename[5] = "test";
                                        char extension[5] = ".png";
                                        retrieveImageData(&varData, length, key, filename, extension);
                                    }

                                    // printf("Key: %lu Data: %lu Var: %s\n", key, *((int32_t *)recordBuffer), varData);
                                    if (varData != NULL) {
                                        free(varData);
                                        varDataFound++;
                                    }

                                    if (i % queryStepSize == 0) {
                                        l = i / queryStepSize - 1;
                                        printf("Num: %lu KEY: %lu\n", i, key);
                                        if (l < NUM_STEPS && l >= 0) {
                                            rtimes[l][r] = millis() - start;
                                            rreads[l][r] = state->numReads;
                                            rhits[l][r] = state->bufferHits;
                                        }
                                    }
                                    i++;
                                }
                            } else {
                                /* Data value query for given value range */
                                int32_t *itKey, *itData;
                                sbitsIterator it;
                                it.minKey = NULL;
                                it.maxKey = NULL;
                                int32_t mv = 800;
                                int32_t v = 1000;
                                it.minData = &mv;
                                it.maxData = &v;
                                int32_t rec, reads;

                                start = millis();
                                mv = 280;
                                // for (int i = 0; i < 1000; i++)
                                // for (int i = 0; i < 16; i++)
                                for (int i = 0; i < 65; i++) {
                                    // mv = (rand() % 60 + 30) * 10;
                                    // mv += 30;
                                    mv += 10;
                                    v = mv;

                                    resetStats(state);
                                    sbitsInitIterator(state, &it);
                                    rec = 0;
                                    reads = state->numReads;
                                    // printf("Min: %d Max: %d\n", mv, v);
                                    while (sbitsNext(state, &it, (void **)&itKey, (void **)&itData)) {
                                        // printf("Key: %d  Data: %d\n", *itKey, *itData);
                                        if (*((int32_t *)itData) < *((int32_t *)it.minData) ||
                                            *((int32_t *)itData) > *((int32_t *)it.maxData)) {
                                            printf("Key: %d Data: %d Error\n", *itKey, *itData);
                                        }
                                        rec++;
                                    }
                                    // printf("Read records: %d\n", rec);
                                    // printStats(state);
                                    printf("Num: %lu KEY: %lu Perc: %d Records: %d Reads: %d \n", i, mv, ((state->numReads - reads) * 1000 / (state->nextPageWriteId - 1)), rec, (state->numReads - reads));

                                    if (i % 100 == 0) {
                                        l = i / 100 - 1;
                                        printf("Num: %lu KEY: %lu Records: %d Reads: %d\n", i, mv, rec, (state->numReads - reads));
                                        if (l < NUM_STEPS && l >= 0) {
                                            rtimes[l][r] = millis() - start;
                                            rreads[l][r] = state->numReads;
                                            rhits[l][r] = state->bufferHits;
                                        }
                                    }
                                }
                            }
                        }

                        end = millis();
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

                        printStats(state);

                        printf("Done\n");

                        // Optional: Test iterator
                        // testIterator(state);
                        // printStats(state);

                        // Free memory
                        sbitsClose(state);
                        free(recordBuffer);
                        free(state->buffer);
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
                }
            }
        }
    }
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8Bucket(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and
 * max value.
 */
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm) {
    /* Note: Assuming int key is right at the start of the data record */
    uint8_t *bmval = (uint8_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = 255; /* Everything */
    } else {
        int8_t i = 0;
        uint8_t val = 128;
        if (min != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(min, bm);

            /* Assume here that bits are set in increasing order based on
             * smallest value */
            /* Find first set bit */
            while ((val & *bmval) == 0 && i < 8) {
                i++;
                val = val / 2;
            }
            val = val / 2;
            i++;
        }
        if (max != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(max, bm);

            while ((val & *bmval) == 0 && i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        } else {
            while (i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        }
    }
}

int8_t inBitmapInt8Bucket(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8Bucket(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */

    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15; // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }

    /* Always set last bit if value bigger than largest cutoff */
    if (num == 0)
        num = 1;
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    // Temperature data in F. Scaled by 10. */
    int16_t stepSize = 10;

    int32_t current = 320;
    int8_t count = 0;

    while (val > current && count < 63) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

int8_t int32Comparator(void *a, void *b) {
    uint32_t a1, a2;
    memcpy(&a1, a, sizeof(uint32_t));
    memcpy(&a2, b, sizeof(uint32_t));
    if (a1 < a2)
        return -1;
    if (a1 > a2)
        return 1;
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

void writeDataToFile(void *data, char *filename, uint32_t length) {
    SD_FILE *file = fopen(filename, "w+b");
    if (file == NULL) {
        printf("Failed to open the file\n");
        return;
    }

    // Write the data to the file
    size_t bytes_written = fwrite(data, 1, length, file);
    if (bytes_written != length) {
        printf("Failed to write to the file\n");
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

void retrieveImageData(void **varData, uint32_t length, int32_t key, char *filename, char *filetype) {
    int numDigits = log10(key) + 1;
    char *keyAsString = (char *)calloc(numDigits, sizeof(char));
    itoa(key, keyAsString, 10);
    uint32_t filenameLength = strlen(filename);
    uint32_t filetypeLength = strlen(filetype);
    uint32_t totalLength = filenameLength + numDigits + filetypeLength;
    char *file = (char *)calloc(totalLength, sizeof(char));
    strncpy(file, filename, filenameLength);
    strncpy(file + filenameLength, keyAsString, numDigits);
    strncpy(file + filenameLength + numDigits, filetype, filetypeLength);
    strncpy(file + totalLength, "\0", 1);
    writeDataToFile(*varData, file, length);
}

uint8_t dataEquals(void *varData, uint32_t length, Node *node) {
    return length == node->length && memcmp(varData, node->data, length) == 0;
}
