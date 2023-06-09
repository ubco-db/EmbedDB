/******************************************************************************/
/**
 * @file		test_sbits.c
 * @author		Ramon Lawrence
 * @brief		This file does performance/correctness testing of sequential
 * bitmap indexing for time series (SBITS).
 * @copyright	Copyright 2021
 *                         The University of British Columbia,
 *             Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 *         modification, are permitted provided that the following conditions are
 * met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *         this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *         this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *         AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *         ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *         CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *         SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *         INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *         CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *         ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#ifndef PIO_UNIT_TESTING

#include <errno.h>
#include <string.h>
#include <time.h>

#include "sbits.h"
#include "utilityFunctions.h"

void testIterator(sbitsState *state) {
    /* Iterator with filter on keys */
    uint32_t i, start, end;
    sbitsIterator it;
    uint32_t mv = 1;
    it.minKey = &mv;
    uint32_t v = 1299;
    it.maxKey = NULL;
    uint32_t md = 90;
    it.minData = &md;
    it.maxData = NULL;

    resetStats(state);

    printf("\nInitializing iterator.\n");

    sbitsInitIterator(state, &it);
    i = 0;
    int8_t success = 1;
    int32_t *itKey, *itData;

    while (sbitsNext(state, &it, (void **)&itKey, (void **)&itData)) {
        // printf("Key: %d  Data: %d\n", *itKey, *itData);
        i++;
    }
    printf("Read records: %d\n", i);

    printStats(state);

    /* Iterator with filter on data */
    it.minKey = NULL;
    it.maxKey = NULL;
    mv = 90;
    v = 100;
    it.minData = &mv;
    it.maxData = &v;

    start = millis();
    resetStats(state);
    printf("\nInitializing iterator.\n");

    sbitsInitIterator(state, &it);
    i = 0;

    while (sbitsNext(state, &it, (void **)&itKey, (void **)&itData)) {
        // printf("Key: %d  Data: %d\n", *itKey, *itData);
        if (*((int32_t *)itData) < *((int32_t *)it.minData) ||
            *((int32_t *)itData) > *((int32_t *)it.maxData)) {
            success = 0;
            printf("Key: %d Data: %d Error\n", *itKey, *itData);
        }
        i++;
    }
    printf("Read records: %d\n", i);
    printf("Success: %d\n", success);

    end = millis();
    printf("Elapsed Time: %lu ms\n", end - start);
    printStats(state);
}

/**
 * Helper function to modify keys for different test scenarios
 */
uint32_t keyModifier(uint32_t inputKey) { return inputKey * 2; }

/**
 * Runs all tests and collects benchmarks
 */
void runalltests_sbits(void *storage) {
    printf("\nSTARTING SBITS TESTS.\n");
    int8_t M = 4;
    int32_t numRecords = 1000;  // default values
    int32_t testRecords = 1000; // default values
    uint8_t useRandom = 0;      // default values
    size_t splineMaxError = 0;  // default values
    uint32_t numSteps = 10;
    uint32_t stepSize = numRecords / numSteps;
    count_t r, numRuns = 1, l;
    uint32_t times[numSteps][numRuns];
    uint32_t reads[numSteps][numRuns];
    uint32_t writes[numSteps][numRuns];
    uint32_t overwrites[numSteps][numRuns];
    uint32_t hits[numSteps][numRuns];
    uint32_t rtimes[numSteps][numRuns];
    uint32_t rreads[numSteps][numRuns];
    uint32_t rhits[numSteps][numRuns];
    int8_t seqdata = 0;
    SD_FILE *infile, *infileRandom;
    uint32_t minRange, maxRange;
    char infileBuffer[512];

    if (seqdata != 1) { /* Open file to read input records */

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

        // infile = fopen("data/seatac_data_100KSorted.bin", "r+b");
        // infileRandom = fopen("data/seatac_data_100KSorted_randomized.bin",
        // "r+b"); minRange = 1314604380; maxRange = 1609487580; numRecords =
        // 100001; testRecords = 100001;

        infile = fopen("data/uwa500K.bin", "r+b");
        // infileRandom =
        // fopen("data/uwa_data_only_2000_500KSorted_randomized.bin", "r+b");
        minRange = 946713600;
        maxRange = 977144040;
        numRecords = 20000; // 500000;
        testRecords = 500000;

        splineMaxError = 1;
        useRandom = 0;

        stepSize = numRecords / numSteps;
    }

    for (r = 0; r < numRuns; r++) {
        /* Configure SBITS state */
        sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
        if (state == NULL) {
            printf("Unable to allocate state. Exiting.\n");
            return;
        }

        state->recordSize = 16;
        state->keySize = 4;
        state->dataSize = 12;
        state->pageSize = 512;
        state->bitmapSize = 0;
        state->bufferSizeInBlocks = M;
        state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
        if (state->buffer == NULL) {
            printf("Unable to allocate buffer. Exiting.\n");
            return;
        }
        int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);
        if (recordBuffer == NULL) {
            printf("Unable to allocate record buffer. Exiting.\n");
            return;
        }

        /* Address level parameters */
        state->storageType = FILE_STORAGE;
        state->storage = storage;
        state->startAddress = 0;
        state->endAddress = 6000 * state->pageSize; // state->pageSize * numRecords / 10; /* Modify this value lower to test wrap around */
        state->eraseSizeInPages = 4;
        // state->parameters = SBITS_USE_MAX_MIN | SBITS_USE_BMAP |
        // SBITS_USE_INDEX;
        state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
        // state->parameters =  0;
        if (SBITS_USING_INDEX(state->parameters) == 1)
            state->endAddress += state->pageSize * (state->eraseSizeInPages * 2);
        if (SBITS_USING_BMAP(state->parameters))
            state->bitmapSize = 8;

        /* Setup for data and bitmap comparison functions */
        state->inBitmap = inBitmapInt64;
        state->updateBitmap = updateBitmapInt64;
        state->compareKey = int32Comparator;
        state->compareData = int32Comparator;

        /* Initialize SBITS structure with parameters */
        if (sbitsInit(state, splineMaxError) != 0) {
            printf("Initialization error.\n");
            return;
        }

        /* Data record is empty. Only need to reset to 0 once as reusing struct.
         */
        int32_t i;
        for (i = 0; i < state->recordSize - 4; i++) { // 4 is the size of the key
            recordBuffer[i + sizeof(int32_t)] = 0;
        }

        /* Erase whole chip (so do not need to erase before write) */
        uint32_t start = millis();
        // dferase_chip();
        printf("Chip erase time: %lu ms\n", millis() - start);

        printf("\n\nINSERT TEST:\n");
        /* Insert records into structure */
        start = millis();

        if (seqdata == 1) {
            for (i = 0; i < numRecords; i++) {
                // printf("Inserting record %lu\n", i);
                memcpy(recordBuffer, &i, sizeof(int32_t));
                int32_t data = i % 100;
                memcpy(recordBuffer + state->keySize, &data, sizeof(int32_t));
                sbitsPut(state, recordBuffer, (void *)(recordBuffer + state->keySize));

                if (i % stepSize == 0) {
                    // printf("Num: %lu KEY: %lu\n", i, i);
                    l = i / stepSize - 1;
                    if (l < numSteps && l >= 0) {
                        times[l][r] = millis() - start;
                        reads[l][r] = state->numReads;
                        writes[l][r] = state->numWrites;
                        overwrites[l][r] = 0;
                        hits[l][r] = state->bufferHits;
                    }
                }
            }
        } else { /* Read data from a file */
            int8_t headerSize = 16;
            i = 0;
            fseek(infile, 0, SEEK_SET);
            // uint32_t readCounter = 0;
            while (1) {
                /* Read page */
                if (0 == fread(infileBuffer, state->pageSize, 1, infile))
                    break;
                // readCounter++;
                /* Process all records on page */
                int16_t count = *((int16_t *)(infileBuffer + 4));
                for (int j = 0; j < count; j++) {
                    // printf("Inserting record %lu\n", i);
                    void *buf = (infileBuffer + headerSize + j * state->recordSize);

                    // printf("Key: %lu, Data: %lu, Page num: %lu, i: %lu\n",
                    // *(id_t*)buf, *(id_t*)(buf + 4), i/31, i);
                    sbitsPut(state, buf, (void *)((int8_t *)buf + 4));
                    // if ( i < 100000)
                    //   printf("%lu %d %d %d\n", *((uint32_t*) buf),
                    //   *((int32_t*) (buf+4)), *((int32_t*) (buf+8)),
                    //   *((int32_t*) (buf+12)));

                    if (i % stepSize == 0) {
                        printf("Num: %lu KEY: %lu\n", i, *((int32_t *)buf));
                        l = i / stepSize - 1;
                        if (l < numSteps && l >= 0) {
                            times[l][r] = millis() - start;
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
        sbitsFlush(state);
        fflush(state->file);
        uint32_t end = millis();

        l = numSteps - 1;
        times[l][r] = end - start;
        reads[l][r] = state->numReads;
        writes[l][r] = state->numWrites;
        overwrites[l][r] = 0;
        hits[l][r] = state->bufferHits;

        printf("Elapsed Time: %lu ms\n", times[l][r]);
        printf("Records inserted: %lu\n", numRecords);

        printStats(state);
        resetStats(state);

        printf("\n\nQUERY TEST:\n");
        /* Verify that all values can be found and test query performance */
        start = millis();

        if (seqdata == 1) {
            for (i = 0; i < numRecords; i++) {
                int32_t key = i;
                int8_t result = sbitsGet(state, &key, recordBuffer);

                if (result != 0)
                    printf("ERROR: Failed to find: %lu\n", key);
                if (seqdata == 1 && *((int32_t *)recordBuffer) != key % 100) {
                    printf("ERROR: Wrong data for: %lu\n", key);
                    printf("Key: %lu Data: %lu\n", key, *((int32_t *)recordBuffer));
                    return;
                }

                if (i % stepSize == 0) {
                    l = i / stepSize - 1;
                    if (l < numSteps && l >= 0) {
                        rtimes[l][r] = millis() - start;
                        rreads[l][r] = state->numReads;
                        rhits[l][r] = state->bufferHits;
                    }
                }
            }
        } else { /* Data from file */
            int8_t headerSize = 16;
            i = 0;
            int8_t queryType = 1;

            if (queryType == 1) { /* Query each record from original data set. */
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
                        void *buf = (infileBuffer + headerSize + j * state->recordSize);
                        int32_t *key = (int32_t *)buf;

                        int8_t result = sbitsGet(state, key, recordBuffer);
                        if (result != 0)
                            printf("ERROR: Failed to find key: %lu, i: %lu\n", *key, i);
                        if (*((int32_t *)recordBuffer) != *((int32_t *)((int8_t *)buf + 4))) {
                            printf("ERROR: Wrong data for: Key: %lu Data: %lu\n", *key, *((int32_t *)recordBuffer));
                            printf("%lu %d %d %d\n", *((uint32_t *)buf),
                                   *((int32_t *)((int8_t *)buf + 4)),
                                   *((int32_t *)((int8_t *)buf + 8)),
                                   *((int32_t *)((int8_t *)buf + 12)));
                            result = sbitsGet(state, key, recordBuffer);
                            // return;
                        }

                        if (i % stepSize == 0) {
                            l = i / stepSize - 1;
                            printf("Num: %lu KEY: %lu\n", i, *key);
                            if (l < numSteps && l >= 0) {
                                rtimes[l][r] =
                                    millis() - start;
                                rreads[l][r] = state->numReads;
                                rhits[l][r] = state->bufferHits;
                            }
                        }
                        i++;
                        if (i == numRecords || i == testRecords) /* Allows ending test after set
                                                 number of records rather than
                                                 processing entire file */
                            goto donetest;
                    }
                }
            donetest:
                numRecords = i;
            } else if (queryType == 2) { /* Query random values in range. May
                                            not exist in data set. */
                i = 0;
                int32_t num = maxRange - minRange;
                printf("Rge: %d Rand max: %d\n", num, RAND_MAX);
                while (i < numRecords) {
                    double scaled =
                        ((double)rand() * (double)rand()) / RAND_MAX / RAND_MAX;
                    int32_t key = (num + 1) * scaled + minRange;

                    // printf("Key :%d\n", key);
                    if (i == 2) {
                        sbitsGet(state, &key, recordBuffer);
                    } else {
                        sbitsGet(state, &key, recordBuffer);
                    }

                    if (i % stepSize == 0) {
                        l = i / stepSize - 1;
                        printf("Num: %lu KEY: %lu\n", i, key);
                        if (l < numSteps && l >= 0) {
                            rtimes[l][r] =
                                millis() - start;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                    i++;
                }
            } else { /* Data value query for given value range */
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
                for (int i = 0; i < 65; i++) // 65
                {
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
                    printf(
                        "Num: %lu KEY: %lu Perc: %d Records: %d Reads: %d \n",
                        i, mv,
                        ((state->numReads - reads) * 1000 /
                         (state->nextPageWriteId - 1)),
                        rec, (state->numReads - reads));

                    if (i % 100 == 0) {
                        l = i / 100 - 1;
                        printf("Num: %lu KEY: %lu Records: %d Reads: %d\n", i, mv, rec, (state->numReads - reads));
                        if (l < numSteps && l >= 0) {
                            rtimes[l][r] = millis() - start;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                }
            }
        }

        end = millis();
        l = numSteps - 1;
        rtimes[l][r] = end - start;
        rreads[l][r] = state->numReads;
        rhits[l][r] = state->bufferHits;
        printf("Elapsed Time: %lu ms\n", rtimes[l][r]);
        printf("Records queried: %lu\n", i);

        printStats(state);

        // Optional: Test iterator
        // testIterator(state);
        // printStats(state);

        free(recordBuffer);
        free(state->buffer);
        sbitsClose(state);
        free(state);
    }

    printf("\nComplete.\n");

    // Prints results
    uint32_t sum;
    for (count_t i = 1; i <= numSteps; i++) {
        printf("Stats for %lu:\n", i * stepSize);

        printf("Reads:   ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += reads[i - 1][r];
            printf("\t%lu", reads[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Writes: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += writes[i - 1][r];
            printf("\t%lu", writes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Overwrites: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += overwrites[i - 1][r];
            printf("\t%lu", overwrites[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Totwrites: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += overwrites[i - 1][r] + writes[i - 1][r];
            printf("\t%lu", overwrites[i - 1][r] + writes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Buffer hits: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += hits[i - 1][r];
            printf("\t%lu", hits[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("Write Time: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += times[i - 1][r];
            printf("\t%lu", times[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Time: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += rtimes[i - 1][r];
            printf("\t%lu", rtimes[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Reads: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += rreads[i - 1][r];
            printf("\t%lu", rreads[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);

        printf("R Buffer hits: ");
        sum = 0;
        for (r = 0; r < numRuns; r++) {
            sum += rhits[i - 1][r];
            printf("\t%lu", rhits[i - 1][r]);
        }
        printf("\t%lu\n", sum / r);
    }
}

#endif
