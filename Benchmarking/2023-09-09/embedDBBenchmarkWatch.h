#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../src/embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "sdcard_c_iface.h"

#if defined(MEGA)
#include "SDFileInterface.h"
#endif

#if defined(DUE)
#include "SDFileInterface.h"
#endif

#if defined(MEMBOARD)
#include "SDFileInterface.h"
#include "dataflashFileInterface.h"
#include "dataflash_c_iface.h"
#endif

#define STORAGE_TYPE 0  // 0 = SD Card, 1 = Dataflash

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

char const dataFileName[] = "data/watch_only_100K.bin";
char const randomizedDataFileName[] = "data/watch_only_100K_randomized.bin";

void testRawPerformance() { /* Tests storage raw read and write performance */
    printf("Starting RAW performance test.\n");

    char buffer[512];

#if STORAGE_TYPE == 0
    /* SD Card */
    uint32_t start;
    SD_FILE *fp = fopen("speedTestTemp.bin", "w+b");
    if (fp == NULL) {
        printf("Error opening file.\n");
        return;
    }

    // Test time to write 100000 blocks
    int numWrites = 1000;
    start = millis();

    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
    }
    printf("Write time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));
    fflush(fp);

    start = millis();

    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
    }
    printf("Random write time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));
    fflush(fp);

    // Time to read 1000 blocks
    fseek(fp, 0, SEEK_SET);
    start = millis();
    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Read time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));

    fseek(fp, 0, SEEK_SET);
    // Time to read 1000 blocks randomly
    start = millis();
    srand(1);
    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Random Read time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));
#endif
#if STORAGE_TYPE == 1
    /* Dataflash */

    // Test time to write 100000 blocks
    int numWrites = 1000;
    uint32_t start = millis();

    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        if (0 == dfwrite(i, buffer, 512)) {
            printf("Write error.\n");
        }
    }
    printf("Write time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));

    start = millis();

    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        unsigned long num = rand() % numWrites;
        if (0 == dfwrite(num, buffer, 512)) {
            printf("Write error.\n");
        }
    }
    printf("Random write time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));

    // Time to read 1000 blocks
    start = millis();
    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        if (0 == dfread(i, buffer, 512)) {
            printf("Read error.\n");
        }
    }
    printf("Read time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));

    // Time to read 1000 blocks randomly
    start = millis();
    srand(1);
    for (int i = 0; i < numWrites; i++) {
		for (int j = 0; j < 128; j++) {
			((uint32_t*)buffer)[j]++;
		}
        unsigned long num = rand() % numWrites;
        if (0 == dfread(num, buffer, 512)) {
            printf("Read error.\n");
        }
    }
    printf("Random Read time: %lums (%d KB/s)\n", millis() - start, (int)((double)numWrites * 512 / 1000000 / (millis() - start) * 1000 * 1000));
#endif
}

void runBenchmark() {
    printf("\n");
    // testRawPerformance();
    printf("\n");

#define numRuns 3
    uint32_t timeInsert[numRuns],
        timeSelectAll[numRuns],
        timeSelectKeySmallResult[numRuns],
        timeSelectKeyLargeResult[numRuns],
        timeSelectSingleDataResult[numRuns],
        timeSelectDataSmallResult[numRuns],
        timeSelectDataLargeResult[numRuns],
        timeSelectKeyData[numRuns],
        timeSeqKV[numRuns],
        timeRandKV[numRuns];
    uint32_t numRecords, numRecordsSelectAll, numRecordsSelectKeySmallResult, numRecordsSelectKeyLargeResult, numRecordsSelectSingleDataResult, numRecordsSelectDataSmallResult, numRecordsSelectDataLargeResult, numRecordsSelectKeyData, numRecordsSeqKV, numRecordsRandKV;
    uint32_t numWrites, numReadsSelectAll, numReadsSelectKeySmallResult, numReadsSelectKeyLargeResult, numReadsSelectSingleDataResult, numReadsSelectDataSmallResult, numReadsSelectDataLargeResult, numReadsSelectKeyData, numReadsSeqKV, numReadsRandKV;
    uint32_t numIdxWrites, numIdxReadsSelectSingleDataResult, numIdxReadsSelectDataSmallResult, numIdxReadsSelectDataLargeResult, numIdxReadsSelectKeyData;

    for (int run = 0; run < numRuns; run++) {
        ///////////
        // Setup //
        ///////////
        embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));
        state->keySize = 4;
        state->dataSize = 12;
        state->compareKey = int32Comparator;
        state->compareData = int32Comparator;
        state->pageSize = 512;
        state->eraseSizeInPages = 4;
        state->numSplinePoints = 310;
        state->numDataPages = 20000;
        state->numIndexPages = 100;
#if STORAGE_TYPE == 0
        state->fileInterface = getSDInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        state->dataFile = setupSDFile(dataPath);
        state->indexFile = setupSDFile(indexPath);
#endif
#if STORAGE_TYPE == 1
        state->fileInterface = getDataflashInterface();
        state->dataFile = setupDataflashFile(0, 20000);
        state->indexFile = setupDataflashFile(21000, 100);
#endif
        state->bufferSizeInBlocks = 4;
        state->buffer = calloc(state->bufferSizeInBlocks, state->pageSize);
        state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        state->bitmapSize = 2;
        state->inBitmap = inCustomUWABitmap;
        state->updateBitmap = updateCustomUWABitmap;
        state->buildBitmapFromRange = buildCustomUWABitmapFromRange;
        embedDBInit(state, 1);
        char *recordBuffer = (char *)malloc(state->recordSize);

        printf("A\n");

        ////////////////////////
        // Insert uwa dataset //
        ////////////////////////
        SD_FILE *dataset = fopen(dataFileName, "rb");
        uint32_t start = millis();

        numRecords = 0;

        char dataPage[512];
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
                numRecords++;
            }
        }
        embedDBFlush(state);

        timeInsert[run] = millis() - start;
        numWrites = state->numWrites;
        numIdxWrites = state->numIdxWrites;

        printf("B\n");

        /////////////////////
        // SELECT * FROM r //
        /////////////////////
        start = millis();

        embedDBIterator itSelectAll;
        itSelectAll.minKey = NULL;
        itSelectAll.maxKey = NULL;
        itSelectAll.minData = NULL;
        itSelectAll.maxData = NULL;
        embedDBInitIterator(state, &itSelectAll);

        numRecordsSelectAll = 0;
        numReadsSelectAll = state->numReads;

        while (embedDBNext(state, &itSelectAll, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectAll++;
        }

        timeSelectAll[run] = millis() - start;

        numReadsSelectAll = state->numReads - numReadsSelectAll;

        printf("C\n");

        ///////////////
        // SELECT 5% //
        ///////////////
        start = millis();

        embedDBIterator itSelectKeySmallResult;
        uint32_t minKeySelectKeySmallResult = 7026900;
        itSelectKeySmallResult.minKey = &minKeySelectKeySmallResult;
        itSelectKeySmallResult.maxKey = NULL;
        itSelectKeySmallResult.minData = NULL;
        itSelectKeySmallResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectKeySmallResult);

        numRecordsSelectKeySmallResult = 0;
        numReadsSelectKeySmallResult = state->numReads;

        while (embedDBNext(state, &itSelectKeySmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeySmallResult++;
        }

        timeSelectKeySmallResult[run] = millis() - start;

        numReadsSelectKeySmallResult = state->numReads - numReadsSelectKeySmallResult;

        printf("D\n");

        ////////////////
        // SELECT 80% //
        ////////////////
        start = millis();

        embedDBIterator itSelectKeyLargeResult;
        uint32_t minKeySelectKeyLargeResult = 949287;
        itSelectKeyLargeResult.minKey = &minKeySelectKeyLargeResult;
        itSelectKeyLargeResult.maxKey = NULL;
        itSelectKeyLargeResult.minData = NULL;
        itSelectKeyLargeResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectKeyLargeResult);

        numRecordsSelectKeyLargeResult = 0;
        numReadsSelectKeyLargeResult = state->numReads;

        while (embedDBNext(state, &itSelectKeyLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeyLargeResult++;
        }

        timeSelectKeyLargeResult[run] = millis() - start;

        numReadsSelectKeyLargeResult = state->numReads - numReadsSelectKeyLargeResult;

        printf("E\n");

        ////////////////////////////////////////////
        // SELECT * FROM r WHERE data = -93050848 //
        ////////////////////////////////////////////

        start = millis();

        embedDBIterator itSelectSingleDataResult;
        int32_t minDataSelectSingleResult = -93050848;
        int32_t maxDataSelectSingleResult = -93050848;
        itSelectSingleDataResult.minKey = NULL;
        itSelectSingleDataResult.maxKey = NULL;
        itSelectSingleDataResult.minData = &minDataSelectSingleResult;
        itSelectSingleDataResult.maxData = &maxDataSelectSingleResult;
        embedDBInitIterator(state, &itSelectSingleDataResult);

        numRecordsSelectSingleDataResult = 0;
        numReadsSelectSingleDataResult = state->numReads;
        numIdxReadsSelectSingleDataResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectSingleDataResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectSingleDataResult++;
        }

        timeSelectSingleDataResult[run] = millis() - start;

        numReadsSelectSingleDataResult = state->numReads - numReadsSelectSingleDataResult;
        numIdxReadsSelectSingleDataResult = state->numIdxReads - numIdxReadsSelectSingleDataResult;

        printf("F\n");

        /////////////////////////////////////////////
        // SELECT * FROM r WHERE data >= 871998912 //
        /////////////////////////////////////////////
        start = millis();

        embedDBIterator itSelectDataSmallResult;
        int32_t minDataSelectDataSmallResult = 871998912;
        itSelectDataSmallResult.minKey = NULL;
        itSelectDataSmallResult.maxKey = NULL;
        itSelectDataSmallResult.minData = &minDataSelectDataSmallResult;
        itSelectDataSmallResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectDataSmallResult);

        numRecordsSelectDataSmallResult = 0;
        numReadsSelectDataSmallResult = state->numReads;
        numIdxReadsSelectDataSmallResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectDataSmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataSmallResult++;
        }

        timeSelectDataSmallResult[run] = millis() - start;

        numReadsSelectDataSmallResult = state->numReads - numReadsSelectDataSmallResult;
        numIdxReadsSelectDataSmallResult = state->numIdxReads - numIdxReadsSelectDataSmallResult;

        printf("G\n");

        ////////////////////////////////////////////
        // SELECT * FROM r WHERE data >= -5000000 //
        ////////////////////////////////////////////
        start = millis();

        embedDBIterator itSelectDataLargeResult;
        int32_t minDataSelectDataLargeResult = -5000000;
        itSelectDataLargeResult.minKey = NULL;
        itSelectDataLargeResult.maxKey = NULL;
        itSelectDataLargeResult.minData = &minDataSelectDataLargeResult;
        itSelectDataLargeResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectDataLargeResult);

        numRecordsSelectDataLargeResult = 0;
        numReadsSelectDataLargeResult = state->numReads;
        numIdxReadsSelectDataLargeResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectDataLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataLargeResult++;
        }

        timeSelectDataLargeResult[run] = millis() - start;

        numReadsSelectDataLargeResult = state->numReads - numReadsSelectDataLargeResult;
        numIdxReadsSelectDataLargeResult = state->numIdxReads - numIdxReadsSelectDataLargeResult;

        printf("H\n");

        //////////////////////////////////////////////////////////////////////////////
        // SELECT * FROM r WHERE key >= 1970000 AND data >= 0 AND data <= 871998912 //
        //////////////////////////////////////////////////////////////////////////////
        start = millis();

        embedDBIterator itSelectKeyData;
        uint32_t minKeySelectKeyData = 1970000;
        int32_t minDataSelectKeyData = 0, maxDataSelectKeyData = 871998912;
        itSelectKeyData.minKey = &minKeySelectKeyData;
        itSelectKeyData.maxKey = NULL;
        itSelectKeyData.minData = &minDataSelectKeyData;
        itSelectKeyData.maxData = &maxDataSelectKeyData;
        embedDBInitIterator(state, &itSelectKeyData);

        numRecordsSelectKeyData = 0;
        numReadsSelectKeyData = state->numReads;
        numIdxReadsSelectKeyData = state->numIdxReads;

        while (embedDBNext(state, &itSelectKeyData, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeyData++;
        }

        timeSelectKeyData[run] = millis() - start;

        numReadsSelectKeyData = state->numReads - numReadsSelectKeyData;
        numIdxReadsSelectKeyData = state->numIdxReads - numIdxReadsSelectKeyData;

        printf("I\n");

        //////////////////////////
        // Sequential Key-Value //
        //////////////////////////
        fseek(dataset, 0, SEEK_SET);
        start = millis();

        numRecordsSeqKV = 0;
        numReadsSeqKV = state->numReads;

		int page = 0;
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsSeqKV++;
            }
        }

        timeSeqKV[run] = millis() - start;
        numReadsSeqKV = state->numReads - numReadsSeqKV;

        fclose(dataset);

        printf("J\n");

        //////////////////////
        // Random Key-Value //
        //////////////////////
        SD_FILE *randomDataset = fopen(randomizedDataFileName, "rb");

        start = millis();

        numRecordsRandKV = 0;
        numReadsRandKV = state->numReads;

        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsRandKV++;
            }
        }

        timeRandKV[run] = millis() - start;
        numReadsRandKV = state->numReads - numReadsRandKV;

        fclose(randomDataset);

        printf("K\n");

        /////////////////
        // Close EMBEDDB //
        /////////////////
        embedDBClose(state);
#if STORAGE_TYPE == 0
        tearDownSDFile(state->dataFile);
        tearDownSDFile(state->indexFile);
#endif
#if STORAGE_TYPE == 1
        tearDownDataflashFile(state->dataFile);
        tearDownDataflashFile(state->indexFile);
#endif
        free(state->fileInterface);
        free(state->buffer);
        free(recordBuffer);
    }

    // Calculate averages
    int sum = 0;
    printf("\nINSERT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeInsert[i]);
        sum += timeInsert[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Num Records inserted: %lu\n", numRecords);
    printf("Num data pages written: %lu\n", numWrites);
    printf("Num index pages written: %lu\n", numIdxWrites);

    sum = 0;
    printf("\nSELECT * FROM r\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectAll[i]);
        sum += timeSelectAll[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectAll);
    printf("Num reads: %lu\n", numReadsSelectAll);

    sum = 0;
    printf("\nSELECT Continuous 5%% (key >= 7026900)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectKeySmallResult[i]);
        sum += timeSelectKeySmallResult[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectKeySmallResult);
    printf("Num reads: %lu\n", numReadsSelectKeySmallResult);

    sum = 0;
    printf("\nSELECT Continuous 80%% (key >= 949287)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectKeyLargeResult[i]);
        sum += timeSelectKeyLargeResult[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectKeyLargeResult);
    printf("Num reads: %lu\n", numReadsSelectKeyLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data = -93050848\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectSingleDataResult[i]);
        sum += timeSelectSingleDataResult[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectSingleDataResult);
    printf("Num reads: %lu\n", numReadsSelectSingleDataResult);
    printf("Num idx reads: %lu\n", numIdxReadsSelectSingleDataResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= 871998912\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectDataSmallResult[i]);
        sum += timeSelectDataSmallResult[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectDataSmallResult);
    printf("Num reads: %lu\n", numReadsSelectDataSmallResult);
    printf("Num idx reads: %lu\n", numIdxReadsSelectDataSmallResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= -5000000\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectDataLargeResult[i]);
        sum += timeSelectDataLargeResult[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectDataLargeResult);
    printf("Num reads: %lu\n", numReadsSelectDataLargeResult);
    printf("Num idx reads: %lu\n", numIdxReadsSelectDataLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE key >= 1970000 AND data >= 0 AND data <= 871998912\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSelectKeyData[i]);
        sum += timeSelectKeyData[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSelectKeyData);
    printf("Num reads: %lu\n", numReadsSelectKeyData);
    printf("Num idx reads: %lu\n", numIdxReadsSelectKeyData);

    sum = 0;
    printf("\nSequential Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeSeqKV[i]);
        sum += timeSeqKV[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsSeqKV);
    printf("Num reads: %lu\n", numReadsSeqKV);

    sum = 0;
    printf("\nRandom Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%lu ", timeRandKV[i]);
        sum += timeRandKV[i];
    }
    printf("~ %lums\n", sum / numRuns);
    printf("Result size: %lu\n", numRecordsRandKV);
    printf("Num reads: %lu\n", numReadsRandKV);
}

void updateCustomUWABitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)data;

    /*  Custom, equi-depth buckets
     *  Bucket  0: -870853696 -> -95660744 (0, 6249)
     *	Bucket  1: -95588688 -> -53529536 (6250, 12499)
     *	Bucket  2: -53504944 -> -30433654 (12500, 18749)
     *	Bucket  3: -30433654 -> -16937704 (18750, 24999)
     *	Bucket  4: -16911074 -> -8323669 (25000, 31249)
     *	Bucket  5: -8323669 -> -3089904 (31250, 37499)
     *	Bucket  6: -3089904 -> -205993 (37500, 43749)
     *	Bucket  7: -205993 -> 2210423 (43750, 49999)
     *	Bucket  8: 2210423 -> 3888215 (50000, 56249)
     *	Bucket  9: 3888215 -> 6309509 (56250, 62499)
     *	Bucket 10: 6309509 -> 11543274 (62500, 68749)
     *	Bucket 11: 11543274 -> 20408630 (68750, 74999)
     *	Bucket 12: 20426446 -> 32905578 (75000, 81249)
     *	Bucket 13: 32905578 -> 55082704 (81250, 87499)
     *	Bucket 14: 55082704 -> 98510328 (87500, 93749)
     *	Bucket 15: 98510328 -> 871998912 (93750, 99999)
     */

    uint16_t mask = 1;

    int8_t mode = 0;  // 0 = equi-depth, 1 = equi-width
    if (mode == 0) {
        if (temp < 373) {
            mask <<= 0;
        } else if (temp < -53529536) {
            mask <<= 1;
        } else if (temp < -30433654) {
            mask <<= 2;
        } else if (temp < -16937704) {
            mask <<= 3;
        } else if (temp < -8323669) {
            mask <<= 4;
        } else if (temp < -3089904) {
            mask <<= 5;
        } else if (temp < -205993) {
            mask <<= 6;
        } else if (temp < 2210423) {
            mask <<= 7;
        } else if (temp < 3888215) {
            mask <<= 8;
        } else if (temp < 6309509) {
            mask <<= 9;
        } else if (temp < 11543274) {
            mask <<= 10;
        } else if (temp < 20408630) {
            mask <<= 11;
        } else if (temp < 32905578) {
            mask <<= 12;
        } else if (temp < 55082704) {
            mask <<= 13;
        } else if (temp < 98510328) {
            mask <<= 14;
        } else {
            mask <<= 15;
        }
    } else {
        int shift = (temp - 303) / 16;
        if (shift < 0) {
            shift = 0;
        } else if (shift > 15) {
            shift = 15;
        }
        mask <<= shift;
    }

    *(uint16_t *)bm |= mask;
}

int8_t inCustomUWABitmap(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateCustomUWABitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomUWABitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomUWABitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomUWABitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}
