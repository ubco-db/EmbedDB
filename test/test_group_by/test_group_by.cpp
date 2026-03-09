/*****************************************************************************/
/**
 * @file        Test_group_by.cpp
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Test for EmbedDB advanced queries
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

#include <string.h>

#ifdef ARDUINO
#include <Arduino.h>
#endif

#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/advancedQueries.h"
#endif

#if defined(MEMBOARD)
#include "memboardTestSetup.h"
#endif

#if defined(MEGA)
#include "megaTestSetup.h"
#endif

#if defined(DUE)
#include "dueTestSetup.h"
#endif

#include "unity.h"

#ifdef ARDUINO
#include "SDFileInterface.h"
#define FILE_TYPE SD_FILE
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile
#define JOIN_FILE "expected_join_output.bin"
#define DATA_PATH_UWA "dataFileUWA.bin"
#define INDEX_PATH_UWA "indexFileUWA.bin"
#define DATA_PATH_SEA "dataFileSEA.bin"
#define INDEX_PATH_SEA "indexFileSEA.bin"
#else
#include <math.h>

#include "desktopFileInterface.h"
#define FILE_TYPE FILE
#define JOIN_FILE "data/expected_join_output.bin"
#define DATA_PATH_UWA "build/artifacts/dataFileUWA.bin"
#define INDEX_PATH_UWA "build/artifacts/indexFileUWA.bin"
#define DATA_PATH_SEA "build/artifacts/dataFileSEA.bin"
#define INDEX_PATH_SEA "build/artifacts/indexFileSEA.bin"
#endif

typedef struct DataSource {
    FILE_TYPE* fp;
    int8_t* pageBuffer;
    int32_t pageRecord;
} DataSource;

void insertData(embedDBState* state, const char* filename);
void* nextRecord(DataSource* source);
int8_t sameTempGroup(const void* lastRecord, const void* record);
void writeTempGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord);

embedDBState* stateSEA;
embedDBSchema* baseSchema;
DataSource* seaData;

void setUpEmbedDB() {
    // Init state for SEA dataset
    stateSEA = (embedDBState*)calloc(1, sizeof(embedDBState));
    stateSEA->keySize = 4;
    stateSEA->dataSize = 12;
    stateSEA->compareKey = int32Comparator;
    stateSEA->compareData = int32Comparator;
    stateSEA->pageSize = 512;
    stateSEA->eraseSizeInPages = 4;
    stateSEA->numDataPages = 20000;
    stateSEA->numIndexPages = 1000;
    stateSEA->numSplinePoints = 120;

    /* Setup file IO for SEA data*/
    char dataPath2[] = DATA_PATH_SEA, indexPath2[] = INDEX_PATH_SEA;
    stateSEA->fileInterface = getFileInterface();
    stateSEA->dataFile = setupFile(dataPath2);
    stateSEA->indexFile = setupFile(indexPath2);

    stateSEA->bufferSizeInBlocks = 4;
    stateSEA->buffer = malloc(stateSEA->bufferSizeInBlocks * stateSEA->pageSize);
    stateSEA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    stateSEA->bitmapSize = 2;
    stateSEA->inBitmap = inBitmapInt16;
    stateSEA->updateBitmap = updateBitmapInt16;
    stateSEA->buildBitmapFromRange = buildBitmapInt16FromRange;
    embedDBInit(stateSEA, 1);

    /* insert SEA data */
    const char seaDatafileName[] = "data/sea100K.bin";
    insertData(stateSEA, seaDatafileName);

    // Init base schema
    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    ColumnType colTypes[] = {embedDB_COLUMN_UINT32, embedDB_COLUMN_INT32, embedDB_COLUMN_INT32, embedDB_COLUMN_INT32};
    baseSchema = embedDBCreateSchema(4, colSizes, colSignedness, colTypes);

    seaData = (DataSource*)malloc(sizeof(DataSource));
    seaData->fp = fopen("data/sea100K.bin", "rb");
    seaData->pageBuffer = (int8_t*)calloc(1, 512);
    seaData->pageRecord = 0;
}

void test_aggregate() {
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateSEA, &it);

    embedDBOperator* scanOp = createTableScanOperator(stateSEA, &it, baseSchema);
    int32_t selVal = 100;
    embedDBOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);

    embedDBOperator* sortOp = createOrderByOperator(stateSEA, selectOp, 1, -1, int32Comparator);

    embedDBAggregateFunc groupName = {NULL, NULL, writeTempGroup, NULL, 4};
    embedDBAggregateFunc* counter = createCountAggregate();
    embedDBAggregateFunc* maxWind = createMaxAggregate(3, -4);
    embedDBAggregateFunc* avgWind = createAvgAggregate(3, 4);
    embedDBAggregateFunc* sum = createSumAggregate(2);
    embedDBAggregateFunc aggFunctions[] = {groupName, *counter, *maxWind, *avgWind, *sum};
    uint32_t functionsLength = 5;

    typedef struct {
        uint32_t count;
        int32_t maxWind;
        int32_t sum;
        int32_t minTemp;
        int32_t avgSum;
    } Truth;

    Truth* answers = (Truth*)calloc(1000, sizeof(Truth));
    uint32_t expectedGroups = 0;

    fseek(seaData->fp, 0, SEEK_SET);
    seaData->pageRecord = 0;

    void* rawRec;
    while ((rawRec = nextRecord(seaData)) != NULL) {
        int32_t* r = (int32_t*)rawRec;
        if (r[3] < selVal) continue;

        int32_t temp = r[1];
        if (answers[temp].count == 0) expectedGroups++;
        answers[temp].count++;
        if (r[3] > answers[temp].maxWind || answers[temp].count == 1) answers[temp].maxWind = r[3];
        if (r[1] < answers[temp].minTemp || answers[temp].count == 1) answers[temp].minTemp = r[1];
        answers[temp].sum += r[2];
        answers[temp].avgSum += r[3];
    }

    embedDBOperator* aggOp = createAggregateOperator(sortOp, sameTempGroup, aggFunctions, functionsLength);
    aggOp->init(aggOp);

    int32_t groupsFound = 0;
    while (exec(aggOp)) {
        groupsFound++;
        int32_t* res = (int32_t*)aggOp->recordBuffer;
#ifdef DEBUG
        printf("RAW RECORD: ");
        for (int i = 0; i < 8; i++) {
            printf("[%d]: %ld  ", i, (long)res[i]);
        }
        printf("\n");
        fflush(stdout);
#endif
        int32_t label = res[0];

        TEST_ASSERT_TRUE_MESSAGE(label >= 0 && label < 1200, "DB returned a label out of range");
        TEST_ASSERT_TRUE_MESSAGE(answers[label].count > 0, "DB returned a group that shouldn't exist");

        TEST_ASSERT_EQUAL_UINT32_MESSAGE(answers[label].count, res[1], "Count is wrong");

        TEST_ASSERT_EQUAL_INT32_MESSAGE(answers[label].maxWind, res[2], "Max Wind is wrong");

        float expectedAvg = (float)answers[label].avgSum / answers[label].count;

        float actualAvg = ((float*)res)[3];

        TEST_ASSERT_EQUAL_FLOAT_MESSAGE(expectedAvg, actualAvg, "Average mismatch");

        TEST_ASSERT_EQUAL_INT32_MESSAGE((int32_t)answers[label].sum, res[4], "Sum is wrong");
    }

    TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedGroups, groupsFound, "Number of unique groups mismatch");

    free(answers);

    // Free states
    for (uint32_t i = 0; i < functionsLength; i++) {
        if (aggFunctions[i].state != NULL) {
            free(aggFunctions[i].state);
        }
    }

    aggOp->close(aggOp);
    embedDBFreeOperatorRecursive(&aggOp);
}

void tearDown(void) {
    embedDBClose(stateSEA);
    tearDownFile(stateSEA->dataFile);
    tearDownFile(stateSEA->indexFile);
    free(stateSEA->fileInterface);
    free(stateSEA->buffer);
    free(stateSEA);

    embedDBFreeSchema(&baseSchema);

    fclose(seaData->fp);
    free(seaData->pageBuffer);
    free(seaData);
}

int runUnityTests(void) {
    UNITY_BEGIN();

    RUN_TEST(test_aggregate);

    return UNITY_END();
}

void insertData(embedDBState* state, const char* filename) {
    FILE_TYPE* fp = fopen(filename, "rb");
    char fileBuffer[512];
    int numRecords = 0;
    while (fread(fileBuffer, state->pageSize, 1, fp)) {
        uint16_t count = EMBEDDB_GET_COUNT(fileBuffer);
        for (int i = 1; i <= count; i++) {
            embedDBPut(state, fileBuffer + i * state->recordSize, fileBuffer + i * state->recordSize + state->keySize);
            numRecords++;
        }
    }
    fclose(fp);
    embedDBFlush(state);
}

void* nextRecord(DataSource* source) {
    uint16_t count = EMBEDDB_GET_COUNT(source->pageBuffer);
    if (count <= source->pageRecord) {
        // Read next page
        if (!fread(source->pageBuffer, 512, 1, source->fp)) {
            return NULL;
        }
        source->pageRecord = 0;
    }
    return source->pageBuffer + ++source->pageRecord * 16;
}

int8_t sameTempGroup(const void* lastRecord, const void* record) {
    int32_t t1 = *(int32_t*)((char*)lastRecord + 4);
    int32_t t2 = *(int32_t*)((char*)record + 4);
    return t1 == t2;
}

void writeTempGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    int32_t temp = *(int32_t*)((char*)lastRecord + 4);
    uint16_t offset = getColOffsetFromSchema(schema, aggFunc->colNum);
    memcpy((int8_t*)recordBuffer + offset, &temp, sizeof(int32_t));
}

void setUp() {
    setUpEmbedDB();
}

#ifdef ARDUINO

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

#else

int main() {
    return runUnityTests();
}

#endif
