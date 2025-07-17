/*****************************************************************************/
/**
 * @file        Test_advanced_queries.cpp
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

#include <math.h>
#include <string.h>

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
#define fopen sd_fopen
#define fread sd_fread
#define fclose sd_fclose
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile
#define JOIN_FILE "expected_join_output.bin"
#define DATA_PATH_UWA "dataFileUWA.bin"
#define INDEX_PATH_UWA "indexFileUWA.bin"
#define DATA_PATH_SEA "dataFileSEA.bin"
#define INDEX_PATH_SEA "indexFileSEA.bin"
#else
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
uint32_t dayGroup(const void* record);
int8_t sameDayGroup(const void* lastRecord, const void* record);
void writeDayGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord);
void customShiftInit(embedDBOperator* op);
int8_t customShiftNext(embedDBOperator* op);
void customShiftClose(embedDBOperator* op);

embedDBState *stateUWA, *stateSEA;
embedDBSchema* baseSchema;
DataSource *uwaData, *seaData;

void setUpEmbedDB() {
    // Init state for UWA dataset
    stateUWA = (embedDBState*)calloc(1, sizeof(embedDBState));
    stateUWA->keySize = 4;
    stateUWA->dataSize = 12;
    stateUWA->compareKey = int32Comparator;
    stateUWA->compareData = int32Comparator;
    stateUWA->pageSize = 512;
    stateUWA->eraseSizeInPages = 4;
    stateUWA->numDataPages = 20000;
    stateUWA->numIndexPages = 1000;
    stateUWA->numSplinePoints = 30;

    /* Setup file IO for UWA data */
    char dataPath[] = DATA_PATH_UWA, indexPath[] = INDEX_PATH_UWA;
    stateUWA->fileInterface = getFileInterface();
    stateUWA->dataFile = setupFile(dataPath);
    stateUWA->indexFile = setupFile(indexPath);

    stateUWA->bufferSizeInBlocks = 4;
    stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
    stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    stateUWA->bitmapSize = 2;
    stateUWA->inBitmap = inBitmapInt16;
    stateUWA->updateBitmap = updateBitmapInt16;
    stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
    embedDBInit(stateUWA, 1);

    /* insert UWA data */
    const char uwaDatafileName[] = "data/uwa500K.bin";
    insertData(stateUWA, uwaDatafileName);

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

    // Open data sources for comparison
    uwaData = (DataSource*)malloc(sizeof(DataSource));
    uwaData->fp = fopen("data/uwa500K.bin", "rb");
    uwaData->pageBuffer = (int8_t*)calloc(1, 512);
    uwaData->pageRecord = 0;

    seaData = (DataSource*)malloc(sizeof(DataSource));
    seaData->fp = fopen("data/sea100K.bin", "rb");
    seaData->pageBuffer = (int8_t*)calloc(1, 512);
    seaData->pageRecord = 0;
}

void test_projection() {
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projCols[] = {0, 1, 3};
    embedDBOperator* projOp = createProjectionOperator(scanOp, 3, projCols);
    projOp->init(projOp);

    TEST_ASSERT_EQUAL_UINT8_MESSAGE(3, projOp->schema->numCols, "Output schema has wrong number of columns");
    int8_t expectedColSizes[] = {4, -4, -4};
    TEST_ASSERT_EQUAL_INT8_ARRAY_MESSAGE(expectedColSizes, projOp->schema->columnSizes, 3, "Output schema col sizes are wrong");

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = (int32_t*)projOp->recordBuffer;
    while (exec(projOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[3], recordBuffer[2], "Third column is wrong");
    }

    projOp->close(projOp);
    embedDBFreeOperatorRecursive(&projOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(500000, recordsReturned, "Projection didn't return the right number of records");
}

void test_selection() {
    int32_t maxTemp = 400;
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    int32_t selVal = 200;
    embedDBOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
    uint8_t projCols[] = {0, 1, 3};
    embedDBOperator* projOp = createProjectionOperator(selectOp, 3, projCols);
    projOp->init(projOp);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = (int32_t*)projOp->recordBuffer;
    while (exec(projOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        while (expectedRecord[1] > maxTemp || expectedRecord[3] < selVal) {
            expectedRecord = (int32_t*)nextRecord(uwaData);
        }
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[3], recordBuffer[2], "Third column is wrong");
    }

    projOp->close(projOp);
    embedDBFreeOperatorRecursive(&projOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(4, recordsReturned, "Selection didn't return the right number of records");
}

void test_aggregate() {
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    int32_t selVal = 150;
    embedDBOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
    embedDBAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
    embedDBAggregateFunc* counter = createCountAggregate();
    embedDBAggregateFunc* maxWind = createMaxAggregate(3, -4);
    embedDBAggregateFunc* avgWind = createAvgAggregate(3, 4);
    embedDBAggregateFunc* sum = createSumAggregate(2);
    embedDBAggregateFunc* minTemp = createMinAggregate(1, -4);
    embedDBAggregateFunc aggFunctions[] = {groupName, *counter, *maxWind, *avgWind, *sum, *minTemp};
    uint32_t functionsLength = 6;
    embedDBOperator* aggOp = createAggregateOperator(selectOp, sameDayGroup, aggFunctions, functionsLength);
    aggOp->init(aggOp);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = (int32_t*)aggOp->recordBuffer;
    while (exec(aggOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        while (expectedRecord[3] < selVal) {
            expectedRecord = (int32_t*)nextRecord(uwaData);
        }
        uint32_t firstInGroupDay = dayGroup(expectedRecord);
        uint32_t day = 0;
        uint32_t count = 0;
        int64_t sum = 0;
        int32_t minTmp = INT32_MAX, maxWnd = INT32_MIN, avgSum = 0;
        ;
        do {
            count++;
            sum += expectedRecord[2];
            avgSum += expectedRecord[3];
            if (expectedRecord[3] > maxWnd) {
                maxWnd = expectedRecord[3];
            }
            if (expectedRecord[1] < minTmp) {
                minTmp = expectedRecord[1];
            }
            expectedRecord = (int32_t*)nextRecord(uwaData);
            while (expectedRecord[3] < selVal) {
                expectedRecord = (int32_t*)nextRecord(uwaData);
                if (expectedRecord == NULL) {
                    goto done;
                }
            }
            day = dayGroup(expectedRecord);
        } while (day == firstInGroupDay);
    done:
        uwaData->pageRecord--;
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(firstInGroupDay, recordBuffer[0], "Group label is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(count, recordBuffer[1], "Count is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(maxWnd, recordBuffer[2], "Max is wrong");
        TEST_ASSERT_EQUAL_FLOAT_MESSAGE((float)avgSum / count, ((float*)recordBuffer)[3], "Average is wrong");
        TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&sum, (int64_t*)(recordBuffer + 4), sizeof(int64_t), "Sum is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(minTmp, recordBuffer[6], "Min is wrong");
    }

    // Free states
    for (uint32_t i = 0; i < functionsLength; i++) {
        if (aggFunctions[i].state != NULL) {
            free(aggFunctions[i].state);
        }
    }

    aggOp->close(aggOp);
    embedDBFreeOperatorRecursive(&aggOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(90, recordsReturned, "Aggregate didn't return the right number of records");
}

void test_join() {
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBIterator it2;
    uint32_t year2015 = 1420099200;      // 2015-01-01
    uint32_t year2016 = 1451635200 - 1;  // 2016-01-01
    it2.minKey = &year2015;
    it2.maxKey = &year2016;
    it2.minData = NULL;
    it2.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);   // Iterator on uwa
    embedDBInitIterator(stateSEA, &it2);  // Iterator on sea

    // Prepare uwa table
    embedDBOperator* scan1 = createTableScanOperator(stateUWA, &it, baseSchema);
    embedDBOperator* shift = (embedDBOperator*)malloc(sizeof(embedDBOperator));  // Custom operator to shift the year 2000 to 2015 to make the join work
    shift->input = scan1;
    shift->init = customShiftInit;
    shift->next = customShiftNext;
    shift->close = customShiftClose;

    // Prepare sea table
    embedDBOperator* scan2 = createTableScanOperator(stateSEA, &it2, baseSchema);
    scan2->init(scan2);

    // Join tables
    embedDBOperator* join = createKeyJoinOperator(shift, scan2);

    // Project the timestamp and the two temp columns
    uint8_t projCols2[] = {0, 1, 5};
    embedDBOperator* proj = createProjectionOperator(join, 3, projCols2);

    proj->init(proj);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = (int32_t*)proj->recordBuffer;

    FILE_TYPE* fp = fopen(JOIN_FILE, "rb");

    int32_t expectedRecord[3];
    while (exec(proj)) {
        recordsReturned++;
        fread(expectedRecord, sizeof(int32_t), 3, fp);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedRecord[2], recordBuffer[2], "Third column is wrong");
    }
    fclose(fp);

    proj->close(proj);
    free(scan1);
    free(shift);
    free(scan2);
    free(join);
    free(proj);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(9942, recordsReturned, "Join didn't return the right number of records");
}

void tearDown(void) {
    embedDBClose(stateUWA);
    tearDownFile(stateUWA->dataFile);
    tearDownFile(stateUWA->indexFile);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);

    embedDBClose(stateSEA);
    tearDownFile(stateSEA->dataFile);
    tearDownFile(stateSEA->indexFile);
    free(stateSEA->fileInterface);
    free(stateSEA->buffer);
    free(stateSEA);

    embedDBFreeSchema(&baseSchema);

    fclose(uwaData->fp);
    free(uwaData->pageBuffer);
    free(uwaData);
    fclose(seaData->fp);
    free(seaData->pageBuffer);
    free(seaData);
}

int runUnityTests(void) {
    UNITY_BEGIN();

    RUN_TEST(test_projection);
    RUN_TEST(test_selection);
    RUN_TEST(test_aggregate);
    RUN_TEST(test_join);

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

uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
}

void customShiftInit(embedDBOperator* op) {
    op->input->init(op->input);
    op->schema = copySchema(op->input->schema);
    op->recordBuffer = malloc(16);
}

int8_t customShiftNext(embedDBOperator* op) {
    if (op->input->next(op->input)) {
        memcpy(op->recordBuffer, op->input->recordBuffer, 16);
        *(uint32_t*)op->recordBuffer += 473385600;  // Add the number of seconds between 2000 and 2015
        return 1;
    }
    return 0;
}

void customShiftClose(embedDBOperator* op) {
    op->input->close(op->input);
    embedDBFreeSchema(&op->schema);
    free(op->recordBuffer);
    op->recordBuffer = NULL;
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
