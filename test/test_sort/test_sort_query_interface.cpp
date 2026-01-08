#ifdef DIST
#include "embedDB.h"
#else
#include "embedDBUtility.h"
#include "query-interface/advancedQueries.h"

#endif

#define STORAGE_TYPE 0

#if defined(MEMBOARD) && STORAGE_TYPE == 1
#include "dataflashFileInterface.h"
#include "memboardTestSetup.h"
#endif

#if defined(MEGA)
#include "megaTestSetup.h"
#endif

#if defined(DUE)
#include "dueTestSetup.h"
#endif

#ifdef ARDUINO
#include "SDFileInterface.h"
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile
#define DATA_FILE_PATH "dataFile.bin"
#define clock millis
#define DATA_FILE_PATH_UWA "dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "indexFileSEA.bin"
#else
#define FILE_TYPE FILE
#include "desktopFileInterface.h"
#define DATA_FILE_PATH_UWA "build/artifacts/dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "build/artifacts/indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "build/artifacts/dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "build/artifacts/indexFileSEA.bin"
#endif

#include "unity.h"

#define DEBUG

embedDBState* state;
embedDBSchema* baseSchema;

void setUp() {
    if (STORAGE_TYPE == 1) {
        TEST_FAIL_MESSAGE("Dataflash is not currently supported. Defaulting to SD card interface.");
    }
    state = (embedDBState*)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->numSplinePoints = 120;
    /* Setup files */
    char dataPath[] = DATA_FILE_PATH_UWA, indexPath[] = INDEX_FILE_PATH_UWA;
    state->fileInterface = getFileInterface();

    state->dataFile = state->fileInterface->setup(dataPath);
    state->indexFile = state->fileInterface->setup(indexPath);
#ifdef ARDUINO
    state->fileInterface->tempFilePath
#endif

        state->bufferSizeInBlocks = 4;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->buildBitmapFromRange = buildBitmapInt16FromRange;
    int8_t initResult = embedDBInit(state, 1);
    if (initResult != 0) {
        TEST_FAIL_MESSAGE("There was an error setting up the state of the UWA dataset.");
    }
    state->rules = NULL;
    state->numRules = 0;

    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    ColumnType colTypes[] = {embedDB_COLUMN_UINT32, embedDB_COLUMN_INT32, embedDB_COLUMN_INT32, embedDB_COLUMN_INT32};
    baseSchema = embedDBCreateSchema(4, colSizes, colSignedness, colTypes);
}

void tearDown() {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
    embedDBFreeSchema(&baseSchema);
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

    printf("\nInserted %d Records\n", numRecords);
}

void insertNValues(embedDBState* state, int n, int mode) {
    int key, value;

    switch (mode) {
        case 0:
            for (int i = 0; i <= n; i++) {
                key = i;
                value = i;
                embedDBPut(state, &key, &value);
            }
            break;
        case 1:
            key = 1;
            for (int i = n; i >= 0; i--) {
                value = i;
                embedDBPut(state, &key, &value);
                key++;
            }
            for (int i = 0, data = n; i <= n; i++) {
                key = i + 1;
                embedDBGet(state, (void*)&key, (void*)&value);
                TEST_ASSERT_MESSAGE(value == data, "value isn't equal to extracted data");
                data--;
            }
            break;
        default:
            break;
    }
}

void debugBinData(embedDBOperator* op, uint32_t numValues, uint8_t col) {
    op->init(op);
    int32_t* buffer = (int32_t*)op->recordBuffer;
    printf("\n");
    for (int i = 0; i <= numValues; ++i) {
        exec(op);
        printf("%i ", (int32_t)buffer[col]);
    }
    printf("\n");
    fflush(stdout);
}

void runTestSequentialValues() {
    // Insert test data
#ifdef ARDUINO
    insertNValues(state, 1, 0);
#else
    insertNValues(state, 67, 1);
#endif
 
    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(state, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(state, &it, baseSchema);
    uint8_t projColsOB[] = {0, 1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(state, projColsOrderBy, 1, -1, int32Comparator);
    //debugBinData(orderByOp, 67, 1);

    orderByOp->init(orderByOp);

    int32_t* recordBuffer = (int32_t*)orderByOp->recordBuffer;
    uint32_t previous = 0;
    int recordCount = 0;

    while (exec(orderByOp)) {
        TEST_ASSERT_GREATER_OR_EQUAL_INT32_MESSAGE(previous, ((int32_t)recordBuffer[1]), "Sort value is not greater than or equal to previous value.");
        previous = ((int32_t)recordBuffer[1]);
        recordCount++;
        printf("%d ", previous);
        fflush(stdout);
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);
}

void runTestUsingSEA100k() {
    // Insert data
    const char datafileName[] = "data/sea100K.bin";
    insertData(state, datafileName);

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(state, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(state, &it, baseSchema);
    //debugBinData(scanOpOrderBy, 20, 1);
    uint8_t projColsOB[] = {0, 1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    //debugBinData(projColsOrderBy, 100000, 1);
    embedDBOperator* orderByOp = createOrderByOperator(state, projColsOrderBy, 1, -1, int32Comparator);
    orderByOp->init(orderByOp);
    //debugBinData(orderByOp, 100000, 1);
    int32_t* recordBuffer = (int32_t*)orderByOp->recordBuffer;
    uint32_t previous = 0;
    // Result of the sort
    uint32_t count = 1;
    while (exec(orderByOp)) {
        TEST_ASSERT_GREATER_OR_EQUAL_INT32_MESSAGE(previous, ((int32_t)recordBuffer[1]) / 10.0, "Sort value is not greater than or equal to previous value previous values.");
        previous = ((int32_t)recordBuffer[1]) / 10.0;
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(runTestSequentialValues);
    RUN_TEST(runTestUsingSEA100k);
    return UNITY_END();
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