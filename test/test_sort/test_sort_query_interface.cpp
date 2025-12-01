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

embedDBState* stateUWA;
embedDBSchema* baseSchema;

void setUp() {
    if (STORAGE_TYPE == 1) {
        TEST_FAIL_MESSAGE("Dataflash is not currently supported. Defaulting to SD card interface.");
    }
    stateUWA = (embedDBState*)malloc(sizeof(embedDBState));
    stateUWA->keySize = 4;
    stateUWA->dataSize = 12;
    stateUWA->compareKey = int32Comparator;
    stateUWA->compareData = int32Comparator;
    stateUWA->pageSize = 512;
    stateUWA->eraseSizeInPages = 4;
    stateUWA->numDataPages = 20000;
    stateUWA->numIndexPages = 1000;
    stateUWA->numSplinePoints = 30;
    /* Setup files */
    char dataPath[] = DATA_FILE_PATH_UWA, indexPath[] = INDEX_FILE_PATH_UWA;
    stateUWA->fileInterface = getFileInterface();

    stateUWA->dataFile = stateUWA->fileInterface->setup(dataPath);
    stateUWA->indexFile = stateUWA->fileInterface->setup(indexPath);
#ifdef ARDUINO
    stateUWA->fileInterface->tempFilePath
#endif

        stateUWA->bufferSizeInBlocks = 4;
    stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
    stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    stateUWA->bitmapSize = 2;
    stateUWA->inBitmap = inBitmapInt16;
    stateUWA->updateBitmap = updateBitmapInt16;
    stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
    int8_t initResult = embedDBInit(stateUWA, 1);
    if (initResult != 0) {
        TEST_FAIL_MESSAGE("There was an error setting up the state of the UWA dataset.");
    }
    stateUWA->rules = NULL;
    stateUWA->numRules = 0;

    int8_t colSizes[] = {4, 12};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_UNSIGNED};
    ColumnType colTypes[] = {embedDB_COLUMN_UINT32, embedDB_COLUMN_UINT32};
    baseSchema = embedDBCreateSchema(2, colSizes, colSignedness, colTypes);
}

void tearDown() {
    embedDBClose(stateUWA);
    tearDownFile(stateUWA->dataFile);
    tearDownFile(stateUWA->indexFile);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);
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
            for (int i = 0, data = 10; i <= n; i++) {
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

void runTestSequentialValues() {
    // Insert test data
#ifdef ARDUINO
    insertNValues(stateUWA, 1, 0);
#else
    insertNValues(stateUWA, 10, 1);
#endif

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projColsOB[] = {0, 1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, projColsOrderBy, 1, -1, int32Comparator);

    orderByOp->init(orderByOp);

    int32_t* recordBuffer = (int32_t*)orderByOp->recordBuffer;
    uint32_t previous = 0;
    int recordCount = 0;

    while (exec(orderByOp)) {
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(previous, ((uint32_t)recordBuffer[1]), "Sort value is not greater than or equal to previous value.");
        previous = ((uint32_t)recordBuffer[1]);
        recordCount++;

        // Safety break to prevent infinite loop
        if (recordCount >= 10) {
            break;
        }
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);
}

void runTestUsingUWA500k() {
    // Insert data
    const char datafileName[] = "data/uwa500K.bin";
    insertData(stateUWA, datafileName);

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projColsOB[] = {0, 3};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, projColsOrderBy, 3, -1, int32Comparator);
    orderByOp->init(orderByOp);
    int32_t* recordBuffer = (int32_t*)orderByOp->recordBuffer;
    uint32_t previous = 0;
    // Result of the sort

    while (exec(orderByOp)) {
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(previous, ((uint32_t)recordBuffer[1]) / 10.0, "Sort value is not greater than or equal to previous value previous values.");
        previous = ((uint32_t)recordBuffer[1]) / 10.0;
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(runTestSequentialValues);
    RUN_TEST(runTestUsingUWA500k);
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