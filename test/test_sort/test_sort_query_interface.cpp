#include "unity.h"
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
#include "query-interface/advancedQueries.h"

#define STORAGE_TYPE 0

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
#define DATA_FILE_PATH_UWA "dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "indexFileSEA.bin"

#else

#include "desktopFileInterface.h"
#define FILE_TYPE FILE
#define DATA_FILE_PATH_UWA "build/artifacts/dataFileUWA.bin"
#define INDEX_FILE_PATH_UWA "build/artifacts/indexFileUWA.bin"
#define DATA_FILE_PATH_SEA "build/artifacts/dataFileSEA.bin"
#define INDEX_FILE_PATH_SEA "build/artifacts/indexFileSEA.bin"

#endif

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
    int key, value, min, max;

    switch (mode) {
        case 0:
            for (int i = 0; i<=n; i++) {
                key = i;
                value = i;
                embedDBPut(state, &key, &value);
            }
            break;
        case 1:
            for (int i = n; i>=0; i--) {
                key = i;
                value = i;
                embedDBPut(state, &key, &value);
            }
            break;
        default:
            break;
    }
}
    

void runTestSequentialValues() {

    if (STORAGE_TYPE == 1) {
        TEST_FAIL_MESSAGE("Dataflash is not currently supported. Defaulting to SD card interface.");
    }

    embedDBState* stateUWA = (embedDBState*)malloc(sizeof(embedDBState));
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
    stateUWA->dataFile = setupFile(dataPath);
    stateUWA->indexFile = setupFile(indexPath);

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

    int8_t colSizes[] = {4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_UNSIGNED};
    embedDBSchema* baseSchema = embedDBCreateSchema(2, colSizes, colSignedness);

    // Insert test data
    insertNValues(stateUWA,10, 0);

    embedDBIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    embedDBInitIterator(stateUWA, &it);

    embedDBOperator* scanOpOrderBy = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projColsOB[] = {0,1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, projColsOrderBy, 1, merge_sort_int32_comparator);
    orderByOp->init(orderByOp);
    int32_t* recordBuffer = (int32_t*)orderByOp->recordBuffer;
    uint32_t previous = 0;

    while (exec(orderByOp)) {
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(previous, ((uint32_t)recordBuffer[1]) / 10.0, "Sort value is not greater than or equal to previous value previous values.");
        previous = ((uint32_t)recordBuffer[1]) / 10.0;
    }

    orderByOp->close(orderByOp);
    embedDBFreeOperatorRecursive(&orderByOp);

    // Close embedDB
    embedDBClose(stateUWA);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);
    embedDBFreeSchema(&baseSchema);

}

void runTestUsingUWA500k() {
    printf("Advanced Query Example.\n");
    embedDBState* stateUWA = (embedDBState*)malloc(sizeof(embedDBState));
    stateUWA->keySize = 4;
    stateUWA->dataSize = 12;
    stateUWA->compareKey = int32Comparator;
    stateUWA->compareData = int32Comparator;
    stateUWA->pageSize = 512;
    stateUWA->eraseSizeInPages = 4;
    stateUWA->numDataPages = 20000;
    stateUWA->numIndexPages = 1000;
    stateUWA->numSplinePoints = 30;

    if (STORAGE_TYPE == 1) {
        TEST_FAIL_MESSAGE("Dataflash is not currently supported. Defaulting to SD card interface.");
    }

    /* Setup files */
    char dataPath[] = DATA_FILE_PATH_UWA, indexPath[] = INDEX_FILE_PATH_UWA;
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
    int8_t initResult = embedDBInit(stateUWA, 1);
    if (initResult != 0) {
        TEST_FAIL_MESSAGE("There was an error setting up the state of the UWA dataset.");
    }

    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

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
    uint8_t projColsOB[] = {0,1};
    embedDBOperator* projColsOrderBy = createProjectionOperator(scanOpOrderBy, 2, projColsOB);
    embedDBOperator* orderByOp = createOrderByOperator(stateUWA, projColsOrderBy, 1, merge_sort_int32_comparator);
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

    // Close embedDB
    embedDBClose(stateUWA);
    tearDownFile(stateUWA->dataFile);
    tearDownFile(stateUWA->indexFile);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);
    embedDBFreeSchema(&baseSchema);
}


int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(runTestSequentialValues);
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