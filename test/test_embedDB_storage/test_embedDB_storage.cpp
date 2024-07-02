#ifdef DIST
#include "embedDB.h"
#else
#include "embedDB/embedDB.h"
#include "embedDBUtility.h"
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

#ifdef ARDUINO
#include "SDFileInterface.h"
#define getFileInterface getSDInterface
#define setupFile setupSDFile
#define tearDownFile tearDownSDFile
#define DATA_FILE_PATH "dataFile.bin"
#define INDEX_FILE_PATH "indexFile.bin"
#define VAR_DATA_FILE_PATH "varFile.bin"
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#define INDEX_FILE_PATH "build/artifacts/indexFile.bin"
#define VAR_DATA_FILE_PATH "build/artifacts/varFile.bin"
#endif

#include "unity.h"

embedDBState *state;

void setUp() {
}

void tearDown() {
    free(state->buffer);
    free(state->fileInterface);
    free(state);
}

void embedDBInit_should_return_erorr_if_numDataPages_is_not_divisible_by_eraseSizeInPages() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    TEST_ASSERT_NOT_NULL_MESSAGE(state, "Unable to allocate embedDBState.");
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 8;
    state->numSplinePoints = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

    /* configure EmbedDB storage */
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(DATA_FILE_PATH);

    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;

    /* Check for off by one */
    state->numDataPages = 407;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated data pages are not divisible by the erase size in pages.");

    /* Check for off by one */
    state->numDataPages = 409;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated data pages are not divisible by the erase size in pages.");

    /* Check for successful init */
    state->numDataPages = 408;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBInit should have failed as the allocated data pages are not divisible by the erase size in pages.");

    /* Tear down successful init */
    embedDBClose(state);
    tearDownFile(state->dataFile);
}

void embedDBInit_should_return_erorr_if_numIndexPages_is_not_divisible_by_eraseSizeInPages() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate buffer for EmbedDB.");

    /* setup EmbedDB storage */
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(DATA_FILE_PATH);
    state->indexFile = setupFile(INDEX_FILE_PATH);

    state->numDataPages = 300;
    state->eraseSizeInPages = 3;
    state->bitmapSize = 1;
    state->parameters = EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Check for off by one */
    state->numIndexPages = 16;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated index pages are not divisible by the erase size in pages.");

    /* Check for off by one */
    state->numIndexPages = 14;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated index pages are not divisible by the erase size in pages.");

    /* Check for off by one */
    state->numIndexPages = 15;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBInit should have failed as the allocated index pages are not divisible by the erase size in pages.");

    /* Tear down successful init */
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
}

void embedDBInit_should_return_erorr_if_numVarPages_is_not_divisible_by_eraseSizeInPages() {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 16;
    state->numSplinePoints = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    TEST_ASSERT_NOT_NULL_MESSAGE(state->buffer, "Failed to allocate EmbedDB buffer.");

    state->fileInterface = getFileInterface();
    char dataPath[] = DATA_FILE_PATH, varPath[] = VAR_DATA_FILE_PATH;
    state->dataFile = setupFile(dataPath);
    state->varFile = setupFile(varPath);

    state->numDataPages = 64;
    state->eraseSizeInPages = 4;
    state->parameters = EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Check for off by one error */
    state->numVarPages = 33;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated variable data pages are not divisible by the erase size in pages.");

    /* Check for off by one error */
    state->numVarPages = 31;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(-1, result, "embedDBInit should have failed as the allocated variable data pages are not divisible by the erase size in pages.");

    /* Check correct init */
    state->numVarPages = 32;
    result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDBInit should have failed as the allocated variable data pages are not divisible by the erase size in pages.");

    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->varFile);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(embedDBInit_should_return_erorr_if_numDataPages_is_not_divisible_by_eraseSizeInPages);
    RUN_TEST(embedDBInit_should_return_erorr_if_numIndexPages_is_not_divisible_by_eraseSizeInPages);
    RUN_TEST(embedDBInit_should_return_erorr_if_numVarPages_is_not_divisible_by_eraseSizeInPages);
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
