#include <math.h>
#include <string.h>

#include "../src/embedDB/embedDB.h"
#include "embedDBUtility.h"

#if defined(MEMBOARD)
#include "memboardTestSetup.h"
#endif

#if defined(MEGA)
#include "megaTestSetup.h"
#endif

#if defined(DUE)
#include "dueTestSetup.h"
#endif

#include "SDFileInterface.h"
#include "unity.h"

int insert_static_record(embedDBState* state, uint32_t key, uint32_t data);
embedDBState* init_state();

embedDBState* state;

void setUp(void) {
    state = init_state();
}

void tearDown(void) {
    free(state->buffer);
    embedDBClose(state);
    tearDownSDFile(state->dataFile);
    free(state->fileInterface);
    free(state);
    state = NULL;
}

// test saving data to buffer, flush, and then retrieval.
void test_single_insert_one_retrieval_flush(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // flush to file storage
    embedDBFlush(state);
    // query data
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL(123, return_data[0]);
    // test
    TEST_ASSERT_EQUAL(123, *return_data);
}

void test_multiple_insert_one_retrieval_flush(void) {
    int numInserts = 100;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }
    embedDBFlush(state);
    uint32_t key = 93;
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL(193, *return_data);
}

// test saving data to buffer and retrieves it
void test_single_insert_one_retrieval_no_flush(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // query data
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test
    TEST_ASSERT_EQUAL(123, *return_data);
}

// insert 5 records into database and retrieve one
void test_multiple_insert_one_retrieval_no_flush(void) {
    int numInserts = 5;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }

    uint32_t key = 3;
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL(103, *return_data);
}

void test_insert_page_query_buffer(void) {
    int numInserts = 32;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }
    uint32_t key = 31;
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL(131, *return_data);
}

// test insert key, flush, and insert again for retrieval
void test_insert_flush_insert_buffer(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 154);
    // query data
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test record is in buffer
    TEST_ASSERT_EQUAL(154, *return_data);
    // flush
    embedDBFlush(state);
    // insert another record
    key = 2;
    insert_static_record(state, key, 12345);
    embedDBGet(state, &key, return_data);
    // test second record is retrieved from buffer
    TEST_ASSERT_EQUAL(12345, *return_data);
    // check if first record is retrieved from file storage
    key = 1;
    embedDBGet(state, &key, return_data);
    TEST_ASSERT_EQUAL(154, *return_data);
}

// test checks to see if queried key > maxKey in buffer
void test_above_max_query(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // insert random records
    int numInserts = 8;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }
    // query for max key not in database
    int key = 55;
    int return_data[] = {0, 0, 0};
    // test if embedDBGet can't retrieve data
    TEST_ASSERT_EQUAL(-1, embedDBGet(state, &key, return_data));
}

// test checks to see if queried key is >= the minKey in the buffer.
void test_flush_before_insert(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // query data
    int return_data[] = {0, 0, 0};
    embedDBGet(state, &key, return_data);
    // test
    TEST_ASSERT_EQUAL(123, *return_data);
}

// test checks retrievel if there is no data and nothing in the buffer
void test_no_data(void) {
    // create a key
    uint32_t key = 1;
    // allocate dataSize record in heap
    void* temp = calloc(1, state->dataSize);
    // query embedDB and returun pointer
    int8_t status = embedDBGet(state, &key, (void*)temp);
    // test
    TEST_ASSERT_EQUAL(-1, status);
    free(temp);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(test_single_insert_one_retrieval_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_flush);
    RUN_TEST(test_insert_page_query_buffer);
    RUN_TEST(test_single_insert_one_retrieval_no_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_no_flush);
    RUN_TEST(test_insert_flush_insert_buffer);
    RUN_TEST(test_above_max_query);
    RUN_TEST(test_flush_before_insert);
    RUN_TEST(test_no_data);
    return UNITY_END();
}

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

/* function puts a static record into buffer without flushing. Creates and frees record allocation in the heap.*/
int insert_static_record(embedDBState* state, uint32_t key, uint32_t data) {
    // calloc dataSize bytes in heap.
    void* dataPtr = calloc(1, state->dataSize);
    // set dataPtr[0] to data
    ((uint32_t*)dataPtr)[0] = data;
    // insert into buffer, save result
    char result = embedDBPut(state, (void*)&key, (void*)dataPtr);
    // free dataPtr
    free(dataPtr);
    // return based on success
    return (result == 0) ? 0 : -1;
}

embedDBState* init_state() {
    embedDBState* state = (embedDBState*)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }
    // configure state variables
    state->recordSize = 16;  // size of record in bytes
    state->keySize = 4;      // size of key in bytes
    state->dataSize = 12;    // size of data in bytes
    state->pageSize = 512;   // page size (I am sure this is in bytes)
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;  // size of the buffer in blocks (where I am assuming that a block is the same as a page size)
    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // check
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }
    // address level parameters
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->eraseSizeInPages = 4;
    // configure file interface
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->fileInterface = getSDInterface();
    state->dataFile = setupSDFile(dataPath);
    state->indexFile = setupSDFile(indexPath);
    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    // init
    size_t splineMaxError = 1;

    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "EmbedDB did not initialize correctly.");

    return state;
}