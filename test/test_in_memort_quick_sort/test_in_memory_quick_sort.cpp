#ifdef DIST
#include "in_memory_sort.h"
#else
#include "query-interface/sort/in_memory_sort.h"
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
#else
#include "desktopFileInterface.h"
#define DATA_FILE_PATH "build/artifacts/dataFile.bin"
#endif

#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

void test_single_element() {
    int arr[] = {42};
    size_t num_elements = 1;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int), 0, merge_sort_int32_comparator, &metrics);

    TEST_ASSERT_EQUAL_INT(42, arr[0]);
}

void test_single_element_offset() {
    int arr[] = {0, 42};
    size_t num_elements = 1;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int) * 2, 4, merge_sort_int32_comparator, &metrics);

    TEST_ASSERT_EQUAL_INT(0, arr[0]);
    TEST_ASSERT_EQUAL_INT(42, arr[1]);
}

void test_sorted_array() {
    int arr[] = {1, 2, 3, 4, 5};
    size_t num_elements = 5;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int), 0, merge_sort_int32_comparator, &metrics);

    for (size_t i = 0; i < num_elements; i++) {
        TEST_ASSERT_EQUAL_INT(i + 1, arr[i]);
    }
}

void test_sorted_array_offset() {
    int arr[] = {0, 31, 0, 42};
    size_t num_elements = 1;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int) * 2, 4, merge_sort_int32_comparator, &metrics);

    TEST_ASSERT_EQUAL_INT(0, arr[0]);
    TEST_ASSERT_EQUAL_INT(31, arr[1]);
    TEST_ASSERT_EQUAL_INT(0, arr[2]);
    TEST_ASSERT_EQUAL_INT(42, arr[3]);
}

void test_unsorted_array() {
    int arr[] = {5, 3, 4, 1, 2};
    size_t num_elements = 5;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int), 0, merge_sort_int32_comparator, &metrics);

    for (size_t i = 0; i < num_elements; i++) {
        TEST_ASSERT_EQUAL_INT(i + 1, arr[i]);
    }
}

void test_unsorted_array_offset() {
    int arr[] = {0, 5, 0, 3, 0, 4, 0, 1, 0, 2};
    size_t num_elements = 5;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int) * 2, 4, merge_sort_int32_comparator, &metrics);

    // After sorting, the array should be [0, 1, 0, 2, 0, 3, 0, 4, 0, 5]
    for (size_t i = 0; i < num_elements; i++) {
        TEST_ASSERT_EQUAL_INT(0, arr[2 * i]);
        TEST_ASSERT_EQUAL_INT(i + 1, arr[2 * i + 1]);
    }
}

void test_array_with_duplicates() {
    int arr[] = {5, 3, 3, 1, 2, 2, 4};
    size_t num_elements = 7;
    metrics_t metrics = {0};
    in_memory_quick_sort(arr, num_elements, sizeof(int), 0, merge_sort_int32_comparator, &metrics);

    // After sorting, the array should be [1, 2, 2, 3, 3, 4, 5]
    TEST_ASSERT_EQUAL_INT(1, arr[0]);
    TEST_ASSERT_EQUAL_INT(2, arr[1]);
    TEST_ASSERT_EQUAL_INT(2, arr[2]);
    TEST_ASSERT_EQUAL_INT(3, arr[3]);
    TEST_ASSERT_EQUAL_INT(3, arr[4]);
    TEST_ASSERT_EQUAL_INT(4, arr[5]);
    TEST_ASSERT_EQUAL_INT(5, arr[6]);
}

int runUnityTests() {
    UNITY_BEGIN();
    RUN_TEST(test_single_element);
    RUN_TEST(test_single_element_offset);
    RUN_TEST(test_sorted_array);
    RUN_TEST(test_sorted_array_offset);
    RUN_TEST(test_unsorted_array);
    RUN_TEST(test_unsorted_array_offset);
    RUN_TEST(test_array_with_duplicates);
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