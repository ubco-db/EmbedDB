
#ifndef SORT_WRAPPER_H
#define SORT_WRAPPER_H

#include <string.h>

#include "../advancedQueries.h"
#include "../schema.h"
#include "adaptive_sort.h"
#include "external_sort.h"
#include "flash_minsort.h"
#include "in_memory_sort.h"

typedef struct embedDBOperator embedDBOperator;

typedef struct sortData {
    uint32_t count;
    uint16_t recordSize;
    int8_t colNum;
    int8_t keyOffset;
    int8_t keySize;
    int8_t (*compareFn)(void *a, void *b);
    int32_t tupleLimit;

    void *readBuffer;
    embedDBFileInterface *fileInterface;
    file_iterator_state_t *fileIterator;
} sortData;

/**
 * @brief Initializes default metric values
 *
 * @return metrics_t
 */
metrics_t initMetric();

/**
 * @brief               Writes row data from the input operator to a file
 *
 * @param data         The operator data
 * @param op            The previous operator
 * @param unsortedFile  A preexisting file that the row data will be written to
 * @param recordSize    The size of the data
 * @param keySize       The size of the key
 * @param keyOffset     The offset of the key with in the record (# of bytes)
 * @return uint32_t     The total number of records written or 0 if an error occurs
 *
 */
uint32_t loadRowData(sortData *data, embedDBOperator *op, void *unsortedFile);

/**
 * @brief Pure in-memory sort that avoids file I/O completely for very small datasets
 * @param data Sort configuration data
 * @param op The operator to read data from
 * @return file_iterator_state_t* Iterator for reading sorted results from memory
 */
file_iterator_state_t *startPureMemorySort(sortData *data, embedDBOperator *op);

/**
 * @brief The data given in the unsortedFile is sorted and stored in the sortedFile
 *
 * @param fileInterface             The file interface
 * @param unsortedFile              The file that is loaded with row data
 * @param sortedFile                An empty file
 * @param recordSize                The size of the records
 * @param count                     The total number of records stored in unsortedFile
 * @return file_iterator_state_t*   An iterator that is used to retrieve the sorted records
 */
file_iterator_state_t *startSort(sortData *data, void *unsortedFile, void *sortedFile);

/**
 * @brief Begins the sorting operation using row data from previous operator
 *
 * @param op The previous operator that will feed row data
 */
void prepareSort(embedDBOperator *op);

/**
 * @brief Reads the next record from the sorted file
 *
 * @param data     The ORDER BY operator data
 * @param buffer    A buffer that is the size of one record
 * @return uint8_t  0: if read was successful. other wise none zero
 */
uint8_t readNextRecord(void *state, void *buffer);

void closeSort(file_iterator_state_t *iteratorState);

typedef struct {
    uint32_t key;
    void *value;
} rowData;

#endif