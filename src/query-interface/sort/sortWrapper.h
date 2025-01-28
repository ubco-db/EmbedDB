
#ifndef SORT_WRAPPER_H
#define SORT_WRAPPER_H

#include "../schema.h"
#include "../advancedQueries.h"
#include "external_sort.h"
#include "in_memory_sort.h"
#include "flash_minsort.h"
#include "adaptive_sort.h"

#include <string.h>
#include <desktopFileInterface.h>

#define SORT_DATA_LOCATION "sort_data.bin"
#define SORT_ORDER_LOCATION "sort_order.bin"

typedef struct embedDBOperator embedDBOperator;
typedef struct sortData sortData;

/**
 * @brief Initalizes default metric values
 * 
 * @return metrics_t 
 */
metrics_t initMetric();

/**
 * @brief               Writes row data from the input operator to a file
 * 
 * @param data         The operator data
 * @param op            The previous operator
 * @param unsortedFile  A prexisting file that the row data will be writen to
 * @param recordSize    The size of the data
 * @param keySize       The size of the key
 * @param keyOffset     The offset of the key with in the record (# of bytes)
 * @return uint32_t     The total number of records written or 0 if an error occurs
 *                      
 */
uint32_t loadRowData(sortData *data, embedDBOperator *op, void *unsortedFile);

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
 * @brief Adds header information and writes buffer to file 
 * 
 * @param buffer            The buffer that is written. Should be atleast the size of pageSize
 * @param blockIndex        The block index
 * @param numberOfValues    The the number of database rows stored in the page
 * @param pageSize          The size of the page
 * @param fileInterface     The interface used to write the file
 * @param file              The file being written to
 * @return int8_t 
 */
int8_t writePageWithHeader(void *buffer, int32_t blockIndex, int16_t numberOfValues, int16_t pageSize, embedDBFileInterface *fileInterface, void *file);

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
uint8_t readNextRecord(sortData *state, void *buffer);

void closeSort(file_iterator_state_t *iteratorState);

typedef struct {
    uint32_t key;
    void *value;
} rowData;

#endif