#include "sortWrapper.h"

#include "query-interface/sort/in_memory_sort.h"
#ifndef ARDUINO
#include "unistd.h"
#endif

// #define PRINT_METRIC
// #define DEBUG
// #define PRINT_ERRORS
#if defined(DEBUG) || defined(PRINT_METRIC) || defined(PRINT_ERRORS)
#include "debug_print.h"
#else
#ifndef debug_log
#define debug_log(...) ((void)0)
#endif

#endif

/**
 * @brief Pure in-memory sort that avoids file I/O completely for very small datasets
 * @param data Sort configuration data
 * @param op The operator to read data from
 * @return file_iterator_state_t* Iterator for reading sorted results from memory
 */
file_iterator_state_t *startPureMemorySort(sortData *data, embedDBOperator *op) {
#ifdef DEBUG
    debug_log("DEBUG: Starting pure in-memory sort\n");
#endif
    int record_count = 0;
    while (exec(op->input)) {
        record_count++;
        if (record_count > 10) {  // Safety limit
#ifdef PRINT_ERRORS
            debug_log("ERROR: Too many records for pure in-memory sort\n");
#endif
            return NULL;
        }
    }

#ifdef DEBUG
    debug_log("DEBUG: Found %d records for pure in-memory sort\n", record_count);
#endif

    if (record_count == 0) {
#ifdef DEBUG
        debug_log("DEBUG: No records to sort\n");
#endif
        file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
        if (iteratorState == NULL) {
            return NULL;
        }
        iteratorState->file = NULL;
        iteratorState->fileInterface = data->fileInterface;
        iteratorState->currentRecord = 0;
        iteratorState->recordsRead = 0;
        iteratorState->recordsLeftInBlock = 0;
        iteratorState->recordSize = data->recordSize;
        iteratorState->totalRecords = 0;
        iteratorState->resultFile = 0;
        return iteratorState;
    }

    void *buffer = malloc(record_count * data->recordSize);
    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: Failed to allocate memory for pure in-memory sort\n");
#endif
        return NULL;
    }

    op->input->close(op->input);
    op->input->init(op->input);

    int records_read = 0;
    while (exec(op->input) && records_read < record_count) {
        memcpy((char *)buffer + records_read * data->recordSize,
               op->input->recordBuffer,
               data->recordSize);
        records_read++;
    }

#ifdef DEBUG
    debug_log("DEBUG: Read %d records into memory buffer\n", records_read);
#endif

    // Sort the records in memory using quicksort
    metrics_t metrics = {0};
    int sort_result = in_memory_quick_sort(buffer, records_read, data->recordSize, data->keyOffset, data->compareFn, &metrics);

    if (sort_result != 0) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: In-memory sort failed\n");
#endif
        free(buffer);
        return NULL;
    }

#ifdef DEBUG
    debug_log("DEBUG: Pure in-memory sort completed successfully\n");
#endif

    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: Failed to allocate iterator state\n");
#endif
        free(buffer);
        return NULL;
    }

    iteratorState->file = buffer;
    iteratorState->fileInterface = data->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsRead = 0;
    iteratorState->recordsLeftInBlock = 0;
    iteratorState->recordSize = data->recordSize;
    iteratorState->totalRecords = records_read;
    iteratorState->resultFile = 0;

    return iteratorState;
}

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
int8_t writePageWithHeader(void *buffer, const uint32_t blockIndex, const uint16_t numberOfValues, const uint32_t pageSize, const embedDBFileInterface *fileInterface, void *file) {
    memcpy(buffer, &blockIndex, sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), &numberOfValues, sizeof(uint16_t));

    fileInterface->write(buffer, blockIndex, pageSize, file);
    if (fileInterface->error(file)) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: SORT: Failed to write unsorted data");
#endif
        return 1;
    }

    return 0;
}

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
uint32_t loadRowData(sortData *data, embedDBOperator *op, void *unsortedFile) {
    uint32_t count = 0;
    uint32_t blockIndex = 0;
    uint16_t valuesPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / data->recordSize;

#ifdef DEBUG
    debug_log("DEBUG loadRowData: PAGE_SIZE=%d, BLOCK_HEADER_SIZE=%d, recordSize=%d, valuesPerPage=%d\n",
              PAGE_SIZE, BLOCK_HEADER_SIZE, data->recordSize, valuesPerPage);
#endif

    void *buffer = malloc(PAGE_SIZE);

    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: SORT: buffer malloc failed");
#endif
        return 1;
    }

    // Write row data to file
    while (exec(op->input)) {
        // Write page to file when full
        if (count % valuesPerPage == 0 && count != 0) {
            if (writePageWithHeader(buffer, blockIndex, valuesPerPage, PAGE_SIZE, data->fileInterface, unsortedFile)) {
                free(buffer);
                buffer = NULL;
                return 0;
            }
            blockIndex++;
        }

        // Offset of the data in the page
        uint32_t rowOffset = (count % valuesPerPage) * data->recordSize + BLOCK_HEADER_SIZE;

        if (rowOffset + data->recordSize > PAGE_SIZE) {
#ifdef PRINT_ERRORS
            debug_log("ERROR: SORT: error calculating row offset");
#endif
            free(buffer);
            buffer = NULL;
            return 0;
        }

        // Write data to buffer
        memcpy((uint8_t *)buffer + rowOffset, op->input->recordBuffer, data->recordSize);
#ifdef DEBUG
        if (count < 100) {
            debug_log("DEBUG loadRowData record %d: ", count);
            for (int i = 0; i < data->recordSize; i++) {
                debug_log("%02x ", ((uint8_t *)op->input->recordBuffer)[i]);
            }
            debug_log("\n");

            // Also show what we wrote to the buffer
            debug_log("DEBUG wrote to buffer at offset %d: ", rowOffset);
            for (int i = 0; i < data->recordSize; i++) {
                debug_log("%02x ", ((uint8_t *)buffer)[rowOffset + i]);
            }
            debug_log("\n");
        }
        if (count < 100 || count % 1000 == 0) {
            int32_t *keyPtr = (int32_t *)(op->input->recordBuffer + data->keyOffset);
            debug_log("DEBUG loadRowData: count=%d, rowOffset=%d, key=%d\n", count, rowOffset, *keyPtr);
        }
#endif

        count++;

        // temp limit for debugging
        if (data->tupleLimit != -1 && count >= data->tupleLimit)
            break;
    }

    // Write remaining records
    uint32_t numRemainingRecords = (count % valuesPerPage == 0 && count != 0) ? valuesPerPage : count % valuesPerPage;
    if (writePageWithHeader(buffer, blockIndex, numRemainingRecords, PAGE_SIZE, data->fileInterface, unsortedFile)) {
        free(buffer);
        buffer = NULL;
        return 0;
    }
    data->fileInterface->flush(unsortedFile);

#ifdef DEBUG
    debug_log("DEBUG loadRowData: finished, totalRecords=%d\n", count);
#endif

    // Clean up
    free(buffer);
    buffer = NULL;

    return count;
}

/**
 * @brief Begins the sorting operation using row data from previous operator
 *
 * @param op The previous operator that will feed row data
 */
void prepareSort(embedDBOperator *op) {
    sortData *data = op->state;
    data->keyOffset = getColOffsetFromSchema(op->schema, data->colNum);
    data->recordSize = getRecordSizeFromSchema(op->schema);
    data->keySize = op->schema->columnSizes[data->colNum];
#ifdef DEBUG
    debug_log("DEBUG prepareSort: recordSize=%d, keySize=%d, keyOffset=%d, colNum=%d\n",
              data->recordSize, data->keySize, data->keyOffset, data->colNum);
    debug_log("DEBUG prepareSort: schema has %d columns\n", op->schema->numCols);
    for (int i = 0; i < op->schema->numCols; i++) {
        debug_log("  Column %d: size=%d\n", i, op->schema->columnSizes[i]);
    }
#endif

    // A columns size will be negative if the column is signed
    // and positive if value is unsigned
    if (data->keySize < 0) {
        data->keySize = -1 * data->keySize;
    }
    if (data->fileInterface == NULL || data->fileInterface->setup == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: File interface or setup function not provided while initializing ORDER BY operator\n");
#endif
        return;
    }

    char *tmp1 = data->fileInterface->tempFilePath();
    char *tmp2 = data->fileInterface->tempFilePath();

    void *unsortedFile = data->fileInterface->setup(tmp1);
    void *sortedFile = data->fileInterface->setup(tmp2);
    free(tmp1);
    free(tmp2);

    if (unsortedFile == NULL || sortedFile == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: Failed to allocate file handles while initializing ORDER BY operator\n");
#endif
        return;
    }
    const uint8_t unsortedOpen = data->fileInterface->open(unsortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    const uint8_t sortedOpen = data->fileInterface->open(sortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);

    if (!unsortedOpen || !sortedOpen) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: Failed to open files while initializing ORDER BY operator");
#endif
        return;
    }

    // Load row data
    data->count = loadRowData(data, op, unsortedFile);
    debug_log("finished load row data, starting sort\n");
    // Start sorting
    file_iterator_state_t *iteratorState = startSort(data, unsortedFile, sortedFile);
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: Sort failed");
#endif
        return;
    }

    // Finish
    iteratorState->file = sortedFile;
    data->fileInterface->close(unsortedFile);
    if (data->fileInterface->removeFile) {
        data->fileInterface->removeFile(unsortedFile);
    }
    data->fileIterator = iteratorState;
}

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
file_iterator_state_t *startSort(sortData *data, void *unsortedFile, void *sortedFile) {
    // Initialize external_sort_t structure
    external_sort_t es;
    es.key_size = data->keySize;
    es.value_size = data->recordSize;
    es.record_size = data->recordSize;
    es.key_offset = data->keyOffset;
    es.headerSize = BLOCK_HEADER_SIZE;
    es.page_size = PAGE_SIZE;
    es.num_pages = (uint32_t)ceil((float)data->count / ((es.page_size - es.headerSize) / es.record_size));

    const int buffer_max_pages = 4;

    char *buffer = malloc(buffer_max_pages * es.page_size + es.record_size);
    char *tuple_buffer = buffer + es.page_size * buffer_max_pages;

    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        debug_log("ERROR: SORT: buffer malloc failed m\n");
#endif
        return NULL;
    }

    // Prepare the file iterator data for sorting
    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        debug_log("Error: SORT: iterator malloc failed\n");
#endif
        free(buffer);
        buffer = NULL;
        return NULL;
    }

    iteratorState->file = unsortedFile;
    iteratorState->recordsRead = 0;
    iteratorState->totalRecords = data->count;
    iteratorState->recordSize = es.record_size;
    iteratorState->fileInterface = data->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsLeftInBlock = 0;
    iteratorState->resultFile = 0;

    data->fileIterator = iteratorState;

    // Metrics
    metrics_t metrics = initMetric();

    long result_file_ptr = 0;

    int err;

    // Use adaptive sort on desktop
    int8_t runGenOnly = false;   // Run full sort operation
    int8_t writeReadRatio = 19;  // 1.97 * 10 => 19
    err = adaptive_sort(readNextRecord, iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages, &es, &result_file_ptr, &metrics, data->compareFn, runGenOnly, writeReadRatio, data);

#ifdef PRINT_METRIC
    debug_log("\tComplete. Comparisons: %d  Writes: %d  Reads: %d Memcpys: %d\n", metrics.num_compar, metrics.num_writes, metrics.num_reads, metrics.num_memcpys);
#endif

    iteratorState->resultFile = result_file_ptr;

#ifdef PRINT_ERRORS
    if (8 == err) {
        debug_log("Out of memory!\n");
    } else if (10 == err) {
        debug_log("File Read Error!\n");
    } else if (9 == err) {
        debug_log("File Write Error!\n");
    }
#endif

    // Reset file iterator
    iteratorState->recordsRead = 0;
    iteratorState->currentRecord = 0;

    // Clean up
    free(buffer);
    buffer = NULL;
    return iteratorState;
}

/**
 * @brief Reads the next record from the sorted file
 *
 * @param data     The ORDER BY operator data
 * @param buffer    A buffer that is the size of one record
 * @return uint8_t  0: if read was successful. other wise none zero
 */
uint8_t readNextRecord(void *data, void *buffer) {
    file_iterator_state_t *iteratorState = ((sortData *)data)->fileIterator;

    if (iteratorState->recordsRead >= iteratorState->totalRecords) {
        return 1;  // No more records left to read
    }

    uint32_t recordPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / iteratorState->recordSize;

    // Read next page if current buffer is empty
    if (iteratorState->currentRecord % recordPerPage == 0 || iteratorState->recordsRead == 0) {
        uint32_t seekOffset = iteratorState->resultFile + (iteratorState->currentRecord / recordPerPage) * PAGE_SIZE;

        iteratorState->fileInterface->seek(seekOffset, iteratorState->file);
        iteratorState->fileInterface->readRel(((sortData *)data)->readBuffer, PAGE_SIZE, 1, iteratorState->file);

#ifdef DEBUG
        if (iteratorState->recordsRead == 0 || iteratorState->recordsRead % 1000 == 0) {
            debug_log("DEBUG readNextRecord: pageNum=%d, seekOffset=%d, recordsRead=%d\n",
                      iteratorState->currentRecord / recordPerPage, seekOffset, iteratorState->recordsRead);
        }
#endif

        if (((sortData *)data)->fileInterface->error(iteratorState->file)) {
#ifdef PRINT_ERRORS
            debug_log("ERROR: SORT: next record read failed");
#endif
            return 2;
        }
    }

    // Copy result to output buffer
    uint16_t valuesInPage;
    memcpy(&valuesInPage, ((sortData *)data)->readBuffer + sizeof(uint32_t),
           sizeof(uint16_t));
    uint32_t recordIndexInPage = iteratorState->currentRecord % recordPerPage;
#ifdef DEBUG

#endif

    if (recordIndexInPage >= valuesInPage) {
        return 1;
    }
    uint32_t copyOffset = BLOCK_HEADER_SIZE + iteratorState->recordSize * recordIndexInPage;
    memcpy(buffer, ((sortData *)data)->readBuffer + copyOffset, iteratorState->recordSize);

#ifdef DEBUG
    if (iteratorState->recordsRead < 10 || iteratorState->recordsRead % 1000 == 0) {
        int32_t *keyPtr = (int32_t *)(buffer + ((sortData *)data)->keyOffset);
        debug_log("DEBUG readNextRecord: recordsRead=%d, currentRecord=%d, pageIdx=%d, recordInPage=%d, copyOffset=%d, key=%d\n",
                  iteratorState->recordsRead, iteratorState->currentRecord, iteratorState->currentRecord / recordPerPage,
                  recordIndexInPage, copyOffset, *keyPtr);
        uint32_t blockIdx;
        memcpy(&blockIdx, ((sortData *)data)->readBuffer, sizeof(uint32_t));
        debug_log("READ PAGE hdr: blockIdx=%u values=%u\n",
                  blockIdx, valuesInPage);
        debug_log("PAGE HEADER: page=%d values=%d\n",
                  iteratorState->currentRecord / recordPerPage,
                  valuesInPage);
        int32_t *key0 = (int32_t *)(((sortData *)data)->readBuffer + BLOCK_HEADER_SIZE + ((sortData *)data)->keyOffset);
        int32_t *keyLast = (int32_t *)(((sortData *)data)->readBuffer + BLOCK_HEADER_SIZE + (recordPerPage - 1) * iteratorState->recordSize + ((sortData *)data)->keyOffset);
        debug_log("  First key on page: %d, Last key on page: %d\n", *key0, *keyLast);
    }
#endif

    iteratorState->recordsRead++;
    iteratorState->currentRecord++;

    // #ifdef DEBUG
    //         printf("DEBUG: ROWDATA from file:\n");
    //         for (int i = 0; i < iteratorState->recordSize - SORT_KEY_SIZE; i++) {
    //             printf("%2x ", ((uint8_t *)buffer)[i]);
    //         }
    //         printf("\n");
    // #endif
    return 0;
}

void closeSort(file_iterator_state_t *iteratorState) {
    if (iteratorState->file != NULL) {
        iteratorState->fileInterface->close(iteratorState->file);
        if (iteratorState->fileInterface->removeFile) {
            iteratorState->fileInterface->removeFile(iteratorState->file);
        }
        iteratorState->file = NULL;
    }
}

/**
 * @brief Initializes default metric values
 *
 * @return metrics_t
 */
metrics_t initMetric() {
    metrics_t metrics;
    metrics.num_reads = 0;
    metrics.num_compar = 0;
    metrics.num_memcpys = 0;
    metrics.num_runs = 0;
    metrics.num_writes = 0;
    return metrics;
}
