#include "sortWrapper.h"

metrics_t initMetric();
uint32_t loadRowData(orderByInfo *state, embedDBOperator *op, void *unsortedFile, uint16_t recordSize, uint8_t keySize, uint8_t keyCol);
file_iterator_state_t *startSort(orderByInfo *state, void *unsortedFile, void *sortedFile, uint16_t recordSize, uint32_t count);

/**
 * @brief   
 * 
 * @param op 
 */
void initSort(embedDBOperator* op) {
    orderByInfo *state = op->state;
    
    // Set up files
    void *unsortedFile = setupFile(SORT_DATA_LOCATION);
    void *sortedFile = setupFile(SORT_ORDER_LOCATION);
    
    if (unsortedFile == NULL|| sortedFile == NULL) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    const uint8_t unsortedOpen = state->fileInterface->open(unsortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    const uint8_t sortedOpen = state->fileInterface->open(sortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    
    if (!unsortedOpen || !sortedOpen) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    uint32_t count;
    uint16_t colOffset = getColOffsetFromSchema(op->schema, ((struct orderByInfo *)op->state)->colNum);
    uint16_t recordSize = getRecordSizeFromSchema(op->schema);
    int8_t colSize = op->schema->columnSizes[((struct orderByInfo *)op->state)->colNum]; // TODO: check the sign of the column size

    if (colSize < 0) {
        colSize = -1 * colSize;
    }

    // Output rows to file
    count = loadRowData(state, op, unsortedFile, recordSize, colSize, colOffset);

    // Sort data
    file_iterator_state_t *iteratorState = startSort(state, unsortedFile, sortedFile, recordSize, count);

    if (iteratorState == NULL) {
        printf("ERROR: Failed to init iterator state");
    }

    state->fileIterator = iteratorState;

    state->fileIterator->file = sortedFile;
    state->colSize = colSize;
}


/**
 * @brief               Writes row data from the input operator to a file
 * 
 * @param state         The operator state
 * @param op            The previous operator
 * @param unsortedFile  A prexisting file that the row data will be writen to
 * @param recordSize    The size of the data
 * @param keySize       The size of the key
 * @param keyOffset     The offset of the key with in the record (# of bytes)
 * @return uint32_t     The total number of records written or 0 if an error occurs
 *                      
 */
uint32_t loadRowData(orderByInfo *state, embedDBOperator *op, void *unsortedFile, uint16_t recordSize, uint8_t keySize, uint8_t keyOffset) {
    uint32_t count = 0;
    int32_t blockIndex = 0;
    int16_t valuesPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / (recordSize + keySize);

    void *buffer = malloc(PAGE_SIZE);

    if (buffer == NULL) {
        printf("ERROR: SORT: buffer malloc failed");
        return 0;
    }

    // Write row data to file
    while (exec(op->input)) {
        // Write full page to file
        if (count % valuesPerPage == 0 && count != 0) {
            
            memcpy(buffer, &blockIndex, sizeof(int32_t));
            memcpy((char *)buffer + sizeof(uint32_t), &valuesPerPage, sizeof(int16_t));
            
            state->fileInterface->write(buffer, blockIndex, PAGE_SIZE, unsortedFile);
            
            if (state->fileInterface->error(unsortedFile)) {
                printf("ERROR: SORT: Failed to write unsorted data");
                free(buffer);
                return 0;
            }

            blockIndex++;
        }

        // Offset of the data in the page
        uint32_t rowOffset = (count % valuesPerPage * (recordSize + keySize)) + BLOCK_HEADER_SIZE;

        if (rowOffset + keySize + recordSize > PAGE_SIZE) {
            printf("ERROR: SORT: error calculating row offset");
            free(buffer);
            return 0;
        }

        // Write key and data to buffer
        memcpy((char *)buffer + rowOffset, op->input->recordBuffer + keyOffset, keySize);
        memcpy((char *)buffer + rowOffset + keySize, op->input->recordBuffer, recordSize);


        // if (count > 25) // temp limit for debugging
            // break;

        count++;
    }

    // Write remaining records
    memcpy(buffer, &blockIndex, sizeof(int32_t));
    memcpy((char *)buffer + sizeof(uint32_t), &count, sizeof(int16_t));
    state->fileInterface->write(buffer, blockIndex, BLOCK_HEADER_SIZE + count % valuesPerPage * (recordSize + keySize), unsortedFile);
    
    if (state->fileInterface->error(unsortedFile)) {
        printf("ERROR: SORT: Failed to write unsorted data");
        free(buffer);
        return 0;
    }

    state->fileInterface->flush(unsortedFile);

    // Clean up
    free(buffer);

    return count;
}


file_iterator_state_t *startSort(orderByInfo *state, void *unsortedFile, void *sortedFile, uint16_t recordSize, uint32_t count) {
    
    // Initialize external_sort_t structure
    external_sort_t es;
    es.key_size = sizeof(int32_t);
    es.value_size = recordSize;
    es.record_size = recordSize + es.key_size;
    es.headerSize = BLOCK_HEADER_SIZE;
    es.page_size = PAGE_SIZE;
    es.num_pages = (uint32_t)(es.record_size * count) / es.page_size;

    // Check for when record is exactly page size
    if (es.num_pages == 0 || es.num_pages % es.page_size != 0) {
        es.num_pages++;
    }

    const int buffer_max_pages = 2; // Assume a buffer size of 2 pages
    char *buffer = malloc(buffer_max_pages * es.page_size + es.record_size);
    char *tuple_buffer = buffer + es.page_size * buffer_max_pages;

    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: SORT: buffer malloc failed m\n");
#endif
        return NULL;
    }

    // Prepare the file iterator state for sorting
    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        printf("Error: SORT: iterator malloc failed\n");
#endif
        free(buffer);
        return NULL;
    }

    iteratorState->file = unsortedFile;
    iteratorState->recordsRead = 0;
    iteratorState->totalRecords = count; // Total records from the previous while loop
    iteratorState->recordSize = es.record_size;
    iteratorState->fileInterface = state->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsLeftInBlock = 0;

    // Metrics
    metrics_t metrics = initMetric();

    // Sort the data from unsortedFile and store it in sortedFile
    long result_file_ptr = 0;
    int err = flash_minsort(iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages * es.page_size, &es, &result_file_ptr, &metrics, merge_sort_int32_comparator);

#ifdef PRINT_ERRORS
    if (8 == err) {
        printf("Out of memory!\n");
    } else if (10 == err) {
        printf("File Read Error!\n");
    } else if (9 == err) {
        printf("File Write Error!\n");
    }
#endif
    
    // Clean up
    free(buffer);
    return iteratorState;
}

uint8_t readNextRecord(orderByInfo *state, void *buffer) {
    file_iterator_state_t *iteratorState = state->fileIterator;
    uint32_t recordPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / iteratorState->recordSize;

    if (iteratorState->recordsRead >= iteratorState->totalRecords) {
        return 1; // No more records left to read
    }


    if (iteratorState->currentRecord % recordPerPage == 0) {  // TODO: records per page
        state->fileInterface->read(state->readBuffer, iteratorState->currentRecord / recordPerPage, PAGE_SIZE, iteratorState->file);

        if (state->fileInterface->error(iteratorState->file)) {
            printf("ERROR: SORT: next record read failed");
            return 1;
        }

    }

    // Copy result
    memcpy(buffer, state->readBuffer + BLOCK_HEADER_SIZE + SORT_KEY_SIZE + iteratorState->recordSize * (iteratorState->currentRecord % recordPerPage), iteratorState->recordSize - SORT_KEY_SIZE);

    iteratorState->recordsRead++;
    iteratorState->currentRecord++;

    return 0;
}

void closeSort() {

}

metrics_t initMetric() {
    metrics_t metrics;
    metrics.num_reads = 0;
    metrics.num_compar = 0;
    metrics.num_memcpys = 0;
    metrics.num_runs = 0;
    metrics.num_writes = 0;
    return metrics;
}

