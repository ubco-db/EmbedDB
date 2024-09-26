#include "sortWrapper.h"

metrics_t initMetric();
uint8_t loadRowData(orderByInfo *state, embedDBOperator *op, void *outputFile, uint16_t recordSize, uint8_t keySize, uint8_t keyCol);
file_iterator_state_t *startSort(orderByInfo *state, void *unsortedFile, void *sortedFile, uint16_t recordSize, uint8_t count);


void initSort(embedDBOperator* op) {
    orderByInfo *state = op->state;
    
    // Set up files
    void *rowData = setupFile(SORT_DATA_LOCATION);
    void *orderedData = setupFile(SORT_ORDER_LOCATION);
    
    if (rowData == NULL|| orderedData == NULL) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    const uint8_t fileOpen1 = state->fileInterface->open(rowData, EMBEDDB_FILE_MODE_W_PLUS_B);
    const uint8_t fileOpen2 = state->fileInterface->open(orderedData, EMBEDDB_FILE_MODE_W_PLUS_B);
    
    if (!fileOpen1 || !fileOpen2) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    uint8_t count;
    uint16_t colOffset = getColOffsetFromSchema(op->schema, ((struct orderByInfo *)op->state)->colNum);
    uint16_t recordSize = getRecordSizeFromSchema(op->schema);
    int8_t colSize = op->schema->columnSizes[((struct orderByInfo *)op->state)->colNum]; // TODO: check the sign of the column size

    if (colSize < 0) {
        colSize = -1 * colSize;
    }

    // Output rows to file
    count = loadRowData(state, op, rowData, recordSize, colSize, ((struct orderByInfo *)op->state)->colNum);

    // Sort data
    file_iterator_state_t *iteratorState = startSort(state, rowData, orderedData, recordSize, count);

    state->fileIterator = iteratorState;
    state->colSize = colSize;
}

uint8_t loadRowData(orderByInfo *state, embedDBOperator *op, void *outputFile, uint16_t recordSize, uint8_t keySize, uint8_t keyCol) {
    uint8_t count = 0;
    
    void *buffer = malloc(recordSize + keySize);


    while (exec(op->input)) {
        // Write row data

        memcpy(buffer, op->input->recordBuffer + keyCol * keySize, keySize); // TODO: find proper first byte
        memcpy(buffer + keySize, op->input->recordBuffer, recordSize);


        if (state->fileInterface->write(buffer, count++, recordSize, outputFile) == 0) {
            #ifdef PRINT_ERRORS
                printf("ERROR: SORT: Failed to write data to output file\n");
            #endif
            break;  // Exit the loop if writing fails
        }
    }

    // Flush the rowData file and check for success
    if (state->fileInterface->flush(outputFile)) {
        #ifdef PRINT_ERRORS
            printf("ERROR: Failed to flush output file\n");
        #endif
    }

    // Clean up
    free(buffer);

    return count;
}

file_iterator_state_t *startSort(orderByInfo *state, void *unsortedFile, void *sortedFile, uint16_t recordSize, uint8_t count) {
    // Perform external sorting on rowData
    external_sort_t es;

    // Initialize external_sort_t structure
    es.key_size = sizeof(int32_t); // TODO: update to use proper key size
    es.value_size = recordSize - es.key_size;
    es.headerSize = BLOCK_HEADER_SIZE;
    es.record_size = recordSize;
    es.page_size = 512; // Example page size
    es.num_pages = (uint32_t)(es.record_size * count) / es.page_size; // TODO: add check for when record is exactly the page size

    printf("Count: %d/n", count);

    if (es.num_pages == 0 || es.num_pages % es.page_size != 0) {
        es.num_pages++;
    }

    printf("Num pages: %d/n", es.num_pages);

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
        return NULL;
    }

    iteratorState->file = unsortedFile;
    iteratorState->recordsRead = 0;
    iteratorState->totalRecords = count; // Total records from the previous while loop
    iteratorState->recordSize = es.record_size;
    iteratorState->fileInterface = state->fileInterface;

    // Metrics
    metrics_t metrics = initMetric();

    // Sort the data from rowData and store it in orderedData
    long result_file_ptr = 0;
    int err = flash_minsort(iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages * es.page_size, &es, &result_file_ptr, &metrics, merge_sort_int32_comparator);

    if (err) {
#ifdef PRINT_ERRORS
        printf("ERROR: Sorting failed with code %d\n", err);
#endif
        free(buffer);
        return NULL;
    }

    // Clean up
    free(buffer);
    return iteratorState;
}

uint8_t readNextRecord(file_iterator_state_t *state, void *buffer) {
    if (state->recordsRead >= state->totalRecords) {
        return -1; // No more records left to read
    }

    if (!state->fileInterface->read(state->readBuffer, state->currentRecord, state->recordSize, state->file)) {
        #ifdef PRINT_ERRORS
            printf("ERROR: Failed to read record %d\n", state->recordsRead);
        #endif
        return -1;
    }

    // Copy result
    memcpy(buffer, (uint8_t *)state->readBuffer, state->recordSize);

    state->recordsRead++;
    state->currentRecord++;

    return 0;
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

