#include "sortWrapper.h"

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
int8_t writePageWithHeader(void *buffer, int32_t blockIndex, int16_t numberOfValues, int16_t pageSize, embedDBFileInterface *fileInterface, void *file) {
    memcpy((uint8_t *)buffer, &blockIndex, sizeof(int32_t));
    memcpy((uint8_t *)buffer + sizeof(uint32_t), &numberOfValues, sizeof(int16_t));
    
    fileInterface->write(buffer, blockIndex, pageSize, file); 

    if (fileInterface->error(file)) {
        printf("ERROR: SORT: Failed to write unsorted data");
        return 1;
    }

    return 0;
}

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
uint32_t loadRowData(sortData *data, embedDBOperator *op, void *unsortedFile) {
    uint32_t count = 0;
    int32_t blockIndex = 0;
    int16_t valuesPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / data->recordSize;

    void *buffer = malloc(PAGE_SIZE);

    if (buffer == NULL) {
        printf("ERROR: SORT: buffer malloc failed");
        return 0;
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
        uint32_t rowOffset = count % valuesPerPage * data->recordSize + BLOCK_HEADER_SIZE;

        if (rowOffset + data->recordSize > PAGE_SIZE) {
            printf("ERROR: SORT: error calculating row offset");
            free(buffer);
            buffer = NULL;
            return 0;
        }

        // Write data to buffer
        memcpy((uint8_t *)buffer + rowOffset, op->input->recordBuffer, data->recordSize);
        
        count++;

        // temp limit for debugging
        if (count >= 10000)
            break;
    }

    // Write remaining records
    int16_t numRemainingRecords = (count % valuesPerPage == 0 && count != 0) ? valuesPerPage : count % valuesPerPage;
    if(writePageWithHeader(buffer, blockIndex, numRemainingRecords, PAGE_SIZE, data->fileInterface, unsortedFile)) {
        free(buffer);
        buffer = NULL;
        return 0;
    }

    data->fileInterface->flush(unsortedFile);

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
void prepareSort(embedDBOperator* op) {
    sortData *data = op->state;
    data->keyOffset = getColOffsetFromSchema(op->schema, data->colNum);
    data->recordSize = getRecordSizeFromSchema(op->schema);
    data->keySize = op->schema->columnSizes[data->colNum];

    // A columns size will be negative if the column is signed
    // and positive if value is unsigned
    if (data->keySize < 0) {
        data->keySize = -1 * data->keySize;
    }

    // Set up files
    void *unsortedFile = setupFile(SORT_DATA_LOCATION);
    void *sortedFile = setupFile(SORT_ORDER_LOCATION);
    
    if (unsortedFile == NULL|| sortedFile == NULL) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    const uint8_t unsortedOpen = data->fileInterface->open(unsortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    const uint8_t sortedOpen = data->fileInterface->open(sortedFile, EMBEDDB_FILE_MODE_W_PLUS_B);
    
    if (!unsortedOpen || !sortedOpen) {
        #ifdef PRINT_ERRORS
                printf("ERROR: Failed to open files while initializing ORDER BY operator");
        #endif
        return;
    }

    // Load row data
    data->count = loadRowData(data, op, unsortedFile);

    // Start sorting
    file_iterator_state_t *iteratorState = startSort(data, unsortedFile, sortedFile);
    if (iteratorState == NULL) {
        printf("ERROR: Sort failed");
    }

    // Finish
    iteratorState->file = sortedFile;
    data->fileInterface->close(unsortedFile);
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
    es.num_pages = (uint32_t) ceil((float)data->count / ((es.page_size - es.headerSize) / es.record_size));

    const int buffer_max_pages = 4; 
    char *buffer = malloc(buffer_max_pages * es.page_size + es.record_size);
    char *tuple_buffer = buffer + es.page_size * buffer_max_pages;

    if (buffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: SORT: buffer malloc failed m\n");
#endif
        return NULL;
    }

    // Prepare the file iterator data for sorting
    file_iterator_state_t *iteratorState = malloc(sizeof(file_iterator_state_t));
    if (iteratorState == NULL) {
#ifdef PRINT_ERRORS
        printf("Error: SORT: iterator malloc failed\n");
#endif
        free(buffer);
        buffer = NULL;
        return NULL;
    }

    iteratorState->file = unsortedFile;
    iteratorState->recordsRead = 0;
    iteratorState->totalRecords = data->count; // Total records from the previous while loop
    iteratorState->recordSize = es.record_size;
    iteratorState->fileInterface = data->fileInterface;
    iteratorState->currentRecord = 0;
    iteratorState->recordsLeftInBlock = 0;
    iteratorState->resultFile = 0;

    data->fileIterator = iteratorState;

    // Metrics
    metrics_t metrics = initMetric();

    long result_file_ptr = 0;
    int8_t runGenOnly = false; // Run full sort operation
    int8_t writeReadRatio = 19; // 1.97 * 10 => 19

    // Sort the data from unsortedFile and store it in sortedFile
    // int err = flash_minsort(iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages * es.page_size, &es, &result_file_ptr, &metrics, data->compareFn);
    int err = adaptive_sort(readNextRecord, iteratorState, tuple_buffer, sortedFile, buffer, buffer_max_pages, &es, &result_file_ptr, &metrics, data->compareFn, runGenOnly, writeReadRatio, data);
    
    iteratorState->resultFile = result_file_ptr;


#ifdef PRINT_ERRORS
    if (8 == err) {
        printf("Out of memory!\n");
    } else if (10 == err) {
        printf("File Read Error!\n");
    } else if (9 == err) {
        printf("File Write Error!\n");
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
    
    uint32_t recordPerPage = (PAGE_SIZE - BLOCK_HEADER_SIZE) / iteratorState->recordSize;

    if (iteratorState->recordsRead >= iteratorState->totalRecords) {
        return 1; // No more records left to read
    }

    // Read next page if current buffer is empty
    if (iteratorState->currentRecord % recordPerPage == 0 || iteratorState->recordsRead == 0) {
        iteratorState->fileInterface->seek(iteratorState->currentRecord / recordPerPage * PAGE_SIZE + iteratorState->resultFile, iteratorState->file);
        iteratorState->fileInterface->readRel(((sortData *)data)->readBuffer, PAGE_SIZE, 1, iteratorState->file);
        
        if (((sortData *)data)->fileInterface->error(iteratorState->file)) {
            printf("ERROR: SORT: next record read failed");
            return 2;
        }
    }

    // Copy result to ouput buffer
    memcpy(buffer, ((sortData *)data)->readBuffer + BLOCK_HEADER_SIZE + iteratorState->recordSize * (iteratorState->currentRecord % recordPerPage), iteratorState->recordSize);
    iteratorState->recordsRead++;
    iteratorState->currentRecord++;


    #ifdef DEBUG
    printf("DEBUG: ROWDATA:\n");
    for (int i = 0; i < iteratorState->recordSize - SORT_KEY_SIZE; i++) {
        printf("%2x ", ((uint8_t *)buffer)[i]);
    }
    printf("\n");
    #endif

    return 0;
}


void closeSort(file_iterator_state_t *iteratorState) {
    iteratorState->fileInterface->close(iteratorState->file);
    iteratorState->file = NULL;
}

/**
 * @brief Initalizes default metric values
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

