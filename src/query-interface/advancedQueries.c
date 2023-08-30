/******************************************************************************/
/**
 * @file		advancedQueries.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief       Source code file for the advanced query interface for EmbedDB
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include "advancedQueries.h"

#include <string.h>

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	Returns -1, 0, 1 as a comparator normally would
 */
int8_t compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    // Check the sign bits of the most significant bytes
    int sign1 = bytes1[numBytes - 1] & 0x80;
    int sign2 = bytes2[numBytes - 1] & 0x80;

    if (sign1 != sign2) {
        // Different signs, negative number is smaller
        return (sign1 ? -1 : 1);
    }

    // Same sign, perform regular byte-wise comparison
    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

/**
 * @return	0 or 1 to indicate if inequality is true
 */
int8_t compare(void* a, uint8_t operation, void* b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void* num1, const void* num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
    switch (operation) {
        case SELECT_GT:
            return compFunc(a, b, numBytes) > 0;
        case SELECT_LT:
            return compFunc(a, b, numBytes) < 0;
        case SELECT_GTE:
            return compFunc(a, b, numBytes) >= 0;
        case SELECT_LTE:
            return compFunc(a, b, numBytes) <= 0;
        case SELECT_EQ:
            return compFunc(a, b, numBytes) == 0;
        case SELECT_NEQ:
            return compFunc(a, b, numBytes) != 0;
        default:
            return 0;
    }
}

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator) {
    return operator->next(operator);
}

void initTableScan(embedDBOperator* operator) {
    if (operator->input != NULL) {
#ifdef PRINT_ERRORS
        printf("WARNING: TableScan operator should not have an input operator\n");
#endif
    }
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: TableScan operator needs its schema defined\n");
#endif
        return;
    }

    if (operator->schema->numCols<2) {
#ifdef PRINT_ERRORS
        printf("ERROR: When creating a table scan, you must include at least two columns: one for the key and one for the data from the iterator\n");
#endif
        return;
    }

    // Check that the provided key schema matches what is in the state
    embedDBState* embedDBstate = (embedDBState*)(((void**)operator->state)[0]);
    if (operator->schema->columnSizes[0] <= 0 || abs(operator->schema->columnSizes[0]) != embedDBstate->keySize) {
#ifdef PRINT_ERRORS
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
#endif
        return;
    }
    if (getRecordSizeFromSchema(operator->schema) != (embedDBstate->keySize + embedDBstate->dataSize)) {
#ifdef PRINT_ERRORS
        printf("ERROR: Size of provided schema doesn't match the size that will be returned by the provided iterator\n");
#endif
        return;
    }

    // Init buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextTableScan(embedDBOperator* operator) {
    // Check that a schema was set
    if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Must provide a base schema for a table scan operator\n");
#endif
        return 0;
    }

    // Get next record
    embedDBState* state = (embedDBState*)(((void**)operator->state)[0]);
    embedDBIterator* it = (embedDBIterator*)(((void**)operator->state)[1]);
    if (!embedDBNext(state, it, operator->recordBuffer, (int8_t*)operator->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(embedDBOperator* operator) {
    embedDBFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
    free(operator->state);
    operator->state = NULL;
}

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: All parameters must be provided to create a TableScan operator\n");
#endif
        return NULL;
    }

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }

    operator->state = malloc(2 * sizeof(void*));
    if (operator->state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating TableScan operator\n");
#endif
        return NULL;
    }
    memcpy(operator->state, &state, sizeof(void*));
    memcpy((int8_t*)operator->state + sizeof(void*), &it, sizeof(void*));

    operator->schema = copySchema(baseSchema);
    operator->input = NULL;
    operator->recordBuffer = NULL;

    operator->init = initTableScan;
    operator->next = nextTableScan;
    operator->close = closeTableScan;

    return operator;
}

void initProjection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Get state
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection schema\n");
#endif
            return;
        }
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate space for projection while building schema\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
        }
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextProjection(embedDBOperator* operator) {
    uint8_t numCols = *(uint8_t*)operator->state;
    uint8_t* cols = (uint8_t*)operator->state + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const embedDBSchema* inputSchema = operator->input->schema;

    // Get next record
    if (operator->input->next(operator->input)) {
        for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            if (col == cols[nextProjCol]) {
                memcpy((int8_t*)operator->recordBuffer + nextProjColPos, (int8_t*)operator->input->recordBuffer + curColPos, colSize);
                nextProjColPos += colSize;
                nextProjCol++;
            }
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. Zero indexed. Column indexes must be strictly increasing i.e. columns must stay in the same order, can only remove columns from input
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols) {
    // Ensure column numbers are strictly increasing
    uint8_t lastCol = cols[0];
    for (uint8_t i = 1; i < numCols; i++) {
        if (cols[i] <= lastCol) {
#ifdef PRINT_ERRORS
            printf("ERROR: Columns in a projection must be strictly ascending for performance reasons");
#endif
            return NULL;
        }
        lastCol = cols[i];
    }
    // Create state
    uint8_t* state = malloc(numCols + 1);
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }
    state[0] = numCols;
    memcpy(state + 1, cols, numCols);

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while creating Projection operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initProjection;
    operator->next = nextProjection;
    operator->close = closeProjection;

    return operator;
}

void initSelection(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Projection operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = copySchema(operator->input->schema);
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
#endif
            return;
        }
    }
}

int8_t nextSelection(embedDBOperator* operator) {
    embedDBSchema* schema = operator->input->schema;

    int8_t colNum = *(int8_t*)operator->state;
    uint16_t colPos = getColOffsetFromSchema(schema, colNum);
    int8_t operation = *((int8_t*)operator->state + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (operator->input->next(operator->input)) {
        void* colData = (int8_t*)operator->input->recordBuffer + colPos;

        if (compare(colData, operation, *(void**)((int8_t*)operator->state + 2), isSigned, colSize)) {
            memcpy(operator->recordBuffer, operator->input->recordBuffer, getRecordSizeFromSchema(operator->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(embedDBOperator* operator) {
    operator->input->close(operator->input);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal) {
    int8_t* state = malloc(2 + sizeof(void*));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    state[0] = colNum;
    state[1] = operation;
    memcpy(state + 2, &compVal, sizeof(void*));

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating Selection operator\n");
#endif
        return NULL;
    }
    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initSelection;
    operator->next = nextSelection;
    operator->close = closeSelection;

    return operator;
}

/**
 * @brief	A private struct to hold the state of the aggregate operator
 */
struct aggregateInfo {
    int8_t (*groupfunc)(const void* lastRecord, const void* record);  // Function that determins if both records are in the same group
    embedDBAggregateFunc* functions;                                  // An array of aggregate functions
    uint32_t functionsLength;                                         // The length of the functions array
    void* lastRecordBuffer;                                           // Buffer for the last record read by input->next
    uint16_t bufferSize;                                              // Size of the input buffer (and lastRecordBuffer)
    int8_t isLastRecordUsable;                                        // Is the data in lastRecordBuffer usable for checking if the recently read record is in the same group? Is set to 0 at start, and also after the last record
};

void initAggregate(embedDBOperator* operator) {
    if (operator->input == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Aggregate operator needs an input operator\n");
#endif
        return;
    }

    // Init input
    operator->input->init(operator->input);

    struct aggregateInfo* state = operator->state;
    state->isLastRecordUsable = 0;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        operator->schema->numCols = state->functionsLength;
        operator->schema->columnSizes = malloc(state->functionsLength);
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
        for (uint8_t i = 0; i < state->functionsLength; i++) {
            operator->schema->columnSizes[i] = state->functions[i].colSize;
            state->functions[i].colNum = i;
        }
    }

    // Init buffers
    state->bufferSize = getRecordSizeFromSchema(operator->input->schema);
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
    if (state->lastRecordBuffer == NULL) {
        state->lastRecordBuffer = malloc(state->bufferSize);
        if (state->lastRecordBuffer == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
#endif
            return;
        }
    }
}

int8_t nextAggregate(embedDBOperator* operator) {
    struct aggregateInfo* state = operator->state;
    embedDBOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].reset != NULL) {
            state->functions[i].reset(state->functions + i, input->schema);
        }
    }

    int8_t recordsInGroup = 0;

    // Check flag used to indicate whether the last record read has been added to a group
    if (state->isLastRecordUsable) {
        recordsInGroup = 1;
        for (int i = 0; i < state->functionsLength; i++) {
            if (state->functions[i].add != NULL) {
                state->functions[i].add(state->functions + i, input->schema, state->lastRecordBuffer);
            }
        }
    }

    int8_t exitType = 0;
    while (input->next(input)) {
        // Check if record is in the same group as the last record
        if (!state->isLastRecordUsable || state->groupfunc(state->lastRecordBuffer, input->recordBuffer)) {
            recordsInGroup = 1;
            for (int i = 0; i < state->functionsLength; i++) {
                if (state->functions[i].add != NULL) {
                    state->functions[i].add(state->functions + i, input->schema, input->recordBuffer);
                }
            }
        } else {
            exitType = 1;
            break;
        }

        // Save this record
        memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);
        state->isLastRecordUsable = 1;
    }

    if (!recordsInGroup) {
        return 0;
    }

    if (exitType == 0) {
        // Exited because ran out of records, so all read records have been added to a group
        state->isLastRecordUsable = 0;
    }

    // Perform final compute on all functions
    for (int i = 0; i < state->functionsLength; i++) {
        if (state->functions[i].compute != NULL) {
            state->functions[i].compute(state->functions + i, operator->schema, operator->recordBuffer, state->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(state->lastRecordBuffer, input->recordBuffer, state->bufferSize);

    return 1;
}

void closeAggregate(embedDBOperator* operator) {
    operator->input->close(operator->input);
    operator->input = NULL;
    embedDBFreeSchema(&operator->schema);
    free(((struct aggregateInfo*)operator->state)->lastRecordBuffer);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength) {
    struct aggregateInfo* state = malloc(sizeof(struct aggregateInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    state->groupfunc = groupfunc;
    state->functions = functions;
    state->functionsLength = functionsLength;
    state->lastRecordBuffer = NULL;

    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
#endif
        return NULL;
    }

    operator->state = state;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initAggregate;
    operator->next = nextAggregate;
    operator->close = closeAggregate;

    return operator;
}

struct keyJoinInfo {
    embedDBOperator* input2;
    int8_t firstCall;
};

void initKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
#ifdef PRINT_ERRORS
        printf("ERROR: The first columns of the two tables must be the key and must be the same size. Make sure you haven't projected them out.\n");
#endif
        return;
    }

    // Setup schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(embedDBSchema));
        if (operator->schema == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        operator->schema->numCols = schema1->numCols + schema2->numCols;
        operator->schema->columnSizes = malloc(operator->schema->numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
#ifdef PRINT_ERRORS
            printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
            return;
        }
        memcpy(operator->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(operator->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
    }

    // Allocate recordBuffer
    operator->recordBuffer = malloc(getRecordSizeFromSchema(operator->schema));
    if (operator->recordBuffer == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while initializing join operator\n");
#endif
        return;
    }

    state->firstCall = 1;
}

int8_t nextKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    // We've already used this match
    void* record1 = input1->recordBuffer;
    void* record2 = input2->recordBuffer;

    int8_t colSize = abs(schema1->columnSizes[0]);

    if (state->firstCall) {
        state->firstCall = 0;

        if (!input1->next(input1) || !input2->next(input2)) {
            // If this case happens, you goofed, but I'll handle it anyway
            return 0;
        }
        goto check;
    }

    while (1) {
        // Advance the input with the smaller value
        int8_t comp = compareUnsignedNumbers(record1, record2, colSize);
        if (comp == 0) {
            // Move both forward because if they match at this point, they've already been matched
            if (!input1->next(input1) || !input2->next(input2)) {
                return 0;
            }
        } else if (comp < 0) {
            // Move record 1 forward
            if (!input1->next(input1)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        } else {
            // Move record 2 forward
            if (!input2->next(input2)) {
                // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                return 0;
            }
        }

    check:
        // See if these records join
        if (compareUnsignedNumbers(record1, record2, colSize) == 0) {
            // Copy both records into the output
            uint16_t record1Size = getRecordSizeFromSchema(schema1);
            memcpy(operator->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t*)operator->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(embedDBOperator* operator) {
    struct keyJoinInfo* state = operator->state;
    embedDBOperator* input1 = operator->input;
    embedDBOperator* input2 = state->input2;
    embedDBSchema* schema1 = input1->schema;
    embedDBSchema* schema2 = input2->schema;

    input1->close(input1);
    input2->close(input2);

    embedDBFreeSchema(&operator->schema);
    free(operator->state);
    operator->state = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2) {
    embedDBOperator* operator= malloc(sizeof(embedDBOperator));
    if (operator== NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }

    struct keyJoinInfo* state = malloc(sizeof(struct keyJoinInfo));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to malloc while creating join operator\n");
#endif
        return NULL;
    }
    state->input2 = input2;

    operator->input = input1;
    operator->state = state;
    operator->recordBuffer = NULL;
    operator->schema = NULL;
    operator->init = initKeyJoin;
    operator->next = nextKeyJoin;
    operator->close = closeKeyJoin;

    return operator;
}

void countReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    *(uint32_t*)aggFunc->state = 0;
}

void countAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    (*(uint32_t*)aggFunc->state)++;
}

void countCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(uint32_t));
}

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate() {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = countReset;
    aggFunc->add = countAdd;
    aggFunc->compute = countCompute;
    aggFunc->state = malloc(sizeof(uint32_t));
    aggFunc->colSize = 4;
    return aggFunc;
}

void sumReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    if (abs(inputSchema->columnSizes[*((uint8_t*)aggFunc->state + sizeof(int64_t))]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    *(int64_t*)aggFunc->state = 0;
}

void sumAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* recordBuffer) {
    uint8_t colNum = *((uint8_t*)aggFunc->state + sizeof(int64_t));
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)recordBuffer + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t*)aggFunc->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t*)aggFunc->state) += val;
    }
}

void sumCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), aggFunc->state, sizeof(int64_t));
}

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    aggFunc->reset = sumReset;
    aggFunc->add = sumAdd;
    aggFunc->compute = sumCompute;
    aggFunc->state = malloc(sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t*)aggFunc->state + sizeof(int64_t)) = colNum;
    aggFunc->colSize = -8;
    return aggFunc;
}

struct minMaxState {
    uint8_t colNum;  // Which column of input to use
    void* current;   // The value currently regarded as the min/max
};

void minReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for min aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0xff, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as -1, not MAX_INT
        memset((int8_t*)state->current + colSize - 1, 0x7f, 1);
    }
}

void minAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_LT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

void minMaxCompute(embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), ((struct minMaxState*)aggFunc->state)->current, abs(outputSchema->columnSizes[aggFunc->colNum]));
}

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating min aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = minReset;
    aggFunc->add = minAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

void maxReset(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    if (aggFunc->colSize != colSize) {
#ifdef PRINT_ERRORS
        printf("WARNING: Your provided column size for max aggregate function doesn't match the column size in the input schema\n");
#endif
    }
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    memset(state->current, 0, colSize);
    if (isSigned) {
        // If the number is signed, flip MSB else it will read as 0, not MIN_INT
        memset((int8_t*)state->current + colSize - 1, 0x80, 1);
    }
}

void maxAdd(embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct minMaxState* state = aggFunc->state;
    int8_t colSize = inputSchema->columnSizes[state->colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = abs(colSize);
    void* newValue = (int8_t*)record + getColOffsetFromSchema(inputSchema, state->colNum);
    if (compare(newValue, SELECT_GT, state->current, isSigned, colSize)) {
        memcpy(state->current, newValue, colSize);
    }
}

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    struct minMaxState* state = malloc(sizeof(struct minMaxState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    state->current = malloc(abs(colSize));
    if (state->current == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating max aggregate function\n");
#endif
        return NULL;
    }
    aggFunc->state = state;
    aggFunc->colSize = colSize;
    aggFunc->reset = maxReset;
    aggFunc->add = maxAdd;
    aggFunc->compute = minMaxCompute;

    return aggFunc;
}

struct avgState {
    uint8_t colNum;   // Column to take avg of
    int8_t isSigned;  // Is input column signed?
    uint32_t count;   // Count of records seen in group so far
    int64_t sum;      // Sum of records seen in group so far
};

void avgReset(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema) {
    struct avgState* state = aggFunc->state;
    if (abs(inputSchema->columnSizes[state->colNum]) > 8) {
#ifdef PRINT_ERRORS
        printf("WARNING: Can't use this sum function for columns bigger than 8 bytes\n");
#endif
    }
    state->count = 0;
    state->sum = 0;
    state->isSigned = embedDB_IS_COL_SIGNED(inputSchema->columnSizes[state->colNum]);
}

void avgAdd(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record) {
    struct avgState* state = aggFunc->state;
    uint8_t colNum = state->colNum;
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = embedDB_IS_COL_SIGNED(colSize);
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)record + getColOffsetFromSchema(inputSchema, colNum);
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        state->sum += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        val += (uint64_t)state->sum;
        memcpy(&state->sum, &val, sizeof(uint64_t));
    }
    state->count++;
}

void avgCompute(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord) {
    struct avgState* state = aggFunc->state;
    if (aggFunc->colSize == 8) {
        double avg = state->sum / (double)state->count;
        if (state->isSigned) {
            avg = state->sum / (double)state->count;
        } else {
            avg = (uint64_t)state->sum / (double)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(double));
    } else {
        float avg;
        if (state->isSigned) {
            avg = state->sum / (float)state->count;
        } else {
            avg = (uint64_t)state->sum / (float)state->count;
        }
        memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(outputSchema, aggFunc->colNum), &avg, sizeof(float));
    }
}

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize) {
    embedDBAggregateFunc* aggFunc = malloc(sizeof(embedDBAggregateFunc));
    if (aggFunc == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    struct avgState* state = malloc(sizeof(struct avgState));
    if (state == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: Failed to allocate while creating avg aggregate function\n");
#endif
        return NULL;
    }
    state->colNum = colNum;
    aggFunc->state = state;
    if (outputFloatSize > 8 || (outputFloatSize < 8 && outputFloatSize > 4)) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 8.");
#endif
        aggFunc->colSize = 8;
    } else if (outputFloatSize < 4) {
#ifdef PRINT_ERRORS
        printf("WARNING: The size of the output float for AVG must be exactly 4 or 8. Defaulting to 4.");
#endif
        aggFunc->colSize = 4;
    } else {
        aggFunc->colSize = outputFloatSize;
    }
    aggFunc->reset = avgReset;
    aggFunc->add = avgAdd;
    aggFunc->compute = avgCompute;

    return aggFunc;
}

/**
 * @brief	Completely free a chain of functions recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator) {
    if ((*operator)->input != NULL) {
        embedDBFreeOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->state != NULL) {
        free((*operator)->state);
        (*operator)->state = NULL;
    }
    if ((*operator)->schema != NULL) {
        embedDBFreeSchema(&(*operator)->schema);
    }
    if ((*operator)->recordBuffer != NULL) {
        free((*operator)->recordBuffer);
        (*operator)->recordBuffer = NULL;
    }
    free(*operator);
    (*operator) = NULL;
}
