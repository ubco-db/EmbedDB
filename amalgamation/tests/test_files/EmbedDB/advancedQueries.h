/******************************************************************************/
/**
 * @file        advancedQueries.h
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Header file for the advanced query interface for EmbedDB
 * @copyright   Copyright 2024
 *              EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *  may be used to endorse or promote products derived from this software without
 *  specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "../embedDB/embedDB.h"
#include "schema.h"

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
#define SELECT_EQ 4
#define SELECT_NEQ 5

typedef struct embedDBAggregateFunc {
    /**
     * @brief	Resets the state
     */
    void (*reset)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	record	The record being added
     */
    void (*add)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* inputSchema, const void* record);

    /**
     * @brief	Finalize aggregate result into the record buffer and modify the schema accordingly. Is called once right before aggroup returns.
     */
    void (*compute)(struct embedDBAggregateFunc* aggFunc, embedDBSchema* outputSchema, void* recordBuffer, const void* lastRecord);

    /**
     * @brief	A user-allocated space where the operator saves its state. E.g. a sum operator might have 4 bytes allocated to store the sum of all data
     */
    void* state;

    /**
     * @brief	How many bytes will the compute insert into the record
     */
    int8_t colSize;

    /**
     * @brief	Which column number should compute write to
     */
    uint8_t colNum;
} embedDBAggregateFunc;

typedef struct embedDBOperator {
    /**
     * @brief	The input operator to this operator
     */
    struct embedDBOperator* input;

    /**
     * @brief	Initialize the operator. Usually includes setting/calculating the output schema, allocating buffers, etc. Recursively inits input operator as its first action.
     */
    void (*init)(struct embedDBOperator* operator);

    /**
     * @brief	Puts the next tuple to be outputed by this operator into @c operator->recordBuffer. Needs to call next on the input operator if applicable
     * @return	Returns 0 or 1 to indicate whether a new tuple was outputted to operator->recordBuffer
     */
    int8_t (*next)(struct embedDBOperator* operator);

    /**
     * @brief	Recursively closes this operator and its input operator. Frees anything allocated in init.
     */
    void (*close)(struct embedDBOperator* operator);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* state;

    /**
     * @brief	The output schema of this operator
     */
    embedDBSchema* schema;

    /**
     * @brief	The output record of this operator
     */
    void* recordBuffer;
} embedDBOperator;

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more rows to return
 */
int8_t exec(embedDBOperator* operator);

/**
 * @brief	Completely free a chain of operators recursively after it's already been closed.
 */
void embedDBFreeOperatorRecursive(embedDBOperator** operator);

///////////////////////////////////////////
// Pre-built operators for basic queries //
///////////////////////////////////////////

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
embedDBOperator* createTableScanOperator(embedDBState* state, embedDBIterator* it, embedDBSchema* baseSchema);

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. *Zero indexed*
 */
embedDBOperator* createProjectionOperator(embedDBOperator* input, uint8_t numCols, uint8_t* cols);

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
embedDBOperator* createSelectionOperator(embedDBOperator* input, int8_t colNum, int8_t operation, void* compVal);

/**
 * @brief	Creates an operator that will find groups and preform aggregate functions over each group.
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that records in groups are always next to each other and sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	functions		An array of aggregate functions, each of which will be updated with each record read from the iterator
 * @param	functionsLength			The number of embedDBAggregateFuncs in @c functions
 */
embedDBOperator* createAggregateOperator(embedDBOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), embedDBAggregateFunc* functions, uint32_t functionsLength);

/**
 * @brief	Creates an operator for perfoming an equijoin on the keys (sorted and distinct) of two tables
 */
embedDBOperator* createKeyJoinOperator(embedDBOperator* input1, embedDBOperator* input2);

//////////////////////////////////
// Prebuilt aggregate functions //
//////////////////////////////////

/**
 * @brief	Creates an aggregate function to count the number of records in a group. To be used in combination with an embedDBOperator produced by createAggregateOperator
 */
embedDBAggregateFunc* createCountAggregate();

/**
 * @brief	Creates an aggregate function to sum a column over a group. To be used in combination with an embedDBOperator produced by createAggregateOperator. Column must be no bigger than 8 bytes.
 * @param	colNum	The index (zero-indexed) of the column which you want to sum. Column must be <= 8 bytes
 */
embedDBAggregateFunc* createSumAggregate(uint8_t colNum);

/**
 * @brief	Creates an aggregate function to find the min value in a group
 * @param	colNum	The zero-indexed column to find the min of
 * @param	colSize	The size, in bytes, of the column to find the min of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMinAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an aggregate function to find the max value in a group
 * @param	colNum	The zero-indexed column to find the max of
 * @param	colSize	The size, in bytes, of the column to find the max of. Negative number represents a signed number, positive is unsigned.
 */
embedDBAggregateFunc* createMaxAggregate(uint8_t colNum, int8_t colSize);

/**
 * @brief	Creates an operator to compute the average of a column over a group. **WARNING: Outputs a floating point number that may not be compatible with other operators**
 * @param	colNum			Zero-indexed column to take average of
 * @param	outputFloatSize	Size of float to output. Must be either 4 (float) or 8 (double)
 */
embedDBAggregateFunc* createAvgAggregate(uint8_t colNum, int8_t outputFloatSize);

#endif
