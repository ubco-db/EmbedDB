# Simple Query Interface

EmbedDB has a library with quick and easy-to-use query operators. To use them, import the [`advancedQueries.h`](../src/query-interface/advancedQueries.h) header file. The built-in operators are not going to be the best solution in terms of performance because they are built to be highly compatible and simple to use. If performance is your priority consider a custom solution using the functions described in the [usage documentation](usageInfo.md).

For a complete code example see [advancedQueryExamples.c](../src/advancedQueryExamples.c), but this document is a guide on each provided operator as well as how to create custom operators.

## Table of Contents

-   [Schema](#schema)
-   [Using Operators](#using-operators)
-   [Built-in Operators](#built-in-operators)
    -   [Table Scan](#table-scan)
    -   [Projection](#projection)
    -   [Selection](#selection)
    -   [Aggregate Functions](#aggregate-functions)
    -   [Key Equijoin](#key-equijoin)
-   [Custom Operators](#custom-operators)
    -   [Variables](#variables)
    -   [Functions](#functions)
        -   [Init](#init)
        -   [Next](#next)
        -   [Close](#close)

## Schema

The schema object is used to track the size, number and positions of columns in an operator's input/output. Unlike in the base implementation of EmbedDB, where the key and data are always separate, this library combines them into a single record pointer, and so the schema includes both the key and data columns.

To create an `EmbedDBSchema` object:

```c
// 4 columns, 4 bytes each
int8_t colSizes[] = {4, 4, 4, 4};

// The first column (key) is unsigned by requirement of EmbedDB, while the rest are signed values
int8_t colSignedness[] = {EmbedDB_COLUMN_UNSIGNED, EmbedDB_COLUMN_SIGNED, EmbedDB_COLUMN_SIGNED, EmbedDB_COLUMN_SIGNED};

// Create schema. Args: 4 columns, their sizes, their signedness
EmbedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);
```

Freeing `embedDBSchema`:

```c
embedDBFreeSchema(&baseSchema);
```

## Using Operators

Set up a chain of operators where a table scan is at the bottom and you progressively apply operators on top of the last. Notice that `scanOp` doesn't have an operator in its construction, but the rest do. Then you only need to `init()` the top level operator which will recursively initialize all operators in the chain. During initialization, the intermediate schemas will be calculated and buffers will be allocated.

```c
embedDBOperator* scanOp = createTableScanOperator(state, &it, baseSchema);
int32_t selVal = 200;
embedDBOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
embedDBOperator* projOp = createProjectionOperator(selectOp, 3, projCols);
projOp->init(projOp);
```

After initialization, the top level operator can be executed so that the next record will end up in the `recordBuffer` of the top level operator. You can then read the data in this buffer/perform any other calculations if required.

```c
int32_t* recordBuffer = projOp->recordBuffer;
while(exec(projOp)) {
	printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
}
```

To free the operators, first `close()` the top level operator, which will recursively close the whole chain. Then you can either call `embedDBFreeOperatorRecursive()` to free the whole chain of operators, or, if any of the operators have two inputs (such as a join), you can call `free()` each operator individually.

```c
projOp->close(projOp);
// This
embedDBFreeOperatorRecursive(&projOp);
// Or this (required if any operator in chain has more than one input)
free(scanOp);
free(selectOp);
free(projOp);
```

## Built-in Operators

### Table Scan

The table scan is the base of all queries. It uses an `embedDBIterator` to read records from the database.

```c
// Create and init iterator object with any constraints desired
embedDBIterator it;
int32_t maxTemp = 400;
it.minKey = NULL;
it.maxKey = NULL;
it.minData = NULL;
it.maxData = &maxTemp;
embedDBInitIterator(state, &it);

// Create schema object of the records returned by the operator
int8_t colSizes[] = {4, 4, 4, 4};
int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

// Create operator
embedDBOperator* scanOp = createTableScanOperator(state, &it, baseSchema);
```

### Projection

Projects out columns of the input operator. Projected columns must be in the order in which they are in the input operator. Column indexes are zero-indexed

```c
uint8_t projCols[] = {0, 1, 3}; // Must be strictly increasing. i.e. cannot have column 3 before column 1
embedDBOperator* projOp1 = createProjectionOperator(scanOp, 3, projCols);
```

### Selection

Performs a `SELECT * WHERE x` on the output of an operator. Supports >, >=, <, <=, ==, and != through the defined constants `SELECT_GT`, `SELECT_GTE`, etc.

The following selects tuples where column 3 (zero-indexed) is >= 200

```c
int32_t selVal = 200;
embedDBOperator* selectOp2 = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
```

### Aggregate Functions

This operator allows you to run a `GROUP BY` and perform an aggregate function on each group. In order to use this operator, you will need another type of object: `embedDBAggregateFunc`. The output of an aggregate operator is dictated by the list of `embedDBAggregateFunc` provided to `createAggregateOperator()`.

First, this is the code to setup the operator:

```c
uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
}
```

```c
embedDBAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
embedDBAggregateFunc* counter = createCountAggregate();
embedDBAggregateFunc* sum = createSumAggregate(2);
embedDBAggregateFunc aggFunctions[] = {groupName, *counter, *sum};
uint32_t numFunctions = 3;
embedDBOperator* aggOp3 = createAggregateOperator(selectOp3, sameDayGroup, aggFunctions, numFunctions);
```

But let's break it down.

An `embedDBAggregateFunc` has three functions in it:

-   `reset` - Gets called once at the start of the aggregate operator's `next`. Resets the state, clearing any data that is accumulated over the course of aggregating a single group. After running, the function should be ready to accept records from a new group.
-   `add` - Gets called for every record in a group, "adding" a record to the group. Should read the record for any information necessary for computing its aggregate value. E.g. a sum function should read one of the column values and add it to a variable stored in the function's state.
-   `compute` - Gets called once after the end of a group is detected and right before returning the row from the aggregate operator.

You can create your own aggregate functions by implementing these three functions, or if one of them isn't needed, you can pass NULL as the function pointer. The other two arguments required are a pointer to the function's state buffer and the size of the column that will be outputted from the function. The size is in bytes, and a positive number represents an unsigned number, whereas a negative number is a signed number. (E.g. A colSize of -8 means it's a `int64` and +8 means `uint64`)

There are two built-in aggregate functions though, and can be created with their respective `create` functions. The sum function simply takes the zero-indexed column to sum as long as the column is <= 8 bytes and the count aggregate doesn't need any info and can count up to 4.2 billion records. The `groupName` function is a function that will insert.

After creating the aggregate functions, they must be put into an array. The order that they are in the array will be the order in which their columns will be in the output table of the operator. The other argument for creating an aggregate operator, other than the input operator, is a function that can determine if two records belong to the same group. Take the `sameDayGroup()` function as an example. It takes two record pointers, reads the first 4 bytes of each as a uint32 because that's the key of the record. Then, since the key is a unix timestamp, divides by 86400, the number of seconds in a day, to find what group each record belongs in.

### Key Equijoin

Simple joins can be performed on two instances of an EmbedDB table. It can only be done on a sorted, unsigned key. The code for it is incredibly simple though. Just provide two operators that have a sorted, unsigned number, with the same size as their first column, and they will join.

```c
embedDBOperator* join4 = createKeyJoinOperator(scan_1, scan_2);
```

The output schema of this operator includes all columns of both inputs. I.e. joining tables with columns (a, b, c) and (a, d, e) will result in a table with columns (a, b, c, a, d, e)

A common use case may be comparing two different datasets. They may have slightly different timestamps making them hard to join. A way to help them join would be to write a custom operator that shifts one of the datasets by a set amount (as seen in the join example of [advancedQueryExamples.c](../src/advancedQueryExamples.c)) and/or rounds the timestamp. Say you have a sample being taken every minute, but the time it was taken may differ by a few seconds on each sample. Rounding to the minute on both datasets would help them to join using this simple equijoin.

## Custom Operators

Custom operators introduce the possibility of including behaviours into your query that are custom, more complex, or optimized for your dataset. This is a guide on how to make one for yourself.

The following is the struct that must be constructed for a operator to work.

```c
typedef struct embedDBOperator {
    struct embedDBOperator* input;
    void (*init)(struct embedDBOperator* operator);
    int8_t (*next)(struct embedDBOperator* operator);
    void (*close)(struct embedDBOperator* operator);
    void* state;
    embedDBSchema* schema;
    void* recordBuffer;
} embedDBOperator;
```

### Variables

-   `input` - The operator that your operator will pull records from, one at a time
-   `state` - A buffer where the operator can store information about its state or configuration of its behaviour. If the state is going to be storing more than one variable, it is recommended that you create a custom struct that can be stored here for better organization of data.
-   `schema` - This will be the schema of the **_output_** of this operator. This should be set, at the latest, in the init function. For example, a projection operator might have an input of columns (a, b, c) and project columns 0 and 2. `schema` then should be a schema that describes columns a and c.
-   `recordBuffer` - This is where the output record must be copied to during each call of `next()`. This buffer must be allocated, at the latest, during init. Its size should match the output schema of the operator. A helpful function may be `createBufferFromSchema()` which takes a schema, totals the sizes of all columns, and uses `calloc` to create a buffer of that size.

### Functions

#### Init

```c
void init(embedDBOperator* operator)
```

The first thing this operator must do is recursively call `init` of _any_ input operators. This way any information needed, like the schema of the input will become available to this method for its own initialization.

Another common operation to do during initialization is to verify that the operator will be performing a valid operation. E.g. a join may check that the two columns being joined have the same format.

One other thing to during init is to allocate buffers. The `recordBuffer` is required for the operator to be valid. Be sure that it is big enough to fit the entire output tuple and that the `schema` properly describes the layout of the buffer.

#### Next

```c
int8_t next(embedDBOperator* operator)
```

This is the heart of the operator. It will be called for each output row. The responsibilities of the next function include getting rows from the input, performing its own operation, and copying the output tuple to `operator->recordBuffer` before returning.

The return of `next()` is boolean. It should return 1 if it wrote a new tuple to `recordBuffer`, and 0 if it did not. 0 also indicates that there are no more records at all for this query.

To get records from the input, you would call `operator->input->next(operator->input)`. Be sure to read the return of the call to see if there is a tuple to read from `operator->input->recordBuffer`. During the execution of next, you may read multiple rows from the input, as would be the case of a selection that needs to keep reading from the input until it finds a row that matches the predicate. Remember that you can get the schema of the input tuple by reading `operator->input->schema`.

#### Close

```c
void close(embedDBOperator* operator)
```

The close function is responsible for, first, calling close on any input operators, and then freeing any buffers that were allocated to `recordBuffer` or `state`. It also should free the schema with `embedDBFreeSchema()`.
