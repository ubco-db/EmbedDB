#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "embedDB/embedDB.h"
#include "query-interface/advancedQueries.h"

int8_t groupFunction3(const void* lastRecord, const void* record) {
    return 1;
}

embedDBOperator* createOperator3(embedDBState* state, void*** allocatedValues) {
    int32_t* minData = (int32_t*)malloc(4);
    *minData = 1;
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    it->minKey = NULL;
    it->maxKey = NULL;
    it->minData = minData;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    uint8_t numCols = 4;
    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness);
    embedDBOperator* scanOp = createTableScanOperator(state, it, schema);
    embedDBAggregateFunc* counter0 = createCountAggregate();
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *counter0;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction3, aggFuncs, 1);
    aggOp->init(aggOp);

    embedDBFreeSchema(&schema);
    free(counter0);

    *allocatedValues = (void**)malloc(3 * sizeof(void*));
    ((void**)*allocatedValues)[0] = minData;
    ((void**)*allocatedValues)[1] = it;
    ((void**)*allocatedValues)[2] = aggFuncs;

    return aggOp;
}

int execOperatorQuery3(embedDBState* state) {
    void** allocatedValues;
    embedDBOperator* op = createOperator3(state, &allocatedValues);
    void* recordBuffer = op->recordBuffer;
    int32_t* C1 = (int32_t*)((int8_t*)recordBuffer + 0);

    // Count records
    int count = 0;
    while (exec(op)) {
        count += *C1;
    }

    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 3; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);

    return count;
}
