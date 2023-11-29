#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "embedDB/embedDB.h"
#include "query-interface/advancedQueries.h"

int embedDBFloor4(double x) {
    int xi = (int)x;
    return x < xi ? xi - 1 : xi;
}

int8_t groupFunction4(const void* lastRecord, const void* record) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t value = *((uint32_t*)((int8_t*)record + 0));
    return embedDBFloor4((lastValue / 706000.0)) == embedDBFloor4((value / 706000.0));
}

void customAggregateFunc04(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t calculatedValue = embedDBFloor4((lastValue / 706000.0));
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &calculatedValue, sizeof(uint32_t));
}

embedDBOperator* createOperator4(embedDBState* state, void*** allocatedValues) {
    int32_t* minData = (int32_t*)malloc(4);
    *minData = 500000001;
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
    embedDBAggregateFunc* group = (embedDBAggregateFunc*)calloc(1, sizeof(embedDBAggregateFunc));
    group->compute = customAggregateFunc04;
    group->colSize = 4;
    embedDBAggregateFunc* counter1 = createCountAggregate();
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(2 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *group;
    aggFuncs[1] = *counter1;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction4, aggFuncs, 2);
    aggOp->init(aggOp);

    embedDBFreeSchema(&schema);
    free(group);
    free(counter1);

    *allocatedValues = (void**)malloc(3 * sizeof(void*));
    ((void**)*allocatedValues)[0] = minData;
    ((void**)*allocatedValues)[1] = it;
    ((void**)*allocatedValues)[2] = aggFuncs;

    return aggOp;
}

int execOperatorQuery4(embedDBState* state) {
    void** allocatedValues;
    embedDBOperator* op = createOperator4(state, &allocatedValues);
    void* recordBuffer = op->recordBuffer;
    int32_t* Bucket = (int32_t*)((int8_t*)recordBuffer + 0);
    int32_t* Count = (int32_t*)((int8_t*)recordBuffer + 4);

    // Count records
    int count = 0;
    while (exec(op)) {
        count++;
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
