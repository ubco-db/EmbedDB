#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "embedDB/embedDB.h"
#include "query-interface/advancedQueries.h"

int embedDBFloor2(double x) {
    int xi = (int)x;
    return x < xi ? xi - 1 : xi;
}

int8_t groupFunction2(const void* lastRecord, const void* record) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t value = *((uint32_t*)((int8_t*)record + 0));
    return embedDBFloor2((lastValue / 86400.0)) == embedDBFloor2((value / 86400.0));
}

void customAggregateFunc02(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t calculatedValue = embedDBFloor2((lastValue / 86400.0));
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &calculatedValue, sizeof(uint32_t));
}

embedDBOperator* createOperator2(embedDBState* state, void*** allocatedValues) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    it->minKey = NULL;
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    uint8_t numCols = 4;
    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
    embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness);
    embedDBOperator* scanOp = createTableScanOperator(state, it, schema);
    embedDBAggregateFunc* group = (embedDBAggregateFunc*)calloc(1, sizeof(embedDBAggregateFunc));
    group->compute = customAggregateFunc02;
    group->colSize = 4;
    embedDBAggregateFunc* avg1 = createAvgAggregate(1, 4);
    embedDBAggregateFunc* MAXC3 = createMaxAggregate(3, -4);
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(3 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *group;
    aggFuncs[1] = *avg1;
    aggFuncs[2] = *MAXC3;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction2, aggFuncs, 3);
    int32_t* havingValue = (int32_t*)malloc(sizeof(int32_t));
    *havingValue = 150;
    embedDBOperator* havingOp = createSelectionOperator(aggOp, 2, SELECT_GT, havingValue);
    havingOp->init(havingOp);

    embedDBFreeSchema(&schema);
    free(group);
    free(avg1);
    free(MAXC3);

    *allocatedValues = (void**)malloc(3 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;
    ((void**)*allocatedValues)[2] = havingValue;

    return havingOp;
}

int execOperatorQuery2(embedDBState* state) {
    void** allocatedValues;
    embedDBOperator* op = createOperator2(state, &allocatedValues);
    void* recordBuffer = op->recordBuffer;
    int32_t* Day = (int32_t*)((int8_t*)recordBuffer + 0);
    float* C2 = (float*)((int8_t*)recordBuffer + 4);
    int32_t* C3 = (int32_t*)((int8_t*)recordBuffer + 8);

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
