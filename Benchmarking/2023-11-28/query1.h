#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "embedDB/embedDB.h"
#include "query-interface/advancedQueries.h"

int embedDBFloor(double x) {
    int xi = (int)x;
    return x < xi ? xi - 1 : xi;
}

int8_t groupFunction(const void* lastRecord, const void* record) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t value = *((uint32_t*)((int8_t*)record + 0));
    return embedDBFloor((lastValue / 86400.0)) == embedDBFloor((value / 86400.0));
}

void customAggregateFunc0(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    uint32_t lastValue = *((uint32_t*)((int8_t*)lastRecord + 0));
    uint32_t calculatedValue = embedDBFloor((lastValue / 86400.0));
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &calculatedValue, sizeof(uint32_t));
}

embedDBOperator* createOperator(embedDBState* state, void*** allocatedValues) {
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
    group->compute = customAggregateFunc0;
    group->colSize = 4;
    embedDBAggregateFunc* MINC2 = createMinAggregate(1, -4);
    embedDBAggregateFunc* MAXC3 = createMaxAggregate(1, -4);
    embedDBAggregateFunc* avg3 = createAvgAggregate(1, 4);
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(4 * sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *group;
    aggFuncs[1] = *MINC2;
    aggFuncs[2] = *MAXC3;
    aggFuncs[3] = *avg3;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 4);
    aggOp->init(aggOp);

    embedDBFreeSchema(&schema);
    free(group);
    free(MINC2);
    free(MAXC3);
    free(avg3);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;

    return aggOp;
}

int execOperatorQuery1(embedDBState* state) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(state, &allocatedValues);
    void* recordBuffer = op->recordBuffer;
    int32_t* Day = (int32_t*)((int8_t*)recordBuffer + 0);
    int32_t* C2 = (int32_t*)((int8_t*)recordBuffer + 4);
    int32_t* C3 = (int32_t*)((int8_t*)recordBuffer + 8);
    float* C4 = (float*)((int8_t*)recordBuffer + 12);

    // Count records
    int count = 0;
    while (exec(op)) {
        count++;
    }

    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);

    return count;
}
