#include "streamingQueries.h"

StreamingQuery* SQIF(StreamingQuery *query, StreamingQueryType type) {
    query->type = type;
    return query;
}

StreamingQuery* is(StreamingQuery *query, SelectOperation operation, float threshold) {
    query->operation = operation;
    query->threshold = threshold;
    return query;
}

StreamingQuery* forLast(StreamingQuery *query, uint32_t numLastEntries) {
    query->numLastEntries = numLastEntries;
    return query;
}

StreamingQuery* then(StreamingQuery *query, void (*callback)(const float value)) {
    query->callback = callback;
    return query;
}

StreamingQuery* createStreamingQuery(embedDBState *state, embedDBSchema *schema) {
    StreamingQuery *query = (StreamingQuery*)malloc(sizeof(StreamingQuery));
    if (query != NULL) {
        query->state = state;
        query->schema = schema;
        query->SQIF = SQIF;
        query->is = is;
        query->forLast = forLast;
        query->then = then;
    }
    return query;
}


int8_t streamingQueryPut(StreamingQuery *query, void *key, void *data) {
    int8_t result = embedDBPut(query->state, key, data);
    if (result != 0) {
        printf("Error inserting record\n");
        return result;
    }

    if (query->type == GET_AVG) {
        if (query->operation == SELECT_GT) {
            if (query->numLastEntries > 0) {
                float avgTemp = GetAvg(query, query->state, key);
                if (avgTemp > query->threshold) {
                    query->callback(avgTemp);
                }
            }
        }
    }


    return result;
}

float GetAvg(StreamingQuery *query, embedDBState *state, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createAvgOperator(query, state, &allocatedValues, key);
    if (op == NULL) {
        return -1;
    }
    void* recordBuffer = op->recordBuffer;
    float* C1 = (float*)((int8_t*)recordBuffer + 0);
    // Print as csv
    exec(op);
    float avgTemp = *C1;
    printf("average temperature in the last %i seconds: %f\n", query->numLastEntries, avgTemp);
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return avgTemp;
}

embedDBOperator* createAvgOperator(StreamingQuery *query, embedDBState* state, void*** allocatedValues, void *key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    uint32_t minKeyVal = *(uint32_t*)key - (query->numLastEntries-1);
    uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
    if (minKeyPtr != NULL) {
        *minKeyPtr = minKeyVal;
        it->minKey = minKeyPtr;
    }
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(state, it);

    embedDBOperator* scanOp = createTableScanOperator(state, it, query->schema);
    embedDBAggregateFunc* avg0 = createAvgAggregate(1, 4);
    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1*sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *avg0;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 1);
    aggOp->init(aggOp);

    free(avg0);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;

    return aggOp;
}

int8_t groupFunction(const void* lastRecord, const void* record) {
    return 1;
}