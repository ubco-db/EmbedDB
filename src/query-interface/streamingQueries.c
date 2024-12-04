#include "streamingQueries.h"

StreamingQuery* IF(StreamingQuery *query, uint8_t colNum, StreamingQueryType type) {
    query->type = type;
    query->colNum = colNum;
    return query;
}

StreamingQuery* IFCustom(StreamingQuery *query, uint8_t colNum, void* (*executeCustom)(StreamingQuery *query, void *key), CustomReturnType returnType) {
    query->type = GET_CUSTOM;
    query->colNum = colNum;
    query->executeCustom = executeCustom;
    query->returnType = returnType;
    return query;
}

StreamingQuery* is(StreamingQuery *query, SelectOperation operation, void* threshold) {
    query->operation = operation;
    query->threshold = threshold;
    return query;
}

StreamingQuery* ofLast(StreamingQuery *query, uint32_t numLastEntries) {
    query->numLastEntries = numLastEntries;
    return query;
}

StreamingQuery* then(StreamingQuery *query, void (*callback)(void* aggregateValue, void* currentValue, void* context)) {
    query->callback = callback;
    return query;
}

StreamingQuery* createStreamingQuery(embedDBState *state, embedDBSchema *schema, void* context) {
    StreamingQuery *query = (StreamingQuery*)malloc(sizeof(StreamingQuery));
    if (query != NULL) {
        query->state = state;
        query->schema = schema;
        query->context = context;
        query->IF = IF;
        query->IFCustom = IFCustom;
        query->is = is;
        query->ofLast = ofLast;
        query->then = then;
    }
    return query;
}


int8_t streamingQueryPut(StreamingQuery **queries, size_t queryCount, void *key, void *data) {
    int8_t result = embedDBPut(queries[0]->state, key, data);
    if (result != 0) {
        printf("Error inserting record\n");
        return result;
    }

    for (int i = 0; i < queryCount; i++) {
        switch (queries[i]->type) {
            case GET_AVG:
                handleGetAvg(queries[i], key, data);
                break;
            case GET_MAX:
            case GET_MIN:
                handleGetMinMax(queries[i], key, data);
                break;
            case GET_CUSTOM:
                handleCustomQuery(queries[i], key, data);
                break;
            default:
                printf("ERROR: Unsupported query type\n");
        }
    
    }

    return result;
}

float GetAvg(StreamingQuery *query, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, &allocatedValues, key);

    void* recordBuffer = op->recordBuffer;
    float* C1 = (float*)((int8_t*)recordBuffer + 0);
    // Print as csv
    exec(op);
    float avg = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return avg;
}

int32_t GetMinMax32(StreamingQuery *query, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, &allocatedValues, key);

    void* recordBuffer = op->recordBuffer;
    int32_t* C1 = (int32_t*)((int8_t*)recordBuffer + 0);
    // Print as csv
    exec(op);
    int32_t minmax = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return minmax;
}

int64_t GetMinMax64(StreamingQuery *query, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, &allocatedValues, key);

    void* recordBuffer = op->recordBuffer;
    int64_t* C1 = (int64_t*)((int8_t*)recordBuffer + 0);
    // Print as csv
    exec(op);
    int64_t minmax = *C1;
    op->close(op);
    embedDBFreeOperatorRecursive(&op);
    recordBuffer = NULL;
    for (int i = 0; i < 2; i++) {
        free(allocatedValues[i]);
    }
    free(allocatedValues);
    return minmax;
}

embedDBOperator* createOperator(StreamingQuery *query, void*** allocatedValues, void *key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    if(query->state->keySize == 4){
        uint32_t minKeyVal = *(uint32_t*)key - (query->numLastEntries-1);
        uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    }else if(query->state->keySize == 8){
        uint64_t minKeyVal = *(uint64_t*)key - (uint64_t)(query->numLastEntries-1);
        uint64_t *minKeyPtr = (uint64_t *)malloc(sizeof(uint64_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    }else{
        printf("ERROR: Unsupported key size\n");
        return NULL;
    }
    
    it->maxKey = NULL;
    it->minData = NULL;
    it->maxData = NULL;
    embedDBInitIterator(query->state, it);

    embedDBOperator* scanOp = createTableScanOperator(query->state, it, query->schema);

    embedDBAggregateFunc* aggFunc = NULL;    

    switch(query->type) {
        case GET_AVG:
            aggFunc = createAvgAggregate(query->colNum, 4);
            break;
        case GET_MAX:
            aggFunc = createMaxAggregate(query->colNum, query->schema->columnSizes[query->colNum]);
            break;
        case GET_MIN:
            aggFunc = createMinAggregate(query->colNum, query->schema->columnSizes[query->colNum]);
            break;
        default:
            printf("ERROR: Unsupported query type\n");
    }

    embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(1*sizeof(embedDBAggregateFunc));
    aggFuncs[0] = *aggFunc;
    embedDBOperator* aggOp = createAggregateOperator(scanOp, groupFunction, aggFuncs, 1);
    aggOp->init(aggOp);

    free(aggFunc);

    *allocatedValues = (void**)malloc(2 * sizeof(void*));
    ((void**)*allocatedValues)[0] = it;
    ((void**)*allocatedValues)[1] = aggFuncs;

    return aggOp;

}

int8_t groupFunction(const void* lastRecord, const void* record) {
    return 1;
}

void executeComparison(StreamingQuery* query, void *aggregateValue, Comparator comparator, void *data) {
    int8_t comparisonResult = comparator(aggregateValue, query->threshold);

    switch (query->operation) {
        case GreaterThan:
            if (comparisonResult > 0) query->callback(aggregateValue, data, query->context);
            break;
        case LessThan:
            if (comparisonResult < 0) query->callback(aggregateValue, data, query->context);
            break;
        case GreaterThanOrEqual:
            if (comparisonResult >= 0) query->callback(aggregateValue, data, query->context);
            break;
        case LessThanOrEqual:
            if (comparisonResult <= 0) query->callback(aggregateValue, data, query->context);
            break;
        case Equal:
            if (comparisonResult == 0) query->callback(aggregateValue, data, query->context);
            break;
        case NotEqual:
            if (comparisonResult != 0) query->callback(aggregateValue, data, query->context);
            break;
        default:
            printf("ERROR: Unsupported operation\n");
    }
}

void handleGetAvg(StreamingQuery* query, void* key, void *data) {
    float avg = GetAvg(query, key);
    executeComparison(query, &avg, floatComparator, data);
}

void handleGetMinMax(StreamingQuery* query, void* key, void *data) {
    int columnSize = abs(query->schema->columnSizes[query->colNum]);

    if (columnSize == 4) { // 32-bit integer
        int32_t minmax = GetMinMax32(query, key);
        executeComparison(query, &minmax, int32Comparator, data);
    } 
    else if (columnSize == 8) { // 64-bit integer
        int64_t minmax = GetMinMax64(query, key);
        executeComparison(query, &minmax, int64Comparator, data);
    } 
    else {
        printf("ERROR: Unsupported column size\n");
    }
}

void handleCustomQuery(StreamingQuery* query, void* key, void *data) {
    void* result = query->executeCustom(query, key);
    switch(query->returnType) {
        case INT32:
            executeComparison(query, result, int32Comparator, data);
            break;
        case INT64:
            executeComparison(query, result, int64Comparator, data);
            break;
        case FLOAT:
            executeComparison(query, result, floatComparator, data);
            break;
        case DOUBLE:
            executeComparison(query, result, doubleComparator, data);
            break;
        default:
            printf("ERROR: Unsupported return type\n");
    }
}