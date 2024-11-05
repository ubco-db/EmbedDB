#include "streamingQueries.h"

StreamingQuery* SQIF(StreamingQuery *query, uint8_t colNum, StreamingQueryType type) {
    query->type = type;
    query->colNum = colNum;
    return query;
}

StreamingQuery* is(StreamingQuery *query, SelectOperation operation, void* threshold) {
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

    switch(query->type) {
        case GET_AVG:
            float avg = GetAvg(query, query->state, key);
            switch(query->operation) {
                case GreaterThan:
                    if (avg > *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                case LessThan:
                    if (avg < *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                case GreaterThanOrEqual:
                    if (avg >= *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                case LessThanOrEqual:
                    if (avg <= *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                case Equal:
                    if (avg == *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                case NotEqual:
                    if (avg != *(float*)(query->threshold)) {
                        query->callback(avg);
                    }
                    break;
                default:
                    printf("ERROR: Unsupported operation\n");
            }
            break;
        case GET_MAX:
        case GET_MIN:
            if(abs(query->schema->columnSizes[query->colNum]) == 4){
                int32_t minmax = GetMinMax32(query, query->state, key);
                switch(query->operation) {
                    case GreaterThan:
                        if (minmax > *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case LessThan:
                        if (minmax < *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case GreaterThanOrEqual:
                        if (minmax >= *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case LessThanOrEqual:
                        if (minmax <= *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case Equal:
                        if (minmax == *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case NotEqual:
                        if (minmax != *(int32_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    default:
                        printf("ERROR: Unsupported operation\n");
                }
            }
            else if(abs(query->schema->columnSizes[query->colNum]) == 8){
                int64_t minmax = GetMinMax64(query, query->state, key);
                switch(query->operation) {
                    case GreaterThan:
                        if (minmax > *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case LessThan:
                        if (minmax < *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case GreaterThanOrEqual:
                        if (minmax >= *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case LessThanOrEqual:
                        if (minmax <= *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case Equal:
                        if (minmax == *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    case NotEqual:
                        if (minmax != *(int64_t*)(query->threshold)) {
                            query->callback(minmax);
                        }
                        break;
                    default:
                        printf("ERROR: Unsupported operation\n");
                }
            }
            else
                printf("ERROR: Unsupported column size\n");
            break;
        default:
            printf("ERROR: Unsupported query type\n");
    }

    return result;
}

float GetAvg(StreamingQuery *query, embedDBState *state, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, state, &allocatedValues, key);

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

int32_t GetMinMax32(StreamingQuery *query, embedDBState *state, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, state, &allocatedValues, key);

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

int64_t GetMinMax64(StreamingQuery *query, embedDBState *state, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(query, state, &allocatedValues, key);

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

embedDBOperator* createOperator(StreamingQuery *query, embedDBState* state, void*** allocatedValues, void *key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    if(state->keySize == 4){
        uint32_t minKeyVal = *(uint32_t*)key - (query->numLastEntries-1);
        uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    }else if(state->keySize == 8){
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
    embedDBInitIterator(state, it);

    embedDBOperator* scanOp = createTableScanOperator(state, it, query->schema);

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