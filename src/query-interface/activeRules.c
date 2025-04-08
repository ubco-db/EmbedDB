#include "activeRules.h"

activeRule* IF(activeRule *rule, uint8_t colNum, ActiveQueryType type) {
    rule->type = type;
    rule->colNum = colNum;
    return rule;
}

activeRule* IFCustom(activeRule *rule, uint8_t colNum, void* (*executeCustom)(activeRule *rule, void *key), CustomReturnType returnType) {
    rule->type = GET_CUSTOM;
    rule->colNum = colNum;
    rule->executeCustom = executeCustom;
    rule->returnType = returnType;
    return rule;
}

activeRule* is(activeRule *rule, SelectOperation operation, void* threshold) {
    rule->operation = operation;
    rule->threshold = threshold;
    return rule;
}

activeRule* ofLast(activeRule *rule, void* numLastEntries) {
    rule->numLastEntries = numLastEntries;
    return rule;
}

activeRule* where(activeRule *rule, void* minData, void* maxData) {
    rule->minData = minData;
    rule->maxData = maxData;
    return rule;
}

activeRule* then(activeRule *rule, void (*callback)(void* aggregateValue, void* currentValue, void* context)) {
    rule->callback = callback;
    return rule;
}

activeRule* createActiveRule(embedDBSchema *schema, void* context) {
    activeRule *rule = (activeRule*)malloc(sizeof(activeRule));
    if (rule != NULL) {
        rule->minData = NULL; // Default to no min data
        rule->maxData = NULL; // Default to no max data
        rule->schema = copySchema(schema);
        rule->context = context;
        rule->IF = IF;
        rule->IFCustom = IFCustom;
        rule->is = is;
        rule->ofLast = ofLast;
        rule->where = where;
        rule->then = then;
        rule->enabled = true; // Default to enabled
    }
    return rule;
}




void executeRules(embedDBState* state, void *key, void *data) {
    for (int i = 0; i < state->numRules; i++) {
        if(state->rules[i]->enabled == false) {
            continue; // Skip disabled rules
        }
        switch (state->rules[i]->type) {
            case GET_AVG:
                handleGetAvg(state, state->rules[i], key, data);
                break;
            case GET_MAX:
            case GET_MIN:
                handleGetMinMax(state, state->rules[i], key, data);
                break;
            case GET_CUSTOM:
                handleCustomQuery(state, state->rules[i], key, data);
                break;
            default:
                printf("ERROR: Unsupported rule type\n");
        }
    
    }
}

float GetAvg(embedDBState *state, activeRule *rule, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(state, rule, &allocatedValues, key);

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

int32_t GetMinMax32(embedDBState *state, activeRule *rule, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(state, rule, &allocatedValues, key);

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

int64_t GetMinMax64(embedDBState *state, activeRule *rule, void *key) {
    void** allocatedValues;
    embedDBOperator* op = createOperator(state, rule, &allocatedValues, key);

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

embedDBOperator* createOperator(embedDBState *state, activeRule * rule, void*** allocatedValues, void *key) {
    embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));
    if(state->keySize == 4){
        uint32_t minKeyVal = *(uint32_t*)key - (*(uint32_t*)rule->numLastEntries - 1);
        uint32_t *minKeyPtr = (uint32_t *)malloc(sizeof(uint32_t));
        if (minKeyPtr != NULL) {
            *minKeyPtr = minKeyVal;
            it->minKey = minKeyPtr;
        }
    }else if(state->keySize == 8){
        uint64_t minKeyVal = *(uint64_t*)key - (*(uint64_t*)rule->numLastEntries - 1);
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
    it->minData = rule->minData;
    it->maxData = rule->maxData;
    embedDBInitIterator(state, it);

    embedDBOperator* scanOp = createTableScanOperator(state, it, rule->schema);

    embedDBAggregateFunc* aggFunc = NULL;    

    switch(rule->type) {
        case GET_AVG:
            aggFunc = createAvgAggregate(rule->colNum, 4);
            break;
        case GET_MAX:
            aggFunc = createMaxAggregate(rule->colNum, rule->schema->columnSizes[rule->colNum]);
            break;
        case GET_MIN:
            aggFunc = createMinAggregate(rule->colNum, rule->schema->columnSizes[rule->colNum]);
            break;
        default:
            printf("ERROR: Unsupported rule type\n");
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

void executeComparison(activeRule* rule, void *aggregateValue, Comparator comparator, void *data) {
    int8_t comparisonResult = comparator(aggregateValue, rule->threshold);

    switch (rule->operation) {
        case GreaterThan:
            if (comparisonResult > 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case LessThan:
            if (comparisonResult < 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case GreaterThanOrEqual:
            if (comparisonResult >= 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case LessThanOrEqual:
            if (comparisonResult <= 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case Equal:
            if (comparisonResult == 0) rule->callback(aggregateValue, data, rule->context);
            break;
        case NotEqual:
            if (comparisonResult != 0) rule->callback(aggregateValue, data, rule->context);
            break;
        default:
            printf("ERROR: Unsupported operation\n");
    }
}

void handleGetAvg(embedDBState *state, activeRule *rule, void* key, void *data) {
    float avg = GetAvg(state, rule, key);
    executeComparison(rule, &avg, floatComparator, data);
}

void handleGetMinMax(embedDBState *state, activeRule *rule, void* key, void *data) {
    int columnSize = abs(rule->schema->columnSizes[rule->colNum]);

    if (columnSize == 4) { // 32-bit integer
        int32_t minmax = GetMinMax32(state, rule, key);
        executeComparison(rule, &minmax, int32Comparator, data);
    } 
    else if (columnSize == 8) { // 64-bit integer
        int64_t minmax = GetMinMax64(state, rule, key);
        executeComparison(rule, &minmax, int64Comparator, data);
    } 
    else {
        printf("ERROR: Unsupported column size\n");
    }
}

void handleCustomQuery(embedDBState *state, activeRule *rule, void* key, void *data) {
    void* result = rule->executeCustom(rule, key);
    switch(rule->returnType) {
        case DBINT32:
            executeComparison(rule, result, int32Comparator, data);
            break;
        case DBINT64:
            executeComparison(rule, result, int64Comparator, data);
            break;
        case DBFLOAT:
            executeComparison(rule, result, floatComparator, data);
            break;
        case DBDOUBLE:
            executeComparison(rule, result, doubleComparator, data);
            break;
        default:
            printf("ERROR: Unsupported return type\n");
    }
}
