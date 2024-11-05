#include <embedDB/embedDB.h>
#include "advancedQueries.h"

typedef enum {
    GET_AVG,
    GET_MAX,
    GET_MIN
} StreamingQueryType;

typedef enum {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Equal,
    NotEqual
} SelectOperation;

typedef struct StreamingQuery {
    uint32_t numLastEntries;
    void* threshold;
    embedDBSchema *schema;        
    embedDBState *state;
    StreamingQueryType type;
    SelectOperation operation;
    uint8_t colNum;
    void (*callback)(const float value);

    struct StreamingQuery* (*SQIF)(struct StreamingQuery *query, uint8_t colNum, StreamingQueryType type);
    struct StreamingQuery* (*is)(struct StreamingQuery *query, SelectOperation operation, void* threshold);
    struct StreamingQuery* (*forLast)(struct StreamingQuery *query, uint32_t numLastEntries);
    struct StreamingQuery* (*then)(struct StreamingQuery *query, void (*callback)(const float value));
} StreamingQuery;

StreamingQuery* SQIF(StreamingQuery *query, uint8_t colNum, StreamingQueryType type);
StreamingQuery* is(StreamingQuery *query, SelectOperation operation, void* threshold);
StreamingQuery* forLast(StreamingQuery *query, uint32_t numLastEntries);
StreamingQuery* then(StreamingQuery *query, void (*callback)(const float value));
StreamingQuery* createStreamingQuery(embedDBState *state, embedDBSchema *schema);


int8_t streamingQueryPut(StreamingQuery *query, void *key, void *data);

/**
 * @brief Checks if the average of n last records is greater than a given threshold
 */
float GetAvg(StreamingQuery *query, embedDBState *state, void *key);
int32_t GetMinMax32(StreamingQuery *query, embedDBState *state, void *key);
int64_t GetMinMax64(StreamingQuery *query, embedDBState *state, void *key);
embedDBOperator* createOperator(StreamingQuery *query, embedDBState* state, void*** allocatedValues, void *key);
int8_t groupFunction(const void* lastRecord, const void* record);

