#ifndef EMBEDDB_STREAMING_QUERIES_H_
#define EMBEDDB_STREAMING_QUERIES_H_

#include "advancedQueries.h"
#include <embedDB/embedDB.h>
#include <embedDBUtility.h>
#include <string.h>

/**
 * @enum StreamingQueryType
 * @brief Enum representing the type of streaming query.
 */
typedef enum {
    GET_AVG,    /**< Get average value */
    GET_MAX,    /**< Get maximum value */
    GET_MIN     /**< Get minimum value */
} StreamingQueryType;

/**
 * @enum SelectOperation
 * @brief Enum representing the selection operation.
 */
typedef enum {
    GreaterThan,            /**< Greater than operation */
    LessThan,               /**< Less than operation */
    GreaterThanOrEqual,     /**< Greater than or equal operation */
    LessThanOrEqual,        /**< Less than or equal operation */
    Equal,                  /**< Equal operation */
    NotEqual                /**< Not equal operation */
} SelectOperation;

/**
 * @struct StreamingQuery
 * @brief Struct representing a streaming query.
 */
typedef struct StreamingQuery {
    uint32_t numLastEntries;    /**< Number of last entries to consider */
    void* threshold;            /**< Threshold value for comparison */
    embedDBSchema *schema;      /**< Schema of the database */
    embedDBState *state;        /**< State of the database */
    StreamingQueryType type;    /**< Type of the streaming query */
    SelectOperation operation;  /**< Selection operation */
    uint8_t colNum;             /**< Column number to preform query on*/
    void (*callback)(void* value);  /**< Callback function */

    struct StreamingQuery* (*IF)(struct StreamingQuery *query, uint8_t colNum, StreamingQueryType type);
    struct StreamingQuery* (*is)(struct StreamingQuery *query, SelectOperation operation, void* threshold);
    struct StreamingQuery* (*forLast)(struct StreamingQuery *query, uint32_t numLastEntries);
    struct StreamingQuery* (*then)(struct StreamingQuery *query, void (*callback)(void* value));
} StreamingQuery;

/**
 * @brief Comparator function pointer type.
 * @param value1 Pointer to the first value.
 * @param value2 Pointer to the second value.
 * @return Comparison result.
 */
typedef int8_t (*Comparator)(void* value1, void* value2);

/**
 * @brief IF method for setting the column the query will perform on and the type of query
 * @param query Pointer to the StreamingQuery.
 * @param colNum Column number to perform query on.
 * @param type Type of the streaming query (Avg, max, min, custom).
 * @return Pointer to the StreamingQuery.
 */
StreamingQuery* IF(StreamingQuery *query, uint8_t colNum, StreamingQueryType type);

/**
 * @brief is method for setting the selection operation and value to compare query result with.
 * @param query Pointer to the StreamingQuery.
 * @param operation Selection operation (<, >, <=, >=, =, !=).
 * @param threshold Value to compare query result with.
 * @return Pointer to the StreamingQuery.
 */
StreamingQuery* is(StreamingQuery *query, SelectOperation operation, void* threshold);

/**
 * @brief forLast method for setting the number of last entries to consider.
 * @param query Pointer to the StreamingQuery.
 * @param numLastEntries Number of last entries.
 * @return Pointer to the StreamingQuery.
 */
StreamingQuery* forLast(StreamingQuery *query, uint32_t numLastEntries);

/**
 * @brief then method for setting the callback function.
 * @param query Pointer to the StreamingQuery.
 * @param callback Callback function.
 * @return Pointer to the StreamingQuery.
 */
StreamingQuery* then(StreamingQuery *query, void (*callback)(void* value));

/**
 * @brief Creates a new StreamingQuery.
 * @param state Pointer to the embedDBState.
 * @param schema Pointer to the embedDBSchema.
 * @return Pointer to the newly created StreamingQuery.
 */
StreamingQuery* createStreamingQuery(embedDBState *state, embedDBSchema *schema);

/**
 * @brief Inserts a record, performs a query on that record and the last n specified records (numLastEntries), 
 * compares query result with threshold, and calls callback function if comparison returns true.
 * @param query Pointer to the StreamingQuery.
 * @param key Pointer to the key.
 * @param data Pointer to the data.
 * @return Result of the insertion operation (0 or 1).
 */
int8_t streamingQueryPut(StreamingQuery *query, void *key, void *data);

/**
 * @brief Gets the average value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given streaming query and key, executes the operator,
 * and retrieves the average value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param query A pointer to the StreamingQuery structure.
 * @param key A pointer to the key used for the query.
 * @return The average value retrieved from the query.
 */
float GetAvg(StreamingQuery *query, void *key);

/**
 * @brief Gets the minimum or maximum 32-bit integer value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given streaming query and key, executes the operator,
 * and retrieves the minimum or maximum value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param query A pointer to the StreamingQuery structure.
 * @param key A pointer to the key used for the query.
 * @return The minimum or maximum 32-bit integer value retrieved from the query.
 */
int32_t GetMinMax32(StreamingQuery *query, void *key);

/**
 * @brief Gets the minimum or maximum 64-bit integer value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given streaming query and key, executes the operator,
 * and retrieves the minimum or maximum value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param query A pointer to the StreamingQuery structure.
 * @param key A pointer to the key used for the query.
 * @return The minimum or maximum 64-bit integer value retrieved from the query.
 */
int64_t GetMinMax64(StreamingQuery *query, void *key);

/**
 * @brief Creates an operator for executing a streaming query.
 *
 * This function initializes an iterator and sets up the necessary
 * aggregate function based on the query type. It supports queries
 * for average, maximum, and minimum values.
 *
 * @param query A pointer to the StreamingQuery structure containing the query details.
 * @param allocatedValues A pointer to a pointer where allocated values will be stored.
 *                        This will be used to store the iterator and aggregate functions.
 * @param key A pointer to the key used for the query.
 * 
 * @return A pointer to the created embedDBOperator structure.
 *         Returns NULL if an unsupported key size is encountered.
 */
embedDBOperator* createOperator(StreamingQuery *query, void*** allocatedValues, void *key);

/**
 * @brief Group function for the aggregate operator.
 * @param lastRecord Pointer to the last record.
 * @param record Pointer to the current record.
 * @return Always returns 1.
 */
int8_t groupFunction(const void* lastRecord, const void* record);

/**
 * @brief Executes a comparison operation for a streaming query.
 *
 * This function performs a comparison between a given value and the query's threshold
 * using the specified comparator function. Depending on the result of the comparison,
 * it invokes the query's callback function if the condition is met.
 *
 * @param query Pointer to the StreamingQuery structure.
 * @param value Pointer to the value to be compared.
 * @param comparator Function pointer to the comparator function.
 */
void executeComparison(StreamingQuery* query, void *value, Comparator comparator);

/**
 * @brief Handles the average value retrieval and comparison for a streaming query.
 *
 * This function retrieves the average value for the last n specified records (numLastEntries) including current value and
 * performs a comparison using the executeComparison function.
 *
 * @param query Pointer to the StreamingQuery structure.
 * @param key Pointer to the key for the current record.
 */
void handleGetAvg(StreamingQuery* query, void* key);

/**
 * @brief Handles the minimum or maximum value retrieval and comparison for a streaming query.
 *
 * This function retrieves the minimum or maximum value of the last n specified records (numLastEntries) including current value 
 * performs a comparison using the executeComparison function.
 *
 * @param query Pointer to the StreamingQuery structure.
 * @param key Pointer to the key for the current record.
 */
void handleGetMinMax(StreamingQuery* query, void* key);



#endif // EMBEDDB_STREAMING_QUERIES_H_
