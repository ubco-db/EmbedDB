#ifndef _ACTIVERULES_H
#define _ACTIVERULES_H

#if defined(__cplusplus)
extern "C" {
#endif

#include "advancedQueries.h"
// #include <embedDB/embedDB.h>
#include <embedDBUtility.h>
#include <string.h>

/**
 * @enum ActiveQueryType
 * @brief Enum representing the type of active rule.
 */
typedef enum {
    GET_AVG,    /**< Get average value */
    GET_MAX,    /**< Get maximum value */
    GET_MIN,     /**< Get minimum value */
    GET_CUSTOM  /**< Perform custom rule */
} ActiveQueryType;

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

typedef enum {
    DBINT32,
    DBINT64,
    DBFLOAT,
    DBDOUBLE
} CustomReturnType;

/**
 * @struct activeRule
 * @brief Struct representing a active rule.
 */
typedef struct activeRule {
    void* numLastEntries;    /**< Number of last entries to consider */
    void* threshold;            /**< Threshold value for comparison */
    embedDBSchema *schema;      /**< Schema of the database */
    ActiveQueryType type;    /**< Type of the active rule */
    SelectOperation operation;  /**< Selection operation */
    uint8_t colNum;             /**< Column number to preform rule on*/
    void* context;              /**< Context for callback function */
    void (*callback)(void* aggregateValue, void* currentValue, void* context);  /**< Callback function */
    void* (*executeCustom)(struct activeRule *rule, void *key); /**< Execute custom rule */
    CustomReturnType returnType; /**< Return type of custom rule */

    void* minData;              /**< Minimum data value */
    void* maxData;              /**< Maximum data value */

    bool enabled;           /**< Flag to indicate if the rule is enabled */

    struct activeRule* (*IF)(struct activeRule *rule, uint8_t colNum, ActiveQueryType type);
    struct activeRule* (*IFCustom)(struct activeRule *rule, uint8_t colNum, void* (*executeCustom)(struct activeRule *rule, void *key), CustomReturnType returnType);
    struct activeRule* (*ofLast)(struct activeRule *rule, void* numLastEntries);
    struct activeRule* (*where)(struct activeRule *rule, void* minData, void* maxData);
    struct activeRule* (*is)(struct activeRule *rule, SelectOperation operation, void* threshold);
    struct activeRule* (*then)(struct activeRule *rule, void (*callback)(void* aggregateValue, void* currentValue, void* context));
} activeRule;

/**
 * @brief Comparator function pointer type.
 * @param value1 Pointer to the first value.
 * @param value2 Pointer to the second value.
 * @return Comparison result.
 */
typedef int8_t (*Comparator)(void* value1, void* value2);

/**
 * @brief IF method for setting the column the rule will perform on and the type of rule
 * @param rule Pointer to the activeRule.
 * @param colNum Column number to perform rule on.
 * @param type Type of the active rule (Avg, max, min, custom).
 * @return Pointer to the activeRule.
 */
activeRule* IF(activeRule *rule, uint8_t colNum, ActiveQueryType type);

/**
 * @brief IF method for setting the column a custom rule will be performed on.
 * @param rule Pointer to the activeRule.
 * @param colNum Column number to perform rule on.
 * @param executeCustom Custom rule function.
 * @param returnType Return type of the custom rule.
 * @return Pointer to the activeRule.
 */
activeRule* IFCustom(activeRule *rule, uint8_t colNum, void* (*executeCustom)(activeRule *rule, void *key), CustomReturnType returnType);

/**
 * @brief is method for setting the selection operation and value to compare rule result with.
 * @param rule Pointer to the activeRule.
 * @param operation Selection operation (<, >, <=, >=, =, !=).
 * @param threshold Value to compare rule result with.
 * @return Pointer to the activeRule.
 */
activeRule* is(activeRule *rule, SelectOperation operation, void* threshold);

/**
 * @brief ofLast method for setting the number of last entries to consider.
 * @param rule Pointer to the activeRule.
 * @param numLastEntries Number of last entries.
 * @return Pointer to the activeRule.
 */
activeRule* forLast(activeRule *rule, void* numLastEntries);

/**
 * @brief where method for setting the range of values to consider.
 * @param rule Pointer to the activeRule.
 * @param minData minimum value considered.
 * @param maxData maximum value considered.
 */
activeRule* where(activeRule *rule, void* minData, void* maxData);

/**
 * @brief then method for setting the callback function.
 * @param rule Pointer to the activeRule.
 * @param callback Callback function.
 * @return Pointer to the activeRule.
 */
activeRule* then(activeRule *rule, void (*callback)(void* aggregateValue, void* currentValue, void* context));

/**
 * @brief Creates a new activeRule.
 * @param state Pointer to the embedDBState.
 * @param schema Pointer to the embedDBSchema.
 * @return Pointer to the newly created activeRule.
 */
activeRule* createActiveRule(embedDBSchema *schema, void* context);

/**
 * @brief Inserts a record, performs a rule on that record and the last n specified records (numLastEntries), 
 * compares rule result with threshold, and calls callback function if comparison returns true.
 * @param rule Pointer to the activeRule.
 * @param key Pointer to the key.
 * @param data Pointer to the data.
 * @return Result of the insertion operation (0 or 1).
 */
void executeRules(embedDBState *state, void *key, void *data);

/**
 * @brief Gets the average value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given active rule and key, executes the operator,
 * and retrieves the average value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param rule A pointer to the activeRule structure.
 * @param key A pointer to the key used for the rule.
 * @return The average value retrieved from the rule.
 */
float GetAvg(embedDBState *state, activeRule *rule, void *key);

/**
 * @brief Gets the minimum or maximum 32-bit integer value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given active rule and key, executes the operator,
 * and retrieves the minimum or maximum value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param rule A pointer to the activeRule structure.
 * @param key A pointer to the key used for the rule.
 * @return The minimum or maximum 32-bit integer value retrieved from the rule.
 */
int32_t GetMinMax32(embedDBState *state, activeRule *rule, void *key);

/**
 * @brief Gets the minimum or maximum 64-bit integer value of last n specified records (numLastEntries) including current value.
 *
 * This function creates an operator for the given active rule and key, executes the operator,
 * and retrieves the minimum or maximum value from the result. It then cleans up the allocated resources
 * and returns the retrieved value.
 *
 * @param rule A pointer to the activeRule structure.
 * @param key A pointer to the key used for the rule.
 * @return The minimum or maximum 64-bit integer value retrieved from the rule.
 */
int64_t GetMinMax64(embedDBState *state, activeRule *rule, void *key);

/**
 * @brief Creates an operator for executing a active rule.
 *
 * This function initializes an iterator and sets up the necessary
 * aggregate function based on the rule type. It supports queries
 * for average, maximum, and minimum values.
 *
 * @param rule A pointer to the activeRule structure containing the rule details.
 * @param allocatedValues A pointer to a pointer where allocated values will be stored.
 *                        This will be used to store the iterator and aggregate functions.
 * @param key A pointer to the key used for the rule.
 * 
 * @return A pointer to the created embedDBOperator structure.
 *         Returns NULL if an unsupported key size is encountered.
 */
embedDBOperator* createOperator(embedDBState *state, activeRule *rule, void*** allocatedValues, void *key);

/**
 * @brief Group function for the aggregate operator.
 * @param lastRecord Pointer to the last record.
 * @param record Pointer to the current record.
 * @return Always returns 1.
 */
int8_t groupFunction(const void* lastRecord, const void* record);

/**
 * @brief Executes a comparison operation for a active rule.
 *
 * This function performs a comparison between a given value and the rule's threshold
 * using the specified comparator function. Depending on the result of the comparison,
 * it invokes the rule's callback function if the condition is met.
 *
 * @param rule Pointer to the activeRule structure.
 * @param value Pointer to the value to be compared.
 * @param comparator Function pointer to the comparator function.
 */
void executeComparison(activeRule* rule, void *value, Comparator comparator, void *data);

/**
 * @brief Handles the average value retrieval and comparison for a active rule.
 *
 * This function retrieves the average value for the last n specified records (numLastEntries) including current value and
 * performs a comparison using the executeComparison function.
 *
 * @param rule Pointer to the activeRule structure.
 * @param key Pointer to the key for the current record.
 */
void handleGetAvg(embedDBState *state, activeRule* rule, void* key, void *data);

/**
 * @brief Handles the minimum or maximum value retrieval and comparison for a active rule.
 *
 * This function retrieves the minimum or maximum value of the last n specified records (numLastEntries) including current value 
 * performs a comparison using the executeComparison function.
 *
 * @param rule Pointer to the activeRule structure.
 * @param key Pointer to the key for the current record.
 */
void handleGetMinMax(embedDBState *state, activeRule* rule, void* key, void *data);

/**
 * @brief Handles a custom active rule and executes the appropriate comparison based on the return type.
 *
 * @param rule A pointer to the activeRule structure containing the rule details.
 * @param key A pointer to the key used for executing the custom rule.
 *
 * The function executes the custom rule using the provided key and then performs a comparison
 * based on the return type specified in the rule. Supported return types include DBINT32, DBINT64,
 * DBFLOAT, and DBDOUBLE. If the return type is unsupported, an error message is printed.
 */
void handleCustomQuery(embedDBState *state, activeRule* rule, void* key, void *data);


#ifdef __cplusplus
}
#endif
#endif // _ACTIVERULES_H
