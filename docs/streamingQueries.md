# Streaming Queries in EmbedDB

### Streaming Queries Overview
Streaming queries are used to dynamically monitor and act on data based on specific conditions. Each time new data is added, the system checks if the defined criteria are met and triggers a callback function if they are. 

---

### How to Create a Streaming Query
To define a streaming query, use the following code structure:

```cpp
StreamingQuery* query = createStreamingQuery(state, schema, context);
query->IF(query, columnNumber, GET_AVG)
     ->ofLast(query, numLastEntries)
     ->is(query, conditionOperation, &threshold)
     ->then(query, callbackFunction);

streamingQueryPut(queries, numQueries, &key, dataPtr);
```

#### Parameters:
- **`state`**: The `embeddb` state object for database interaction.
- **`schema`**: Schema definition of your table.
- **`context`**: A structure containing variables accessible within your callback function.

**Example Context Definition:**
```cpp
typedef struct {
    int int1;
    int int2;
    float array[10];
    float float1;
} CallbackContext;

CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
```

#### Query Components:
1. **`columnNumber`**: The target column in the schema for the query.
2. **`GET_AVG`**: Predefined aggregate functions like `GET_AVG`, `GET_MAX`, and `GET_MIN`. You can also define custom aggregates with `IFCustom`.
3. **`numLastEntries`**: The sliding window size (entries to include in the aggregate). Note: This operates on keys. If keys do not increment by 1, the exact range is calculated as `(currentKey - (numLastEntries - 1))`.
4. **`conditionOperation`**: The condition to compare the aggregate result with the threshold. Options: 
   - `GreaterThan`, `LessThan`
   - `GreaterThanOrEqual`, `LessThanOrEqual`
   - `Equal`, `NotEqual`
5. **`threshold`**: The comparison value for the condition. The type must match the aggregate result (e.g., `float` for `GET_AVG`).
6. **`callbackFunction`**: A user-defined function triggered when the condition is met.

---

### Custom Aggregate Queries
For custom logic, use the `IFCustom` method to define your own aggregate operation. Example:
```cpp
void* GetWeightedAverage(StreamingQuery* query, void* key) {
    int currentKey = *(int*)key;
    int slidingWindowStart = currentKey - (query->numLastEntries - 1);

    float totalWeight = 0;
    float weightedSum = 0;

    for (int k = slidingWindowStart; k <= currentKey; k++) {
        int32_t record = 0;
        if (embedDBGet(state, (void*)&k, (void*)&record) != 0) {
            continue;
        }
        float weight = query->numLastEntries - 1 - (currentKey - k);  // Linear decay
        if (weight > 0) {
            weightedSum += record * weight;
            totalWeight += weight;
        }
    }

    float* weightedAverage = (float*)malloc(sizeof(float));
    *weightedAverage = weightedSum / totalWeight;
    return (void*)weightedAverage;
}
```

To integrate this aggregate function:
```cpp
query->IFCustom(query, columnNumber, GetWeightedAverage, FLOAT)
     ->ofLast(query, 10)
     ->is(query, GreaterThan, (void*)&threshold)
     ->then(query, callbackFunction);
```

---

### Callback Function
The callback function is executed when the query criteria are met. It receives:
1. **`aggregateValue`**: Result of the query’s aggregate function.
2. **`currentValue`**: Value of the most recent data.
3. **`context`**: User-defined structure passed during query creation.

Example callback:
```cpp
void callbackFunction(void* aggregateValue, void* currentValue, void* ctx) {
    CallbackContext* context = (CallbackContext*)ctx;
    context->float1 = *(float*)aggregateValue;  // Store value for future queries or to use outside of the callback function
}
```

---

### Chaining Queries
You can chain queries by sharing context variables between them. For example:

*Assuming for this example your keys are timestamps in seconds*
1. Query 1 calculates a weighted average over the last 10 seconds and stores it in `float1` according to above callback function. By carefully choosing the `is` method conditions you can ensure the callback function is always called ensuring the `float1` is always updated with the most recent weighted average:
    ```cpp
    queries[0]->IFCustom(queries[0], 1, GetWeightedAverage, FLOAT)
               ->ofLast(queries[0], 10)
               ->is(queries[0], GreaterThanOrEqual, (void*)&value)
               ->then(queries[0], callbackFunction);
    ```

2. Query 2 compares this weighted average with the average of the last 10 seconds and calls a different function if the weighted average is less than or equal to the average:
    ```cpp
    queries[1]->IF(queries[1], 1, GET_AVG)
               ->ofLast(queries[1], 10)
               ->is(queries[1], LessThanOrEqual, (void*)&(context->float1))
               ->then(queries[1], anotherCallbackFunction);
    ```

---

### Example Workflow
1. Define your schema, state, and context.
2. Create and configure your queries.
3. Push data using `streamingQueryPut`.

Example:
```cpp
for (int32_t i = 2; i < 22; i += 2) {
    *((uint32_t*)dataPtr) = data[j++];
    streamingQueryPut(queries, 2, &i, dataPtr);
}
```

Each time data is pushed, all queries are evaluated (in the order they are inserted into the `queries` array) against the new data, and any matching conditions trigger their respective callbacks.

## Generalized SQL Queries and Their Conversion

### 1. SELECT with Aggregation

#### SQL Query
```sql
SELECT AVG(column) FROM table WHERE key BETWEEN currentKey - (numLastEntries - 1) AND currentKey HAVING AVG(column) >= threshold;
```

#### Conversion
To perform an average aggregation on a column with a condition, use the `GET_AVG` type in `streamingQueries`.

```cpp
StreamingQuery* query = createStreamingQuery(state, schema, context);
query->IF(query, columnNumber, GET_AVG)
    ->ofLast(query, numLastEntries)
    ->is(query, GreaterThanOrEqual, &threshold)
    ->then(query, callbackFunction);
```
`GET_AVG` could be replaced by `GET_MIN`, or `GET_MAX`. `GreaterThanOrEqual` could be replaced by `LessThanOrEqual`, `GreaterThan`, `LessThan`, `Equal`, or `NotEqual`.

### 3. Custom Aggregation

#### SQL Query
```sql
SELECT custom_aggregation(column) FROM table WHERE key BETWEEN currentKey - (numLastEntries - 1) AND currentKey HAVING custom_aggregation(column) >= threshold;
```

#### Conversion
For custom aggregations, use the `GET_CUSTOM` type and provide a custom execution function.

```cpp
StreamingQuery* query = createStreamingQuery(state, schema, context);
query->IFCustom(query, columnNumber, custom_aggregation, returnType)
    ->ofLast(query, numLastEntries)
    ->is(query, GreaterThanOrEqual, &threshold)
    ->then(query, callbackFunction);
```
Again, `GreaterThanOrEqual` could be replaced by `LessThanOrEqual`, `GreaterThan`, `LessThan`, `Equal`, or `NotEqual`.


### 4. Multiple Queries

#### SQL Query
```sql
SELECT aggregate1(columnNumber1), aggregate2(columnNumber2) FROM table WHERE key BETWEEN currentKey - (numLastEntries - 1) AND currentKey HAVING aggregate1(columnNumber1) >= threshold1 AND aggregate2(columnNumber2) <= threshold2;
```

#### Conversion
To perform multiple queries, create multiple `StreamingQuery` objects and execute them sequentially.

```cpp
typedef struct {
    bool agg1GTEThreshold1
    float agg1Result;
} CallbackContext;

void* aggregate1(StreamingQuery* query, void* key){
    //Some aggregate function
    CallbackContext* context = (CallbackContext*)(query->context);
    context->agg1GTEThreshold1 = true; //set to false before comparison with threshold
    return (void*)result;
}

int main(){
    //Allocate memory for queries & context
    CallbackContext* context = (CallbackContext*)malloc(sizeof(CallbackContext));
    StreamingQuery **queries = (StreamingQuery**)malloc(2*sizeof(StreamingQuery*));

    //Set up queries
    StreamingQuery* queries[0] = createStreamingQuery(state, schema, context);
    queries[0]->IFCustom(queries[0], columnNumber1, aggregate1, FLOAT)
        ->ofLast(queries[0], numLastEntries)
        ->is(queries[0], GreaterThanOrEqual, &threshold1)
        ->then(queries[0], [](void* aggValue, void* currentValue, void* ctx){
            CallbackContext* context = (CallbackContext*)ctx;
            context->agg1GTEThreshold1 = true;
            context->agg1Result = *(float*)aggValue;
    });

    StreamingQuery* queries[1] = createStreamingQuery(state, schema, context);
    queries[1]->IFCustom(queries[1], columnNumber2, aggregate1, FLOAT)
        ->ofLast(queries[1], numLastEntries)
        ->is(queries[1], LessThanOrEqual, &threshold2)
        ->then(queries[1], [](void* aggValue, void* currentValue, void* ctx){
            CallbackContext* context = (CallbackContext*)ctx;
            if(context->agg1GTEThreshold1){
                //whatever you want to do if aggregate1(columnNumber1) >= threshold1 and aggregate2(columnNumber2) <= threshold2
            }
        });

    //Insert data
    streamingQueryPut(queries, 2, &key, dataPtr);
}
```

## Untested but should work

### 1. Queries Across Tables

#### SQL Query

```sql
SELECT 
    aggregate1(table1.columnNumber1), 
    aggregate2(table2.columnNumber1)
FROM 
    table1, table2
WHERE 
    table1.key BETWEEN currentKey - (numLastEntries - 1) AND currentKey
    AND table2.key BETWEEN currentKey - (numLastEntries - 1) AND currentKey
HAVING 
    aggregate1(table1.columnNumber1) >= threshold1 
    AND aggregate2(table2.columnNumber1) <= threshold2;
```

#### Conversion
Same as the [Multiple Queries](#4-multiple-queries) code but use the schema and state for `table1` in the first query and the schema and state for `table2` in the second query. Note that the first state is the one used to insert data so ensure the state you want to insert data into is used in the first query. 

### 2. Group By

By creating a custom aggregate function with the aide of [advancedQueries' Group By function](advancedQueries.md), creating a streaming query with a Group By clause should be possible.

### 3. COUNT

Use [advancedQueries](advancedQueries.md) to create a custom function to count the number rows matching a chosen criteria. Create a custom `streamingQuery` with the function and chosen key range.

## Unsupported SQL Queries

### 4. LIMIT Queries
EmbedDB does not support the SQL `LIMIT` function that returns a certain number of rows. EmbedDB only supports queries on rows withing a key range.

# Conclusion
In general, if a query can be created with the aide of `advancedQueries`, it can be turned into a streaming query over a key range using `streamingQueries`. 