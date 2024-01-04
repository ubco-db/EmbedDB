# Usage & Setup Documentation

This guide provides comprehensive instructions for configuring, initializing, and using EmbedDB in your application, covering everything from setup, record management, to teardown. 

## Table of Contents

-   [Configure EmbedDB State](#configure-records)
    -   [Create an EmbedDB state](#create-an-embeddb-state)
    -   [Size of records](#configure-size-of-records)
    -   [Comparator Functions](#comparator-functions)
	-	[Storage Addresses](#configure-file-storage)
    -   [Memory Buffers](#configure-memory-buffers)
    -   [Other Parameters](#other-parameters)
    -   [Final Initialization](#final-initilization)
-   [Setup Index](#setup-index-method-and-optional-radix-table)
-   [Insert Records](#insert-put-items-into-table)
-   [Query Records](#query-get-items-from-table)
-   [Iterate over Records](#iterate-through-items-in-table)
    -   [Filter by key](#iterator-with-filter-on-keys)
    -   [Filter by data](#iterator-with-filter-on-data)
    -   [Iterate with vardata](#iterate-over-records-with-vardata)
- [Print Errors](#print-errors)
- [Flush EmbedDB](#flush-embeddb)
-   [Disposing of EmbedDB state](#disposing-of-embedDB-state)

## Configure Records

### Create an EmbedDB State

Allocate memory for the EmbedDB state structure.

```c
embedDBState* state = (embedDBState*) malloc(sizeof(embedDBState));
```

### Configure Size of Records

These attributes are only for fixed-size data/keys. If you require variable-sized records, see below. 

- **Key Size**: Maximum allowed up to 8 bytes.
- **Data Size**: No limit, but at least one record can fit on a page after the header. 

```c
state->keySize = 4;  
state->dataSize = 12;  
```
*Note `state->recordsize` is not necessarily the sum of `state->keySize` and `state->dataSize`. If you have variable data enabled, a 4-byte pointer exists in the fixed record that points to the record in variable storage.*

### Comparator Functions

Customize comparator functions for keys and data. Example implementations that you can use can be found in [utilityFunctions](../src/embedDB/utilityFunctions.c).


```c
// Function pointers that can compare two keys/data
state->compareKey = int32Comparator;
state->compareData = dataComparator;
```

### Configure File Storage

Configure the number of bytes per page and the minimum erase size for your storage medium.

*Note: it is not necessary to allocate the nVarPages pages if you are not using variable data. If you do wish to use these options, you must enable them in [Other Parameters](#other-parameters)*.

**Page and Erase Size**
```c
state->pageSize = 512;
state->eraseSizeInPages = 4;
```

**Allocated File Pages:**

```c
state->numDataPages = 1000;
state->numIndexPages = 48;
state->numVarPages = 1000;
```

**File Interface Setup**

Setup the file interface that allows EmbedDB to work with any storage device. More info on: [Setting up a file interface](fileInterface.md).

```c
char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
state->fileInterface = getSDInterface();
state->dataFile = setupSDFile(dataPath);
state->indexFile = setupSDFile(indexPath);
state->varFile = setupSDFile(varPath);
```

### Configure Memory Buffers

Allocate memory buffers based on your requirements. Since EmbedDB has support for variable records and indexing, additional buffers need to be created to support those features. If you would like to use variable records, you must enable them in [Other Parameters](#other-parameters).

-   Required:
    -   2 blocks for record read/write buffers
-   Optional:
    -   2 blocks for index read/write buffers (Writing the bitmap index to file)
    -   2 blocks for variable data read/write buffers (If you need to have a variable sized portion of the record) 

```c
// ONLY USING READ/WRITE
state->bufferSizeInBlocks = 2; //2 buffers is required for read/write buffers
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);

// INDEX OR VARIABLE RECORDS
state->bufferSizeInBlocks = 4; //4 buffers is needed when using index or variable
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);

// BOTH INDEX AND VARIABLE RECORDS. 
state->bufferSizeInBlocks = 6; //6 buffers is needed when using index and variable
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);
```



### Other parameters:

Here is how you can enable EmbedDB to use other included features. Below is an explanation of all the features EmbedDB comes with. 


```c
// Set EmbedDb to include bitmap in header, writing bitmap to a file, and support varaible data.
state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA;
```

- `EMBEDDB_USE_INDEX` - Writes the bitmap to a file for fast queries on the data (Usually used in conjuction with EMBEDDB_USE_BMAP). 
- `EMBEDDB_USE_BMAP` - Includes the bitmap in each page header so that it is easy to tell if a buffered page may contain a given key. 
- `EMBEDDB_USE_MAX_MIN` - Includes the max and min records in each page header. 
- `EMBEDDB_USE_VDATA` - Enables including variable-sized data with each record. 
- `EMBEDDB_RESET_DATA` - Disables data recovery.


*Note: If `EMBEDDB_RESET_DATA` is not enabled, embedDB will check if the file already exists, and if it does, it will attempt at recovering the data.*

### Bitmap 

The bitmap is used for indexing data. It must be enabled as shown above but it is not mandatory. Depending on if `EMBEDDB_USE_INDEX` is enabled, the data will be saved in two locations (datafile.bin) and on the index file. 

Just like the comparator, you may customize the bitmap's functions to your liking. A sample implementation can be found in [utilityFunctions](../src/embedDB/utilityFunctions.c).


```c
// define how many buckets data will fall into. 
state->bitmapSize = 

// Include function pointers that can build/update a bitmap of the specified size
state->inBitmap = inBitmapInt64;
state->updateBitmap = updateBitmapInt64;
state->buildBitmapFromRange = buildBitmapInt64FromRange;
```


### Final initialization

```c
size_t splineMaxError = 1; // Modify this value to change spline error tolerance
embedDBInit(state, splineMaxError);
```

## Setup Index Method and Optional Radix Table

```c
// In embedDB.c
#define SEARCH_METHOD 2
#define RADIX_BITS 0
#define ALLOCATED_SPLINE_POINTS 300
```

The `SEARCH_METHOD` defines the method used for indexing physical data pages.

-   0 Uses a linear function to approximate data page locations.
-   1 Performs a binary search over all data pages.
-   **2 Uses a Spline structure (with optional Radix table) to index data pages. _This is the recommended option_**

The `RADIX_BITS` constant defines how many bits are indexed by the Radix table when using `SEARCH_METHOD 2`.
Setting this constant to 0 will omit the Radix table, and indexing will rely solely on the Spline structure.

`ALLOCATED_SPLINE_POINTS` sets how many spline points will be allocated during initialization. This is a set amount and will not grow as points are added. The amount you need will depend on how much your key rate varies and what `maxSplineError` is set to during embedDB initialization.

## Insert (put) items into table

### Overview

Use the `embedDBPut` function to insert fixed length records into the database. Variable length records can be inserted using `embeDBPutVar` only when `EMBEDDB_USE_VDATA` is enabled. Note that keys are always assumed to be unsigned numbers and **must always be inserted in ascending order.**

### Inserting Fixed-Size Data

`key` is the key to insert. `dataPtr` points to associated data value. 

**Method:**

```c
embedDBPut(state, (void*) key, (void*) dataPtr)
```
**Parameters**
<pre>
- state:				EmbedDB algorithm state structure.
- key:					The key for the record. 
- dataPtr:				Fixed-size data for the record. 
</pre>


**Returns** 
<pre>
0 if success, Non-zero value if error. 
</pre>

**Example:**

```c
// Initialize key (note keys must always be inserted in ascending order)
uint32_t key = 123;
// calloc dataPtr in the heap
void* dataPtr = calloc(1, state->dataSize);
// set value to be inserted into EmbedDB
*((uint32_t*)dataPtr) = 12345678;
// perform insert of key and data 
embedDBPut(state, (void*) &key, dataPtr);
// free dynamic memory
free(dataPtr);
```

*Void pointers here are used to support different data-types*

### Inserting Variable-Length Data

EmbedDB has support for variable length records, but only when `EMBEDDB_USE_VDATA` is enabled. `varPtr` points to the variable sized data that you would like to insert and `length` specifies how many bytes that record takes up. It is important to note that when inserting variable-length data, EmbedDB still inserts fixed-size records just like the above example Another pointer is created in the fixed record that points to the variable one. If an individual record does not have any variable data, simply set `varPtr = NULL` and `length = 0`.

**Method:**

```c 
embedDBPutVar(state, (void*) &key, (void*) dataPtr, (void*) varPtr, (uint32_t) length);
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure.
key: 				Key for record. 
dataPtr: 			Data for record. 
varPtr:				Variable length data for record.  
length: 			Length of the variable length data, in bytes.
</pre>

**Returns** 
<pre>
0 if success. Non-zero value if error. 
</pre>



**Example:**

```c
// init key 
uint32_t key = 124; 
// calloc dataPtr in the heap
void* dataPtr = calloc(1, state->dataSize);
// set value to be inserted into embedDB
*((uint32_t*)dataPtr) = 12345678;
// Create variable-length data
char varPtr[] = "Hello World"; // ~ 12 bytes long including null terminator 
// specify the length in bytes
uint32_t length = 12;
// insert variable record 
embedDBPutVar(state, (void*)&key, (void*)dataPtr, (void*)varPtr, length);
// free dynamic memory
free(dataPtr);
dataPtr = NULL;
```

## Query (get) items from table

### Overview

Use the `embedDBGet` function to retrieve fixed-length records from the database. You can use `embedDBGetVar` to retrieve variable-length records only when `EMBEDDB_USE_VDATA` is enabled. EmbedDB will handle searching the write buffer for you, so there is no need to flush the storage. 


### Fixed-Length Record

`keyPtr` points to a key that `embedDBGet` will search for and `returnDataPtr` points to a pre-allocated space for this function to copy data into. It is important to note that `returnDataPtr` must be >= `state->recordSize` to allocate the appropriate amount of space for this incoming data. 

**Method:**

```c
embedDBGet(state, (void*) keyPtr, (void*) returnDataPtr);
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure.			
keyPtr:				Key for record.
returnDataPtr:		        Pre-allocated memory to copy data into. 
</pre>

**Returns**
<pre>
0 if success. Non-zero value if error.
</pre>

**Example:**

*Recall earlier that we inserted a key as 123 with a value of 12345678 in our insert example. We also initialized EmbedDB to have a `state->dataSize` to 4 and `state->recordSize` to 12.*

```c
// key you would like to retrieve data for
int key = 123; 
// Ensure that allocated memory is >= state->recordSize. You may use dynamic memory as well. 
int returnDataPtr[] = {0, 0, 0};
// query embedDB
embedDBGet(state, (void*) &key, (void*) returnDataPtr);
// do something with the retrieved data
```

### Variable-Length Records

Variable-length-data can be read only when the `EMBEDDB_USE_VDATA` parameter is enabled. A variable-length data stream must be created to retrieve variable-length records. `varStream` is an un-allocated `embedDBVarDataStream`; it will only return a data stream when there is data to read. Variable data is read in chunks from this stream. The size of these chunks are the length parameter for `embedDBVarDataStreamRead`. `bytesRead` is the number of bytes read into the buffer and is <=`varBufSize`. 

In a similar fashion to reading static records, you must pre-allocate storage for `embedDBVarDataStreamRead` to insert variable-length records into. Since variable length records are inserted alongside fixed length records, we can also retrieve that fixed-length record as well, so ensure there is a seperate pre-allocated storage in the memory when retrieving the fixed length record. You may even retrieve the fixed length record by doing `embedDBGet` for a key that has a variable record. 


<ins>**Method**</ins> 
```c
embedDBGetVar(embedDBState *state, void *key, void *data, embedDBVarDataStream **varData); 
```
**Parameters**
<pre>
state:				EmbedDB algorithm state structure. 
key:				Key for record.
data:				Pre-allocated memory to copy data for record.
varData:			Return variable for data as an embedDBVarDataStream.
</pre>

**Returns**
<pre>
Return 0 for success. 
Return -1 for error reading file or failed memory allocation. 
Return 1 if variable data was deleted for making room for new data.
</pre>

<ins>**Method**</ins> 
```c
embedDBVarDataStreamRead(embedDBState *state, embedDBVarDataStream *stream, void *buffer, uint32_t length)
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure. 		
stream: 			Variable data stream. 
buffer: 			Buffer to read variable data into. 
length: 			Number of bytes to read. Must be <= buffer size. 		
</pre>

**Returns**
<pre>
Number of bytes read.
</pre>


**Example:** 

*Recall in the embedDBPutVar example, we inserted a key of 1234, with fixed data being 12234565
and a 12 byte variable record that contained "Hello World".*


```c
// key you would like to retrieve data for. 
uint32_t key = 124; 

// declare a varDataStream. 
embedDBVarDataStream *varStream = NULL;	 

// allocate memory to read fixed data into.
char fixedRec[] = {0,0,0};

// allocate buffer to read variable record into. 
uint32_t varRecSize = 12; // This must be at least the same size of the variable. record.
void *varRecBufPtr = malloc(varRecSize);


// retrieve fixed record and create a data stream. 
embedDBGetVar(state, (void*) key, (void*) fixedRec, &varStream);

/* process fixed record */ 

// read data from varStream 
if (varStream != NULL) {
	uint32_t bytesRead;
	// as long as there is bytes to read
        while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varRecBufPtr, varRecSize)) > 0) {
		// Process each incoming chunk of data in varRecBufPtr that is the size of varRecSize.
	}
	free(varStream);
	varStream = NULL;
}
free(varRecBufPtr);
varRecBufPtr = NULL;
```

## Iterate Through Items in Table

### Overview

EmbedDB has support for iterating through keys and data sequentially for both fixed and variable records. A comparator function must be initialized as discussed when [setting up EmbedDBState](#comparator-functions). Remember, `EMBEDDB_USE_VDATA` must be enabled to use variable data. 

You must first declare an `embedDBIterator` type and specify the minKey/maxKey or minData/maxData depending on the type of filter you would like to perform. `embedDBInitIterator` will initialize this iterator and use indexing to predict where the record will be in storage. `embedDBNext` will copy the requested key and data into pre allocated storage until there are no more records to read. `embedDBNext` will also locate records that are held in the write buffer.

It is important that you pre allocate enough storage for the key and data to fit into. Also make sure to call `embedDBCloseIterator` to close the iterator after use. 


Here are the methods that are common to both approaches. 

<ins>**Method**</ins> 
```c
embedDBInitIterator(embedDBState *state, embedDBIterator *it);
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure.
it: 				EmbedDB iterator state structure.
</pre>

**Returns**
<pre>
void
</pre>

<ins>**Method**</ins> 
```c
embedDBNext(embedDBState *state, embedDBIterator *it, void *key, void *data);
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure.
it: 				EmbedDB iteraator state structure. 
key: 				Return variable for key (pre allocated).
data:				Return variable for data (pre allocated).
</pre>

**Returns**
<pre>
1 if successful 
0 if no more records
</pre>

<ins>**Method**</ins> 
```c
embedDBCloseIterator(embedDBIterator *it)
```

**Parameters**
<pre>
it:				EmbedDB iterator structure. 
</pre>

**Returns**
<pre>
void
</pre>


### Iterator with filter on keys

EmbedDB can iterate through a range of keys sequentially. `minKey` specifies the minimum key to begin the search at and `maxKey` is where the search will stop. Since we are not iterating by data, ensure that `it.minData` and `it.maxData` is set to `NULL`.



**Example**

*Assume that we inserted over 1000 records*

```c
// declare EmbedDB iterator. 
embedDBIterator it;

// ensure that itKey is the same size as state->recordSize.
uint32_t *itKey;				 

// Allocate memory for itData matching the size of state->datasize.
uint32_t* itData[] = {0,0,0};

// specify min and max key to perform search on. 
uint32_t minKey = 1, maxKey = 1000;

// initalize buffer variables.
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

embedDBInitIterator(state, &it);

// while there are records to read. 
while (embedDBNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

embedDBCloseIterator(&it);
```

### Iterator with filter on data

EmbedDB can iterate through a range of data sequentially. This time, `minData` specifies the minimum data value to begin the search at and `maxData` is where the search will stop. Since we are not iterating by key, ensure that `it.minKey` and `it.maxKey` is set to `NULL`.


**Example**

*Assume that several records are inserted that contain data spanning from values 0-150.*

```c
// declare EmbedDB iterator. 
embedDBIterator it;

// ensure that itKey is the same size as state->recordSize.
uint32_t *itKey;				 

// Allocate memory for itData matching the size of state->datasize.
uint32_t* itData[] = {0,0,0};

// specify min and max data to perform search on. 
uint32_t minData = 90, maxData = 100;

it.minKey = NULL;
it.maxKey = NULL;
it.minData = &minData;
it.maxData = &maxData;

embedDBInitIterator(state, &it);

while (embedDBNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

embedDBCloseIterator(&it);
```

## Iterate over records with vardata

### Overview 

As [mentioned earlier](#iterate-through-items-in-table), EmbedDB can iterate over variable records when `EMBEDDB_USE_VDATA` is enabled. The strategy is similar to the above and, in that we are going to initialize an EmbedDB iterator with our minimum/maximum keys or data to iterate over, however, we need to declare and initialize an `embedDBVarDataStream` to perform the iteration, which is similar to how variable records are retrieved. Be sure to use `embedDBCloseIterator` to close the iterator after use. Also ensure that you free the `embedDBVarDataStream`.


These are methods common to iteratring over variable records specifically. It is recommended you read this entire section to gain an understanding of how the iterator works. 

<ins>**Method**</ins> 
```c
embedDBNextVar(embedDBState *state, embedDBIterator *it, void *key, void *data, embedDBVarDataStream **varData)
```

**Parameters**
<pre>
state:				EmbedDB algorithm state structure.
it:			        EmbedDB iterator state structure. 
key:				Return variable for key (pre allocated).
data:				Return variable for data (pre allocated).
varData:			Return variable for variable data as an embedDBVarDataStream.
</pre>


**Returns**
<pre>
1 if successful 
0 if no more records
NULL if three is no variable data. 
</pre>

### Filter on Keys

`minKey` specifies the minimum key to begin the search at and `maxKey` is where the search will stop. Since we are not iterating by data, ensure that `it.minData` and `it.maxData` is set to `NULL`. The process is very similar to retreiving variable records. 

```c
// declare embedDB iterator
embedDBIterator it;
// Memory to store key and fixed data into. 
uint32_t *itKey;
int itData[] = {0,0,0};

uint32_t minKey = 23, maxKey = 356;
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

embedDBVarDataStream *varStream = NULL;
// Choose any size. Must be at least the size of the variable record if you would like the entire record on each iteration. 
uint32_t varBufSize = 12; 
void *varDataBuffer = malloc(varBufSize);

embedDBInitIterator(state, &it);

while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
	/* Process fixed part of record */
	
	/* Process vardata if this record has it */
	if (varStream != NULL) {
		uint32_t numBytesRead = 0;
		while ((numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
			/* Process the data read into the buffer */
		}
		free(varStream);embedDBVarDataStreamRead
		varStream = NULL;
	}
}

free(varDataBuffer);
varDataBuffer = NULL;
embedDBCloseIterator(&it);
```

### Filter on Data

Filtering on data is similar to above. Set the minData and maxData for the fixed-size record and ensure minKey and maxKey are set to `NULL.`

```c
	embedDBIterator it;
    uint32_t *itKey;

    int itData[] = {0,0,0};

    uint32_t minData = 1, maxData = 3;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = &minData;
    it.maxData = &maxData;

    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);

    embedDBInitIterator(state, &it);

    while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
        // Process fixed part of record 
        printf("itData = %d\n", *itData);
            // Process vardata if this record has it
            if (varStream != NULL) {
                uint32_t numBytesRead = 0;
                while ((numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
                    // Process the data read into the buffer 
                    
                }
                free(varStream);
                varStream = NULL;
            }
        }

    free(varDataBuffer);
	varDataBuffer = NULL;
    embedDBCloseIterator(&it);


```

## Print Errors

EmbedDB has a macro used to `PRINT ERRORS` that EmbedDB might generate. This is useful for debugging but not every board will have a terminal output. 

To use, ensure you add `PRINT_ERRORS` as a compilation flag when making EmbedDB.

## Flush EmbedDB

Flush the EmbedDB buffer to write the currently buffered page to storage manually. EmbedDB will automatically handle this for you when a read/write buffer is full. 

```c
embedDBFlush(state);
```

## Disposing of EmbedDB state

**Be sure to flush buffers before closing, if needed.**

```c
embedDBClose(state);
tearDownFile(state->dataFile); 
tearDownFile(state->indexFile);	
tearDownFile(state->varFile);
free(state->fileInterface);
free(state->buffer);
free(state);
```

*Note: Ensure to only tearDown files specified in initialization.*
