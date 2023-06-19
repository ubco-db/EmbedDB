# Usage & Setup Documentation

## Table of Contents

-   [Configure SBITS State](#configure-records)
    -   [Create an sbits state](#create-an-sbits-state)
    -   [Size of records](#configure-size-of-records)
    -   [Comparator Functions](#comparator-functions)
    -   [Storage Addresses](#configure-storage-addresses)
    -   [Memory Buffers](#configure-memory-buffers)
    -   [Other Parameters](#other-parameters)
    -   [Final Initialization](#final-initilization)
-   [Setup Index](#setup-index-method-and-optional-radix-table)
-   [Insert Records](#insert-put-items-into-table)
-   [Query Records](#query-get-items-from-tree)
-   [Iterate over Records](#iterate-through-items-in-tree)
    -   [Filter by key](#iterator-with-filter-on-keys)
    -   [Filter by data](#iterator-with-filter-on-data)
    -   [Iterate with vardata](#iterate-over-records-with-vardata)
-   [Disposing of sbits state](#disposing-of-sbits-state)

## Configure Records

### Create an sbits state

```c
sbitsState* state = (sbitsState*) malloc(sizeof(sbitsState));
```

### Configure size of records

These attributes are only for fixed-size data/keys. If you require variable-sized records keep reading

```c
state->keySize = 4;  // Allowed up to 8
state->dataSize = 12;  // No limit as long as at least one record can fit on a page after the page header
```

### Comparator functions

There are example implementations of these in src/sbits/utilityFunctions.c \
These can be customized for your own keys and data.

```c
// Function pointers that can compare two keys/data
state->compareKey = int32Comparator;
state->compareData = dataComparator;
```

### Configure storage addresses

```c
state->pageSize = 512;
state->eraseSizeInPages = 4;

// If your storage medium has a file system (e.g. sd card) then the start address doesn't matter, only the different between the start and end.
// If you do not have a file system, then be sure that you do not overlap memory regions.
state->startAddress = 0;
state->endAddress = 1000 * state->pageSize;

// If variable sized data will be stored
state->varAddressStart = 2000 * state->pageSize;
state->varAddressEnd = state->varAddressStart + 1000 * state->pageSize;
```

### Configure memory buffers

```c
/* How to choose the number of blocks to use:
* Required:
*     - 2 blocks for record read/write buffers
* Optional:
*     - 2 blocks for index read/write buffers (Writing the bitmap index to file)
*     - 2 blocks for variable data read/write buffers (If you need to have a variable sized portion of the record)
*/
state->bufferSizeInBlocks = 6;
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);
```

### Other parameters:

```c
state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_USE_VDATA;
```

`SBITS_USE_INDEX` - Writes the bitmap to a file for fast queries on the data (Usually used in conjuction with SBITS_USE_BMAP) \
`SBITS_USE_BMAP` - Includes the bitmap in each page header so that it is easy to tell if a buffered page may contain a given key

```c
state->bitmapSize = 8;

// Include function pointers that can build/update a bitmap of the specified size
state->inBitmap = inBitmapInt64;
state->updateBitmap = updateBitmapInt64;
state->buildBitmapFromRange = buildBitmapInt64FromRange;
```

`SBITS_USE_MAX_MIN` - Includes the max and min records in each page header \
`SBITS_USE_VDATA` - Enables including variable-sized data with each record

### Final initilization

```c
size_t splineMaxError = 1; // Modify this value to change spline error tolerance
sbitsInit(state, splineMaxError);
```

## Setup Index Method and Optional Radix Table

```c
// In sbits.c
#define SEARCH_METHOD 2
#define RADIX_BITS 0
#define ALLOCATED_SPLINE_POINTS 300
```

The `SEARCH_METHOD` defines the method used for indexing physical data pages.

-   0 Uses a linear function to approximate data page locations.
-   1 Performs a binary search over all data pages.
-   **2 Uses a Spline structure (with optional Radix table) to index data pages. _This is the recommended option_**

The `RADIX_BITS` constant defines how many bits are indexed by the Radix table when using `SEARCH_METHOD 2`.
Setting this constant to 0 will omit the Radix table, indexing will rely solely on the Spline structure.

`ALLOCATED_SPLINE_POINTS` sets how many spline points will be allocated during initialization. This is a set amount and will not grow as points are added. The amount you need will depend on how much your key rate varies and what `maxSplineError` is set to during sbits initialization.

## Insert (put) items into table

`key` is the key to insert. `dataPtr` points to associated data value. Keys are always assumed to be unsigned numbers and must always be inserted in ascending order.

```c
uint32_t key = 123;
void* dataPtr = calloc(1, state->dataSize);
*((uint32_t*)data) = 12345678;
sbitsPut(state, (void*) &key, dataPtr);
```

Insert records when the `SBITS_USE_VDATA` parameter is used. `varPtr` points to `length` bytes of data that can be inserted alongside the regular record. If a record does not have any variable data, `varPtr = NULL` and `length = 0`

```c
sbitsPutVar(state, (void*) &key, (void*) dataPtr, (void*) varPtr, (uint32_t) length);
```

Flush the sbits buffer to write the currently buffered page to storage. Not necessary unless you need to query anything that is still in buffer or you are done all inserts.

```c
sbitsFlush(state);
```

## Query (get) items from tree

`keyPtr` points to key to search for. `dataPtr` must point to pre-allocated space to copy data into.

```c
sbitsGet(state, (void*) keyPtr, (void*) dataPtr);
```

If the `SBITS_USE_VDATA` parameter is used, then `varPtr` is an un-allocated where the method can copy the vardata into and `length` is the number of bytes that were `malloc`ed and copied into `varPtr`

```c
void* varPtr = NULL;
uint32_t length = 0;
sbitsGetVar(state, (void*) keyPtr, (void*) dataPtr, (void**) &varPtr, &length);
```

## Iterate through items in tree

### Iterator with filter on keys

```c
sbitsIterator it;
uint32_t *itKey, *itData;

uint32_t minKey = 1, maxKey = 1000;
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

sbitsCloseIterator(&it);
```

### Iterator with filter on data

```c
it.minKey = NULL;
it.maxKey = NULL;
uint32_t minData = 90, maxData = 100;
it.minData = &minData;
it.maxData = &maxData;

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

sbitsCloseIterator(&it);
```

### Iterate over records with vardata

```c
uint32_t minKey = 23, maxKey = 356;
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

sbitsVarDataStream *varStream = NULL;
uint32_t varBufSize = 8;  // Choose any size
void *varDataBuffer = malloc(varBufSize);

sbitsInitIterator(state, &it);

while (sbitsNextVar(state, &it, &itKey, itData, &varStream)) {
	/* Process fixed part of record */
	...
	/* Process vardata if this record has it */
	if (varStream != NULL) {
		uint32_t numBytesRead = 0;
		while ((numBytesRead = sbitsVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
			/* Process the data read into the buffer */
			for (uint32_t i = 0; i < numBytesRead; i++) {
				printf("%02X ", (uint8_t*)varDataBuffer + i);
			}
		}
		printf("\n");

		free(varStream);
		varStream = NULL;
	}
}

free(varDataBuffer);
sbitsCloseIterator(&it);
```

## Disposing of sbits state

**Be sure to flush buffers before closing, if needed.**

```c
free(state->buffer);
sbitsClose(state);
free(state);
```
