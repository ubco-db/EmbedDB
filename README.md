# SBITS with Learned Indexing

SBITS on Raw memory (NOR, NAND) with learned indexing

1. Uses the minimum of two page buffers for performing all operations. The memory usage is less than 1.5 KB for 512 byte pages.
2. Performance is several times faster than using B-trees and hash-based indexes. Simplifies data management without worrying about low-level details and storing data in raw files.
3. No use of dynamic memory (i.e. malloc()). All memory is pre-allocated at creation of the index.
4. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
5. Option to store time series data with or without an index. Adding an index allows for faster retrieval of records based on data value.
6. Support for iterator to traverse data in sorted order.
7. Easy to use and include in existing projects. 
8. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed for building and execution on an embedded device using Platform.io.**

## License
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Code Files

* test_sbits.h - test file demonstrating how to get, put, and iterate through data in index
* main.cpp - main Arduino code file
* sbits.h, sbits.c - implementation of SBITS index structure supporting arbitrary key-value data items

## Documentation

A paper describing SBITS use for time series indexing is [available from the publisher](https://www.scitepress.org/Link.aspx?doi=10.5220/0010318800920099) and a [pre-print is also available](SBITS_time_series_index.pdf).

## Usage

### Setup Index and Configure Memory

```c
/* Configure SBITS state */
sbitsState* state = (sbitsState*) malloc(sizeof(sbitsState));

state->recordSize = 16;
state->keySize = 4;
state->dataSize = 12;        
state->pageSize = 512;
state->bitmapSize = 0;
state->bufferSizeInBlocks = M;
state->buffer  = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);    
int8_t* recordBuffer = (int8_t*) malloc(state->recordSize); 

/* Address level parameters */
state->startAddress = 0;
state->endAddress = state->pageSize * numRecords / 10;  /* Modify this value lower to test wrap around */	
state->eraseSizeInPages = 4;
state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
if (SBITS_USING_INDEX(state->parameters) == 1)
    state->endAddress += state->pageSize * (state->eraseSizeInPages *2);    
if (SBITS_USING_BMAP(state->parameters))
    state->bitmapSize = 8;
    
/* Setup for data and bitmap comparison functions */
state->inBitmap = inBitmapInt16;
state->updateBitmap = updateBitmapInt16;
state->inBitmap = inBitmapInt64;
state->updateBitmap = updateBitmapInt64;
state->compareKey = int32Comparator;
state->compareData = int32Comparator;

/* Initialize SBITS structure with parameters */
size_t splineMaxError = 1; /* Modify this value to change spline error tolerance */
sbitsInit(state, splineMaxError);
```

### Setup Index Method and Optional Radix Table
```c
// In sbits.c

/**
 * 0 = Value-based search
 * 1 = Binary serach
 * 2 = Modified linear search (Spline)
 * 3 = Pure C PGM
 * 4 = Pure C one-level PGM
 */
#define SEARCH_METHOD 2

/**
 * Number of bits to be indexed by the Radix Search structure
 * Note: The Radix search structure is only used with Spline (SEARCH_METHOD == 2)
 * To use a pure Spline index without a Radix table, set RADIX_BITS to 0
 */
#define RADIX_BITS 0

```

The SEARCH_METHOD defines the method used for indexing physical data pages.
* 0 Uses a linear function to approximate data page locations.
* 1 Performs a binary search over all data pages.
* 2 Uses a Spline structure (with optional Radix table) to index data pages.
* 3 Uses a multi-level PGM structure to index data pages.
* 4 Uses a single-level PGM structure to index data pages.

The RADIX_BITS constant defines how many bits are indexed by the Radix table when using SEARCH_METHOD 2.
Setting this constant to 0 will omit the Radix table, indexing will rely solely on the Spline structure.

### Insert (put) items into tree

```c
/* keyPtr points to key to insert. dataPtr points to associated data value. */
sbitsPut(state, (void*) keyPtr, (void*) dataPtr);

/* flush the sbits buffer when done inserting points. */
sbitsFlush(state);
```

### Query (get) items from tree

```c
/* keyPtr points to key to search for. dataPtr must point to pre-allocated space to copy data into. */
int8_t result = sbitsGet(state, (void*) keyPtr, (void*) dataPtr);
```

### Iterate through items in tree

```c
/* Iterator with filter on keys */
sbitsIterator it;
int32_t *itKey, *itData;

uint32_t minKey = 1, maxKey = 1000;     
it.minKey = &minKey; 
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;    

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData))
{                      
	/* Process record */	
}

/* Iterator with filter on data */       
it.minKey = NULL;    
it.maxKey = NULL;
uint32_t minData = 90, maxData = 100;  
it.minData = &minData;
it.maxData = &maxData;    

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData))
{                      
	/* Process record */	
}
```
#### Ramon Lawrence, David Ding, Ivan Carvalho<br>University of British Columbia Okanagan
