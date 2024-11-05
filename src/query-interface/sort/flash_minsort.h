#if !defined(NO_OUTPUT_BUFFER_SORT_BLOCK_HEAP_H)
#define NO_OUTPUT_BUFFER_BLOCK_HEAP_H

#if defined(ARDUINO)
#include "../../../../serial/serial_c_iface.h"
#include "../../../../file/kv_stdio_intercept.h"
#include "../../../../file/sd_stdio_c_iface.h"
#endif

#include <stdint.h>

#include "external_sort.h"

// #define BUFFER_OUTPUT_BLOCK_START_OFFSET  		OUTPUT_BLOCK_ID * es->page_size
// #define BUFFER_OUTPUT_BLOCK_START_RECORD_OFFSET  OUTPUT_BLOCK_ID * es->page_size + BLOCK_HEADER_SIZE
// Simplification if OUTPUT_BLOCK_ID is 0
#define BUFFER_OUTPUT_BLOCK_START_OFFSET  			0
#define BUFFER_OUTPUT_BLOCK_START_RECORD_OFFSET 	BLOCK_HEADER_SIZE

#define SORT_KEY_SIZE       4
#define INT_SIZE            4

#define true 1
#define false 0

#if defined(__cplusplus)
extern "C" {
#endif

/**
@brief      Flash Minsort implemented with full tuple reads.
@param      iteratorState
                Structure stores state of iterator (file info etc.)
@param      tupleBuffer
                Pre-allocated space to store one tuple (row) of input being sorted
@param      outputFile
                Already opened file to store sorting output (and in-progress temporary results)
@param      buffer
                Pre-allocated space used by algorithm during sorting
@param      bufferSizeInByes
                Size of buffer in byes
@param      es
                Sorting state info (block size, record size, etc.)
@param      resultFilePtr
                Offset within output file of first output record
@param      metric
                Tracks algorithm metrics (I/Os, comparisons, memory swaps)
@param      compareFn
                Record comparison function for record ordering
*/
int flash_minsort(
        void    *iteratorState,
		void    *tupleBuffer,
        void    *outputFile,		
		char    *buffer,        
		int     bufferSizeInBytes,
		external_sort_t *es,
		long    *resultFilePtr,
		metrics_t *metric,
        int8_t  (*compareFn)(void *a, void *b)
);

/*
typedef struct OpState
{   char type;
    unsigned long int blocks_written;
    unsigned long int blocks_read;
    unsigned long int tuples_read; 
    unsigned long int bytes_read;
    unsigned long int tuples_out;

    struct OpState *left;
    struct OpState *right;
    TupleSlot* tupleSlot;
} OpState;
*/
typedef struct MinSortState
{
    int8_t  *buffer;
    int8_t    *min;
    int8_t    *min_initialized;
    
    uint64_t nextIdx; 
    void    *current;           // current smallest value
    void    *next;              // keep track of next smallest value for next iteration
    uint32_t regionIdx;
    uint32_t lastBlockIdx;   


    int8_t current_initialized;
    int8_t next_initialized;
    int8_t regionIdx_initialized;
    int8_t lastBlockIdx_initialized;
    

    uint32_t record_size;
    uint64_t num_records;
    uint32_t numBlocks;        
    uint32_t records_per_block;
    uint32_t blocks_per_region;
    uint32_t memoryAvailable;
    uint32_t numRegions;          
     

    void    *iteratorState;

    /* Statistics */
    uint32_t blocksRead;
    uint32_t tuplesRead;
    uint32_t tuplesOut;
    uint32_t bytesRead;    
} MinSortState;


void  init_MinSort(MinSortState* ms, external_sort_t *es, metrics_t *metric, int8_t  (*compareFn)(void *a, void *b));
char* next_MinSort(MinSortState* ms, external_sort_t *es, void *tupleBuffer, metrics_t *metric, int8_t  (*compareFn)(void *a, void *b));
void close_MinSort(MinSortState* ms, external_sort_t *es);

#if defined(__cplusplus)
}
#endif

#endif
