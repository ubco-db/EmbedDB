#if !defined(FLASH_MINSORT_SUBLIST_H)
#define FLASH_MINSORT_SUBLIST_H

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

#if defined(__cplusplus)
extern "C" {
#endif

/**
@brief      Flash Minsort designed to handle regions that are sorted sublists.
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
@param      numSubList
                Number of sublists
*/
int flash_minsort_sublist(
        void    *iteratorState,
		void    *tupleBuffer,
        FILE    *outputFile,		
		char    *buffer,        
		int     bufferSizeInBytes,
		external_sort_t *es,
		long    *resultFilePtr,
		metrics_t *metric,
        int8_t  (*compareFn)(void *a, void *b),
        long    numSubList
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
typedef struct MinSortStateSublist
{
    char* buffer;
    unsigned int* min;
    unsigned int* offset;

    unsigned int current;           // current smallest value
    unsigned int next;              // keep track of next smallest value for next iteration
    unsigned long int nextIdx; 
                       
    unsigned int record_size;
    unsigned long int num_records;
    unsigned int numBlocks;        
    unsigned int records_per_block;
    unsigned int blocks_per_region;
    unsigned int memoryAvailable;
    unsigned int numRegions;          
    unsigned int regionIdx;
    unsigned int lastBlockIdx;    

    void    *iteratorState;
    int*    ptrLastBlock;

    /* Statistics */
    unsigned int blocksRead;
    unsigned int tuplesRead;
    unsigned int tuplesOut;
    unsigned int bytesRead;    
} MinSortStateSublist;


void  init_MinSort_sublist(MinSortStateSublist* ms, external_sort_t *es, metrics_t *metric);
char* next_MinSort_sublist(MinSortStateSublist* ms, external_sort_t *es, void *tupleBuffer, metrics_t *metric);
void close_MinSort_sublist(MinSortStateSublist* ms, external_sort_t *es);

#if defined(__cplusplus)
}
#endif

#endif
