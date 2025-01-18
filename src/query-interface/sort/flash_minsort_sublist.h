#if !defined(FLASH_MINSORT_SUBLIST_H)
#define FLASH_MINSORT_SUBLIST_H

#if defined(ARDUINO)
#include "serial_c_iface.h"
#include "file/kv_stdio_intercept.h"
#include "file/sd_stdio_c_iface.h"
#endif

#include <stdint.h>

#include "external_sort.h"

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
@param      bufferSizeInBytes
                Size of buffer in bytes
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
        ION_FILE *outputFile,		
		char    *buffer,        
		int     bufferSizeInBytes,
		external_sort_t *es,
		long    *resultFilePtr,
		metrics_t *metric,
        int8_t  (*compareFn)(void *a, void *b),
        long    numSubList
);

typedef struct MinSortStateSublist
{
    char* buffer;
    unsigned int* min;
    unsigned long* offset;

    unsigned int current;           // current smallest value
    unsigned int next;              // keep track of next smallest value for next iteration
    unsigned long int nextIdx; 
                       
    unsigned int record_size;
    unsigned long int num_records;
    unsigned int numBlocks;                
    unsigned int memoryAvailable;
    unsigned int numRegions;          
    unsigned int regionIdx;
    unsigned int lastBlockIdx;    
    unsigned long fileOffset;
    
    void    *iteratorState;    

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
