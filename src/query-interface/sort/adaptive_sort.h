#if !defined(ADAPTIVE_SORT_H)
#define ADAPTIVE_SORT_H

#if defined(ARDUINO)
#include "serial_c_iface.h"
#include "file/kv_stdio_intercept.h"
#include "file/sd_stdio_c_iface.h"
#endif

#include <stdint.h>
#include <stdio.h>

#include "external_sort.h"

//block to use as output block. Breaks reading in new block code if changed.
#define OUTPUT_BLOCK_ID 0

#define BUFFER_OUTPUT_BLOCK_START_OFFSET  	        0
#define BUFFER_OUTPUT_BLOCK_START_RECORD_OFFSET 	BLOCK_HEADER_SIZE

#if defined(__cplusplus)
extern "C" {
#endif

/**
@brief      Adaptive sort combining no output buffer sort and MinSort that dynamically determines best sorting
                algorithm based on input distribution. Uses replacement selection.
@param      iterator
                Row iterator for reading input rows
@param      iteratorState
                Structure stores state of iterator (file info etc.)
@param      tupleBuffer
                Pre-allocated space to store one tuple (row) of input being sorted
@param      outputFile
                Already opened file to store sorting output (and in-progress temporary results)
@param      buffer
                Pre-allocated space used by algorithm during sorting
@param      bufferSizeInBlocks
                Size of buffer in blocks
@param      es
                Sorting state info (block size, record size, etc.)
@param      resultFilePtr
                Offset within output file of first output record
@param      metric
                Tracks algorithm metrics (I/Os, comparisons, memory swaps)
@param      compareFn
                Record comparison function for record ordering
@param      runGenOnly
                True if generate sorted runs but not whole merge process
@param      writeToReadRatio
                Write time divided by read time multiplied by 10. If ratio is 2.5 
                (writes over twice as expensive) then value is 25.
*/
int adaptive_sort(
        int     (*iterator)(void *state, void* buffer, external_sort_t *es),
        void    *iteratorState,
	void    *tupleBuffer,
        ION_FILE *outputFile,		
	char    *buffer,        
	int     bufferSizeInBlocks,
	external_sort_t *es,
	long    *resultFilePtr,
	metrics_t *metric,
        int8_t  (*compareFn)(void *a, void *b),
        int8_t  runGenOnly,
        int8_t  writeToReadRatio
);

#if defined(__cplusplus)
}
#endif

#endif
