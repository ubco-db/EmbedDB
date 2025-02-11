#if !defined(EXTERNAL_SORT_H)
#define EXTERNAL_SORT_H

#include <stdint.h>
#include "../../embedDB/embedDB.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct {
    uint16_t	key_size;
    uint16_t    key_offset;
    uint16_t	value_size;
    uint16_t	page_size;
    uint16_t	record_size;
    uint32_t    num_pages;
    uint16_t    num_values_last_page;
    int8_t      headerSize;
    int8_t      (*compare_fcn)(void *a, void *b);
} external_sort_t;

typedef struct {
    int32_t num_reads;
    int32_t num_writes;
    int32_t num_memcpys;
    int32_t num_compar;
    int32_t num_runs;
    double time;
    double genTime;
} metrics_t;

typedef struct {
    int32_t key;
    int8_t	value[12];
} test_record_t;

typedef struct {
	void *file;
	int32_t recordsRead;
	int32_t totalRecords;	
    int32_t currentRecord;
    int32_t recordsLeftInBlock;
    int32_t recordSize;      
    long resultFile; 

    embedDBFileInterface  *fileInterface; 
} file_iterator_state_t;

/* Constant declarations */
#define    BLOCK_HEADER_SIZE    sizeof(int32_t)+sizeof(int16_t)
#define    BLOCK_ID_OFFSET      0
#define    BLOCK_COUNT_OFFSET   sizeof(int32_t)
#define    PAGE_SIZE            512


#if defined(__cplusplus)
}
#endif

#endif
