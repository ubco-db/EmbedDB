#if !defined(IN_MEMORY_SORT_H_)
#define IN_MEMORY_SORT_H_

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdlib.h>
#include <stdint.h>
// #include <alloca.h>

int
in_memory_sort(
	void *data,
	uint32_t num_values,
	int value_size,
	int8_t (*compare_fcn)(void* a, void* b),
	int sort_algorithm
);

/**
 * Compares two records based on an integer key. Uses a and b as pointers to start of record. Assumes key is at start of record.
 */
int8_t
merge_sort_int32_comparator(
        void			*a,
        void			*b
); 

#if defined(__cplusplus)
}
#endif

#endif /* IN_MEMORY_SORT_H_ */
