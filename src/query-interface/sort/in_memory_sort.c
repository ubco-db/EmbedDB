/******************************************************************************/
/**
@file
@author		Kris Wallperington
@brief		Implementation of an in-place, recursive quicksort written by the author.
@copyright	Copyright 2016
				The University of British Columbia,
				IonDB Project Contributors (see AUTHORS.md)
@par
			Licensed under the Apache License, Version 2.0 (the "License");
			you may not use this file except in compliance with the License.
			You may obtain a copy of the License at
					http://www.apache.org/licenses/LICENSE-2.0
@par
			Unless required by applicable law or agreed to in writing,
			software distributed under the License is distributed on an
			"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
			either express or implied. See the License for the specific
			language governing permissions and limitations under the
			License.
*/
/******************************************************************************/
//TODO: quick sort throws a seg fault on pc when sorting large arrays (>20000). This may be due to the stack overflowing
// from the recursive calls to in_memory_quick_sort_helper(...)

#include <string.h>

#include "in_memory_sort.h"

int8_t
merge_sort_int32_comparator(
        void			*a,
        void			*b
) {
	int32_t result = *((int32_t*)a) - *((int32_t*)b);
	if(result < 0) return -1;
	if(result > 0) return 1;
    return 0;
}

void
in_memory_swap(
	void				*tmp_buffer,
	int					value_size,
	char*			a,
	char*			b
) {
	memcpy(tmp_buffer, a, value_size);
	memcpy(a, b, value_size);
	memcpy(b, tmp_buffer, value_size);
}

void*
in_memory_quick_sort_partition(
	void *tmp_buffer,
	int value_size,
	int8_t (*compare_fcn)(void* a, void* b),
	char* low,
	char* high
) {
	char* pivot	= low;
	char* lower_bound	= low - value_size;
	char* upper_bound	= high + value_size;

	while (1) {
		do {
			upper_bound -= value_size;
		} while (compare_fcn(upper_bound, pivot) > 0);

		do {
			lower_bound += value_size;
		} while (compare_fcn(lower_bound, pivot) < 0);

		if (lower_bound < upper_bound) {
			in_memory_swap(tmp_buffer, value_size, lower_bound, upper_bound);
		}
		else {
			return upper_bound;
		}
	}
}

void
in_memory_quick_sort_helper(
	void *tmp_buffer,
	uint32_t num_values,
	int value_size,
	int8_t (*compare_fcn)(void* a, void* b),
	char* low,
	char* high
) {
	if (low < high) {
		char* pivot = in_memory_quick_sort_partition(tmp_buffer, value_size, compare_fcn, low, high);

		in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, compare_fcn, low, pivot);
		in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, compare_fcn, pivot + value_size, high);
	}
}

int
in_memory_quick_sort(
	void *data,
	uint32_t num_values,
	int value_size,
	int8_t (*compare_fcn)(void* a, void* b)
) {
	void* tmp_buffer = malloc(value_size);
	if(NULL == tmp_buffer) return 8;

	/*void* low = data*/
	char* high = (char*)data + (num_values-1)*value_size;
	in_memory_quick_sort_helper(tmp_buffer, num_values, value_size, compare_fcn, (char*)data, high);

	free(tmp_buffer);

	return 0;
}

int
in_memory_sort(
	void *data,
	uint32_t num_values,
	int value_size,
	int8_t (*compare_fcn)(void* a, void* b),
	int sort_algorithm
) {
	int err = 0;
	switch (sort_algorithm) {
		case 1: {
			err = in_memory_quick_sort(data, num_values, value_size, compare_fcn);
			break;
		}
	}

	return err;
}
