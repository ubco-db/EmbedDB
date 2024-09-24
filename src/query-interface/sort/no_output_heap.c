/******************************************************************************/
/**
@file		replacement_heap.c
@author		Riley Jackson, Ramon Lawrence
@brief		File-based replacement selection
@copyright	Copyright 2019
			The University of British Columbia,
			IonDB Project Contributors (see AUTHORS.md)
@par Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
	this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following  disclaimer in the documentation
	and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
	may be used to endorse or promote products derived from this software without
	specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	POSSIBILITY OF SUCH DAMAGE.
*/
/******************************************************************************/


#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <time.h>
#include <math.h>

#include "no_output_heap.h"

/*
 *Starts with empty root and recursively moves tuples into their empty parent. Stops when the input tuple can be inserted into the parent instead while maintaining sorted order.
 */
void heapify(   char* buffer,
                void* input_tuple,
                int32_t size,
                external_sort_t* es,
                metrics_t *metric
) {
    int32_t left, right, smallest;
    int32_t i = 0;
    while (1) {
        left = 2 * i + 1;
        right = left + 1;

        if (left >= size)
            break;

        //find if left or right is smallest
        metric->num_compar++;
        if (right < size && es->compare_fcn(buffer + right*es->record_size, buffer + left*es->record_size) < 0)
            smallest = right;
        else
            smallest = left;

        //is input tuple the smallest
        metric->num_compar++;
        if (es->compare_fcn(input_tuple, buffer + smallest*es->record_size) < 0)
            break;

        //Perform shift
        metric->num_memcpys ++;
        memcpy(buffer + i*es->record_size, buffer + smallest*es->record_size, (size_t)es->record_size);
        i = smallest;
    }
    //insert the tuple
    metric->num_memcpys ++;
    memcpy(buffer + i*es->record_size, input_tuple, (size_t)es->record_size);
}
/*
 * Shifts parent node of current child at idx into the child. Stops shifting parents and inserts the insert_tuple into the idx when
 * the idx points to the position where the input tuple belongs in sorted order.
 */
void shiftUp(char* buffer,
             void* input_tuple,
             int32_t idx,
             external_sort_t* es,
             metrics_t *metric
) {
    int32_t parent;

    while (idx > 0) {
        parent = (idx - 1) / 2;

        metric->num_compar++;
        if (es->compare_fcn(input_tuple, buffer + parent*es->record_size) >= 0) {
            break;
        }
        metric->num_memcpys++;
        memcpy(buffer + idx*es->record_size, buffer + parent*es->record_size, (size_t)es->record_size);
        idx = parent;
    }
    metric->num_memcpys++;
    memcpy(buffer + idx*es->record_size, input_tuple, (size_t)es->record_size);
}


/*
 *Starts with empty root and recursively moves tuples into their empty parent. Stops when the input tuple can be inserted into the parent instead while maintaining sorted order.
 * Heap function assumes root is at end of array and works backwards
 */
void heapify_rev(   char* buffer,
                void* input_tuple,
                int32_t size,
                external_sort_t* es,
                metrics_t *metric
) {
    int32_t left, right, smallest;
    int32_t i = 0;
    while (1) {
        left = 2 * i + 1;
        right = left + 1;

        if (left >= size)
            break;

        //find if left or right is smallest
        metric->num_compar++;
        if (right < size && es->compare_fcn(buffer - right*es->record_size, buffer - left*es->record_size) < 0)
            smallest = right;
        else
            smallest = left;

        //is input tuple the smallest
        metric->num_compar++;
        if (es->compare_fcn(input_tuple, buffer - smallest*es->record_size) < 0)
            break;

        //Perform shift
        metric->num_memcpys ++;
        memcpy(buffer - i*es->record_size, buffer - smallest*es->record_size, (size_t)es->record_size);
        i = smallest;
    }
    //insert the tuple
    metric->num_memcpys ++;
    memcpy(buffer - i*es->record_size, input_tuple, (size_t)es->record_size);
}
/*
 * Shifts parent node of current child at idx into the child. Stops shifting parents and inserts the insert_tuple into the idx when
 * the idx points to the position where the input tuple belongs in sorted order.
 * Heap function assumes root is at end of array and works backwards
 */
void shiftUp_rev(char* buffer,
             void* input_tuple,
             int32_t idx,
             external_sort_t* es,
             metrics_t *metric
) {
    int32_t parent;

    while (idx > 0) {
        parent = (idx - 1) / 2;

        metric->num_compar++;
        if (es->compare_fcn(input_tuple, buffer - parent*es->record_size) >= 0) {
            break;
        }
        metric->num_memcpys++;
        memcpy(buffer - idx*es->record_size, buffer - parent*es->record_size, (size_t)es->record_size);
        idx = parent;
    }
    metric->num_memcpys++;
    memcpy(buffer - idx*es->record_size, input_tuple, (size_t)es->record_size);
}


