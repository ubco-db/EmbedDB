#if !defined(NO_OUTPUT_HEAP_H)
#define NO_OUTPUT_HEAP_H

#if defined(ARDUINO)
#include "../../../../serial/serial_c_iface.h"
#include "../../../../file/kv_stdio_intercept.h"
#include "../../../../file/sd_stdio_c_iface.h"
#endif

#include <stdint.h>

#include "external_sort.h"

#if defined(__cplusplus)
extern "C" {
#endif

void heapify(   char* buffer,
                    void* input_tuple,
                    int32_t size,
                    external_sort_t* es,
                    metrics_t *metric
);

void shiftUp(char* buffer,
                 void* input_tuple,
                 int32_t idx,
                 external_sort_t* es,
                 metrics_t *metric
);


void heapify_rev(   char* buffer,
                void* input_tuple,
                int32_t size,
                external_sort_t* es,
                metrics_t *metric
);

void shiftUp_rev(char* buffer,
             void* input_tuple,
             int32_t idx,
             external_sort_t* es,
             metrics_t *metric
);

#if defined(__cplusplus)
}
#endif

#endif
