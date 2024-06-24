#ifndef PIO_UNIT_TESTING

/**
 * 0 - 2 are for benchmarks
 * 3 is for the example program
 *
 */
#ifndef WHICH_PROGRAM
#define WHICH_PROGRAM 3
#endif

#if WHICH_PROGRAM == 0
#include "benchmarks/sequentialDataBenchmark.h"
#elif WHICH_PROGRAM == 1
#include "benchmarks/variableDataBenchmark.h"
#elif WHICH_PROGRAM == 2
#include "benchmarks/queryInterfaceBenchmark.h"
#elif WHICH_PROGRAM == 3
#include "embedDBExample.h"
#endif

void main() {
#if WHICH_PROGRAM == 0
    runalltests_embedDB();
#elif WHICH_PROGRAM == 1
    test_vardata();
#elif WHICH_PROGRAM == 2
    advancedQueryExample();
#elif WHICH_PROGRAM == 3
    embedDBExample();
#endif
}

#endif
