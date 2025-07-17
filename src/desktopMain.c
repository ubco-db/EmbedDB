#ifndef PIO_UNIT_TESTING

/**
 * 0 - 2 are for benchmarks
 * 3 is for the example program
 *
 */
#ifndef WHICH_PROGRAM
#define WHICH_PROGRAM 0
#endif

#if WHICH_PROGRAM == 0
#include "embedDBExample.h"
#elif WHICH_PROGRAM == 1
#include "benchmarks/sequentialDataBenchmark.h"
#elif WHICH_PROGRAM == 2
#include "benchmarks/variableDataBenchmark.h"
#elif WHICH_PROGRAM == 3
#include "benchmarks/queryInterfaceBenchmark.h"
#elif WHICH_PROGRAM == 4
#include "benchmarks/activeRulesBenchmark.h"
#endif

int main() {
#if WHICH_PROGRAM == 0
    return embedDBExample();
#elif WHICH_PROGRAM == 1
    return runalltests_embedDB();
#elif WHICH_PROGRAM == 2
    return test_vardata();
#elif WHICH_PROGRAM == 3
    return advancedQueryExample();
#elif WHICH_PROGRAM == 4
    return activeRulesBenchmark();
#endif
}

#endif
