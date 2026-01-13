#include "debug_print.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>

#if defined(_WIN32) || defined(_WIN64)
#include <io.h>
#define write _write
#else
#include <unistd.h>
#endif

void debug_log(const char *format, ...) {
    char buf[256];
    va_list ap;
    va_start(ap, format);
    int n = vsnprintf(buf, sizeof(buf), format, ap);
    va_end(ap);
    if (n > 0) {
        if (n > (int)sizeof(buf)) n = sizeof(buf);
        /* Use low-level write to avoid stdio buffering/locks that can block under some debuggers/targets */
        (void)write(2, buf, n);
    }
}
