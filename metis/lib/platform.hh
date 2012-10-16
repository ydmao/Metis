#ifndef PLATFORM_H
#define PLATFORM_H

#include <pthread.h>
#include <sys/unistd.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sched.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/resource.h>

enum { debug = 0 };

int affinity_set(int cpu);
#define threadid_t pthread_t
typedef void *(*thread_entry_t) (void *);

threadid_t getself();
threadid_t create_thread(thread_entry_t, void *arg);

#define TLS __thread

#define barrier() __asm__ __volatile__("mfence": : :"memory")
#define mr_print(flag, x...) \
do \
{ \
    if (flag) \
    { \
        printf(x); \
    } \
}while(0)
#define dprintf(x...) mr_print(debug, x)
#endif
