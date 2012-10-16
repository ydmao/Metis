#include "platform.hh"
#include <string.h>
#include <assert.h>

int
affinity_set(int cpu)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    return sched_setaffinity(0, sizeof(cpuset), &cpuset);
}

pthread_t
getself()
{
    return pthread_self();
}

pthread_t
create_thread(thread_entry_t meth, void *arg)
{
    pthread_t tid;
    assert(pthread_create(&tid, NULL, meth, arg) == 0);
    return tid;
}

