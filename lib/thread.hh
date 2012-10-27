#ifndef THREAD_HH_
#define THREAD_HH_ 1

#include <pthread.h>
#include <inttypes.h>

void mthread_init(int ncore);
void mthread_finalize(void);
void mthread_create(pthread_t * tid, int lid,
		    void *(*start_routine) (void *), void *arg);
void mthread_join(pthread_t tid, int lid, void **exitcode);
#endif
