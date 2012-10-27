#ifndef THREADINFO_HH_
#define THREADINFO_HH_ 1

#include <pthread.h>
#include <assert.h>

struct threadinfo {
    static threadinfo *current() {
        threadinfo *ti = (threadinfo *)pthread_getspecific(key_);
        if (!ti) {
            ti = new threadinfo;
            pthread_setspecific(key_, ti);
        }
        return ti;
    }
    static void initialize() {
        assert(pthread_key_create(&key_, free_threadinfo) == 0);
        created_ = true;
    }
    static bool initialized() {
        return created_;
    }

    int cur_reduce_task_;
    int cur_core_;
  private:
    static void free_threadinfo(void *ti) {
        delete (threadinfo *)ti;
    }
    static bool created_;
    static pthread_key_t key_;
};

#endif
