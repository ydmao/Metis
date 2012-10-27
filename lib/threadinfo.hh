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
        static bool created = false;
        if (created)
            return;
        assert(pthread_key_create(&key_, NULL) == 0);
        created = true;
    }
    int cur_reduce_task_;
    int cur_core_;
  private:
    static pthread_key_t key_;
};

#endif
