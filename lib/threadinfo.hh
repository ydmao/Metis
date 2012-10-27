/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#ifndef THREADINFO_HH_
#define THREADINFO_HH_ 1

#include <pthread.h>
#include <assert.h>
#include <stdlib.h>

struct threadinfo {
    static threadinfo *current() {
        threadinfo *ti = (threadinfo *)pthread_getspecific(key_);
        if (!ti) {
            ti = (threadinfo *)malloc(sizeof(threadinfo));
            pthread_setspecific(key_, ti);
        }
        return ti;
    }
    static void initialize() {
        assert(pthread_key_create(&key_, free) == 0);
        created_ = true;
    }
    static bool initialized() {
        return created_;
    }

    int cur_reduce_task_;
    int cur_core_;
  private:
    static bool created_;
    static pthread_key_t key_;
};

#endif
