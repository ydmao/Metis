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
