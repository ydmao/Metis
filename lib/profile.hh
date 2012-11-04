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
#ifndef PROFILE_HH_
#define PROFILE_HH_ 1

#include <inttypes.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef PROFILE_ENABLED
void prof_enterapp();
void prof_leaveapp();

void prof_enterkcmp();
void prof_leavekcmp();

void prof_worker_start(int phase, int cid);
void prof_worker_end(int phase, int cid);
void prof_print(int ncores);

void prof_phase_init();
void prof_phase_end();

#else

#define prof_enterapp()
#define prof_leaveapp()

#define prof_enterkcmp()
#define prof_leavekcmp()

#define prof_phase_init()
#define prof_phase_end()

#define prof_worker_start(phase, cid)
#define prof_worker_end(phase, cid)
#define prof_print(ncpus)
#endif
#endif
