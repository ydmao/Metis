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
#include <sys/resource.h>
#include <iostream>
#include "profile.hh"
#include "bench.hh"
#include "ibs.hh"
#include "mr-types.hh"
#include "threadinfo.hh"

#ifdef PROFILE_ENABLED
enum { profile_app = 1 };
enum { profile_phases = 1 };
enum { profile_kcmp = 0 };
enum { profile_worker = 1 };
/* Make sure the pmcs are programmed before enabling pmc */
enum { pmc_enabled = 0 };

enum { pmc0, pmc1, pmc2, pmc3, ibslat, ibscnt, tsc, app_tsc, app_kcmp,
       app_pmc1, statcnt };

#define stringify(name) #name

static const char *cname[] = {
    stringify(pmc0),
    stringify(pmc1),
    stringify(pmc2),
    stringify(pmc3),
    stringify(ibslat),
    stringify(ibscnt),
    stringify(tsc),
    stringify(app_tsc),
    stringify(app_kcmp),
    stringify(app_pmc1),
};

static inline uint64_t __read_pmc(int ecx) {
    if (pmc_enabled)
	return read_pmc(ecx);
    else
	return 0;
}

struct __attribute__ ((aligned(JOS_CLINE))) percore_stat {
    uint64_t v[MR_PHASES][statcnt];
    rusage ru_;
    
    void enterkcmp() {
        ++v[cp_][app_kcmp];
    }
    void leavekcmp() {
    }
    void enterapp() {
        last_[app_tsc] = read_tsc();
        last_[app_pmc1] = __read_pmc(1);
    }
    void leaveapp() {
        v[cp_][app_tsc] += read_tsc() - last_[app_tsc];
        v[cp_][app_pmc1] += __read_pmc(1) - last_[app_pmc1];
    }
    void worker_start(int phase, int cid) {
        cp_ = phase;
        v[cp_][app_tsc] = 0;
        v[cp_][app_kcmp] = 0;
        v[cp_][app_pmc1] = 0;
        for (int i = 0; i < 4; ++i)
            last_[i] = __read_pmc(i);

        ibs_start(cid);
        last_[ibscnt] = ibs_read_count(cid);
        last_[ibslat] = ibs_read_latency(cid);
        last_[tsc] = read_tsc();
    }
    void worker_end(int phase, int cid) {
        assert(phase == cp_);
        for (int i = 0; i < 4; ++i)
            v[cp_][i] = __read_pmc(i) - last_[i];

        ibs_stop(cid);
        v[cp_][ibslat] = ibs_read_latency(cid) - last_[ibslat];
        v[cp_][ibscnt] = ibs_read_count(cid) - last_[ibscnt];
        v[cp_][tsc] = read_tsc() - last_[tsc];
    }
    void sum(uint64_t &app_tsc, uint64_t &kcmp) {
        app_tsc = 0;
        kcmp = 0;
        for (int i = 0; i < MR_PHASES; ++i) {
            app_tsc += v[i][app_tsc];
            kcmp += v[i][app_kcmp];
        }
    }

  private:
    int cp_; // current phase
    uint64_t last_[statcnt];
};

static percore_stat stats[JOS_NCPU];

void prof_enterkcmp() {
    if (profile_app) {
        threadinfo *ti = threadinfo::current();
        stats[ti->cur_core_].enterkcmp();
    }
}

void prof_leavekcmp() {
    if (profile_app) {
        threadinfo *ti = threadinfo::current();
        stats[ti->cur_core_].leavekcmp();
    }
}

void prof_enterapp() {
    if (profile_app) {
	threadinfo *ti  = threadinfo::current();
        stats[ti->cur_core_].enterapp();
    }
}

void prof_leaveapp() {
    if (profile_app) {
        threadinfo *ti = threadinfo::current();
        stats[ti->cur_core_].leaveapp();
    }
}

void prof_worker_start(int phase, int cid) {
    stats[cid].worker_start(phase, cid);
}

void prof_worker_end(int phase, int cid) {
    stats[cid].worker_end(phase, cid);
}

static void prof_print_phase(int phase, int ncores, uint64_t scale) {
    uint64_t tots[statcnt];
    memset(tots, 0, sizeof(tots));
    printf("core\t");
#define WIDTH "10"
    for (int i = 0; i < statcnt; ++i)
	printf("%" WIDTH "s", cname[i]);
    printf("\n");
    for (int i = 0; i < ncores; ++i) {
	printf("%d\t", i);
	for (int j = 0; j < statcnt; ++j) {
	    printf("%" WIDTH "ld", stats[i].v[phase][j] / scale);
	    tots[j] += stats[i].v[phase][j] / scale;
	}
	printf("\n");
    }
    printf("total@%d\t", phase);
    for (int i = 0; i < statcnt; ++i)
	printf("%" WIDTH "ld", tots[i]);
    printf("\n");
    printf("total[ibslat] / total[ibscnt] = %ld, total[pmc0] / total[pmc] = %4.2f\n",
	   tots[ibslat] / (tots[ibscnt] + 1),
	   (double) tots[pmc0] / (double) tots[pmc1]);
}

void prof_print(int ncores) {
    if (profile_kcmp) {
	uint64_t tt = 0;
	uint64_t tkcmp = 0;
	for (int i = 0; i < ncores; ++i) {
            uint64_t app_tsc, kcmp;
            std::cout << i << "\t" << cycle_to_ms(app_tsc) << "ms, kcmp " 
                      << kcmp << std::endl;
	    tt += app_tsc;
	    tkcmp += kcmp;
	}
	std::cout << "Average time spent in application is " << cycle_to_ms(tt) 
                  << ", total key_compare " << tkcmp << std::endl;
    }
    if (profile_worker) {
	uint64_t scale = 1000;
	printf("MAP[scale=%ld]\n", scale);
	prof_print_phase(MAP, ncores, scale);
	printf("REDUCE[scale=%ld]\n", scale);
	prof_print_phase(REDUCE, ncores, scale);
	printf("MERGE[scale=%ld]\n", scale);
	prof_print_phase(MERGE, ncores, scale);
    }
}

void prof_phase_init() {
    if (!profile_phases)
	return;
    threadinfo *ti = threadinfo::current();
    assert(getrusage(RUSAGE_SELF, &stats[ti->cur_core_].ru_) == 0);
}

void prof_phase_end() {
    if (!profile_phases)
	return;
    rusage ru;
    assert(getrusage(RUSAGE_SELF, &ru) == 0);
    threadinfo *ti = threadinfo::current();
    percore_stat *st = &stats[ti->cur_core_];
    printf("time(ms) user: %ld, system: %ld\n",
           tv2ms(ru.ru_utime) - tv2ms(st->ru_.ru_utime),
           tv2ms(ru.ru_stime) - tv2ms(st->ru_.ru_stime));
}

#endif
