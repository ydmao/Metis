/* Copyright (c) 2007, Stanford University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Stanford University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <sched.h>
#include <time.h>
#include <sys/time.h>
#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"

enum { pre_fault = 0 };

struct POINT_T {
    char x;
    char y;
};

enum {
    KEY_SX = 0,
    KEY_SY,
    KEY_SXX,
    KEY_SYY,
    KEY_SXY,
};

struct lr : public map_reduce {
    lr(const char *f, int nsplit) : s_(f, nsplit) {
        s_.trim(round_down(s_.size(), sizeof(POINT_T)));
        if (pre_fault)
            printf("ignore this %d\n", s_.prefault());
    }
    int key_compare(const void *v1, const void *v2) {
        prof_enterkcmp();
        long int i1 = (long int) v1;
        long int i2 = (long int) v2;
        int r = i2 - i1;
       prof_leavekcmp();
       return r;
    }
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, NULL, sizeof(POINT_T));
    }
    unsigned partition(void *k, int length) {
        assert(length == sizeof(void *));
        return unsigned(intptr_t(k));
    }
    void map_function(split_t *);
    void reduce_function(void *k, void **v, size_t length);
    int combine_function(void *k, void **v, size_t length);
    defsplitter s_;
};

/** Sorts based on the val output of wordcount */
void lr::map_function(split_t *args) {
    assert(args);
    POINT_T *data = (POINT_T *) args->data;
    assert(data);
    prof_enterapp();
    long long SX, SXX, SY, SYY, SXY;
    SX = SXX = SY = SYY = SXY = 0;
    assert(args->length % sizeof(POINT_T) == 0);
    for (unsigned long i = 0; i < args->length / sizeof(POINT_T); i++) {
	//Compute SX, SY, SYY, SXX, SXY
	SX += data[i].x;
	SXX += data[i].x * data[i].x;
	SY += data[i].y;
	SYY += data[i].y * data[i].y;
	SXY += data[i].x * data[i].y;
    }
    prof_leaveapp();
    map_emit((void *) KEY_SX, (void *) SX, sizeof(void *));
    map_emit((void *) KEY_SXX, (void *) SXX, sizeof(void *));
    map_emit((void *) KEY_SY, (void *) SY, sizeof(void *));
    map_emit((void *) KEY_SYY, (void *) SYY, sizeof(void *));
    map_emit((void *) KEY_SXY, (void *) SXY, sizeof(void *));
}

void lr::reduce_function(void *key_in, void **vals_in, size_t vals_len) {
    prof_enterapp();
    long long *vals = (long long *) vals_in;
    long long sum = 0;
    assert(vals);
    for (size_t i = 0; i < vals_len; i++)
	sum += (uint64_t) vals[i];
    prof_enterapp();
    reduce_emit(key_in, (void *) sum);
}

int lr::combine_function(void *, void **vals_in, size_t vals_len) {
    prof_enterapp();
    long long *vals = (long long *) vals_in;
    long long sum = 0;
    assert(vals);
    for (size_t i = 0; i < vals_len; ++i)
	sum += vals[i];
    vals[0] = sum;
    prof_leaveapp();
    return 1;
}

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -d : debug output\n");
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, quiet = 0;
    int c;
    if (argc < 2) {
	usage(argv[0]);
	exit(EXIT_FAILURE);
    }
    while ((c = getopt(argc - 1, argv + 1, "p:m:q")) != -1) {
	switch (c) {
	case 'p':
	    nprocs = atoi(optarg);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'q':
	    quiet = 1;
	    break;
	default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	    break;
	}
    }
    mapreduce_appbase::initialize();
    lr app(argv[1], map_tasks);
    app.set_ncore(nprocs);
    cond_printf(!quiet, "Linear regression: running...\n");
    app.sched_run();
    app.print_stats();

    long long n;
    double a, b, xbar, ybar, r2;
    long long SX_ll = 0, SY_ll = 0, SXX_ll = 0, SYY_ll = 0, SXY_ll = 0;
    // ADD UP RESULTS
    for (size_t i = 0; i < app.results_.size(); ++i) {
	keyval_t *curr = &app.results_[i];
	switch ((long int) curr->key) {
	case KEY_SX:
	    SX_ll = (long long) curr->val;
	    break;
	case KEY_SY:
	    SY_ll = (long long) curr->val;
	    break;
	case KEY_SXX:
	    SXX_ll = (long long) curr->val;
	    break;
	case KEY_SYY:
	    SYY_ll = (long long) curr->val;
	    break;
	case KEY_SXY:
	    SXY_ll = (long long) curr->val;
	    break;
	default:
	    // INVALID KEY
	    assert(0);
	    break;
	}
    }

    double SX = (double) SX_ll;
    double SY = (double) SY_ll;
    double SXX = (double) SXX_ll;
    double SYY = (double) SYY_ll;
    double SXY = (double) SXY_ll;

    n = (long long) app.s_.size() / sizeof(POINT_T);
    b = (double) (n * SXY - SX * SY) / (n * SXX - SX * SX);
    a = (SY_ll - b * SX_ll) / n;
    xbar = (double) SX_ll / n;
    ybar = (double) SY_ll / n;
    r2 = (double) (n * SXY - SX * SY) * (n * SXY -
					 SX * SY) / ((n * SXX -
						      SX * SX) * (n * SYY -
								  SY * SY));

    if (!quiet) {
	printf("%2d Linear Regression Results:\n", nprocs);
	printf("\ta    = %lf\n", a);
	printf("\tb    = %lf\n", b);
	printf("\txbar = %lf\n", xbar);
	printf("\tybar = %lf\n", ybar);
	printf("\tr2   = %lf\n", r2);
	printf("\tSX   = %lld\n", SX_ll);
	printf("\tSY   = %lld\n", SY_ll);
	printf("\tSXX  = %lld\n", SXX_ll);
	printf("\tSYY  = %lld\n", SYY_ll);
	printf("\tSXY  = %lld\n", SXY_ll);
    }
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
