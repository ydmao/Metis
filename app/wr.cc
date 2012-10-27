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
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <strings.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sched.h>
#include "wr.hh"
#include "bench.hh"
#ifdef JOS_USER
#include "wc-datafile.h"
#include <inc/sysprof.h>
#endif

#define DEFAULT_NDISP 10

static void usage(const char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf
	("  -p #procs : # of processors to use (use all cores by default)\n");
    printf
	("  -m #map tasks : # of map tasks (pre-split input before MR. 16 tasks per core by default)\n");
    printf
	("  -r #reduce tasks : # of reduce tasks (16 tasks per core by default)\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 5, reduce_tasks = 0, quiet = 0;
    int c;
    if (argc < 2)
	usage(argv[0]);
    while ((c = getopt(argc - 1, argv + 1, "p:l:m:r:q")) != -1) {
	switch (c) {
	case 'p':
	    nprocs = atoi(optarg);
	    break;
	case 'l':
	    ndisp = atoi(optarg);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'r':
	    reduce_tasks = atoi(optarg);
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
    wr app(argv[1], map_tasks);
    app.set_ncore(nprocs);
    app.set_group_task(reduce_tasks);
    app.sched_run();
    app.print_stats();
    if (!quiet)
	print_top(&app.results_, ndisp, count(&app.results_));
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
