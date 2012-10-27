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
#include "bench.hh"
#include "wr.hh"
#include "test_util.hh"

#define DEFAULT_NDISP 10

static void usage(char *prog) {
    printf("usage: %s [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use (use all cores by default)\n");
    printf("  -m #map tasks : # of map tasks (16 tasks per core by default)\n");
    printf("  -r #reduce tasks : # of reduce tasks (determined by sampling by default)\n");
    printf("  -l ntops : # of top key/value pairs to display\n");
    printf("  -s inputsize : size of input in MB\n");
    printf("  -q : quiet output (for batch test)\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    affinity_set(0);
    int nprocs = 0, map_tasks = 0, ndisp = 5, reduce_tasks = 0, quiet = 0;
    uint64_t inputsize = 0x80000000;
    int c;
    while ((c = getopt(argc, argv, "p:l:m:r:qs:")) != -1) {
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
	case 's':
	    inputsize = atol(optarg) * 1024 * 1024;
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
    enum { wordlength = 3 };
    uint32_t seed = 0;
    char *fdata = (char *) mmap(NULL, inputsize + 1, PROT_READ | PROT_WRITE,
	MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(fdata != MAP_FAILED);
    uint64_t pos = 0;
    size_t n = 0;
    for (uint64_t i = 0; i < inputsize / (wordlength + 1); ++i, ++n) {
	for (int j = 0; j < wordlength; ++j)
	    fdata[pos++] = rnd(&seed) % 26 + 'A';
	fdata[pos++] = ' ';
    }
    memset(&fdata[pos], 0, inputsize - pos);

    mapreduce_appbase::initialize();
    wr app(fdata, inputsize, map_tasks);
    app.set_ncore(nprocs);
    app.set_group_task(reduce_tasks);
    app.sched_run();
    app.print_stats();
    size_t nw = count(&app.results_);
    CHECK_EQ(n, nw);
    if (!quiet)
	print_top(&app.results_, ndisp, nw);
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
