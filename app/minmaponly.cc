// Supplied by malte.schwarzkopf@cl.cam.ac.uk

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
#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"
#ifdef JOS_USER
#include <inc/sysprof.h>
#endif

#define DEFAULT_NDISP 10

/* Hadoop print all the key/value paris at the end.  This option 
 * enables wordcount to print all pairs for fair comparison. */
//#define HADOOP

static int alphanumeric;

struct minmaponly : public map_only {
    minmaponly(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, " \t\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }
    void map_function(split_t *ma) {
        char k[1024];
        size_t klen;
        split_word sw(ma);
        while (sw.fill(k, sizeof(k), klen))
            map_emit(k, (void *)1, klen);
    }
    void *key_copy(void *src, size_t s) {
        char *key = safe_malloc<char>(s + 1);
        memcpy(key, src, s);
        key[s] = 0;
        return key;
    }
  private:
    defsplitter s_;
};

static void print_top(xarray<keyval_t> *wc_vals, size_t ndisp) {
    size_t occurs = 0;
    for (uint32_t i = 0; i < wc_vals->size(); i++)
	occurs += size_t(wc_vals->at(i)->val);
    printf("\nwordcount: results (TOP %zd from %zu keys, %zd words):\n",
           ndisp, wc_vals->size(), occurs);
#ifdef HADOOP
    ndisp = wc_vals->size();
#else
    ndisp = std::min(ndisp, wc_vals->size());
#endif
    for (size_t i = 0; i < ndisp; i++) {
	keyval_t *w = wc_vals->at(i);
	printf("%15s - %d\n", (char *)w->key_, ptr2int<unsigned>(w->val));
    }
}

static void output_all(xarray<keyval_t> *wc_vals, FILE *fout) {
    for (uint32_t i = 0; i < wc_vals->size(); i++) {
	keyval_t *w = wc_vals->at(i);
	fprintf(fout, "%18s - %lu\n", (char *)w->key_,  (uintptr_t)w->val);
    }
}

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -a : alphanumeric word count\n");
    printf("  -o filename : save output to a file\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 5;
    int quiet = 0;
    int c;
    if (argc < 2)
	usage(argv[0]);
    char *fn = argv[1];
    FILE *fout = NULL;

    while ((c = getopt(argc - 1, argv + 1, "p:s:l:m:qao:")) != -1) {
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
	case 'q':
	    quiet = 1;
	    break;
	case 'a':
	    alphanumeric = 1;
	    break;
	case 'o':
	    fout = fopen(optarg, "w+");
	    if (!fout) {
		fprintf(stderr, "unable to open %s: %s\n", optarg,
			strerror(errno));
		exit(EXIT_FAILURE);
	    }
	    break;
	default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	    break;
	}
    }
    mapreduce_appbase::initialize();
    /* get input file */
    minmaponly app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.sched_run();
    app.print_stats();
    /* get the number of results to display */
    if (!quiet)
	print_top(&app.results_, ndisp);
    if (fout) {
	output_all(&app.results_, fout);
	fclose(fout);
    }
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
