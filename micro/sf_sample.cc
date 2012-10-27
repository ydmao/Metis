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
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include "bench.hh"

struct gstate_type {
    volatile int start;
    union {
	struct {
	    volatile int ready;
	    volatile uint64_t cycles;
	} v;
	char __pad[JOS_CLINE];
    } state[JOS_NCPU] __attribute__ ((aligned(JOS_CLINE)));
};

static gstate_type *gstate;

static uint64_t ncores;

enum { nmallocs = 1000000 };

void *
worker(void *arg)
{
    int c = ptr2int<int>(arg);
    affinity_set(c);
    if (c) {
	gstate->state[c].v.ready = 1;
	while (!gstate->start) ;
    } else {
	for (uint64_t i = 1; i < ncores; i++) {
	    while (!gstate->state[i].v.ready) ;
	    gstate->state[i].v.ready = 0;
	}
	gstate->start = 1;
    }
    uint64_t start = read_tsc();
    for (uint64_t i = 0; i < nmallocs; i++) {
	void *p = malloc(100);
        (void) p;
    }
    uint64_t end = read_tsc();
    gstate->state[c].v.cycles = end - start;
    gstate->state[c].v.ready = 1;
    if (!c) {
	for (uint64_t i = 1; i < ncores; i++)
	    while (!gstate->state[i].v.ready) ;
	uint64_t ncycles = 0;
	for (uint64_t i = 0; i < ncores; i++)
	    ncycles += gstate->state[i].v.cycles;
	printf("Cycles per malloc: %ld\n", ncycles / nmallocs);
    }
    return NULL;
}

int
main(int argc, char **argv)
{
    affinity_set(0);
    if (argc < 2) {
	printf("Usage: <%s> number-cores\n", argv[0]);
	exit(EXIT_FAILURE);
    }
    ncores = atoi(argv[1]);
    assert(ncores <= JOS_NCPU);
    gstate = (gstate_type *)
	mmap(NULL, sizeof(gstate_type), PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
	     -1, 0);
    memset(gstate, 0, sizeof(gstate_type));
    if (gstate == MAP_FAILED) {
	printf("mmap error: %d\n", errno);
	exit(EXIT_FAILURE);
    }
    for (uint64_t i = 1; i < ncores; i++) {
	pthread_t tid;
	pthread_create(&tid, NULL, worker, int2ptr(i));
    }
    uint64_t start = read_tsc();
    worker(int2ptr(0));
    uint64_t end = read_tsc();
    printf("Total time %ld million cycles\n", (end - start) / 1000000);
    munmap(gstate, sizeof(*gstate));
}
