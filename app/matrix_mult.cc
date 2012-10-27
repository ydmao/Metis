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
#include "mm.hh"
#include "bench.hh"

enum { block_based = 1 };

int main(int argc, char *argv[]) {
    int matrix_len = 0;
    int *matrix_A_ptr, *matrix_B_ptr, *fdata_out;
    int nprocs = 0, map_tasks = 0;
    int quiet = 0;
    srand((unsigned) time(NULL));
    if (argc < 2) {
	usage(argv[0]);
	exit(EXIT_FAILURE);
    }

    int c;
    while ((c = getopt(argc, argv, "p:m:ql:")) != -1) {
	switch (c) {
	case 'p':
	    assert((nprocs = atoi(optarg)) >= 0);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'q':
	    quiet = 1;
	    break;
	case 'l':
	    assert((matrix_len = atoi(optarg)) > 0);
	    break;
	default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	}
    }
    matrix_A_ptr = safe_malloc<int>(matrix_len * matrix_len);
    matrix_B_ptr = safe_malloc<int>(matrix_len * matrix_len);
    fdata_out = safe_malloc<int>(matrix_len * matrix_len);

    for (int i = 0; i < matrix_len; i++)
	for (int j = 0; j < matrix_len; j++) {
	    matrix_A_ptr[i * matrix_len + j] = rand();
	    matrix_B_ptr[i * matrix_len + j] = rand();
	}
    mapreduce_appbase::initialize();
    mm app(block_based ? 0 : map_tasks, block_based);

    app.d_.matrix_len = matrix_len;
    app.d_.row_num = 0;
    app.d_.startrow = 0;
    app.d_.startcol = 0;
    app.d_.matrix_A = matrix_A_ptr;
    app.d_.matrix_B = matrix_B_ptr;
    app.d_.output = ((int *) fdata_out);

    app.set_ncore(nprocs);
    app.sched_run();
    app.print_stats();
    if (!quiet) {
	printf("First row of the output matrix:\n");
	for (int i = 0; i < matrix_len; i++)
	    printf("%d\t", fdata_out[i]);
	printf("\nLast row of the output matrix:\n");
	for (int i = 0; i < matrix_len; i++)
	    printf("%d\t", fdata_out[(matrix_len - 1) * matrix_len + i]);
	printf("\n");
    }
    free(matrix_A_ptr);
    free(matrix_B_ptr);
    free(fdata_out);
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
