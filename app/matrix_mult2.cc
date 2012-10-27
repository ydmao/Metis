/**
 * Matrix multiply optimized by Mark Roth (mroth@cs.sfu.ca)
 * SFU Systems Research Group (http://synar.cs.sfu.ca/systems-research.html)
 *
 * The optimizations are: 
 *
 * - Swapping the order of the inner two most loops of matrixmult_map, which
 *   improves performance by ~3.5x on input size 4096 running on a 24 core 
 *   AMD system with 64kb L1 cache and 2 way associativity. Performance also 
 *   seems to increase in general by 30% on other input sizes as cache line 
 *   reuse is increased.
 *
 * - Using processInnerLoop() to let gcc vectorize the inner loop at 
 *   O3. Speed up is ~3x over the above version.
 *
 * - The last optimization helps to prevent L1 collisions by pre-faulting
 *   the matrix pages randomly. This is useful for caches that have a low 
 *   associativity and with inputs that are multiples of 2048. On an AMD 
 *   system with a 64kb 2 way associative cache, the patch makes about a 
 *   20-30% improvement for input size 4096.
 */
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

struct mm2 : public mm {
   mm2(int nsplit, bool block_based) : mm(nsplit, block_based) {}
   void map_function_block(split_t *);
};

/** Extract inner loop to make auto vectorization easier to analyze  */
void processInnerLoop(int* out, int out_offset, int* mat_a, int a_offset, int *mat_b,
                 int b_offset, int start, int end) {
    int a = mat_a[a_offset];
    for (int i = start; i < end; ++i)
	out[out_offset + i] += a * mat_b[b_offset + i];
}

/** Multiplies the allocated regions of matrix to compute partial sums */
void mm2::map_function_block(split_t *args) {
    int end_i, end_j, end_k, a, c;
    prof_enterapp();
    assert(args && args->data);
    mm_data_t *data = (mm_data_t *) (args->data);
    dprintf("%d Start Loop \n", data->row_num);
    int i = data->startrow;
    int j = data->startcol;
    dprintf("do %d %d of %d\n", i, j, data->matrix_len);
    for (int k = 0; k < data->matrix_len; k += block_len) {
	end_i = i + block_len;
	end_j = j + block_len;
	end_k = k + block_len;
	int end = (end_j < data->matrix_len) ? end_j : data->matrix_len;
	for (a = i; a < end_i && a < data->matrix_len; ++a)
            for (c = k; c < end_k && c < data->matrix_len; ++c)
	        processInnerLoop(data->output, data->matrix_len * a, data->matrix_A, 
                                 data->matrix_len * a + c, data->matrix_B, 
                                 data->matrix_len * c, j, end);
    }
    dprintf("Finished Map task %d\n", data->row_num);
    fflush(stdout);
    free(data);
    prof_leaveapp();
}

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
    mm2 app(block_based ? 0 : map_tasks, block_based);

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

