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
#include "bench.hh"

//#define MAPONLY

static int nsplits = 0;

typedef struct {
    int **matrix;
    keyval_t *mean;
    long unit_size;		// size of one row
    int next_start_row;
    int next_cov_row;

    int i;
    int j;
} __attribute__ ((aligned(CACHE_LINE_SIZE))) pca_data_t;

typedef struct {
    int **matrix;
    int start_row;
} __attribute__ ((aligned(CACHE_LINE_SIZE))) pca_map_data_t;

typedef struct {
    int start_row;
    int cov_row;
} __attribute__ ((aligned(CACHE_LINE_SIZE))) pca_cov_loc_t;

typedef struct {
    int **matrix;
    keyval_t *mean;
    int size;			// number of cov_locs
    pca_cov_loc_t *cov_locs;
} __attribute__ ((aligned(CACHE_LINE_SIZE))) pca_cov_data_t;

#define DEF_GRID_SIZE 100	// all values in the matrix are from 0 to this value
#define DEF_NUM_ROWS 10
#define DEF_NUM_COLS 10

pca_data_t pca_data_;
int num_rows;
int num_cols;
int grid_size;

#ifdef MAPONLY
struct pca_mean : map_only {
#else
struct pca_mean : map_reduce {
#endif
    int key_compare(const void *v1, const void *v2) {
        prof_enterkcmp();
        int r = (*(int *)v1) - (*(int *)v2);
        assert(v1 == v2 || r);
        prof_leavekcmp();
        return r;
    }
    bool split(split_t *out, int ncores);
    void map_function(split_t *out);
    void reduce_function(void *key, void **vals, size_t length) {
        assert(length == 1);
        reduce_emit(key, vals[0]);
    }
};

/** dump_points()
 *  Print the values in the matrix to the screen
 */
static void dump_points(int **vals, int rows, int cols) {
    for (int i = 0; i < rows; i++) {
	for (int j = 0; j < cols; j++)
	    dprintf("%5d ", vals[i][j]);
	dprintf("\n");
    }
}

/** generate_points()
 *  Create the values in the matrix
 */
static void generate_points(int **pts, int rows, int cols) {
    for (int i = 0; i < rows; i++)
	for (int j = 0; j < cols; j++)
	    pts[i][j] = rand() % grid_size;
}

/** Assigns one or more points to each map task */
bool pca_mean::split(split_t * out, int ncores) {
    assert(out);
    pca_data_t *pca_data = &pca_data_;
    assert(pca_data->matrix);
    if (nsplits == 0)
	nsplits = ncores * def_nsplits_per_core;
    int req_units =
	num_rows * num_cols * sizeof(int) / nsplits / pca_data->unit_size;
    assert(req_units);

    /* Assign a fixed number of rows to each map task */
    if (pca_data->next_start_row >= num_rows)
	return false;
    prof_enterapp();
    pca_map_data_t *map_data = safe_malloc<pca_map_data_t>();

    /* Allocate last few rows if less than required number of rows */
    if (pca_data->next_start_row + req_units <= num_rows) {
	out->length = req_units;
	out->data = (void *) map_data;
	map_data->matrix = &(pca_data->matrix[pca_data->next_start_row]);
	map_data->start_row = pca_data->next_start_row;
    } else {
	out->length = num_rows - pca_data->next_start_row;
	out->data = (void *) map_data;
	map_data->matrix = &(pca_data->matrix[pca_data->next_start_row]);
	map_data->start_row = pca_data->next_start_row;
    }
    dprintf("Returning %" PRIuPTR " rows starting at %d\n", out->length,
	    map_data->start_row);
    pca_data->next_start_row += req_units;
    prof_leaveapp();
    return true;
}

/** Map task to compute the mean */
void pca_mean::map_function(split_t *args) {
    prof_enterapp();
    pca_map_data_t *data = (pca_map_data_t *) args->data;
    int **matrix = data->matrix;
    /* Compute the mean for the allocated rows to the map task */
    for (uint32_t i = 0; i < args->length; ++i) {
	int *mean = safe_malloc<int>();
	int sum = 0;
	for (int j = 0; j < num_cols; j++)
	    sum += matrix[i][j];
	*mean = sum / num_cols;
	int *curr_row = safe_malloc<int>();
	*curr_row = data->start_row;
	prof_leaveapp();
	map_emit((void *) curr_row, (void *) mean, sizeof(int *));
	prof_enterapp();
	++data->start_row;
    }

    free(data);
    prof_leaveapp();
}

#ifdef MAPONLY
struct pca_cov : public map_only {
#else
struct pca_cov : public map_reduce {
#endif
    int key_compare(const void *v1, const void *v2) {
        prof_enterkcmp();
        int r = 0;
        pca_cov_loc_t *k1 = (pca_cov_loc_t *) v1;
        pca_cov_loc_t *k2 = (pca_cov_loc_t *) v2;
        if (k1->start_row != k2->start_row)
	    r = k1->start_row - k2->start_row;
        else
            r = k1->cov_row - k2->cov_row;
        prof_leavekcmp();
        return r;
    }
    bool split(split_t *out, int ncore);
    void map_function(split_t *ma);
    void reduce_function(void *key, void **vals, size_t length) {
        assert(length == 1);
        reduce_emit(key, vals[0]);
    }
};

bool pca_cov::split(split_t *out, int ncores) {
    assert(out);
    pca_data_t *pca_data = &pca_data_;
    assert(pca_data->matrix);
    assert(pca_data->mean);
    if (nsplits == 0)
	nsplits = ncores * def_nsplits_per_core;
    int req_units = ((num_rows * num_rows - num_rows) / 2 + num_rows) / nsplits;
    assert(req_units);
    if (pca_data->next_start_row >= num_rows && pca_data->next_cov_row >= num_rows)
	return false;
    prof_enterapp();
    /* Allocate memory for the structures */
    pca_cov_loc_t *cov_locs = safe_malloc<pca_cov_loc_t>(req_units);
    pca_cov_data_t *cov_data = safe_malloc<pca_cov_data_t>();
    assert(cov_locs && cov_data);
    cov_data->size = 0;
    /* Compute the boundaries of the region that is to be allocated to the map task */
    while (pca_data->next_start_row < num_rows && cov_data->size < req_units) {
	cov_locs[cov_data->size].start_row = pca_data->next_start_row;
	cov_locs[cov_data->size].cov_row = pca_data->next_cov_row;
	if (pca_data->next_cov_row + 1 >= num_rows) {
	    ++pca_data->next_start_row;
	    pca_data->next_cov_row = pca_data->next_start_row;
	} else
	    ++pca_data->next_cov_row;
	++cov_data->size;
    }

    /* Assign pointers to the matrix with the data */
    cov_data->matrix = pca_data->matrix;
    cov_data->mean = pca_data->mean;
    cov_data->cov_locs = cov_locs;

    dprintf("Returning %d elems starting <%d,%d> till <%d,%d>\n",
	    cov_data->size, cov_locs[0].start_row, cov_locs[0].cov_row,
	    cov_locs[cov_data->size - 1].start_row,
	    cov_locs[cov_data->size - 1].cov_row);
    out->length = 1;
    out->data = (void *) cov_data;
    prof_leaveapp();
    return true;
}

void pca_cov::map_function(split_t * args) {
    assert(args);
    assert(args->length == 1);
    prof_enterapp();
    pca_cov_data_t *cov_data = (pca_cov_data_t *) args->data;
    keyval_t *mean = cov_data->mean;
    long size = cov_data->size;
    long cols = num_cols;

    /* compute the covariance for the allocated region */
    for (int i = 0; i < size; i++) {
	int start_idx = cov_data->cov_locs[i].start_row;
	int cov_idx = cov_data->cov_locs[i].cov_row;
	assert(cov_idx >= start_idx);
	int *start_row = cov_data->matrix[start_idx];
	int *cov_row = cov_data->matrix[cov_idx];
	int sum = 0;
	dprintf("Mean for row %d is %d\n", start_idx,
		*((int *) (mean[start_idx].val)));
	dprintf("Mean for row %d is %d\n", cov_idx,
		*((int *) (mean[cov_idx].val)));

	for (int j = 0; j < cols; j++)
	    sum += (start_row[j] - *((int *) mean[start_idx].val)) *
		(cov_row[j] - *((int *) mean[cov_idx].val));

        int *covariance = safe_malloc<int>();
	*covariance = sum / (num_rows - 1);

	dprintf("Covariance for <%d, %d> is %d\n", start_idx, cov_idx,
		*covariance);
        pca_cov_loc_t *cov_loc = safe_malloc<pca_cov_loc_t>();
	cov_loc->start_row = cov_data->cov_locs[i].start_row;
	cov_loc->cov_row = cov_data->cov_locs[i].cov_row;
	prof_leaveapp();
	map_emit((void *) cov_loc, (void *) covariance, sizeof(pca_cov_loc_t));
	prof_enterapp();
    }

    free(cov_data->cov_locs);
    free(cov_data);
    prof_leaveapp();
}

static void usage(char *fn) {
    printf("usage: %s [options]\n", fn);
    printf("options:\n");
    printf("  -p nprocs : # of processors to use\n");
    printf("  -s split size(KB) : # of kilo-bytes for each split\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -R row : # of matrix\n");
    printf("  -C col : # of matrix\n");
    printf("  -M max : # of max number\n");
}

int main(int argc, char **argv) {
    int nprocs = 0, map_tasks = 0, nreduce_tasks = 0, quiet = 0, c;
    num_rows = DEF_NUM_ROWS;
    num_cols = DEF_NUM_COLS;
    grid_size = DEF_GRID_SIZE;

    if (argc < 2) {
	usage(argv[0]);
	exit(EXIT_FAILURE);
    }

    while ((c = getopt(argc, argv, "p:m:R:M:C:R:r:q")) != -1) {
	switch (c) {
	case 'r':
	    assert((nreduce_tasks = atoi(optarg)) >= 0);
	    break;
	case 'p':
	    assert((nprocs = atoi(optarg)) >= 0);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'q':
	    quiet = 1;
	    break;
	case 'R':
	    assert((num_rows = atoi(optarg)) >= 0);
	    break;
	case 'C':
	    assert((num_cols = atoi(optarg)) >= 0);
	    break;
	case 'M':
	    assert((grid_size = atoi(optarg)) >= 0);
	    break;
	default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	}
    }

    // Allocate space for the matrix
    pca_data_.matrix = safe_malloc<int *>(num_rows);
    for (int i = 0; i < num_rows; i++)
	pca_data_.matrix[i] = safe_malloc<int>(num_cols);

    //Generate random values for all the points in the matrix
    generate_points(pca_data_.matrix, num_rows, num_cols);

    // Print the points
    dump_points(pca_data_.matrix, num_rows, num_cols);

    /* Create the structure to store the mean value */
    pca_data_.unit_size = sizeof(int) * num_cols;	// size of one row
    pca_data_.next_start_row = pca_data_.next_cov_row = 0;
    pca_data_.mean = NULL;

    mapreduce_appbase::initialize();
    pca_mean m;
    m.set_ncore(nprocs);
#ifndef MAPONLY
    m.set_reduce_task(nreduce_tasks);
#endif
    nsplits = map_tasks;
    m.sched_run();
    m.print_stats();

    pca_data_.unit_size = sizeof(int) * num_cols * 2;	// size of two rows
    pca_data_.next_start_row = pca_data_.next_cov_row = 0;
    pca_data_.mean = m.results_.array();	// array of keys and values - 

    pca_cov cov;
    cov.set_ncore(nprocs);
#ifndef MAPONLY
    cov.set_reduce_task(nreduce_tasks);
#endif
    nsplits = map_tasks;
    cov.sched_run();
    cov.print_stats();

    assert(int(cov.results_.size()) == (num_rows * (num_rows - 1) / 2 + num_rows));
    // Free the allocated structures
    int cnt = 0;
    int rows = num_rows;
    cond_printf(!quiet, "\n\nCovariance matrix:\n");
    for (size_t i = 0; i < cov.results_.size(); ++i) {
	cond_printf(!quiet, "%5d ", *((int *) (cov.results_[i].val)));
	++cnt;
	if (cnt == num_rows) {
	    cond_printf(!quiet, "\n");
	    num_rows--;
	    cnt = 0;
	}
	free(cov.results_[i].val);
	free(cov.results_[i].key_);
    }
    for (int i = 0; i < rows; i++) {
	free(m.results_[i].val);
	free(pca_data_.matrix[i]);
    }
    free(pca_data_.matrix);
    m.free_results();
    cov.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
