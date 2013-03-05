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
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY  BE LIABLE FOR ANY
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

#define DEF_NUM_POINTS 100000
#define DEF_NUM_MEANS 100
#define DEF_DIM 3
#define DEF_GRID_SIZE 1000

#define false 0
#define true 1

static int num_points;		// number of vectors
static int dim;			// Dimension of each vector
static int num_means;		// number of clusters
static int grid_size;		// size of each dimension of vector space
static int modified;

static int *inbuf_start = NULL;
static int *inbuf_end = NULL;

enum { with_value_modifier = 0 };
enum { with_combiner = 1 };

static volatile int scanned = 0;
static volatile long *stats = 0;
static pthread_mutex_t lock;

struct kmeans_data_t {
    int **points;
    keyval_t *means;		// each mean is an index and a coordinate.
    int *clusters;
    int next_point;
    int unit_size;
    int nsplits;
};

struct kmeans_map_data_t {
    int nsplits;
    int length;
    int **points;
    keyval_t *means;
    int *clusters;
};

struct kmeans : public map_reduce {
    void map_function(split_t *ma);
    void reduce_function(void *k, void **v, size_t length);
    int combine_function(void *k, void **v, size_t length);
    void *inplace_modify(void *oldv, void *newv);
    unsigned partition(void *k, int) {
        return ptr2int<unsigned>(k);
    } 
    bool split(split_t *out, int ncores);
    bool has_value_modifier() {
        return with_value_modifier;
    }
    int key_compare(const void *s1, const void *s2) {
        prof_enterkcmp();
        int r = *((int *)s1) - *((int *)s2);
        prof_leavekcmp();
        return r;
    }
    kmeans_data_t kd_;
  private:
    void find_clusters(int **points, keyval_t * means, int *clusters, int size);
};

/** dump_means()
 *  Helper function to Print out the mean values
 */
static void dump_means(keyval_t * means, int size) {
    for (int i = 0; i < size; ++i) {
	for (int j = 0; j < dim; ++j)
	    printf("%5d ", ((int *) means[i].val)[j]);
	printf("\n");
    }
}

#if 0
/** Helper function to print out the points */
static void dump_points(int **vals, int rows) {
    for (int i = 0; i < rows; i++) {
	for (int j = 0; j < dim; j++)
	    printf("%5d ", vals[i][j]);
	printf("\n");
    }
}
#endif

static void usage(char *fn) {
    printf("Usage: %s <vector dimension> <num clusters> <num points> <max value> [options]\n", fn);
    printf("options:\n");
    printf("  -p nprocs : # of processors to use\n");
    printf("  -m map mask : # of map mask (pre-split input before MR)\n");
    printf("  -r #reduce tasks: # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
}

/** parse_args()
 *  Parse the user arguments
 */
static void parse_args(int argc, char **argv) {
    if (argc < 5) {
	usage(argv[0]);
	exit(1);
    }
    dim = atoi(argv[1]);
    num_means = atoi(argv[2]);
    num_points = atoi(argv[3]);
    grid_size = atoi(argv[4]);

    if (dim <= 0 || num_means <= 0 || num_points <= 0 || grid_size <= 0) {
	printf
	    ("Illegal argument value. All values must be numeric and greater than 0\n");
	exit(1);
    }
}

/* Generate the points */
void generate_points(int **pts, int size) {
    for (int i = 0; i < size; i++)
	for (int j = 0; j < dim; j++)
	    pts[i][j] = (i * j) % grid_size + 1;
}

/** Get the squared distance between 2 points */
unsigned int get_sq_dist(int *v1, int *v2) {
    unsigned int sum = 0;
    for (int i = 0; i < dim; i++)
	sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
    return sum;
}

/** Helper function to update the total distance sum */
void add_to_sum(int *sum, int *point) {
    for (int i = 0; i < dim; i++)
	sum[i] += point[i];
}

/* Find the cluster that is most suitable for a given set of points */
void kmeans::find_clusters(int **points, keyval_t *means, int *clusters, int size) {
    unsigned int min_dist, cur_dist;
    int min_idx;
    for (int i = 0; i < size; i++) {
	min_dist = get_sq_dist(points[i], (int *)means[0].val);
	min_idx = 0;
	for (int j = 1; j < num_means; j++) {
	    cur_dist = get_sq_dist(points[i], (int *)means[j].val);
	    if (cur_dist < min_dist) {
		min_dist = cur_dist;
		min_idx = j;
	    }
	}

	if (clusters[i] != min_idx) {
	    clusters[i] = min_idx;
	    modified = true;
	}
	//printf("Emitting [%d,%d]\n", *((int *)means[min_idx].key), *(points[i]));
	prof_leaveapp();
	map_emit(means[min_idx].key_, (void *)points[i], sizeof(means[min_idx].key_));
	prof_enterapp();
    }
}

/* Assigns one or more points to each map task */
bool kmeans::split(split_t *out, int ncores) {
    if (kd_.nsplits == 0)
	kd_.nsplits = 16 * ncores;
    int req_units = num_points / kd_.nsplits;
    assert(out && kd_.points && kd_.means && kd_.clusters);
    if (kd_.next_point >= num_points)
	return false;
    prof_enterapp();
    kmeans_map_data_t *out_data = safe_malloc<kmeans_map_data_t>();
    out->length = 1;
    out->data = (void *) out_data;

    out_data->points = (int **)(&kd_.points[kd_.next_point]);
    out_data->means = kd_.means;
    out_data->clusters = (int *) (&kd_.clusters[kd_.next_point]);
    out_data->length = std::min(num_points - kd_.next_point, req_units);
    kd_.next_point += out_data->length;
    prof_leaveapp();
    return true;
}

/** Finds the cluster that is most suitable for a given set of points */
void kmeans::map_function(split_t * split) {
    assert(split->length == 1);
    prof_enterapp();
    kmeans_map_data_t *map_data = (kmeans_map_data_t *)split->data;
    find_clusters(map_data->points, map_data->means, map_data->clusters,
		  map_data->length);
    free(map_data);
    prof_leaveapp();
}

void *kmeans::inplace_modify(void *oldv, void *newv) {
    add_to_sum((int *)oldv, (int *)newv);
    return oldv;
}

/** Updates the sum calculation for the various points */
int kmeans::combine_function(void *key_in, void **vals_in, size_t vals_len) {
    prof_enterapp();
    if (!with_combiner) {
        prof_leaveapp();
        return vals_len;
    }
    int *sum = (int *) calloc(dim, sizeof(int));
    for (size_t i = 0; i < vals_len; i++) {
	add_to_sum(sum, (int *)vals_in[i]);
	if (vals_in[i] < inbuf_start || vals_in[i] > inbuf_end)
	    free(vals_in[i]);
    }
    vals_in[0] = sum;
    prof_leaveapp();
    return 1;
}

/** Updates the sum calculation for the various points */
void kmeans::reduce_function(void *key_in, void **vals_in, size_t vals_len) {
    assert(key_in && vals_in);
    prof_enterapp();
    int *sum = (int *) calloc(dim, sizeof(int));

    for (size_t i = 0; i < vals_len; i++)
	add_to_sum(sum, (int *)vals_in[i]);

    if (!scanned) {
	pthread_mutex_lock(&lock);
	if (!scanned) {
	    long *tmp = safe_malloc<long>(num_means);
	    memset(tmp, 0, sizeof(long) * num_means);
	    for (int i = 0; i < num_points; ++i)
		++tmp[kd_.clusters[i]];
	    stats = tmp;
	    scanned = 1;
	}
	pthread_mutex_unlock(&lock);
    }
    long len = stats[*((int *) key_in)];
    int *mean = safe_malloc<int>(dim);
    for (int i = 0; i < dim; i++)
	mean[i] = sum[i] / len;

    free(sum);
    prof_leaveapp();
    reduce_emit(key_in, (void *)mean);
}

static void init_kmeans(kmeans_data_t &kd, int nsplit) {
    // get points.
    kd.points = safe_malloc<int *>(num_points);
    // We generate the points continously so that it is easy to determine
    // whether a value can be freed in kmeans_combine
    inbuf_start = safe_malloc<int>(num_points * dim);
    for (int i = 0; i < num_points; i++)
	kd.points[i] = &inbuf_start[i * dim];
    inbuf_end = (int *)(intptr_t(inbuf_start) + sizeof(int) * num_points * dim - 1);
    generate_points(kd.points, num_points);
    // get means
    kd.means = safe_malloc<keyval_t>(num_means);
    for (int i = 0; i < num_means; ++i) {
	kd.means[i].val = safe_malloc<int>(dim);
	kd.means[i].key_ = safe_malloc<int>();
	memcpy(kd.means[i].val, kd.points[i], sizeof(int) * dim);
	((int *) kd.means[i].key_)[0] = i;
    }

    kd.next_point = 0;
    kd.unit_size = sizeof(int) * dim;
    kd.nsplits = nsplit;

    kd.clusters = safe_malloc<int>(num_points);
    memset(kd.clusters, -1, sizeof(int) * num_points);
}

int main(int argc, char **argv) {
    int nprocs = 0, ndisp = 0, map_tasks = 0, reduce_tasks = 0;
    int quiet = 0;
    int c;

    parse_args(argc, argv);
    while ((c = getopt(argc - 4, argv + 4, "p:m:l:r:q")) != -1) {
	switch (c) {
	case 'p':
	    assert((nprocs = atoi(optarg)) >= 0);
	    break;
	case 'm':
	    map_tasks = atoi(optarg);
	    break;
	case 'r':
	    reduce_tasks = atoi(optarg);
	    break;
	case 'l':
	    assert((ndisp = atoi(optarg)) >= 0);
	    break;
	case 'q':
	    quiet = 1;
	    break;
	default:
	    usage(argv[0]);
	    exit(EXIT_FAILURE);
	}
    }
    mapreduce_appbase::initialize();
    kmeans app;
    init_kmeans(app.kd_, map_tasks);
    app.set_reduce_task(reduce_tasks);
    app.set_ncore(nprocs);
    modified = true;
    pthread_mutex_init(&lock, NULL);
    while (modified) {
	modified = false;
	app.kd_.next_point = 0;
	dprintf(".");
	scanned = 0;
	if (stats) {
	    free((long *) stats);
	    stats = NULL;
	}
        app.sched_run();
	for (size_t i = 0; i < app.results_.size(); ++i) {
	    int mean_idx = *((int *)app.results_[i].key_);
	    free(app.kd_.means[mean_idx].val);
	    app.kd_.means[mean_idx] = app.results_[i];
	}
        app.free_results();
    }
    app.print_stats();
    if (!quiet)
	dump_means(app.kd_.means, num_means);
    free(inbuf_start);
    free(app.kd_.points);
    for (int i = 0; i < num_means; i++) {
	free(app.kd_.means[i].key_);
	free(app.kd_.means[i].val);
    }
    free(app.kd_.clusters);
    free(app.kd_.means);
    pthread_mutex_destroy(&lock);
    mapreduce_appbase::deinitialize();
    return 0;
}
