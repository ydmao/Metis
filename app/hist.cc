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
#include <time.h>
#include <sys/time.h>

#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"

#define IMG_DATA_OFFSET_POS 10
#define BITS_PER_PIXEL_POS 28

enum { pre_fault = 0 };

int swap;			// to indicate if we need to swap byte order of header information
short red_keys[256];
short green_keys[256];
short blue_keys[256];

/* test_endianess */
static void test_endianess() {
    unsigned int num = 0x12345678;
    char *low = (char *) (&(num));
    if (*low == 0x78) {
	dprintf("No need to swap\n");
	swap = 0;
    } else if (*low == 0x12) {
	dprintf("Need to swap\n");
	swap = 1;
    } else {
	printf("Error: Invalid value found in memory\n");
	exit(1);
    }
}

/* swap_bytes */
static void swap_bytes(char *bytes, int nbytes) {
    for (int i = 0; i < nbytes / 2; ++i) {
	dprintf("Swapping %d and %d\n", bytes[i], bytes[nbytes - 1 - i]);
	char tmp = bytes[i];
	bytes[i] = bytes[nbytes - 1 - i];
	bytes[nbytes - 1 - i] = tmp;
    }
}

struct hist : public map_reduce {
    hist(char *d, size_t length, int nsplit) : s_(d, length, nsplit) {
        if (pre_fault)
            printf("ignore this sum %d\n", s_.prefault());
    }

    bool split(split_t *ma, int ncore) {
        return s_.split(ma, ncore, NULL, 3);
    }
    /* Comparison function */
    int key_compare(const void *s1, const void *s2) {
        prof_enterapp();
        int r = (*(short *)s1) - (*(short *)s2);
        prof_leaveapp();
        return r;
    }
    void map_function(split_t *ma);
    void reduce_function(void *key_in, void **vals_in, size_t vals_len);
    int combine_function(void *key_in, void **vals_in, size_t vals_len);
  private:
    defsplitter s_;
};

/* Map function that computes the histogram values for the portion
 * of the image assigned to the map task 
 */
void hist::map_function(split_t * args) {
    assert(args);
    short *key;
    unsigned char *val;
    unsigned long red[256];
    unsigned long green[256];
    unsigned long blue[256];
    unsigned char *data = (unsigned char *) args->data;
    assert(data);
    prof_enterapp();
    memset(&(red[0]), 0, sizeof(unsigned long) * 256);
    memset(&(green[0]), 0, sizeof(unsigned long) * 256);
    memset(&(blue[0]), 0, sizeof(unsigned long) * 256);
    assert(args->length % 3 == 0);
    for (size_t i = 0; i < args->length; i += 3) {
	val = &(data[i]);
	blue[*val]++;
	val = &(data[i + 1]);
	green[*val]++;
	val = &(data[i + 2]);
	red[*val]++;
    }

    for (int i = 0; i < 256; i++) {
	if (blue[i] > 0) {
	    key = &(blue_keys[i]);
	    prof_leaveapp();
	    map_emit((void *)key, int2ptr(blue[i]), sizeof(short));
	    prof_enterapp();
	}

	if (green[i] > 0) {
	    key = &(green_keys[i]);
	    prof_leaveapp();
	    map_emit((void *)key, int2ptr(green[i]), sizeof(short));
	    prof_enterapp();
	}

	if (red[i] > 0) {
	    key = &(red_keys[i]);
	    prof_leaveapp();
	    map_emit((void *) key, int2ptr(red[i]), sizeof(short));
	    prof_enterapp();
	}
    }
    prof_leaveapp();
}

/* Reduce function that adds up the values for each location in the array */
void hist::reduce_function(void *key_in, void **vals_in, size_t vals_len) {
    short *key = (short *) key_in;
    long *vals = (long *) vals_in;
    long sum = 0;
    assert(key && vals);
    prof_enterapp();
    dprintf("For key %hd, there are %ld vals\n", *key, vals_len);

    for (size_t i = 0; i < vals_len; i++)
	sum += vals[i];
    prof_leaveapp();
    reduce_emit(key, (void *) sum);
}

/* Merge the intermediate date, return the length of data after merge */
int hist::combine_function(void *key_in, void **vals_in, size_t vals_len) {
    short *key = (short *) key_in;
    size_t *vals = (size_t *) vals_in;
    unsigned long sum = 0;
    assert(key);
    assert(vals);
    prof_enterapp();
    for (size_t i = 0; i < vals_len; i++)
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
    printf("  -q : quiet output (for batch test)\n");
    printf("  -d : debug output\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, reduce_tasks = 0, quiet = 0;
    if (argc < 2)
	usage(argv[0]);
    int c;
    while ((c = getopt(argc - 1, argv + 1, "p:m:r:q")) != -1) {
	switch (c) {
	case 'p':
	    nprocs = atoi(optarg);
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
    cond_printf(!quiet, "Histogram: Running... file %s\n", argv[1]);
    mmap_file mf(argv[1]);
    if ((mf[0] != 'B') || (mf[1] != 'M')) {
	printf("File is not a valid bitmap file. Exiting\n");
	exit(1);
    }

    test_endianess();		// will set the variable "swap"
    unsigned short *bitsperpixel = (unsigned short *)(&mf[BITS_PER_PIXEL_POS]);
    if (swap)
	swap_bytes((char *) (bitsperpixel), sizeof(*bitsperpixel));
    if (*bitsperpixel != 24) {	// ensure its 3 bytes per pixel
	printf("Error: Invalid bitmap format - ");
	printf("This application only accepts 24-bit pictures. Exiting\n");
	exit(1);
    }
    uint16_t *data_pos = (uint16_t *)(&mf[IMG_DATA_OFFSET_POS]);
    if (swap)
	swap_bytes((char *)data_pos, sizeof(*data_pos));
    size_t imgdata_bytes = mf.size_ - *data_pos;
    imgdata_bytes = round_down(imgdata_bytes, 3);
    cond_printf(!quiet, "File stat: %ld bytes, %ld pixels\n", imgdata_bytes,
	        imgdata_bytes / 3);

    // We use this global variable arrays to store the "key" for each histogram
    // bucket. This is to prevent memory leaks in the mapreduce scheduler
    for (int i = 0; i < 256; ++i) {
	blue_keys[i] = 1000 + i;
	green_keys[i] = 2000 + i;
	red_keys[i] = 3000 + i;
    }
    mapreduce_appbase::initialize();
    hist app(&mf[*data_pos], imgdata_bytes, map_tasks);
    app.set_reduce_task(reduce_tasks);
    app.set_ncore(nprocs);
    app.sched_run();
    app.print_stats();

    short pix_val;
    long freq;
    short prev = 0;
    cond_printf(!quiet, "\n\nBlue\n");
    cond_printf(!quiet, "----------\n\n");
    for (size_t i = 0; i < app.results_.size(); ++i) {
	keyval_t *curr = &app.results_[i];
	pix_val = *((short *) curr->key);
	freq = (long) curr->val;

	if (pix_val - prev > 700) {
	    if (pix_val >= 2000) {
	        cond_printf(!quiet, "\n\nRed\n");
		cond_printf(!quiet, "----------\n\n");
	    } else if (pix_val >= 1000) {
		cond_printf(!quiet, "\n\nGreen\n");
		cond_printf(!quiet, "----------\n\n");
	    }
	}
	cond_printf(!quiet, "%hd - %ld\n", pix_val, freq);
	prev = pix_val;
    }
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
