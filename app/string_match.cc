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

#define DEFAULT_UNIT_SIZE 5
#define SALT_SIZE 2
#define MAX_REC_LEN 1024
#define OFFSET 5

struct str_data_t {
    int keys_file_len;
    int encrypted_file_len;
    long bytes_comp;
    char *keys_file;
    char *encrypt_file;
};

struct str_map_data_t {
    char *keys_file;
    char *encrypt_file;
};

static const char *key1 = "Helloworld";
static const char *key2 = "howareyou";
static const char *key3 = "ferrari";
static const char *key4 = "whotheman";
static char *key1_final;
static char *key2_final;
static char *key3_final;
static char *key4_final;
static uint64_t nsplits = 0;

static str_data_t str_data;

/** Function to get the next word */
int getnextline(char *output, int max_len, char *file) {
    int i = 0;
    while (i < max_len - 1) {
	if (file[i] == '\0')
	    return i + 1;
	if (file[i] == '\r')
	    return i + 2;
	if (file[i] == '\n')
	    return i + 1;
	output[i] = file[i];
	i++;
    }
    return i;
}

struct sm : public map_reduce {
    int key_compare(const void *v1, const void *v2) {
        prof_enterkcmp();
        int r = strcmp((char *) v1, (char *) v2);
        prof_leavekcmp();
        return r;
    }
    void map_function(split_t *ma);
    bool split(split_t *out, int ncores);
    void reduce_function(void *key_in, void **vals_in, size_t vals_len);
    int combine_function(void *key_in, void **vals_in, size_t vals_len);
};

/** Simple Cipher to generate a hash of the word */
static void compute_hashes(const char *word, char *final_word) {
    int len = strlen(word);
    for (int i = 0; i < len; i++)
	final_word[i] = word[i] + OFFSET;
}

bool sm::split(split_t * out, int ncores) {
    prof_enterapp();
    /* Make a copy of the mm_data structure */
    str_data_t *data = &str_data;
    str_map_data_t *map_data = safe_malloc<str_map_data_t>();
    map_data->encrypt_file = data->encrypt_file;
    map_data->keys_file = data->keys_file + data->bytes_comp;
    if (nsplits == 0)
	nsplits = ncores * def_nsplits_per_core;
    uint64_t split_size = data->keys_file_len / nsplits;
    /* Check whether the various terms exist */
    assert(out && split_size >= 0);
    assert(data->bytes_comp <= data->keys_file_len);
    if (data->bytes_comp == data->keys_file_len) {
	free(map_data);
	prof_leaveapp();
	return false;
    }
    /* Assign the required number of bytes */
    int req_bytes = split_size;
    int available_bytes = data->keys_file_len - data->bytes_comp;
    out->length = (req_bytes < available_bytes) ? req_bytes : available_bytes;
    out->data = map_data;

    char *final_ptr = map_data->keys_file + out->length;
    int counter = data->bytes_comp + out->length;

    /* make sure we end at a word */
    while (counter <= data->keys_file_len && *final_ptr != '\n'
	   && *final_ptr != '\r' && *final_ptr != '\0') {
	counter++;
	final_ptr++;
    }
    if (*final_ptr == '\r')
	counter += 2;
    else if (*final_ptr == '\n')
	counter++;

    out->length = counter - data->bytes_comp;
    data->bytes_comp = counter;
    prof_leaveapp();
    return true;
}

/* Map Function that checks the hash of each word to the given hashes */
void sm::map_function(split_t *args) {
    assert(args);
    prof_enterapp();
    str_map_data_t *data_in = (str_map_data_t *)args->data;
    int key_len;
    uint64_t total_len = 0;
    char *key_file = data_in->keys_file;
    char cur_word[MAX_REC_LEN];
    char cur_word_final[MAX_REC_LEN];
    bzero(cur_word, MAX_REC_LEN);
    bzero(cur_word_final, MAX_REC_LEN);
    int cnt1 = 0, cnt2 = 0, cnt3 = 0, cnt4 = 0;	/* avoid compiler complaining */
    while ((total_len < args->length)
	   && ((key_len = getnextline(cur_word, MAX_REC_LEN, key_file)) >= 0)) {
	compute_hashes(cur_word, cur_word_final);
	if (strcmp(key1, cur_word_final)) {
	    cnt1++;
	    dprintf("FOUND: WORD IS %s\n", cur_word);
	}
	if (strcmp(key2, cur_word_final)) {
	    cnt2++;
	    dprintf("FOUND: WORD IS %s\n", cur_word);
	}
	if (strcmp(key3, cur_word_final)) {
	    cnt3++;
	    dprintf("FOUND: WORD IS %s\n", cur_word);
	}
	if (strcmp(key4, cur_word_final)) {
	    cnt4++;
	    dprintf("FOUND: WORD IS %s\n", cur_word);
	}
	key_file = key_file + key_len;
	bzero(cur_word, MAX_REC_LEN);
	bzero(cur_word_final, MAX_REC_LEN);
	total_len += key_len;
    }
    free(data_in);
    prof_leaveapp();
    map_emit((void *)key1, (void *) (size_t) cnt1, strlen(key1));
    map_emit((void *)key2, (void *) (size_t) cnt2, strlen(key2));
    map_emit((void *)key3, (void *) (size_t) cnt3, strlen(key3));
    map_emit((void *)key4, (void *) (size_t) cnt4, strlen(key4));
}

int sm::combine_function(void *key_in, void **vals_in, size_t vals_len) {
    char *key = (char *) key_in;
    long *vals = (long *) vals_in;
    long sum = 0;
    prof_enterapp();
    assert(key && vals);
    for (size_t i = 0; i < vals_len; i++)
	sum += vals[i];
    vals_in[0] = int2ptr(sum);
    prof_leaveapp();
    return 1;
}

void sm::reduce_function(void *key_in, void **vals_in, size_t vals_len) {
    char *key = (char *) key_in;
    long *vals = (long *) vals_in;
    long sum = 0;
    prof_enterapp();
    assert(key && vals);
    for (size_t i = 0; i < vals_len; i++)
	sum += vals[i];
    prof_leaveapp();
    reduce_emit(key, (void *) sum);
}

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -s split size(KB) : # of kilo-bytes for each split\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -d : debug output\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, reduce_tasks = 0, quiet = 0;
    /* Option to provide the encrypted words in a file as opposed to source code */
    //fname_encrypt = "encrypt.txt";
    if (argc < 2) {
	usage(argv[0]);
	exit(EXIT_FAILURE);
    }
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

    srand((unsigned) time(NULL));

    cond_printf(!quiet, "String Match: Running...\n");
    // Read in the file
    mmap_file mf(argv[1]);
    //dprintf("Encrypted Size is %ld\n",finfo_encrypt.st_size);
    cond_printf(!quiet, "Keys Size is %ld\n", mf.size_);

    str_data.keys_file_len = mf.size_;
    str_data.encrypted_file_len = 0;
    str_data.bytes_comp = 0;
    str_data.keys_file = mf.d_;
    str_data.encrypt_file = NULL;
    //str_data.encrypted_file_len = finfo_encrypt.st_size;
    //str_data.encrypt_file  = ((char *)fdata_encrypt);

    cond_printf(!quiet, "String Match: Calling String Match\n");

    key1_final = safe_malloc<char>(strlen(key1));
    key2_final = safe_malloc<char>(strlen(key2));
    key3_final = safe_malloc<char>(strlen(key3));
    key4_final = safe_malloc<char>(strlen(key4));

    compute_hashes(key1, key1_final);
    compute_hashes(key2, key2_final);
    compute_hashes(key3, key3_final);
    compute_hashes(key4, key4_final);

    mapreduce_appbase::initialize();
    sm app;
    app.set_ncore(nprocs);
    app.set_reduce_task(reduce_tasks);
    nsplits = map_tasks;
    app.sched_run();
    app.print_stats();

    if (!quiet) {
	printf("\nstring match: results:\n");
	for (size_t i = 0; i < app.results_.size(); ++i) {
	    keyval_t *curr = &app.results_[i];
	    printf("%15s - %d\n", (char *)curr->key, (unsigned) (size_t) curr->val);
	}
    }
    free(key1_final);
    free(key2_final);
    free(key3_final);
    free(key4_final);
    /*echeck(munmap(fdata_encrypt, finfo_encrypt.st_size + 1) < 0);
       echeck(close(fd_encrypt) < 0); */
    app.free_results();
    mapreduce_appbase::deinitialize();
    return 0;
}
