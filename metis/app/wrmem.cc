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

#define DEFAULT_NDISP 10

struct wr : public map_group {
    wr(char *d, uint64_t length, int nsplit) {
        pos_ = 0;
        len_ = length;
        d_ = (char *)d;
        nsplit_ = nsplit;
        pthread_mutex_init(&mu_, 0); 
    }

    void map_function(split_t *);
    int split(split_t *ret, int ncore);
    int key_compare(const void *k1, const void *k2) {
        return strcmp((const char *)k1, (const char *)k2);
    }
    void *keycopy(void *src, size_t s) {
        char *key;
        assert(key = (char *)malloc(s + 1));
        memcpy(key, src, s);
        key[s] = 0;
        return key;
    }
  private:
    uint64_t pos_;
    uint64_t len_;
    int nsplit_;
    char *d_;
    pthread_mutex_t mu_;
};

/* Divide input on a word border i.e. a space. */
int wr::split(split_t *out, int ncores) {
    assert(d_);
    if (nsplit_ == 0)
        nsplit_ = ncores * def_nsplits_per_core;
    pthread_mutex_lock(&mu_);
    /* EOF, return FALSE for no more data */
    if (pos_ >= len_) {
	pthread_mutex_unlock(&mu_);
	return 0;
    }
    out->data = &d_[pos_];
    out->length = len_ / nsplit_;
    if ((unsigned long)(pos_ + out->length) > len_)
	out->length = len_ - pos_;

    /* set the length to end at a space */
    for (pos_ += (long) out->length; pos_ < len_ &&
	 d_[pos_] != ' ' && d_[pos_] != '\t' &&
	 d_[pos_] != '\r' && d_[pos_] != '\n' &&
	 d_[pos_] != 0; ++pos_, ++out->length);

    pthread_mutex_unlock(&mu_);
    return 1;
}

/* keycopy version of the map function. Go through the splits and reverse
 * index each word */
void wr::map_function(split_t *a) {
    assert(a && a->data);
    char *data = (char *)a->data;
    bool inword = false;
    char k[1024];
    int ilen = 0;
    char *index = NULL;
    for (uint32_t i = 0; i < a->length; ++i) {
	char letter = toupper(data[i]);
        if (inword) {
	    if ((letter < 'A' || letter > 'Z') && letter != '\'') {
		k[ilen] = 0;
		map_emit(k, index, ilen);
                inword = false;
	    } else {
		k[ilen++] = letter;
		assert(size_t(ilen) < sizeof(k));
	    }
        } else if (letter >= 'A' && letter <= 'Z') {
            index = &data[i];
	    k[0] = letter;
	    ilen = 1;
            inword = true;
	}
    }
    if (inword) {
	k[ilen] = 0;
	map_emit(k, index, ilen);
    }
}

static void print_top(final_data_kvs_len_t * wc_vals, int ndisp) {
    uint64_t occurs = 0;
    for (uint32_t i = 0; i < wc_vals->length; i++) {
	keyvals_len_t *curr = &wc_vals->data[i];
	occurs += (uint64_t) curr->len;
    }
    printf("\nwordreverseindex: results (TOP %d from %zu keys, %" PRIu64
	   " words):\n", ndisp, wc_vals->length, occurs);
    for (uint32_t i = 0; i < (uint32_t) ndisp && i < wc_vals->length; i++) {
	keyvals_len_t *curr = &wc_vals->data[i];
	printf("%15s - %d\n", (char *) curr->key,
	       (unsigned) (size_t) curr->len);
    }
}

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

int
main(int argc, char *argv[])
{
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
    int n = 0;
    for (uint64_t i = 0; i < inputsize / (wordlength + 1); i++) {
	for (uint32_t j = 0; j < wordlength; j++)
	    fdata[pos++] = rnd(&seed) % 26 + 'A';
	fdata[pos++] = ' ';
        ++n;
    }
    memset(&fdata[pos], 0, inputsize - pos);

    wr app((char *)fdata, inputsize, map_tasks);
    app.set_ncore(nprocs);
    app.set_group_task(reduce_tasks);
    app.sched_run();
    app.print_stats();
    if (!quiet)
	print_top(&app.results_, ndisp);
    app.join();
    return 0;
}
