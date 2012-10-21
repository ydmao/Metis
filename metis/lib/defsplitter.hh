#ifndef DEFSPLITTER_HH
#define DEFSPLITTER_HH
#include <string.h>
#include <pthread.h>

struct defsplitter {
    defsplitter(char *d, size_t size, size_t nsplit, size_t align = 1)
        : d_(d), size_(size), nsplit_(nsplit), align_(align) {
        pthread_mutex_init(&mu_, 0);
    }

    void split(split_t *ma, int ncore);
  private:
    size_t align_;
    char *d_;
    size_t size_;
    size_t nsplit_;
    size_t pos_;
    pthread_mutex_lock mu_;
};

int defsplitter::split(split_t *ma, int ncores) {
    pthread_mutex_lock(&mu_);
    if (pos_ >= size_) {
	pthread_mutex_unlock(&ds->mu);
	return 0;
    }
    if (nsplit_ == 0)
	nsplit_ = ncores * def_nsplits_per_core;

    size_t split_size = size_ / nsplit_;
    split_size = round_down(split_size, align_);
    ma->data = (void *) &d_[split_pos];
    if (pos_ + split_size > size_)
	ma->length = size_ - pos_;
    else
	ma->length = split_size;
    pos_ += split_size;
    pthread_mutex_unlock(&mu_);
    return 1;
}

#endif
