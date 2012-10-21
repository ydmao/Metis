#ifndef DEFSPLITTER_HH
#define DEFSPLITTER_HH
#include <string.h>
#include <pthread.h>
#include <algorithm>

struct defsplitter {
    defsplitter(char *d, size_t size, size_t nsplit)
        : d_(d), size_(size), nsplit_(nsplit), pos_(0), fd_(-1) {
        pthread_mutex_init(&mu_, 0);
    }
    virtual ~defsplitter() {
        if (fd_ >= 0) {
            assert(munmap(d_, size_ + 1) == 0);
            assert(close(fd_) == 0);
        }
    }
    defsplitter(const char *f, size_t nsplit) : nsplit_(nsplit), pos_(0) {
        assert((fd_ = open(f, O_RDONLY)) >= 0);
        struct stat fst;
        assert(fstat(fd_, &fst) == 0);
        size_ = fst.st_size;
        d_ = (char *)mmap(0, size_ + 1, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, 0);
        assert(d_);
    }

    bool split(split_t *ma, int ncore, const char *stop);
  private:
    char *d_;
    size_t size_;
    size_t nsplit_;
    size_t pos_;
    int fd_;
    pthread_mutex_t mu_;
};

bool defsplitter::split(split_t *ma, int ncores, const char *stop) {
    pthread_mutex_lock(&mu_);
    if (pos_ >= size_) {
	pthread_mutex_unlock(&mu_);
	return false;
    }
    if (nsplit_ == 0)
	nsplit_ = ncores * def_nsplits_per_core;

    ma->data = (void *) &d_[pos_];
    ma->length = std::min(size_ - pos_, size_ / nsplit_);
    pos_ += ma->length;
    for (; pos_ < size_ && stop && !strchr(stop, d_[pos_]); ++pos_, ++ma->length);
    pthread_mutex_unlock(&mu_);
    return true;
}

#endif
