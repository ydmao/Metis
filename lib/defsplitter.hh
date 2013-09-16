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
#ifndef DEFSPLITTER_HH_
#define DEFSPLITTER_HH_ 1
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <ctype.h>

struct mmap_file {
    mmap_file(const char *f) {
        assert((fd_ = open(f, O_RDONLY)) >= 0);
        struct stat fst;
        assert(fstat(fd_, &fst) == 0);
        size_ = fst.st_size;
        d_ = (char *)mmap(0, size_ + 1, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, 0);
        assert(d_);
    }
    mmap_file() : fd_(-1) {}
    virtual ~mmap_file() {
        if (fd_ >= 0) {
            assert(munmap(d_, size_ + 1) == 0);
            assert(close(fd_) == 0);
        }
    }
    char &operator[](off_t i) {
        return d_[i];
    }
    size_t size_;
    char *d_;
  private:
    int fd_;
};

struct defsplitter {
    defsplitter(char *d, size_t size, size_t nsplit)
        : d_(d), size_(size), nsplit_(nsplit), pos_(0) {
        pthread_mutex_init(&mu_, 0);
    }
    defsplitter(const char *f, size_t nsplit) : nsplit_(nsplit), pos_(0), mf_(f) {
        pthread_mutex_init(&mu_, 0);
        size_ = mf_.size_;
        d_ = mf_.d_;
    }
    int prefault() {
        int sum = 0;
        for (size_t i = 0; i < size_; i += 4096)
            sum += d_[i];
        return sum;
    }
    bool split(split_t *ma, int ncore, const char *stop, size_t align = 0);
    void trim(size_t sz) {
        assert(sz <= size_);
        size_ = sz;
    }
    size_t size() const {
        return size_;
    }

  private:
    char *d_;
    size_t size_;
    size_t nsplit_;
    size_t pos_;
    mmap_file mf_;
    pthread_mutex_t mu_;
};

bool defsplitter::split(split_t *ma, int ncores, const char *stop, size_t align) {
    pthread_mutex_lock(&mu_);
    if (pos_ >= size_) {
	pthread_mutex_unlock(&mu_);
	return false;
    }
    if (nsplit_ == 0)
	nsplit_ = ncores * def_nsplits_per_core;

    ma->data = (void *) &d_[pos_];
    ma->length = std::min(size_ - pos_, std::max(size_ / nsplit_, sizeof(char)));
    if (align) {
        ma->length = round_down(ma->length, align);
        assert(ma->length);
    }
    pos_ += ma->length;
    for (; pos_ < size_ && stop && !strchr(stop, d_[pos_]); ++pos_, ++ma->length);
        
    pthread_mutex_unlock(&mu_);
    return true;
}

struct split_word {
    split_word(split_t *ma) : ma_(ma), pos_(0) {
        assert(ma_ && ma_->data);
    }
    char *fill(char *k, size_t maxlen, size_t &klen, bool upper = true) {
        char *d = (char *)ma_->data;
        klen = 0;
        for (; pos_ < ma_->length && whitespace(d[pos_]); ++pos_)
            ;
        if (pos_ == ma_->length)
            return NULL;
        char *index = &d[pos_];
        for (; pos_ < ma_->length && !whitespace(d[pos_]); ++pos_) {
            k[klen++] = upper ? toupper(d[pos_]) : d[pos_];
	    assert(klen < maxlen);
        }
	k[klen] = 0;
        return index;
    }
  private:
    bool whitespace(char c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\0' || c == '\t';
    }
    split_t *ma_;
    size_t pos_;
};

#endif
