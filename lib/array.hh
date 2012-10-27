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
#ifndef ARRAY_HH_
#define ARRAY_HH_ 1

#include <algorithm>
#include "bsearch.hh"

template <typename T>
struct xarray_iterator;

template <typename T>
struct xarray {
    explicit xarray(size_t n) {
        init();
        resize(n);
    }
    xarray() {
        init();
    }
    ~xarray() {
        clear();
    }
    void remove(size_t p) {
        assert(p < n_ && !multiplex());
        // Don't use &a_[p] to get the address of the @p-th element!
        // T may overload the & operator!
        memmove(a_ + p, a_ + (p + 1), sizeof(T) * (n_ - p - 1));
        --n_;
    }
    void zero() {
        bzero(a_, sizeof(T) * size());
    }
    void assign(const xarray<T> &a) {
        a_ = a.a_;
        n_ = a.n_;
        capacity_ = a.capacity_;
    }
    void clear() {
        if (a_ && !multiplex())
            set_capacity(0);
        init();
    }
    static size_t elem_size() {
        return sizeof(T);
    }
    size_t size() const {
        return n_;
    }
    /* @brief: resize without resizing the underlying array */
    void trim(size_t n, bool hard = false) {
        assert(hard || n <= n_);
        n_ = n;
    }
    void resize(size_t n) {
        assert(!multiplex());
        if (capacity_ < n)
           set_capacity(n);
        n_ = n;
    }
    T &operator[](int index) {
        return a_[index];
    }
    T *at(int index) {
        return &a_[index];
    }
    T &back() {
        assert(size() && !multiplex());
        return a_[size() - 1];
    }
    void push_back(const T &e) {
        make_room();
        a_[n_++] = e;
    }
    typedef xarray_iterator<T> iterator;
    typedef T element_type;

    iterator begin() {
        return iterator(this);
    }
    iterator end() {
        return iterator(this, n_);
    }
    template <typename F>
    int lower_bound(const T *key, const F &cmp, bool *bfound) {
        *bfound = false;
        if (!a_)
            return 0;
        return xsearch::lower_bound(key, a_, n_, cmp, bfound);
    }
    void insert(size_t pos, const T *e) {
        make_room();
        if (pos < n_)
            memmove(a_ + (pos + 1), a_ + pos, sizeof(T) * (n_ - pos));
        a_[pos] = *e;
        ++n_;
    }
    template <typename F>
    bool atomic_insert(const T *e, const F &cmp, int *ppos = NULL) {
        bool bfound = false;
        int pos = lower_bound(e, cmp, &bfound);
        if (ppos)
            *ppos = pos;
        if (bfound)
            return false;
        insert(pos, e);
        return true;
    }
    void set_array(T *e, int n) {
        set_capacity(0);
        a_ = e;
        n_ = n;
        capacity_ = n;
    }
    size_t capacity() {
        return capacity_;
    }
    T *array() {
        return a_;
    }
    T multiplex_value() const {
        if (size() == 0)
            return NULL;
        assert(multiplex());
        return reinterpret_cast<T>(a_);
    }
    void set_multiplex_value(const T &v) {
        if (!multiplex())
            assert(size() == 0);
        a_ = reinterpret_cast<T *>(v);
        n_ = 1;
        capacity_ = (size_t(1) << 63);
    }
    bool multiplex() const {
        return capacity_ & (size_t(1) << 63);
    }
    void shallow_free() {
        set_capacity(0);
        init();
    }
    void init() {
        n_ = 0;
        a_ = NULL;
        capacity_ = 0;
    }
    void copy(T *dst, ssize_t off, size_t n) const {
        memcpy(dst, &a_[off], n * sizeof(T));
    }
    size_t transfer(xarray<T> *dst) {
        assert(dst->size() == 0);
        swap(*dst);
        return dst->size();
    }
    void swap(xarray<T> &dst) {
        std::swap(a_, dst.a_);
        std::swap(n_, dst.n_);
        std::swap(capacity_, dst.capacity_);
    }
    void append(xarray<T> &src) {
        append(src.array(), src.size());
    }
    void append(T *x, size_t n) {
        if (!n)
            return;
        set_capacity(n_ + n);
        memcpy(a_ + n_, x, n * sizeof(T));
        n_ += n;
    }
    template <typename F>
    void sort(const F &cmp) {
        qsort(a_, size(), sizeof(T), cmp);
    }
    void set_capacity(size_t c) {
        if (c) {
            if (!capacity_)
                a_ = reinterpret_cast<T *>(malloc(c * sizeof(T)));
            else
                a_ = reinterpret_cast<T *>(realloc(a_, c * sizeof(T)));
        } else if (capacity_) {
            free(a_);
            a_ = NULL;
        }
        capacity_ = c;
    }
  private:
    void make_room() {
        assert(!multiplex());
        if (n_ == capacity_)
            set_capacity(std::max(size_t(4), capacity_) * 2);
    }
    size_t capacity_;
    size_t n_;
    T *a_;
    friend struct xarray_iterator<T>;
};

template <typename T>
struct xarray_iterator {
    xarray_iterator(xarray<T> *p, int i) : p_(p), i_(i) {}
    explicit xarray_iterator(xarray<T> *p) : p_(p), i_(0) {}
    xarray_iterator() : p_(NULL), i_(0) {}
    xarray_iterator(const xarray_iterator &a) : p_(a.p_), i_(a.i_) {}
    bool operator==(const xarray_iterator &a) const {
        assert(p_ == a.p_);
        return p_ == a.p_ && i_ == a.i_;
    }
    bool operator!=(const xarray_iterator &a) const {
        return !(*this == a);
    }
    void operator++(int) {
        ++i_;
    }
    void operator++() {
        ++i_;
    }
    T &operator*() {
        return p_->a_[i_];
    }
    T *operator->() {
        return &p_->a_[i_];
    }
    T *current() {
        return &p_->a_[i_];
    }
    xarray_iterator<T> parent_end() {
        return p_->end();
    }
  private:
    xarray<T> *p_;
    size_t i_;
};

template <typename T>
inline size_t sum_subarray(xarray<xarray<T> > &a) {
    size_t n = 0;
    for (size_t i = 0; i < a.size(); ++i)
        n += a[i].size();
    return n;
}

template <typename T>
inline void shallow_free_subarray(xarray<xarray<T> > &a,
                                  int first = 0, int step = 1) {
    for (size_t i = first; i < a.size(); i += step)
        a[i].shallow_free();
}


#endif
