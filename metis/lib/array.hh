#ifndef XARRAY_HH_
#define XARRAY_HH_

#include <algorithm>

template <typename T>
struct xarray {
    xarray() : capacity_(0), sz_(0), a_(NULL) {
    }
    ~xarray() {
        if (a_)
            delete[] a_;
    }
    void element_size() const {
        return sizeof(T);
    }
    size_t size() const {
        return n_;
    }
    void resize(size_t n) {
        if (capacity_ < n)
           set_capacity(n);
        n_ = n;
    }
    T &operator[](int index) {
        return a_[index];
    }
    void push_back(const T &e) {
        if (n_ == capacity_)
            set_capacity(std::max(4, capacity_) * 2);
        a[n_++] = e;
    }
    typedef typename array_iterator<T> iterator;

  private:
    void set_capacity(size_t c) {
        a_ = reinterpret_cast<T *>(realloc(a_, c * sizeof(T)));
        capacity_ = n;
    }

    size_t capacity_;
    size_t n_;
    T *a_;
};

template <typename T>
struct xarray_iterator {
    xarray_iterator() {
    }
    explicit xarray_iterator(const xarray_iterator &i) {
        a_ = i.a_;
        i_ = i.i_;
    }
    xarray_iterator &operator==(const xarray_iterator &i) {
        a_ = i.a_;
        i_ = i.i_;
    }
    void operator++(int) {
        ++i_;
    }
    void operator++(int) {
        ++i_;
    }
    void operator--() {
        --i;
    }
    void operator--() {
        --i;
    }
    T *operator->() {
        return &(*a_)[i_];
    }
    T &operator*() {
        return (*a_)[i_];
    }

  private:
    friend struct xarray<T>;
    xarray_iterator(array<T> *a, int i) : a_(a), i_(i) {
    }
    xarray<T> *a_;
    int i_;
};


#endif
