#ifndef XARRAY_HH_
#define XARRAY_HH_

#include <algorithm>

template <typename T>
struct xarray_iterator;

template <typename T>
struct xarray {
    xarray() : capacity_(0), n_(0), a_(NULL) {
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
        make_room();
        a_[n_++] = e;
    }
    void push_back(const T *e) {
        make_room();
        a_[n_++] = *e;
    }
    typedef xarray_iterator<T> iterator;

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
        return bsearch_eq(key, a_, n_, sizeof(T), cmp, bfound);
    }
    void insert(int pos, const T *e) {
        make_room();
        if (size_t(pos) < n_)
            memmove(&a_[pos + 1], &a_[pos], sizeof(T) * (n_ - pos));
        a_[pos] = *e;
        ++n_;
    }
    template <typename F>
    bool insert_new(const T *e, const F &cmp, int *ppos = NULL) {
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
        resize(0);
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
    void shallow_free() {
        resize(0);
    }
    void init() {
        resize(0);
    }
    size_t copy(T *dst) {
        memcpy(dst, a_, n_ * sizeof(T));
        return n_;
    }
  private:
    void make_room() {
        if (n_ == capacity_)
            set_capacity(std::max(size_t(4), capacity_) * 2);
    }
    void set_capacity(size_t c) {
        a_ = reinterpret_cast<T *>(realloc(a_, c * sizeof(T)));
        capacity_ = c;
    }

    size_t capacity_;
    size_t n_;
    T *a_;
    friend struct xarray_iterator<T>;
};

template <typename T>
struct xarray_iterator {
    xarray_iterator(xarray<T> *p, int i) : p_(p), i_(i) {}
    xarray_iterator(xarray<T> *p) : p_(p), i_(0) {}
    xarray_iterator() : p_(NULL), i_(0) {}
    xarray_iterator(const xarray_iterator &a) : p_(a.p_), i_(a.i_) {}
    bool operator==(const xarray_iterator &a) {
        return p_ == a.p_ && i_ == a.i_;
    }
    bool operator!=(const xarray_iterator &a) {
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
    T *operator&() {
        return &p_->a_[i_];
    }
  private:
    xarray<T> *p_;
    int i_;
};


#endif
