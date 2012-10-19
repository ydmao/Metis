#ifndef XARRAY_HH_
#define XARRAY_HH_

#include <algorithm>
#include "bsearch.hh"

template <typename T>
struct xarray_iterator;

template <typename T>
struct xarray {
    xarray() {
        init();
    }
    ~xarray() {
        clear();
    }
    void assign(const xarray<T> &a) {
        a_ = a.a_;
        n_ = a.n_;
        capacity_ = a.capacity_;
    }
    void clear() {
        if (a_ && !multiplex())
            resize(0);
        a_ = NULL;
        capacity_ = n_ = 0;
    }
    static size_t elem_size() {
        return sizeof(T);
    }
    size_t size() const {
        return n_;
    }
    /* @brief: resize without resizing the underlying array */
    void trim(size_t n) {
        assert(n <= n_);
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
    T &at(int index) {
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
        resize(0);
    }
    void init() {
        n_ = 0;
        a_ = NULL;
        capacity_ = 0;
    }
    size_t copy(T *dst) const {
        memcpy(dst, a_, n_ * sizeof(T));
        return n_;
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
    T *pull_array(size_t &n) {
        T *olda = a_;
        n = n_;
        a_ = NULL;
        n_ = capacity_ = 0;
        return olda;
    }
    void append(xarray<T> &src) {
        append(src.array(), src.size());
    }
    void append(T *x, size_t n) {
        set_capacity(n_ + n);
        memcpy(&a_[n_], x, n * sizeof(T));
        n_ += n;
    }
    template <typename F>
    void sort(const F &cmp) {
        qsort(a_, size(), sizeof(T), cmp);
    }
    void set_capacity(size_t c) {
        a_ = reinterpret_cast<T *>(realloc(a_, c * sizeof(T)));
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

typedef xarray<void *> xarray_base;

#endif
