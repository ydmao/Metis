#ifndef MAP_BUCKET_MANAGER_HH
#define MAP_BUCKET_MANAGER_HH 1

#include "comparator.hh"
#include "array.hh"
#include "group.hh"
#include "test_util.hh"

struct map_bucket_manager_base {
    virtual ~map_bucket_manager_base() {}
    virtual void init(int rows, int cols) = 0;
    virtual void reset(void) = 0;
    virtual void rehash(int row, map_bucket_manager_base *backup) = 0;
    virtual bool emit(int row, void *key, void *val, size_t keylen,
	              unsigned hash) = 0;
    virtual void prepare_merge(int row) = 0;
    virtual void do_reduce_task(int col) = 0;
    virtual int ncol() const = 0;
    virtual int nrow() const = 0;
    virtual void psrs_output_and_reduce(int ncpus, int lcpu) = 0;
    virtual size_t test_subsize() = 0;
};

template <typename DT, bool S>
struct group_analyzer {};

template <typename DT>
struct group_analyzer<DT, true> {
    static void go(DT **a, int na) {
        group_sorted(a, na, reduce_emit_functor::instance());
    }
};

template <typename DT>
struct group_analyzer<DT, false> {
    static void go(DT **a, int na) {
        group_unsorted(a, na, reduce_emit_functor::instance(),
                       comparator::raw_comp<typename DT::element_type>::impl);
    }
};

template <typename DT, bool S>
struct map_insert_analyzer {
};

template <typename DT>
struct map_insert_analyzer<DT, true> {
    static bool copy_on_new(DT *dst, void *key, void *val, size_t keylen, unsigned hash) {
        return dst->map_insert_sorted_copy_on_new(key, val, keylen, hash);
    }
    typedef typename DT::element_type T;
    static void insert_new_and_raw(DT *dst, T *t) {
        dst->map_insert_sorted_new_and_raw(t);
    }
};

template <typename DT>
struct map_insert_analyzer<DT, false> {
    static bool copy_on_new(DT *dst, void *key, void *val, size_t keylen, unsigned hash) {
        return dst->map_append_copy(key, val, keylen, hash);
    }
    typedef typename DT::element_type T;
    static void insert_new_and_raw(DT *dst, T *t) {
        dst->map_append_raw(t);
    }
};

/* @brief: A map bucket manager using DT as the internal data structure,
   and outputs pairs of OPT type. */
template <bool S, typename DT, typename OPT>
struct map_bucket_manager : public map_bucket_manager_base {
    void init(int rows, int cols);
    void reset(void);
    void rehash(int row, map_bucket_manager_base *backup);
    bool emit(int row, void *key, void *val, size_t keylen,
	      unsigned hash);
    void prepare_merge(int row);
    void do_reduce_task(int col);
    int nrow() const {
        return rows_;
    }
    int ncol() const {
        return cols_;
    }
    void psrs_output_and_reduce(int ncpus, int lcpu);
    size_t test_subsize();
    typedef xarray<OPT> C;  // output bucket type
  private:
    DT *mapdt_bucket(int row, int col) {
        return &mapdt_[row * cols_ + col];
    }
    ~map_bucket_manager() {
        for (size_t i = 0; i < output_.size(); ++i)
            output_[i].shallow_free();
        for (size_t i = 0; i < mapdt_.size(); ++i)
            mapdt_[i].shallow_free();

    }
    const DT *mapdt_bucket(int row, int col) const {
        return &mapdt_[row * cols_ + col];
    } 
    psrs<C> pi_;
    int rows_;
    int cols_;
    xarray<DT> mapdt_;  // intermediate ds holding key/value pairs at map phase
    xarray<C> output_;
};

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::psrs_output_and_reduce(int ncpus, int lcpu) {
    // make sure we are using psrs so that after merge_reduced_buckets,
    // the final results is already in reduce bucket 0
    const int use_psrs = USE_PSRS;
    assert(use_psrs);
    C *out = NULL;
    if (lcpu == main_core)
        out = pi_.init(lcpu, sum_subarray(output_));
    // reduce the output of psrs
    C *myshare = pi_.do_psrs(output_, ncpus, lcpu, comparator::raw_comp<OPT>::impl);
    if (myshare)
        group_one_sorted(*myshare, reduce_emit_functor::instance());
    myshare->init();  // myshare doesn't own the output
    delete myshare;
    // barrier before freeing xo to make sure no one is accessing out anymore.
    pi_.cpu_barrier(lcpu, ncpus);
    if (lcpu == main_core) {
        out->shallow_free();
        delete out;
    }
    // free output_ in parallel
    shallow_free_subarray(output_, lcpu, ncpus);
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::init(int rows, int cols) {
    mapdt_.resize(rows * cols);
    output_.resize(rows * cols);
    for (size_t i = 0; i < mapdt_.size(); ++i) {
        mapdt_[i].init();
        output_[i].init();
    }
    rows_ = rows;
    cols_ = cols;
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::reset() {
    for (size_t i = 0; i < mapdt_.size(); ++i) {
        mapdt_[i].shallow_free();
        output_[i].shallow_free();
    }
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::rehash(int row, map_bucket_manager_base *a) {
    typedef map_bucket_manager<S, DT, OPT> manager_type;
    manager_type *am = static_cast<manager_type *>(a);

    for (int i = 0; i < am->cols_; ++i) {
        DT *src = am->mapdt_bucket(row, i);
        for (auto it = src->begin(); it != src->end(); ++it) {
            DT *dst = mapdt_bucket(row, it->hash % cols_);
            map_insert_analyzer<DT, S>::insert_new_and_raw(dst, &(*it));
            it->init();
        }
    }
}

template <bool S, typename DT, typename OPT>
bool map_bucket_manager<S, DT, OPT>::emit(int row, void *key, void *val,
                                       size_t keylen, unsigned hash) {
    DT *dst = mapdt_bucket(row, hash % cols_);
    return map_insert_analyzer<DT, S>::copy_on_new(dst, key, val, keylen, hash);
}

/** @brief: Copy the intermediate DS into an xarray<OPT> */
template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::prepare_merge(int row) {
    assert(cols_ == 1);
    DT *src = mapdt_bucket(row, 0);
    C *dst = &output_[row];
    CHECK_EQ(size_t(0), dst->size());
    src->transfer(dst);
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::do_reduce_task(int col) {
    DT *a[JOS_NCPU];
    for (int i = 0; i < rows_; ++i)
        a[i] = mapdt_bucket(i, col);
    group_analyzer<DT, S>::go(a, rows_);
    for (int i = 0; i < rows_; ++i)
        a[i]->shallow_free();
}

template <bool S, typename DT, typename OPT>
size_t map_bucket_manager<S, DT, OPT>::test_subsize() {
    size_t n = 0;
    for (size_t i = 0; i < mapdt_.size(); ++i)
        n += mapdt_[i].size();
    return n;
}
#endif
