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
#ifndef MAP_BUCKET_MANAGER_HH_
#define MAP_BUCKET_MANAGER_HH_ 1

#include "array.hh"
#include "group.hh"
#include "test_util.hh"
#include "appbase.hh"

struct map_bucket_manager_base {
    virtual ~map_bucket_manager_base() {}
    virtual void global_init(size_t rows, size_t cols) = 0;
    virtual void per_worker_init(size_t row) = 0;
    virtual void reset(void) = 0;
    virtual void rehash(size_t row, map_bucket_manager_base *backup) = 0;
    virtual bool emit(size_t row, void *key, void *val, size_t keylen,
	              unsigned hash) = 0;
    virtual void prepare_merge(size_t row) = 0;
    virtual void do_reduce_task(size_t col) = 0;
    virtual size_t ncol() const = 0;
    virtual size_t nrow() const = 0;
    virtual void psrs_output_and_reduce(size_t ncpus, size_t lcpu) = 0;
};

template <typename DT, bool S>
struct group_analyzer {};

template <typename DT>
struct group_analyzer<DT, true> {
    static void go(DT **a, size_t na) {
        group_sorted(a, na, static_appbase::internal_reduce_emit,
                     static_appbase::key_free);
    }
};

template <typename DT>
struct group_analyzer<DT, false> {
    static void go(DT **a, size_t na) {
        group_unsorted(a, na, static_appbase::internal_reduce_emit,
                       static_appbase::pair_comp<typename DT::element_type>,
                       static_appbase::key_free);
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
    void global_init(size_t rows, size_t cols);
    void per_worker_init(size_t row);
    void reset(void);
    void rehash(size_t row, map_bucket_manager_base *backup);
    bool emit(size_t row, void *key, void *val, size_t keylen,
	      unsigned hash);
    void prepare_merge(size_t row);
    void do_reduce_task(size_t col);
    size_t nrow() const {
        return rows_;
    }
    size_t ncol() const {
        return cols_;
    }
    void psrs_output_and_reduce(size_t ncpus, size_t lcpu);
    typedef xarray<OPT> C;  // output bucket type
    C* get_output(size_t row) {
        assert(cols_ == 1);
        return &output_[row];
    }
  private:
    DT *mapdt_bucket(size_t row, size_t col) {
        return mapdt_[row]->at(col);
    }
    ~map_bucket_manager() {
        reset();
    }
    psrs<C> pi_;
    size_t rows_;
    size_t cols_;
    xarray<xarray<DT> *> mapdt_;  // intermediate ds holding key/value pairs at map phase
    xarray<C> output_;
};

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::psrs_output_and_reduce(size_t ncpus, size_t lcpu) {
    // make sure we are using psrs so that after merge_reduced_buckets,
    // the final results is already in reduce bucket 0
    const bool use_psrs = USE_PSRS;
    assert(use_psrs);
    C *out = NULL;
    if (lcpu == main_core)
        out = pi_.init(lcpu, sum_subarray(output_));
    // reduce the output of psrs
    C *myshare = pi_.do_psrs(output_, ncpus, lcpu, static_appbase::pair_comp<OPT>);
    if (myshare)
        group_one_sorted(*myshare, static_appbase::internal_reduce_emit,
                         static_appbase::key_free);
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
void map_bucket_manager<S, DT, OPT>::global_init(size_t rows, size_t cols) {
    mapdt_.resize(rows);
    output_.resize(rows * cols);
    for (size_t i = 0; i < output_.size(); ++i)
        output_[i].init();
    rows_ = rows;
    cols_ = cols;
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::per_worker_init(size_t row) {
    mapdt_[row] = safe_malloc<xarray<DT> >();
    mapdt_[row]->init();
    mapdt_[row]->resize(cols_);
    for (size_t i = 0; i < cols_; ++i)
        mapdt_[row]->at(i)->init();
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::reset() {
    for (size_t i = 0; i < output_.size(); ++i)
        output_[i].shallow_free();
    output_.shallow_free();
    for (size_t i = 0; i < rows_; ++i) {
        for (size_t j = 0; j < cols_; ++j)
            mapdt_bucket(i, j)->shallow_free();
        mapdt_[i]->shallow_free();
        free(mapdt_[i]);
    }
    mapdt_.shallow_free();
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::rehash(size_t row, map_bucket_manager_base *a) {
    typedef map_bucket_manager<S, DT, OPT> manager_type;
    manager_type *am = static_cast<manager_type *>(a);

    for (size_t i = 0; i < am->cols_; ++i) {
        DT *src = am->mapdt_bucket(row, i);
        for (auto it = src->begin(); it != src->end(); ++it) {
            DT *dst = mapdt_bucket(row, it->hash % cols_);
            map_insert_analyzer<DT, S>::insert_new_and_raw(dst, &(*it));
            it->init();
        }
    }
}

template <bool S, typename DT, typename OPT>
bool map_bucket_manager<S, DT, OPT>::emit(size_t row, void *k, void *v,
                                          size_t keylen, unsigned hash) {
    DT *dst = mapdt_bucket(row, hash % cols_);
    return map_insert_analyzer<DT, S>::copy_on_new(dst, k, v, keylen, hash);
}

/** @brief: Copy the intermediate DS into an xarray<OPT> */
template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::prepare_merge(size_t row) {
    assert(cols_ == 1);
    DT *src = mapdt_bucket(row, 0);
    C *dst = &output_[row];
    CHECK_EQ(size_t(0), dst->size());
    src->transfer(dst);
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::do_reduce_task(size_t col) {
    DT *a[JOS_NCPU];
    for (size_t i = 0; i < rows_; ++i)
        a[i] = mapdt_bucket(i, col);
    group_analyzer<DT, S>::go(a, rows_);
    for (size_t i = 0; i < rows_; ++i)
        a[i]->shallow_free();
}

#endif
