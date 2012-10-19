#ifndef MAP_BUCKET_MANAGER_HH
#define MAP_BUCKET_MANAGER_HH 1

#include "comparator.hh"
#include "array.hh"
#include "reduce.hh"

struct map_bucket_manager_base {
    virtual void init(int rows, int cols) = 0;
    virtual void destroy(void) = 0;
    virtual void rehash(int row, map_bucket_manager_base *backup) = 0;
    virtual void emit(int row, void *key, void *val, size_t keylen,
	      unsigned hash) = 0;
    virtual void prepare_merge(int row) = 0;
    virtual xarray_base *get_output(int *n, bool *kvs) = 0;
    virtual void do_reduce_task(int col) = 0;
    virtual int ncol() const = 0;
    virtual int nrow() const = 0;
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
    static bool go(DT *dst, void *key, void *val, size_t keylen, unsigned hash) {
        return dst->map_insert_sorted(key, val, keylen, hash);
    }
};

template <typename DT>
struct map_insert_analyzer<DT, false> {
    static bool go(DT *dst, void *key, void *val, size_t keylen, unsigned hash) {
        return dst->map_append(key, val, keylen, hash);
    }
};

/* @brief: A map bucket manager using DT as the internal data structure,
   and outputs pairs of OPT type. */
template <bool S, typename DT, typename OPT>
struct map_bucket_manager : public map_bucket_manager_base {
    void init(int rows, int cols);
    void destroy(void);
    void rehash(int row, map_bucket_manager_base *backup);
    void emit(int row, void *key, void *val, size_t keylen,
	      unsigned hash);
    void prepare_merge(int row);
    xarray_base *get_output(int *n, bool *kvs);
    void do_reduce_task(int col);
    int nrow() const {
        return rows_;
    }
    int ncol() const {
        return cols_;
    }

    typedef xarray<OPT> output_bucket_type;
  private:
    DT *mapdt_bucket(int row, int col) {
        return &mapdt_[row * cols_ + col];
    } 
    const DT *mapdt_bucket(int row, int col) const {
        return &mapdt_[row * cols_ + col];
    } 

    int rows_;
    int cols_;
    xarray<DT> mapdt_;  // intermediate ds holding key/value pairs at map phase
    xarray<output_bucket_type> output_;
};

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
void map_bucket_manager<S, DT, OPT>::destroy() {
    for (size_t i = 0; i < mapdt_.size(); ++i) {
        mapdt_[i].shallow_free();
        output_[i].shallow_free();
    }
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::rehash(int row, map_bucket_manager_base *a) {
    typedef map_bucket_manager<S, DT, OPT> manager_type;
    manager_type *am = static_cast<manager_type *>(a);

    const pair_cmp_t f = comparator::raw_comp<typename DT::element_type>::impl;
    for (int i = 0; i < am->cols_; ++i) {
        DT *src = am->mapdt_bucket(row, i);
        for (auto it = src->begin(); it != src->end(); ++it) {
            DT *dst = mapdt_bucket(row, it->hash % cols_);
            dst->insert_new(&it, f);
            it->init();
        }
    }
}

template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::emit(int row, void *key, void *val,
                                       size_t keylen, unsigned hash) {
    DT *dst = mapdt_bucket(row, hash % cols_);
    bool newkey = map_insert_analyzer<DT, S>::go(dst, key, val, keylen, hash);
    est_newpair(row, newkey);
}

/** @brief: Copy the intermediate DS into an xarray<OPT> */
template <bool S, typename DT, typename OPT>
void map_bucket_manager<S, DT, OPT>::prepare_merge(int row) {
    assert(cols_ == 1);
    for (int i = 0; i < cols_; ++i) {
        DT *src = mapdt_bucket(row, i);
        output_bucket_type *dst = &output_[i];
        assert(src->size() == 0);
        transfer(dst, src);
    }
}

template <bool S, typename DT, typename OPT>
xarray_base *map_bucket_manager<S, DT, OPT>::get_output(int *n, bool *kvs) {
    *kvs = OPT::key_values;
    *n = output_.size();
    return (xarray_base *)output_.array();
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

#endif
