#ifndef RBKTSMGR_H
#define RBKTSMGR_H

#include "mr-conf.hh"
#include "mr-types.hh"
#include "apphelper.hh"
#include "comparator.hh"

struct reduce_bucket_manager {
    static reduce_bucket_manager *instance() {
        static reduce_bucket_manager instance;
        return &instance;
    }
    void init(int n) {
        rb_.resize(n);
        for (int i = 0; i < n; ++i)
            rb_[i].init();
    }
    void destroy() {
        rb_.resize(0);
    }
    xarray<keyval_t> *as_kvarray(int p) {
        return (xarray<keyval_t> *)&rb_[p];
    }
    xarray<keyvals_len_t> *as_kvslen_array(int p) {
        return (xarray<keyvals_len_t> *)&rb_[p];
    }
    void emit(void *key, void *val) {
        xarray<keyval_t> *x = as_kvarray(cur_task_);
        keyval_t tmp(key, val);
        x->push_back(tmp);
    }
    void emit(void *key, void **vals, uint64_t len) {
        xarray<keyvals_len_t> *x = as_kvslen_array(cur_task_);
        keyvals_len_t tmp(key, vals, len);
        x->push_back(tmp);
    }
    void set_current_reduce_task(size_t ir) {
        assert(ir < rb_.size());
        cur_task_ = ir;
    }
    /** @brief: merge the output buckets of reduce phase, i.e. the final output.
        For psrs, the result is stored in rb_[0]; for mergesort, the result are
        spread in rb[0..(ncpus - 1)]. */
    void merge_reduced_buckets(int ncpus, int lcpu);
  private:
    void shallow_free_buckets() {
        for (size_t i = 0; i < rb_.size(); ++i)
            rb_[i].shallow_free();
    }
    size_t subsize() {
        size_t n = 0;
        for (size_t i = 0; i < rb_.size(); ++i)
            n += rb_[i].size();
        return n;
    }
    template <typename C>
    void swap(int i, C *x) {
        if (!x)
            return;
        reinterpret_cast<C *>(&rb_[i])->swap(*x);
    }
    reduce_bucket_manager() {}
    xarray<xarray_base> rb_; // reduce buckets
    static JTLS int cur_task_;
};

#endif
