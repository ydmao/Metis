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
    void init(int n);
    void destroy();
    xarray_base *get(int p);
    void set_rb(int p, keyval_t *elems, int n, int bsorted) {
        assert(the_app.atype == atype_maponly);
        xarray<keyval_t> *b = as_kvarray(p);
        b->set_array(elems, n);
        if (!use_psrs && (!bsorted || the_app.any.outcmp))
	    b->sort(comparator::final_output_pair_comp);
    }
    xarray<keyval_t> *as_kvarray(int p) {
        xarray_base *b = get(p);
        return static_cast<xarray<keyval_t> *>(b);
    }
    xarray<keyvals_len_t> *as_kvslen_array(int p) {
        xarray_base *b = get(p);
        return static_cast<xarray<keyvals_len_t> *>(b);
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
    void set_current_reduce_task(int ir) {
        cur_task_ = ir;
    }
    void merge(int ncpus, int lcpu);
    void merge_reduce(xarray_base *a, int n, bool kvs, int ncpus, int lcpu);
  private:
    template <typename T>
    void cat_all() {
        xarray<T> *a0 = (xarray<T> *)get(0);
        for (size_t i = 1; i < rb_.size(); ++i) {
            xarray<T> *x = (xarray<T> *)get(i);
            a0->append(*x);
            x->pull_array();
        }
    }
    reduce_bucket_manager() {}
    xarray<xarray_base> rb_; // reduce buckets
    static JTLS int cur_task_;
};

#endif
