#ifndef RBKTSMGR_H
#define RBKTSMGR_H

#include "mr-conf.hh"
#include "mr-types.hh"
#include "pchandler.hh"
#include "apphelper.hh"

inline void *extract_key(const void *p) {
    if (app_output_pair_type() == vt_keyval) {
        const keyval_t *x = reinterpret_cast<const keyval_t *>(p);
        return x->key;
    } else {
        const keyvals_len_t *x = reinterpret_cast<const keyvals_len_t *>(p);
        return x->key;
    }
}

struct reduce_bucket_manager {
    static reduce_bucket_manager *instance() {
        static reduce_bucket_manager instance;
        return &instance;
    }
    void set_key_cmp(key_cmp_t keycmp);
    void init(int n);
    void destroy();
    xarray_base *get(int p);
    void set_rb(int p, keyval_t *elems, int n, int bsorted) {
        assert(the_app.atype == atype_maponly);
        xarray<keyval_t> *b = as_kvarray(p);
        b->set_array(elems, n);
        if (!use_psrs && (!bsorted || the_app.any.outcmp))
	    b->sort(pair_cmp);
    }
    static int pair_cmp(const void *p1, const void *p2) {
        if (the_app.any.outcmp)
	    return the_app.any.outcmp(p1, p2);
        else
	    return keycmp_(extract_key(p1), extract_key(p2));
    }
    static int keyvals_pair_cmp(const void *p1, const void *p2) {
        const keyvals_t *x1 = (const keyvals_t *)p1;
        const keyvals_t *x2 = (const keyvals_t *)p2;
        return keycmp_(x1->key, x2->key);
    }
    static int keyval_pair_cmp(const void *p1, const void *p2) {
        const keyval_t *x1 = (const keyval_t *)p1;
        const keyval_t *x2 = (const keyval_t *)p2;
        return keycmp_(x1->key, x2->key);
    }
    static int keyvals_len_pair_cmp(const void *p1, const void *p2) {
        const keyvals_len_t *x1 = (const keyvals_len_t *)p1;
        const keyvals_len_t *x2 = (const keyvals_len_t *)p2;
        return keycmp_(x1->key, x2->key);
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
    static key_cmp_t keycmp_;
    static JTLS int cur_task_;
};

#endif
