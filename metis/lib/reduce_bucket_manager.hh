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
    xarray<void *> *get(int p);
    void set_rb(int p, keyval_t *elems, int n, int bsorted) {
        assert(the_app.atype == atype_maponly);
        xarray<keyval_t> *b = as_kvarray(p);
        b->set_array(elems, n);
        if (!use_psrs && (!bsorted || the_app.any.outcmp))
	    b->sort(comparator::final_output_pair_comp);
    }
    xarray<keyval_t> *as_kvarray(int p) {
        xarray_base *b = get(p);
        return (xarray<keyval_t> *)b;
    }
    xarray<keyvals_len_t> *as_kvslen_array(int p) {
        xarray_base *b = get(p);
        return (xarray<keyvals_len_t> *)b;
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
        for (size_t i = 0; i < rb_.size(); ++i) {
            get(i)->shallow_free();
            assert(get(i)->size() == 0);
        }
    }
    size_t subsize() {
        size_t n = 0;
        for (size_t i = 0; i < rb_.size(); ++i)
            n += get(i)->size();
        return n;
    }
    template <typename C>
    void swap(int i, C *x) {
        if (!x)
            return;
        reinterpret_cast<C *>(get(i))->swap(*x);
    }
    reduce_bucket_manager() {}
    xarray<xarray_base> rb_; // reduce buckets
    static JTLS int cur_task_;
};

#endif
