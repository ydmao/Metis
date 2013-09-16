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
#ifndef REDUCE_BUCKET_MANAGER_HH_
#define REDUCE_BUCKET_MANAGER_HH_ 1

#include "mr-types.hh"
#include "psrs.hh"
#include "appbase.hh"
#include "threadinfo.hh"

struct reduce_bucket_manager_base {
    virtual ~reduce_bucket_manager_base() {}
    virtual void init(int n) = 0;
    virtual void reset() = 0;
    virtual void trim(size_t n) = 0;
    virtual size_t size() = 0;
    virtual void set_current_reduce_task(int i) = 0;
    virtual void merge_reduced_buckets(int ncpus, int lcpu) = 0;
};

template <typename T>
struct reduce_bucket_manager : public reduce_bucket_manager_base {
    void init(int n) {
        rb_.resize(n);
        for (int i = 0; i < n; ++i)
            rb_[i].init();
        set_current_reduce_task(0);
    }
    void reset() {
        rb_.resize(0);
    }
    void trim(size_t n) {
        rb_.trim(n);
    }
    size_t size() {
        return rb_.size();
    }
    typedef xarray<T> C;
    xarray<T> *get(int p) {
        return &rb_[p];
    }
    void emit(const T &p) {
        rb_[current_task()].push_back(p);
    }
    void set_current_reduce_task(int ir) {
        assert(size_t(ir) < rb_.size());
        threadinfo::current()->cur_reduce_task_ = ir;
    }
    /** @brief: merge the output buckets of reduce phase, i.e. the final output.
        For psrs, the result is stored in rb_[0]; for mergesort, the result are
        spread in rb[0..(ncpus - 1)]. */
    void merge_reduced_buckets(int ncpus, int lcpu) {
        C *out = NULL;
        const int use_psrs = USE_PSRS;
        if (!use_psrs) {
            out = mergesort(rb_, ncpus, lcpu,
                            static_appbase::final_output_pair_comp);
            shallow_free_subarray(rb_, lcpu, ncpus);
        } else {
            // only main cpu has output
            if (lcpu == main_core)
                out = pi_.init(lcpu, sum_subarray(rb_));
            assert(out || lcpu != main_core);
            C *myshare = pi_.do_psrs(rb_, ncpus, lcpu,
                                     static_appbase::final_output_pair_comp);
            myshare->init();
            delete myshare;
            // Let one CPU free the input buckets
            if (lcpu == main_core)
                shallow_free_subarray(rb_);
        }
        if (out) {
            rb_[lcpu].swap(*out);
            delete out;
        }
    }
    void set(int p, C *src) {
        assert(get(p)->size() == 0);
        get(p)->swap(*src);
    }
    void transfer(int p, C *dst) {
        assert(dst->size() == 0);
        get(p)->swap(*dst);
    }
  private:
    int current_task() {
        return threadinfo::current()->cur_reduce_task_;
    }
    xarray<C> rb_; // reduce buckets
    psrs<C> pi_;
};

#endif
