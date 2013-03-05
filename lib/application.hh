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
#ifndef APPLICATION_HH_
#define APPLICATION_HH_ 1

#include "mr-types.hh"
#include "profile.hh"
#include "bench.hh"
#include "predictor.hh"
#include "reduce_bucket_manager.hh"
#include "appbase.hh"

struct map_bucket_manager_base;

template <typename T, int at>
struct app_impl_base : public mapreduce_appbase {
    xarray<T> results_;

    int application_type() {
        return at;
    }
    virtual ~app_impl_base() {
        reset();
    }
    /* @brief: set the optional output compare function */
    virtual int final_output_compare(const T *p1, const T *p2) {
        return this->key_compare(p1->key_, p2->key_);
    }
    void free_results() {
        for (size_t i = 0; i < results_.size(); ++i) {
            this->key_free(results_[i].key_);
            results_[i].reset();
        }
        results_.shallow_free();
    }

  protected:
    void set_final_result() {
        rb_.transfer(0, &results_);
    }
    int internal_final_output_compare(const void *p1, const void *p2) {
        return final_output_compare((T *)p1, (T *)p2);
    }
    reduce_bucket_manager<T> rb_;

    reduce_bucket_manager_base *get_reduce_bucket_manager() {
        return &rb_;
    }
    bool skip_reduce_or_group_phase() {
        if (at == atype_maponly)
            return true;
#ifdef MAP_MERGE_REDUCE
#if USE_PSRS
        return true;
#endif
	assert(0 && "TODO: support merge sort in MAP_MERGE_REDUCE mode\n");
#else
        return false;
#endif
    }

    void verify_before_run() {
        assert(!results_.size());
    }
    void reset() {
        rb_.reset();
        mapreduce_appbase::reset();
    }
};

struct map_reduce : public app_impl_base<keyval_t, atype_mapreduce> {
    virtual ~map_reduce() {}
    /* @brief: if not zero, disable the sampling. */
    void set_reduce_task(int nreduce_task) {
        nreduce_or_group_task_ = nreduce_task;
    }
    /* @brief: user defined reduce function.
        Should not be provided when using vm */
    virtual void reduce_function(void *k, void **v, size_t length) {
        assert(0);
    }
    /* @brief: combine @v
       @v: input and output parameter
       @return: the new length of v
        should not be provided when using vm */
    virtual int combine_function(void *k, void **v, size_t length) {
        return length;
    }

    /* @brief: called for each key/value pair to update the value.
       @return: the updated value */
    virtual void *modify_function(void *oldv, void *newv) {
        assert(0 && "Please overload modify_function");
    }
    virtual bool has_value_modifier() const {
        return false;
    }
  protected:
    friend class static_appbase;
    void internal_reduce_emit(keyvals_t &p);
    void map_values_insert(keyvals_t *kvs, void *val);
    void map_values_move(keyvals_t *dst, keyvals_t *src);
};

struct map_group : public app_impl_base<keyvals_len_t, atype_mapgroup> {
    virtual ~map_group() {}
    /* @brief: if not zero, disables the sampling */
    void set_group_task(int group_task) {
        nreduce_or_group_task_ = group_task;
    }
  protected:
    friend class static_appbase;
    void internal_reduce_emit(keyvals_t &p);
};

struct map_only : public app_impl_base<keyval_t, atype_maponly> {
    virtual ~map_only() {}
};

#endif
