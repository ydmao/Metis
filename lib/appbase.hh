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
#ifndef APPBASE_HH_
#define APPBASE_HH_ 1

#include "mr-types.hh"
#include "profile.hh"
#include "bench.hh"
#include "predictor.hh"

struct mapreduce_appbase;
struct map_bucket_manager_base;
struct reduce_bucket_manager_base;

struct static_appbase;

struct mapreduce_appbase {
    mapreduce_appbase();
    virtual void map_function(split_t *) = 0;
    virtual bool split(split_t *ret, int ncore) = 0;
    virtual int key_compare(const void *, const void *) = 0;
    virtual ~mapreduce_appbase();
    /* @brief: optional function invokded for each new key. */
    virtual void *key_copy(void *k, size_t len) {
        return k;
    }

    /* @brief: if you have implemented key_copy, you should also implement key_free */
    virtual void key_free(void *k) {}

    /* @brief: default partition function that partition keys into reduce/group buckets */
    virtual unsigned partition(void *k, int length) {
        size_t h = 5381;
        const char *x = (const char *) k;
        for (int i = 0; i < length; ++i)
	    h = ((h << 5) + h) + unsigned(x[i]);
        return h % unsigned(-1);
    } 
    /* @brief: set the number of cores to use. Metis uses all cores by default. */
    void set_ncore(int ncore) {
        ncore_ = ncore;
    }
    static void initialize();
    static void deinitialize();
    int sched_run();
    void print_stats();
    /* @brief: called in user defined map function. If keycopy function is
        used, Metis calls the keycopy function for each new key, and user
        can free the key when this function returns. */
    void map_emit(void *key, void *val, int key_length);
    /* @brief: called by user-defined reduce function. The key is owned by Metis.
       The user should not emit a key other than the argument to the user defined
       reduce function; otherwise, the output is not guaranteed to ordered. */
    void reduce_emit(void *key, void *val);

  protected:
    friend class static_appbase;
    virtual int application_type() = 0;
    virtual void map_values_insert(keyvals_t *kvs, void *v) {
        kvs->push_back(v);
    }
    virtual void map_values_move(keyvals_t *dst, keyvals_t *src) {
        dst->append(*src);
        src->reset();
    }
    virtual int internal_final_output_compare(const void *p1, const void *p2) = 0;
    virtual reduce_bucket_manager_base *get_reduce_bucket_manager() = 0;
    /* @breif: prepare the application for the next iteraton.
       Everything should be cleaned up, except for that the application should
       free the results. */
    virtual void reset();
    virtual void verify_before_run() = 0;
    uint64_t sched_sample();
    virtual bool skip_reduce_or_group_phase() = 0;
    virtual void set_final_result() = 0;
    int map_worker();
    int reduce_worker();
    int merge_worker();
    static void *base_worker(void *arg);
    void run_phase(int phase, int ncore, uint64_t &t, int first_task = 0);
    map_bucket_manager_base *create_map_bucket_manager(int nrow, int ncol);

    int nreduce_or_group_task_;
    enum { min_group_or_reduce_task_per_core = 16,
           max_group_or_reduce_task_per_core = 100 };
    enum { default_sample_hashtable_size = 10000 };
    enum { sample_percent = 5 };
    enum { combiner_threshold = 8 };
    enum { expected_keys_per_bucket = 10 };

  private:
    uint64_t nsample_;
    int merge_ncore_;

    int ncore_;   
    uint64_t total_sample_time_;
    uint64_t total_map_time_;
    uint64_t total_reduce_time_;
    uint64_t total_merge_time_;
    uint64_t total_real_time_;
    bool clean_;
    
    int next_task() {
        return atomic_add32_ret(&next_task_);
    }
    int next_task_;
    int phase_;
    xarray<split_t> ma_;

    map_bucket_manager_base *m_;
    map_bucket_manager_base *sample_;
    bool sampling_;
    predictor e_[JOS_NCPU];
};

struct static_appbase {
    static int final_output_pair_comp(const void *p1, const void *p2) {
        return the_app_->internal_final_output_compare(p1, p2);
    }
    struct key_comparator {
        template <typename T>
        int operator()(const T *p1, const T *p2) const {
            return static_appbase::key_compare(p1->key_, p2->key_);
        }
    };
    struct value_apply_type {
        void operator()(keyvals_t *p, bool insert, void *v) const {
            p->map_value_insert(v);
        }
    };
    struct key_copy_type {
        void *operator()(void *key, size_t keylen) const {
            return static_appbase::key_copy(key, keylen);
        }
    };
    template <typename T>
    static int pair_comp(const void *p1, const void *p2) {
        const T *x1 = reinterpret_cast<const T *>(p1);
        const T *x2 = reinterpret_cast<const T *>(p2);
        return the_app_->key_compare(x1->key_, x2->key_);
    }
    static int key_compare(const void *k1, const void *k2) {
        return the_app_->key_compare(k1, k2);
    }
    static void *key_copy(void *k, size_t keylen) {
        return the_app_->key_copy(k, keylen);
    }
    static int application_type() {
        return the_app_->application_type();
    }
    static void map_values_insert(keyvals_t *dst, void *v) {
        return the_app_->map_values_insert(dst, v);
    }
    static void map_values_move(keyvals_t *dst, keyvals_t *src) {
        return the_app_->map_values_move(dst, src);
    }
    static void internal_reduce_emit(keyvals_t &p);
    static void set_app(mapreduce_appbase *app) {
        the_app_ = app;
    }
    static void key_free(void *k) {
        the_app_->key_free(k);
    }
  private:
    static mapreduce_appbase *the_app_;
};

#endif
