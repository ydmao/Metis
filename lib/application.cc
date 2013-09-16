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
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <iostream>

#include "application.hh"
#include "bench.hh"
#include "thread.hh"
#include "reduce_bucket_manager.hh"
#include "map_bucket_manager.hh"
#include "btree.hh"
#include "array.hh"

mapreduce_appbase *static_appbase::the_app_ = NULL;

void static_appbase::internal_reduce_emit(keyvals_t &p) {
    if (application_type() == atype_mapreduce)
        static_cast<map_reduce *>(the_app_)->internal_reduce_emit(p);
    else
        static_cast<map_group *>(the_app_)->internal_reduce_emit(p);
}

namespace {
void pprint(const char *key, uint64_t v, const char *delim) {
    std::cout << key << "\t" << v << delim;
}

void cprint(const char *key, uint64_t v, const char *delim) {
    pprint(key, cycle_to_ms(v), delim);
}
}

mapreduce_appbase::mapreduce_appbase() 
    : nsample_(), merge_ncore_(), ncore_(),
      total_sample_time_(), total_map_time_(), total_reduce_time_(),
      total_merge_time_(), total_real_time_(), clean_(true),
      next_task_(), phase_(), m_(NULL), sample_(NULL), sampling_(false) {
    bzero(e_, sizeof(e_));
}

mapreduce_appbase::~mapreduce_appbase() {
    reset();
}

void mapreduce_appbase::initialize() {
    threadinfo::initialize();
}

void mapreduce_appbase::deinitialize() {
    mthread_finalize();
}

map_bucket_manager_base *mapreduce_appbase::create_map_bucket_manager(int nrow, int ncol) {
    enum { index_append, index_btree, index_array };
    int index = (application_type() == atype_maponly) ? index_append : DEFAULT_MAP_DS;
    map_bucket_manager_base *m = NULL;
    switch (index) {
    case index_append:
#ifdef SINGLE_APPEND_GROUP_FIRST
        m = new map_bucket_manager<false, keyval_arr_t, keyvals_t>;
#else
        m = new map_bucket_manager<false, keyval_arr_t, keyval_t>;
#endif
        break;
    case index_btree:
        typedef btree_param<keyvals_t, static_appbase::key_comparator, 
                            static_appbase::key_copy_type, static_appbase::value_apply_type> btree_param_type;
        m = new map_bucket_manager<true, btree_type<btree_param_type>, keyvals_t>;
        break;
    case index_array:
        m = new map_bucket_manager<true, keyvals_arr_t, keyvals_t>;
        break;
    default:
        assert(0);
    }
    m->global_init(nrow, ncol);
    return m;
};

int mapreduce_appbase::map_worker() {
    threadinfo *ti = threadinfo::current();
    (sampling_ ? sample_ : m_)->per_worker_init(ti->cur_core_);
    if (!sampling_ && sample_)
        m_->rehash(ti->cur_core_, sample_);
    int n, next;
    for (n = 0; (next = next_task()) < int(ma_.size()); ++n) {
	map_function(ma_.at(next));
        if (sampling_)
	    e_[ti->cur_core_].task_finished();
    }
    if (!sampling_ && skip_reduce_or_group_phase()) {
        m_->prepare_merge(ti->cur_core_);
        if (application_type() == atype_maponly) {
#ifndef SINGLE_APPEND_GROUP_FIRST
            typedef map_bucket_manager<false, keyval_arr_t, keyval_t> expected_mtype;
            expected_mtype* m = static_cast<expected_mtype*>(m_);
            auto output = m->get_output(ti->cur_core_);

            reduce_bucket_manager_base *rb = get_reduce_bucket_manager();
            typedef reduce_bucket_manager<keyval_t> expected_rtype;
            expected_rtype *x = static_cast<expected_rtype *>(rb);
            assert(x);
            x->set(ti->cur_core_, output);
#endif
        }
    }
    return n;
}

int mapreduce_appbase::reduce_worker() {
    int n, next;
    for (n = 0; (next = next_task()) < nreduce_or_group_task_; ++n) {
        get_reduce_bucket_manager()->set_current_reduce_task(next);
	m_->do_reduce_task(next);
    }
    return n;
}

int mapreduce_appbase::merge_worker() {
    reduce_bucket_manager_base *r = get_reduce_bucket_manager();
    threadinfo *ti = threadinfo::current();
    if (application_type() == atype_maponly || !skip_reduce_or_group_phase())
	r->merge_reduced_buckets(merge_ncore_, ti->cur_core_);
    else {
        r->set_current_reduce_task(ti->cur_core_);
        // must use psrs
        m_->psrs_output_and_reduce(merge_ncore_, ti->cur_core_);
        // merge reduced buckets
	r->merge_reduced_buckets(merge_ncore_, ti->cur_core_);
    }
    return 1;
}

void *mapreduce_appbase::base_worker(void *x) {
    mapreduce_appbase *app = (mapreduce_appbase *)x;
    threadinfo *ti = threadinfo::current();
    prof_worker_start(app->phase_, ti->cur_core_);
    int n = 0;
    const char *name = NULL;
    switch (app->phase_) {
    case MAP:
        n = app->map_worker();
        name = "map";
        break;
    case REDUCE:
        n = app->reduce_worker();
        name = "reduce";
        break;
    case MERGE:
        n = app->merge_worker();
        name = "merge";
        break;
    default:
        assert(0);
    }
    dprintf("total %d %s tasks executed in thread %ld(%d)\n",
	    n, name, pthread_self(), ti->cur_core_);
    prof_worker_end(app->phase_, ti->cur_core_);
    return 0;
}

void mapreduce_appbase::run_phase(int phase, int ncore, uint64_t &t, int first_task) {
    uint64_t t0 = read_tsc();
    prof_phase_init();
    pthread_t tid[JOS_NCPU];
    phase_ = phase;
    next_task_ = first_task;
    for (int i = 0; i < ncore; ++i) {
	if (i == main_core)
	    continue;
	mthread_create(&tid[i], i, base_worker, this);
    }
    mthread_create(&tid[main_core], main_core, base_worker, this);
    for (int i = 0; i < ncore; ++i) {
	if (i == main_core)
	    continue;
	void *ret;
	mthread_join(tid[i], i, &ret);
    }
    prof_phase_end();
    t += read_tsc() - t0;
}

size_t mapreduce_appbase::sched_sample() {
    nsample_ = std::max(size_t(1), sample_percent * ma_.size() / 100);
    const size_t nma = ma_.size();
    assert(nma);
    ma_.trim(nsample_);

    sampling_ = true;
    sample_ = create_map_bucket_manager(ncore_, default_sample_hashtable_size);
    run_phase(MAP, ncore_, total_sample_time_);
    const size_t predicted_nkey = predict_nkey(e_, ncore_, nma);
    size_t predicted_ntask = predicted_nkey / expected_keys_per_bucket;
    predicted_ntask = std::max(predicted_ntask, size_t(ncore_) * min_group_or_reduce_task_per_core);
    predicted_ntask = std::min(predicted_ntask, size_t(ncore_) * max_group_or_reduce_task_per_core);
    ma_.trim(nma, true);
    sampling_ = false;
    return prime_lower_bound(predicted_ntask);
}

int mapreduce_appbase::sched_run() {
    assert(threadinfo::initialized() && "Call mapreduce_apppase::initialize first");
    static_appbase::set_app(this);
    assert(clean_);
    clean_ = false;
    const int max_ncore = get_core_count();
    assert(ncore_ <= max_ncore);
    if (!ncore_)
	ncore_ = max_ncore;

    verify_before_run();
    // initialize threads
    mthread_init(ncore_);

    // pre-split
    ma_.clear();
    split_t ma;
    bzero(&ma, sizeof(ma));
    while (split(&ma, ncore_)) {
        ma_.push_back(ma);
        bzero(&ma, sizeof(ma));
    }
    uint64_t real_start = read_tsc();
    // get the number of reduce tasks by sampling if needed
    if (skip_reduce_or_group_phase()) {
        m_ = create_map_bucket_manager(ncore_, 1);
        get_reduce_bucket_manager()->init(ncore_);
    } else {
	if (!nreduce_or_group_task_)
	    nreduce_or_group_task_ = sched_sample();
        m_ = create_map_bucket_manager(ncore_, nreduce_or_group_task_);
        get_reduce_bucket_manager()->init(nreduce_or_group_task_);
    }

    uint64_t map_time = 0, reduce_time = 0, merge_time = 0;
    // map phase
    run_phase(MAP, ncore_, map_time, nsample_);
    // reduce phase
    if (!skip_reduce_or_group_phase())
	run_phase(REDUCE, ncore_, reduce_time);
    // merge phase
    const int use_psrs = USE_PSRS;
    if (use_psrs) {
        merge_ncore_ = ncore_;
	run_phase(MERGE, merge_ncore_, merge_time);
    } else {
        reduce_bucket_manager_base *r = get_reduce_bucket_manager();
	merge_ncore_ = std::min(int(r->size()) / 2, ncore_);
	while (r->size() > 1) {
	    run_phase(MERGE, merge_ncore_, merge_time);
            r->trim(merge_ncore_);
	    merge_ncore_ /= 2;
	}
    }
    set_final_result();
    total_map_time_ += map_time;
    total_reduce_time_ += reduce_time;
    total_merge_time_ += merge_time;
    total_real_time_ += read_tsc() - real_start;
    reset();  // result everything except for results_
    return 0;
}

void mapreduce_appbase::print_stats(void) {
    prof_print(ncore_);
    uint64_t sum_time = total_sample_time_ + total_map_time_ + 
                        total_reduce_time_ + total_merge_time_;

    std::cout << "Runtime in millisecond [" << ncore_ << " cores]\n\t";
#define SEP "\t"
    cprint("Sample:", total_sample_time_, SEP);
    cprint("Map:", total_map_time_, SEP);
    cprint("Reduce:", total_reduce_time_, SEP);
    cprint("Merge:", total_merge_time_, SEP);
    cprint("Sum:", sum_time, SEP);
    cprint("Real:", total_real_time_, "\n");

    std::cout << "Number of Tasks of last Metis run\n\t";
    if (application_type() == atype_maponly) {
	pprint("Map:", ma_.size(), "\n");
    } else {
	pprint("Sample:", nsample_, SEP);
	pprint("Map:", ma_.size() - nsample_, SEP);
	pprint("Reduce:", nreduce_or_group_task_, "\n");
    }
}

void mapreduce_appbase::map_emit(void *k, void *v, int keylen) {
    unsigned hash = partition(k, keylen);
    threadinfo *ti = threadinfo::current();
    bool newkey = (sampling_ ? sample_ : m_)->emit(ti->cur_core_, k, v, keylen, hash);
    if (sampling_)
        e_[ti->cur_core_].onepair(newkey);
}

void mapreduce_appbase::reduce_emit(void *k, void *v) {
    reduce_bucket_manager_base *rb = get_reduce_bucket_manager();
    typedef reduce_bucket_manager<keyval_t> expected_type;
    expected_type *x = static_cast<expected_type *>(rb);
    assert(x);
    x->emit(keyval_t(k, v));
}

void mapreduce_appbase::reset() {
    sampling_ = false;
    if (m_) {
        delete m_;
        m_ = NULL;
    }
    if (sample_) {
        delete sample_;
        sample_ = NULL;
    }
    bzero(e_, sizeof(e_));
    clean_ = true;
    nsample_ = 0;
}


/** === map_reduce === */
void map_reduce::internal_reduce_emit(keyvals_t &p) {
    if (has_value_modifier()) {
        assert(p.size() == 1);
        keyval_t x(p.key_, p.multiplex_value());
	rb_.emit(x);
        x.init();
        p.init();
    } else {
        reduce_function(p.key_, p.array(), p.size());
        p.trim(0);
    }
}

void map_reduce::map_values_insert(keyvals_t *kvs, void *v) {
    if (has_value_modifier()) {
        if (kvs->size() == 0)
            kvs->set_multiplex_value(v);
        else
	    kvs->set_multiplex_value(modify_function(kvs->multiplex_value(), v));
	return;
    }
    kvs->push_back(v);
    if (kvs->size() >= combiner_threshold) {
	size_t newn = combine_function(kvs->key_, kvs->array(), kvs->size());
        assert(newn <= kvs->size());
        kvs->trim(newn);
    }
}

void map_reduce::map_values_move(keyvals_t *dst, keyvals_t *src) {
    if (!has_value_modifier()) {
        dst->append(*src);
        src->reset();
        return;
    }
    assert(src->multiplex());
    if (dst->size() == 0)
        dst->set_multiplex_value(src->multiplex_value());
    else
        dst->set_multiplex_value(modify_function(dst->multiplex_value(),
                                                 src->multiplex_value()));
    src->reset();
}

/** === map_group === */
void map_group::internal_reduce_emit(keyvals_t &p) {
    keyvals_len_t x(p.key_, p.array(), p.size());
    rb_.emit(x);
    x.init();
    p.init();
}

/** === map_only ===*/

