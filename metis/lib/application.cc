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
#include "metis_runtime.hh"

extern JTLS int cur_lcpu;	// defined in lib/pthreadpool.c
mapreduce_appbase *the_app_ = NULL;

namespace {
void pprint(const char *key, uint64_t v, const char *delim) {
    std::cout << key << "\t" << v << delim;
}

void cprint(const char *key, uint64_t v, const char *delim) {
    pprint(key, cycle_to_ms(v), delim);
}
}

int mapreduce_appbase::map_worker() {
    int n, next;
    rt_->map_worker_init(cur_lcpu);
    for (n = 0; (next = next_task()) < ma_.size(); ++n) {
	map_function(&ma_.at(next));
	rt_->map_task_finished(cur_lcpu);
    }
    rt_->map_worker_finished(cur_lcpu, skip_reduce_or_group_phase());
    return n;
}

int mapreduce_appbase::reduce_worker() {
    int n, next;
    for (n = 0; (next = next_task()) < nreduce_or_group_task_; ++n) {
        get_reduce_bucket_manager()->set_current_reduce_task(next);
	rt_->reduce_do_task(cur_lcpu, next);
    }
    return n;
}

int mapreduce_appbase::merge_worker() {
    rt_->merge(merge_ncpus_, cur_lcpu, skip_reduce_or_group_phase());
    return 1;
}

void *mapreduce_appbase::base_worker(void *x) {
    mapreduce_appbase *app = (mapreduce_appbase *)x;
    prof_worker_start(app->phase_, cur_lcpu);
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
	    n, name, pthread_self(), cur_lcpu);
    prof_worker_end(app->phase_, cur_lcpu);
    return 0;
}

void mapreduce_appbase::run_phase(int phase, int ncore, uint64_t &t, int first_task) {
    uint64_t t0 = read_tsc();
    prof_phase_stat st;
    bzero(&st, sizeof(st));
    prof_phase_init(&st);
    pthread_t tid[JOS_NCPU];
    phase_ = phase;
    next_task_ = first_task;
    for (int i = 0; i < ncore; ++i) {
	if (mthread_is_mainlcpu(i))
	    continue;
	mthread_create(&tid[i], i, base_worker, this);
    }
    mthread_create(&tid[main_lcpu], main_lcpu, base_worker, this);
    for (int i = 0; i < ncore; ++i) {
	if (mthread_is_mainlcpu(i))
	    continue;
	void *ret;
	mthread_join(tid[i], i, &ret);
    }
    prof_phase_end(&st);
    t += read_tsc() - t0;
}

size_t mapreduce_appbase::sched_sample() {
    const size_t nsample_map_task = std::max(size_t(1), sample_percent * ma_.size() / 100);
    const size_t nma = ma_.size();

    ma_.trim(nsample_map_task);
    rt_->sample_init(ncore_, default_sample_reduce_task);
    size_t t = 0;
    run_phase(MAP, ncore_, t);
    size_t predicted_ntask = rt_->sample_finished(nma);
    ma_.trim(nma, true);

    dprintf("sampled %zd from %zd tasks,", nsample_map_task, ma_.size());
    dprintf("time: %zd ms\n", cycle_to_ms(t));
    total_sample_time_ += t;
    nsampled_splits_ = nsample_map_task;
    return std::max(predicted_ntask, size_t(ncore_ * def_gr_tasks_per_cpu));
}

int mapreduce_appbase::sched_run() {
    the_app_ = this;
    rt_ = &metis_runtime::instance();
    rt_->initialize();
    const int max_ncore = get_core_count();
    assert(ncore_ <= max_ncore);
    if (!ncore_)
	ncore_ = max_ncore;
    // initialize thread manager
    mthread_init(ncore_, main_lcpu);

    // pre-split
    split_t ma;
    while (split(&ma, ncore_))
        ma_.push_back(ma);

    nsampled_splits_ = 0;
    // get the number of reduce tasks by sampling if needed
    if (skip_reduce_or_group_phase()) {
	merge_nsplits_ = ncore_;
	rt_->init_map(ncore_, 1, merge_nsplits_);
    } else {
	if (!nreduce_or_group_task_)
	    nreduce_or_group_task_ = sched_sample();
	merge_nsplits_ = nreduce_or_group_task_;
	rt_->init_map(ncore_, nreduce_or_group_task_, merge_nsplits_);
    }
    get_reduce_bucket_manager()->init(merge_nsplits_);

    uint64_t real_start = read_tsc();
    uint64_t map_time = 0, reduce_time = 0, merge_time = 0;
    // map phase
    run_phase(MAP, ncore_, map_time, nsampled_splits_);
    // reduce phase
    if (!skip_reduce_or_group_phase())
	run_phase(REDUCE, ncore_, reduce_time);
    // merge phase
    const int use_psrs = USE_PSRS;
    if (use_psrs)
	run_phase(MERGE, merge_ncpus_, merge_time);
    else {
	merge_ncpus_ = std::min(merge_nsplits_ / 2, ncore_);
	while (merge_nsplits_ > 1) {
	    run_phase(MERGE, merge_ncpus_, merge_time);
	    merge_nsplits_ = merge_ncpus_;
	    merge_ncpus_ /= 2;
	}
    }
    set_final_result();
    total_map_time_ += map_time;
    total_reduce_time_ += reduce_time;
    total_merge_time_ += merge_time;
    total_real_time_ += read_tsc() - real_start;
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
    cprint("Merge:%", total_merge_time_, SEP);
    cprint("Sum:", sum_time, SEP);
    cprint("Real:", total_real_time_, "\n");

    std::cout << "Number of Tasks\n\t";
    if (application_type() == atype_maponly) {
	pprint("Map:", ma_.size(), "\n");
    } else {
	pprint("Sample:", nsampled_splits_, SEP);
	pprint("Map:", ma_.size() - nsampled_splits_, SEP);
	pprint("Reduce:", nreduce_or_group_task_, "\n");
    }
}

void mapreduce_appbase::join() {
    mthread_finalize();
}

void mapreduce_appbase::map_emit(void *k, void *v, int keylen) {
    rt_->map_emit(cur_lcpu, k, v, keylen, partition(k, keylen));
}

void mapreduce_appbase::reduce_emit(void *k, void *v) {
    reduce_bucket_manager_base *rb = get_reduce_bucket_manager();
    typedef reduce_bucket_manager<keyval_t> expected_type;
    expected_type *x = static_cast<expected_type *>(rb);
    assert(x);
    x->emit(keyval_t(k, v));
}

reduce_bucket_manager_base *mapreduce_appbase::get_reduce_bucket_manager() {
    if (application_type() == atype_mapgroup)
        return reduce_bucket_manager<keyvals_len_t>::instance();
    else
        return reduce_bucket_manager<keyval_t>::instance();
}


/** === map_reduce === */
void map_reduce::internal_reduce_emit(keyvals_t &p) {
    if (has_value_modifier()) {
        assert(p.size() == 1);
        const keyval_t x(p.key, p.multiplex_value());
	reduce_bucket_manager<keyval_t>::instance()->emit(x);
        p.init();
    } else {
        reduce_function(p.key, p.array(), p.size());
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
	size_t newn = combine_function(kvs->key, kvs->array(), kvs->size());
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

void map_reduce::set_final_result() {
    reduce_bucket_manager<keyval_t>::instance()->transfer(0, &results_);
}

/** === map_group === */
void map_group::internal_reduce_emit(keyvals_t &p) {
    const keyvals_len_t x(p.key, p.array(), p.size());
    reduce_bucket_manager<keyvals_len_t>::instance()->emit(x);
    p.init();
}

void map_group::set_final_result() {
    reduce_bucket_manager<keyvals_len_t>::instance()->transfer(0, &results_);
}
/** === map_only ===*/

void map_only::set_final_result() {
    reduce_bucket_manager<keyval_t>::instance()->transfer(0, &results_);
}
