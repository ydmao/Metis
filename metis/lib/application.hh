#ifndef MR_SCHED_H
#define MR_SCHED_H

#include "mr-types.hh"
#include "profile.hh"
#include "bench.hh"

struct reduce_bucket_manager_base;
struct metis_runtime;

struct mapreduce_appbase {
    virtual void map_function(split_t *) = 0;
    virtual bool split(split_t *ret, int ncore) = 0;
    virtual int key_compare(const void *, const void *) = 0;

    /* @brief: optional function invokded for each new key. */
    virtual void *key_copy(void *k, size_t len) {
        return k;
    }
    /* @brief: default partition function that partition keys into reduce/group buckets */
    virtual unsigned partition(void *k, int length) {
        size_t h = 5381;
        char *x = (char *) k;
        for (int i = 0; i < length; ++i)
	    h = ((h << 5) + h) + unsigned(x[i]);
        return h % unsigned(-1);
    } 
    /* @brief: set the number of cores to use. Metis uses all cores by default. */
    void set_ncore(int ncore) {
        ncore_ = ncore;
    }
    void join();
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
    virtual int final_output_compare(const void *p1, const void *p2) = 0;

    /* internal use only */
    reduce_bucket_manager_base *get_reduce_bucket_manager();
    virtual int application_type() = 0;
    virtual void map_values_insert(keyvals_t *kvs, void *v) {
        kvs->push_back(v);
    }
    virtual void map_values_move(keyvals_t *dst, keyvals_t *src) {
        dst->append(*src);
        src->reset();
    }
  protected:
    uint64_t sched_sample();
    virtual bool skip_reduce_or_group_phase() = 0;
    virtual void set_final_result() = 0;
    int map_worker();
    int reduce_worker();
    int merge_worker();
    static void *base_worker(void *arg);
    void run_phase(int phase, int ncore, uint64_t &t, int first_task = 0);

    int nreduce_or_group_task_;
    enum { main_lcpu = 0 };
    enum { def_gr_tasks_per_cpu = 16 };
    enum { default_sample_reduce_task = 10000 };
    enum { sample_percent = 5 };
    enum { combiner_threshold = 8 };
  private:

    uint64_t nsampled_splits_;
    int merge_ncpus_;
    int merge_nsplits_;

    int ncore_;   
    uint64_t total_sample_time_;
    uint64_t total_map_time_;
    uint64_t total_reduce_time_;
    uint64_t total_merge_time_;
    uint64_t total_real_time_;
    metis_runtime *rt_;
    
    int next_task() {
        return atomic_add32_ret(&next_task_);
    }
    int next_task_;
    int phase_;
    xarray<split_t> ma_;
};

struct map_reduce_or_group_base : public mapreduce_appbase {
    bool skip_reduce_or_group_phase() {
#ifdef MAP_MERGE_REDUCE
#if USE_PSRS
        return true;
#endif
	printf("TODO: support merge sort in MAP_MERGE_REDUCE mode\n");
	exit(EXIT_FAILURE);
#else
        return false;
#endif
    }
};

struct map_reduce : public map_reduce_or_group_base {
    map_reduce() : map_reduce_or_group_base() {
        bzero(&results_, sizeof(results_));
    }
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
    /* @brief: set the optional output compare function */
    virtual int final_output_compare(const void *p1, const void *p2) {
        const keyval_t *x1 = reinterpret_cast<const keyval_t *>(p1);
        const keyval_t *x2 = reinterpret_cast<const keyval_t *>(p2);
        return key_compare(x1->key, x2->key);
    }
    void set_final_result();
    final_data_kv_t results_;	/* output data, <key, mapped value> */

    void internal_reduce_emit(keyvals_t &p);
    int application_type() {
        return atype_mapreduce;
    }
    void map_values_insert(keyvals_t *kvs, void *val);
    void map_values_move(keyvals_t *dst, keyvals_t *src);
};

struct map_group : public map_reduce_or_group_base {
    map_group() : map_reduce_or_group_base() {
        bzero(&results_, sizeof(results_));
    }
 
    /* @brief: output data, <key, values> */
    final_data_kvs_len_t results_;
    void set_final_result();

    /* @brief: if not zero, disables the sampling */
    void set_group_task(int group_task) {
        nreduce_or_group_task_ = group_task;
    }
    /* @brief: default output compare function */
    virtual int final_output_compare(const void *p1, const void *p2) {
        const keyvals_len_t *x1 = reinterpret_cast<const keyvals_len_t *>(p1);
        const keyvals_len_t *x2 = reinterpret_cast<const keyvals_len_t *>(p2);
        return key_compare(x1->key, x2->key);
    }
    void internal_reduce_emit(keyvals_t &p);
    int application_type() {
        return atype_mapgroup;
    }
};

struct map_only : public mapreduce_appbase {
    map_only() : mapreduce_appbase() {
        bzero(&results_, sizeof(results_));
    }
    /* @brief: set the optional output compare function */
    virtual int final_output_compare(const void *p1, const void *p2) {
        const keyval_t *x1 = reinterpret_cast<const keyval_t *>(p1);
        const keyval_t *x2 = reinterpret_cast<const keyval_t *>(p2);
        return key_compare(x1->key, x2->key);
    }
    final_data_kv_t results_;	/* output data, <key, mapped value> */
    void set_final_result();

    int application_type() {
        return atype_maponly;
    }
  protected:
    bool skip_reduce_or_group_phase() {
        return true;
    }
};

#endif
