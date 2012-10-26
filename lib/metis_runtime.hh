#ifndef KVSTORE_H
#define KVSTORE_H

#include "mr-types.hh"
#include "predictor.hh"

struct map_bucket_manager_base;

struct metis_runtime {
    /* Initialize the data structure for sampling, which involves
       the Map phase only. */
    void sample_init(int rows, int cols);
    uint64_t sample_finished(int ntotal);

    /* Initialize the data structure for Map and Reduce phase */
    void reset();
    void init_map(int rows, int cols);

    /* map phase */
    void map_worker_init(int row);
    void map_task_finished(int row);
    void map_emit(int row, void *key, void *val, size_t keylen,
	          unsigned hash);
    void map_worker_finished(int row, int reduce_skipped);
    /* reduce phase */
    void reduce_do_task(int row, int col);
    void merge(int ncpus, int lcpu, int reduce_skipped);
    metis_runtime()
        : current_manager_(), sample_manager_(), sampling_(false) {
    }
    ~metis_runtime();
  private:
    void create_map_bucket_manager();

    map_bucket_manager_base *current_manager_;
    map_bucket_manager_base *sample_manager_;
    bool sampling_;
    predictor e_[JOS_NCPU];
};

#endif
