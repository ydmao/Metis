#ifndef MBKTSMGR_H
#define MBKTSMGR_H
#include "mr-types.hh"
#include "array.hh"

struct mbkts_mgr_t {
    virtual void init(int rows, int cols) = 0;
    virtual void destroy(void) = 0;
    virtual void rehash(int row, mbkts_mgr_t *backup) = 0;
    virtual void emit(int row, void *key, void *val, size_t keylen,
	      unsigned hash) = 0;
    virtual void prepare_merge(int row) = 0;
    virtual xarray_base *get_output(int *n, bool *kvs) = 0;
    virtual void do_reduce_task(int col) = 0;
};

#endif
