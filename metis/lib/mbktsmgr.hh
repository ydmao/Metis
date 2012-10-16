#ifndef MBKTSMGR_H
#define MBKTSMGR_H
#include "mr-types.hh"
#include "pchandler.hh"

struct mbkts_mgr_t {
    virtual void mbm_set_util(key_cmp_t kcmp) = 0;
    virtual void mbm_mbks_init(int rows, int cols) = 0;
    virtual void mbm_mbks_destroy(void) = 0;
    virtual void mbm_mbks_bak(void) = 0;
    virtual void mbm_rehash_bak(int row) = 0;
    virtual void mbm_map_put(int row, void *key, void *val, size_t keylen,
			 unsigned hash) = 0;
    virtual void mbm_map_prepare_merge(int row) = 0;
    virtual void *mbm_map_get_output(pc_handler_t ** pch, int *narr) = 0;
    /* make sure the pairs of the reduce bucket is sorted by key, if
     * no out_cmp function is provided by application. */
    virtual void mbm_do_reduce_task(int col) = 0;
};

#endif
