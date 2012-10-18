#ifndef BTREE_BUCKET_MANAGER_HH_
#define BTREE_BUCKET_MANAGER_HH_ 1
#include "mbktsmgr.hh"

struct btreebktmgr : public mbkts_mgr_t {
    btreebktmgr() {}
    
    void mbm_mbks_init(int rows, int cols);
    void mbm_mbks_destroy();
    void mbm_mbks_bak();
    void mbm_rehash_bak(int row);
    void mbm_map_put(int row, void *key, void *val, size_t keylen,
			 unsigned hash);
    void mbm_map_prepare_merge(int row);
    xarray_base *mbm_map_get_output(int *n, bool *kvs);
    /* make sure the pairs of the reduce bucket is sorted by key, if
     * no out_cmp function is provided by application. */
    void mbm_do_reduce_task(int col);

  private:
    struct htable_entry_t {
        btree_type v;
    };

    struct mapper_t {
        int map_rows;
        int map_cols;
        htable_entry_t **mbks;

        mapper_t() : map_rows(0), map_cols(0), mbks(0) {
        }
        inline void init(int rows, int cols);
        inline void destroy();
    };

    void map_put_kvs(int row, keyvals_t *kvs);
    void bkt_rehash(htable_entry_t *entry, int row);

    static mapper_t mapper;
    static mapper_t mapper_bak;
    static keyvals_arr_t *map_out;
};

inline void btreebktmgr::mapper_t::init(int rows, int cols) {
    map_rows = rows;
    map_cols = cols;
    mbks = (htable_entry_t **) malloc(rows * sizeof(htable_entry_t *));
    for (int i = 0; i < rows; i++) {
	mbks[i] = (htable_entry_t *) malloc(cols * sizeof(htable_entry_t));
	for (int j = 0; j < cols; j++)
	    mbks[i][j].v.init();
    }
}

inline void btreebktmgr::mapper_t::destroy() {
    if (!mbks)
	return;
    for (int i = 0; i < map_rows; i++) {
	if (!mbks[i])
            continue;
        for (int j = 0; j < map_cols; j++)
  	    mbks[i][j].v.shallow_free();
	free(mbks[i]);
    }
    free(mbks);
    mbks = NULL;
    map_rows = map_cols = 0;
}

#endif
