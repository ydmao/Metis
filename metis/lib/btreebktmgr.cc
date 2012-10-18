#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>
#include "mbktsmgr.hh"
#include "apphelper.hh"
#include "bench.hh"
#include "reduce.hh"
#include "estimation.hh"
#include "btreebktmgr.hh"

btreebktmgr::mapper_t btreebktmgr::mapper;
btreebktmgr::mapper_t btreebktmgr::mapper_bak;
keyvals_arr_t *btreebktmgr::map_out;

void btreebktmgr::mbm_mbks_init(int rows, int cols)
{
    mapper.init(rows, cols);
    if (map_out) {
	free(map_out);
	map_out = NULL;
    }
    map_out = (keyvals_arr_t *) malloc(rows * cols * sizeof(keyvals_arr_t));
    for (int i = 0; i < rows * cols; i++)
	map_out[i].init();
}

void btreebktmgr::mbm_mbks_destroy(void)
{
    mapper.destroy();
    mapper_bak.destroy();
    if (map_out) {
        // TODO: fix memory leak inside each map_out[X]
        free(map_out);
        map_out = NULL;
    }
}

void btreebktmgr::map_put_kvs(int row, keyvals_t *kvs)
{
    const unsigned hash = kvs->hash;
    int col = hash % mapper.map_cols;
    mapper.mbks[row][col].v.insert_kvs(kvs);
}

void btreebktmgr::mbm_map_put(int row, void *key, void *val, size_t keylen, unsigned hash)
{
    assert(mapper.mbks);
    const int col = hash % mapper.map_cols;
    htable_entry_t *entry = &mapper.mbks[row][col];
    int bnewkey = entry->v.insert_kv(key, val, keylen, hash);
    est_newpair(row, bnewkey);
}

void btreebktmgr::mbm_do_reduce_task(int col)
{
    assert(mapper.mbks);
    btree_type *nodes[JOS_NCPU];
    for (int i = 0; i < mapper.map_rows; i++)
	nodes[i] = &mapper.mbks[i][col].v;
    group_sorted(nodes, mapper.map_rows, reduce_emit_functor::instance());
    for (int i = 0; i < mapper.map_rows; i++)
	mapper.mbks[i][col].v.shallow_free();
}

void btreebktmgr::bkt_rehash(htable_entry_t *entry, int row)
{
    btree_type::iterator it = entry->v.begin();
    while (it != entry->v.end()) {
	map_put_kvs(row, &(*it));
        it->init();
        ++it;
    }
    entry->v.shallow_free();
}

void btreebktmgr::mbm_rehash_bak(int row)
{
    if (!mapper_bak.mbks)
	return;
    assert(mapper_bak.mbks[row]);
    for (int i = 0; i < mapper_bak.map_cols; i++)
	bkt_rehash(&mapper_bak.mbks[row][i], row);
    free(mapper_bak.mbks[row]);
    mapper_bak.mbks[row] = NULL;
}

void btreebktmgr::mbm_mbks_bak(void)
{
    memcpy(&mapper_bak, &mapper, sizeof(mapper));
    memset(&mapper, 0, sizeof(mapper));
}

xarray_base *btreebktmgr::mbm_map_get_output(int *n, bool *kvs)
{
    *kvs = true;
    *n = mapper.map_rows * mapper.map_cols;
    return (xarray_base *)map_out;
}

void btreebktmgr::mbm_map_prepare_merge(int row) {
    keyvals_arr_t *out = &map_out[row * mapper.map_cols];
    htable_entry_t *in = &mapper.mbks[row][0];
    for (int i = 0; i < mapper.map_cols; ++i, ++out, ++in) {
        out->resize(in->v.size());
        in->v.copy_kvs(out->array());
    }
}

