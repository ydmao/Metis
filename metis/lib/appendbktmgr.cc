#include <string.h>
#include <assert.h>
#include "mbktsmgr.hh"
#include "apphelper.hh"
#include "platform.hh"
#include "bsearch.hh"
#include "reduce.hh"
#include "estimation.hh"
#include "mr-conf.hh"
#include "appendbktmgr.hh"

typedef struct {
    keyval_arr_t v;
} htable_entry_t;

typedef struct {
    int map_rows;
    int map_cols;
    htable_entry_t **mbks;
} mapper_t;

static mapper_t mapper;
static mapper_t mapper_bak;

static void *map_out = NULL;
static int group_before_merge;

void appendbktmgr::mbm_mbks_init(int rows, int cols) {
    mapper.map_rows = rows;
    mapper.map_cols = cols;
    mapper.mbks = (htable_entry_t **) malloc(rows * sizeof(htable_entry_t *));
    for (int i = 0; i < rows; i++) {
	mapper.mbks[i] =
	    (htable_entry_t *) malloc(cols * sizeof(htable_entry_t));
	for (int j = 0; j < cols; j++)
	    mapper.mbks[i][j].v.init();
    }
    if (map_out) {
	free(map_out);
	map_out = NULL;
    }
    group_before_merge = 0;
#ifdef SINGLE_APPEND_GROUP_MERGE_FIRST
    group_before_merge = 1;
#endif
    if (group_before_merge) {
	keyvals_arr_t *out = new keyvals_arr_t[rows * cols];
	for (int i = 0; i < rows * cols; i++)
	    out[i].init();
	map_out = out;
    } else {
	keyval_arr_t *out = new keyval_arr_t[rows * cols];
	for (int i = 0; i < rows * cols; i++)
	    out[i].init();
	map_out = out;
    }
}

void appendbktmgr::mbm_map_put(int row, void *key, void *val, size_t keylen,
                               unsigned hash) {
    assert(mapper.mbks);
    int col = hash % mapper.map_cols;
    htable_entry_t *bucket = &mapper.mbks[row][col];
    bucket->v.map_insert_kv(key, val, keylen, hash);
    est_newpair(row, 1);
}

void appendbktmgr::mbm_do_reduce_task(int col) {
    if (!mapper.mbks)
	return;
    assert(the_app.atype != atype_maponly);
    keyval_arr_t *pnodes[JOS_NCPU];
    for (int i = 0; i < mapper.map_rows; i++)
	pnodes[i] = &mapper.mbks[i][col].v;
    reduce_or_group_go(pnodes, mapper.map_rows, NULL, NULL);
    for (int i = 0; i < mapper.map_rows; i++)
	mapper.mbks[i][col].v.shallow_free();
}

xarray_base *appendbktmgr::mbm_map_get_output(int *n, bool *kvs) {
    *kvs = group_before_merge;
    *n = mapper.map_rows * mapper.map_cols;
    return (xarray_base *)map_out;
}

void appendbktmgr::mbm_rehash_bak(int row)
{
    if (!mapper_bak.mbks)
	return;
    for (int i = 0; i < mapper_bak.map_cols; i++) {
	htable_entry_t *bucket = &mapper_bak.mbks[row][i];
	for (uint32_t j = 0; j < bucket->v.size(); j++)
	    mbm_map_put(row, bucket->v[j].key, bucket->v[j].val,
			0, bucket->v[j].hash);
	bucket->v.shallow_free();
    }
}

void appendbktmgr::mbm_mbks_bak(void) {
    mapper_bak = mapper;
    memset(&mapper, 0, sizeof(mapper));
}

void appendbktmgr::mbm_map_prepare_merge(int row) {
    assert(mapper.map_cols == 1);
    if (!group_before_merge) {
	((keyval_arr_t *) map_out)[row] = mapper.mbks[row][0].v;
	memset(&mapper.mbks[row][0].v, 0, sizeof(mapper.mbks[row][0].v));
    } else {
	keyval_arr_t *p = &mapper.mbks[row][0].v;
	reduce_or_group_go(&p, 1, keyvals_arr_t::append_kvs,
			   &((keyvals_arr_t *) map_out)[row]);
	mapper.mbks[row][0].v.shallow_free();
    }
}

