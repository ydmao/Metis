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
#include "pch_kvsarray.hh"
#include "pch_kvarray.hh"

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

static key_cmp_t JSHARED_ATTR keycmp = NULL;
static void *map_out = NULL;
static int group_before_merge;
static pch_kvsarray hkvsarr;
static pch_kvarray hkvarr;

void appendbktmgr::mbm_mbks_init(int rows, int cols) {
    mapper.map_rows = rows;
    mapper.map_cols = cols;
    mapper.mbks = (htable_entry_t **) malloc(rows * sizeof(htable_entry_t *));
    for (int i = 0; i < rows; i++) {
	mapper.mbks[i] =
	    (htable_entry_t *) malloc(cols * sizeof(htable_entry_t));
	for (int j = 0; j < cols; j++)
	    hkvarr.pch_init(&mapper.mbks[i][j].v);
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
	    hkvsarr.pch_init(&out[i]);
	map_out = out;
    } else {
	keyval_arr_t *out = new keyval_arr_t[rows * cols];
	for (int i = 0; i < rows * cols; i++)
	    hkvarr.pch_init(&out[i]);
	map_out = out;
    }
}

void appendbktmgr::mbm_set_util(key_cmp_t kcmp) {
    keycmp = kcmp;
    hkvarr.pch_set_util(kcmp);
    if (group_before_merge)
	hkvsarr.pch_set_util(kcmp);
}

void appendbktmgr::mbm_map_put(int row, void *key, void *val, size_t keylen,
                               unsigned hash) {
    assert(mapper.mbks);
    int col = hash % mapper.map_cols;
    htable_entry_t *bucket = &mapper.mbks[row][col];
    hkvarr.pch_insert_kv(&bucket->v, key, val, keylen, hash);
    est_newpair(row, 1);
}

void appendbktmgr::mbm_do_reduce_task(int col) {
    if (!mapper.mbks)
	return;
    assert(the_app.atype != atype_maponly);
    keyval_arr_t *pnodes[JOS_NCPU];
    for (int i = 0; i < mapper.map_rows; i++)
	pnodes[i] = &mapper.mbks[i][col].v;
    reduce_or_group::do_kv(pnodes, mapper.map_rows, NULL, NULL);
    for (int i = 0; i < mapper.map_rows; i++)
	hkvarr.pch_shallow_free(&mapper.mbks[i][col].v);
}

static inline int
keyval_cmp(const void *kvs1, const void *kvs2)
{
    return keycmp(((keyval_t *) kvs1)->key, ((keyval_t *) kvs2)->key);
}

void *appendbktmgr::mbm_map_get_output(pc_handler_t ** phandler,
                                       int *ntasks) {
    if (group_before_merge)
	*phandler = &hkvsarr;
    else
	*phandler = &hkvarr;
    *ntasks = mapper.map_rows * mapper.map_cols;
    return map_out;
}

void appendbktmgr::mbm_rehash_bak(int row)
{
    if (!mapper_bak.mbks)
	return;
    for (int i = 0; i < mapper_bak.map_cols; i++) {
	htable_entry_t *bucket = &mapper_bak.mbks[row][i];
	for (uint32_t j = 0; j < bucket->v.len; j++)
	    mbm_map_put(row, bucket->v.arr[j].key, bucket->v.arr[j].val,
			0, bucket->v.arr[j].hash);
	hkvarr.pch_shallow_free(&bucket->v);
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
	reduce_or_group::do_kv(&p, 1, hkvsarr.pch_append_kvs,
			  &((keyvals_arr_t *) map_out)[row]);
	hkvarr.pch_shallow_free(&mapper.mbks[row][0].v);
    }
}

