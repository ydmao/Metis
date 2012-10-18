#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>
#include "mbktsmgr.hh"
#include "apphelper.hh"
#include "bench.hh"
#include "reduce.hh"
#include "estimation.hh"
#include "arraybktmgr.hh"
#ifdef JOS_USER
#include <inc/compiler.h>
#include <inc/lib.h>
#include <inc/sysprof.h>
#endif

typedef struct {
    keyvals_arr_t v;
} htable_entry_t;

typedef struct {
    int map_rows;
    int map_cols;
    int htable_size;
    htable_entry_t **mbks;
} mapper_t;

static mapper_t mapper;
static mapper_t mapper_bak;
static keyvals_arr_t *map_out = NULL;

void arraybktmgr::mbm_mbks_init(int rows, int cols)
{
    mapper.map_rows = rows;
    mapper.map_cols = cols;
    htable_entry_t **buckets =
	(htable_entry_t **) malloc(rows * sizeof(htable_entry_t *));
    for (int i = 0; i < rows; i++) {
	buckets[i] = (htable_entry_t *) malloc(cols * sizeof(htable_entry_t));
	for (int j = 0; j < cols; j++)
	    buckets[i][j].v.init();
    }
    if (map_out) {
	free(map_out);
	map_out = NULL;
    }
    map_out = (keyvals_arr_t *) malloc(rows * cols * sizeof(keyvals_arr_t));
    for (int i = 0; i < rows * cols; i++)
	map_out[i].init();
    mapper.mbks = buckets;
}

void arraybktmgr::mbm_mbks_destroy(void)
{
    for (int i = 0; i < mapper.map_rows; i++) {
	for (int j = 0; j < mapper.map_cols; j++)
	    mapper.mbks[i][j].v.shallow_free();
	free(mapper.mbks[i]);
    }
    free(mapper.mbks);
    mapper.mbks = NULL;
}

void arraybktmgr::mbm_map_put(int row, void *key, void *val, size_t keylen, unsigned hash)
{
    assert(mapper.mbks);
    int col = hash % mapper.map_cols;
    htable_entry_t *bucket = &mapper.mbks[row][col];
    bool bnewkey = bucket->v.map_insert_kv(key, val, keylen, hash);
    est_newpair(row, bnewkey);
}

void arraybktmgr::mbm_do_reduce_task(int col)
{
    if (!mapper.mbks)
	return;
    keyvals_arr_t *nodes[JOS_NCPU];
    for (int i = 0; i < mapper.map_rows; i++)
	nodes[i] = &mapper.mbks[i][col].v;
    reduce_or_group_go(nodes, mapper.map_rows, NULL, NULL);
    for (int i = 0; i < mapper.map_rows; i++)
	mapper.mbks[i][col].v.shallow_free();
}

void arraybktmgr::mbm_rehash_bak(int row)
{
    if (!mapper_bak.mbks)
	return;
    for (int i = 0; i < mapper_bak.map_cols; i++) {
	htable_entry_t *bucket = &mapper_bak.mbks[row][i];
        keyvals_arr_t::iterator it = bucket->v.begin();
	while (it != bucket->v.end()) {
	    htable_entry_t *dest =
		    &mapper.mbks[row][it->hash % mapper.map_cols];
	    dest->v.insert_new(&it, comparator::keyvals_pair_comp);
            memset(&it, 0, sizeof(keyvals_t));
            ++it;
	}
        bucket->v.shallow_free();
    }
}

void arraybktmgr::mbm_mbks_bak(void)
{
    mapper_bak = mapper;
    memset(&mapper, 0, sizeof(mapper));
}

xarray_base *arraybktmgr::mbm_map_get_output(int *n, bool *kvs)
{
    *n = mapper.map_rows * mapper.map_cols;
    *kvs = true;
    return (xarray_base *)map_out;
}

void arraybktmgr::mbm_map_prepare_merge(int row)
{
    for (int i = 0; i < mapper.map_cols; i++) {
	map_out[row * mapper.map_cols + i] = mapper.mbks[row][i].v;
	memset(&mapper.mbks[row][i].v, 0, sizeof(mapper.mbks[row][i].v));
    }
}

