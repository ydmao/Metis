#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif
#include "value_helper.hh"
#include "apphelper.hh"

enum { combiner_threshold = 8 };

void map_values_insert(keyvals_t * kvs, void *val) {
    if (the_app.atype == atype_mapreduce && the_app.mapreduce.vm) {
	kvs->set_multiplex_value(the_app.mapreduce.vm(kvs->multiplex_value(), val, 0));
	return;
    }
    kvs->push_back(val);
    if (the_app.atype == atype_mapreduce && the_app.mapreduce.combiner
	&& kvs->size() >= combiner_threshold) {
        size_t n = 0;
        void **inout = kvs->pull_array(n);
	size_t newn = the_app.mapreduce.combiner(kvs->key, inout, n);
        kvs->set_array(inout, newn);
    }
}

void map_values_mv(keyvals_t *dst, keyval_t *src) {
    if (the_app.atype == atype_mapreduce && the_app.mapreduce.vm)
        assert(0 && "Not supported yet");
    else
        dst->push_back(src->val);
    src->reset();
}

void map_values_mv(keyvals_t *dst, keyvals_t *src) {
    if (the_app.atype == atype_mapreduce && the_app.mapreduce.vm) {
	assert(src->multiplex());
	if (dst->size() == 0)
            dst->set_multiplex_value(src->multiplex_value());
	else
	    dst->set_multiplex_value(the_app.mapreduce.vm(dst->multiplex_value(),
                                                          src->multiplex_value(), 0));
        src->reset();
    } else {
        dst->append(*src);
        src->reset();
    }
}

void map_values_mv(keyvals_t *dst, keyvals_len_t *src) {
    assert(the_app.atype == atype_mapgroup);  // must be mapgroup
    dst->append(src->vals, src->len);
    src->reset();
}
