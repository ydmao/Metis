#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#ifdef JOS_USER
#include <inc/compiler.h>
#endif
#include "value_helper.hh"
#include "application.hh"

extern mapreduce_appbase *the_app_;

void map_values_insert(keyvals_t *kvs, void *v) {
    the_app_->map_values_insert(kvs, v);
}

void map_values_mv(keyvals_t *dst, keyval_t *src) {
    map_values_insert(dst, src->val);
    src->reset();
}

void map_values_mv(keyvals_t *dst, keyvals_t *src) {
    the_app_->map_values_move(dst, src);
}

void map_values_mv(keyvals_t *dst, keyvals_len_t *src) {
    assert(the_app_->application_type() == atype_mapgroup);  // must be mapgroup
    dst->append(src->vals, src->len);
    src->reset();
}
