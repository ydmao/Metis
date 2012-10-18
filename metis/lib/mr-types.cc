#include "mr-types.hh"
#include "comparator.hh"
#include "value_helper.hh"

extern keycopy_t mrkeycopy;

bool keyval_arr_t::map_insert_kv(void *key, void *val, size_t keylen, unsigned hash) {
    void *ik = (keylen && mrkeycopy) ? mrkeycopy(key, keylen) : key;
    keyval_t tmp(ik, val, hash);
    push_back(&tmp);
    return true;
}

bool keyvals_arr_t::map_insert_kv(void *key, void *val, size_t keylen, unsigned hash) {
    keyvals_t tmp(key, hash);
    int pos = 0;
    bool bnew = insert_new(&tmp, comparator::keyvals_pair_comp, &pos);
    if (bnew && keylen && mrkeycopy)
        at(pos).key = mrkeycopy(key, keylen);
    map_values_insert(&at(pos), val);
    return bnew;
}
