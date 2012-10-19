#ifndef COMPARATOR_HH
#define COMPARATOR_HH

#include "mr-types.hh"

namespace comparator {

key_cmp_t keycmp();
void set_key_compare(key_cmp_t kcmp);
int keyvals_pair_comp(const void *p1, const void *p2);
int keyval_pair_comp(const void *p1, const void *p2);
int keyvals_len_pair_comp(const void *p1, const void *p2);
int final_output_pair_comp(const void *p1, const void *p2);

template <typename T>
inline pair_cmp_t pair_comparator() { 
    assert(0); 
}

template <>
inline pair_cmp_t pair_comparator<keyvals_t>() {
    return keyvals_pair_comp;
}

template <>
inline pair_cmp_t pair_comparator<keyval_t>() {
    return keyval_pair_comp;
}

template <>
inline pair_cmp_t pair_comparator<keyvals_len_t>() {
    return keyvals_len_pair_comp;
}

};

#endif
