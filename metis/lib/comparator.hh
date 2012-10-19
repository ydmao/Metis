#ifndef COMPARATOR_HH
#define COMPARATOR_HH

#include "mr-types.hh"

namespace comparator {

key_cmp_t keycmp();
void set_key_compare(key_cmp_t kcmp);
int final_output_pair_comp(const void *p1, const void *p2);

template <typename T>
inline int generic_pair_compare(const void *p1, const void *p2) {
    const T *x1 = reinterpret_cast<const T *>(p1);
    const T *x2 = reinterpret_cast<const T *>(p2);
    return keycmp()(x1->key, x2->key);
}

template <typename T>
struct raw_comp {
    static int impl(const void *p1, const void *p2) {
        return generic_pair_compare<T>(p1, p2);
    }
};

};

#endif
