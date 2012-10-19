#include "comparator.hh"
#include "apphelper.hh"

extern app_arg_t the_app;

namespace comparator {

static key_cmp_t keycmp_ = NULL;

template <typename T>
int generic_pair_compare(const void *p1, const void *p2) {
    const T *x1 = reinterpret_cast<const T *>(p1);
    const T *x2 = reinterpret_cast<const T *>(p2);
    return keycmp_(x1->key, x2->key);
}

key_cmp_t keycmp() {
    return keycmp_;
}

void set_key_compare(key_cmp_t kcmp) {
    keycmp_ = kcmp;
}

int keyvals_pair_comp(const void *p1, const void *p2) {
    return generic_pair_compare<keyvals_t>(p1, p2);
}

int keyval_pair_comp(const void *p1, const void *p2) {
    return generic_pair_compare<keyval_t>(p1, p2);
}

int keyvals_len_pair_comp(const void *p1, const void *p2) {
    return generic_pair_compare<keyvals_len_t>(p1, p2);
}

int final_output_pair_comp(const void *p1, const void *p2) {
    if (the_app.any.outcmp)
        return the_app.any.outcmp(p1, p2);
    switch (app_output_pair_type()) {
        case vt_keyval: 
            return generic_pair_compare<keyval_t>(p1, p2);
        case vt_keyvals_len:
            return generic_pair_compare<keyvals_len_t>(p1, p2);
        default:
            assert(0);
    };
}

}
