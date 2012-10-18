#include "comparator.hh"
#include "apphelper.hh"

extern app_arg_t the_app;

namespace comparator {

static key_cmp_t keycmp_ = NULL;

key_cmp_t keycmp() {
    return keycmp_;
}

static void *extract_key(const void *p) {
    if (app_output_pair_type() == vt_keyval) {
        const keyval_t *x = reinterpret_cast<const keyval_t *>(p);
        return x->key;
    } else {
        const keyvals_len_t *x = reinterpret_cast<const keyvals_len_t *>(p);
        return x->key;
    }
}

void set_key_compare(key_cmp_t kcmp) {
    keycmp_ = kcmp;
}

int keyvals_pair_comp(const void *p1, const void *p2) {
    const keyvals_t *x1 = (const keyvals_t *)p1;
    const keyvals_t *x2 = (const keyvals_t *)p2;
    return keycmp_(x1->key, x2->key);
}

int keyval_pair_comp(const void *p1, const void *p2) {
    const keyval_t *x1 = (const keyval_t *)p1;
    const keyval_t *x2 = (const keyval_t *)p2;
    return keycmp_(x1->key, x2->key);
}

int keyvals_len_pair_comp(const void *p1, const void *p2) {
    const keyvals_len_t *x1 = (const keyvals_len_t *)p1;
    const keyvals_len_t *x2 = (const keyvals_len_t *)p2;
    return keycmp_(x1->key, x2->key);
}

int final_output_pair_comp(const void *p1, const void *p2) {
    if (the_app.any.outcmp)
        return the_app.any.outcmp(p1, p2);
    else
        return keycmp_(extract_key(p1), extract_key(p2));
}

}
