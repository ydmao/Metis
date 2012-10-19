#include "comparator.hh"
#include "apphelper.hh"

extern app_arg_t the_app;

namespace comparator {

static key_cmp_t keycmp_ = NULL;

key_cmp_t keycmp() {
    return keycmp_;
}

void set_key_compare(key_cmp_t kcmp) {
    keycmp_ = kcmp;
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
