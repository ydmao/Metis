#include "comparator.hh"
#include "application.hh"

namespace comparator {

int key_compare(const void *k1, const void *k2) {
    return the_app_->key_compare(k1, k2);
}

int final_output_pair_comp(const void *p1, const void *p2) {
    return the_app_->internal_final_output_compare(p1, p2);
}

}
