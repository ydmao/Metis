#ifndef TEST_UTIL_HH_
#define TEST_UTIL_HH_ 1

#include <iostream>
#include <assert.h>

template <typename T1, typename T2>
inline void CHECK_EQ(const T1 &expected, const T2 &actual) {
    if (expected != actual) {
        std::cerr <<   "\tActual:   " << actual 
                  << "\n\tExpected: " << expected << std::endl;
        assert(0);
    }
}

template <typename T1, typename T2>
inline void CHECK_GT(const T1 &actual, const T2 &comp) {
    if (actual <= comp) {
        std::cerr <<   "\tActual:     " << actual 
                  << "\n\tExpected: > " << comp << std::endl;
        assert(0);
    }
}

#endif
