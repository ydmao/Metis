#ifndef TEST_UTIL_HH_
#define TEST_UTIL_HH_

#include <iostream>
#include <assert.h>

template <typename T1, typename T2>
inline void CHECK_EQ(const T1 &expected, const T2 &actual) {
    if (expected != actual) {
        std::cerr << "Actual: " << actual 
                  << "\nExpected: " << expected << std::endl;
        assert(0);
    }
}

#endif
