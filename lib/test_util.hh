/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
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
