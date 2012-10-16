#ifndef COMPILER_HH_
#define COMPILER_HH_ 1

#include <assert.h>

template <typename T>
inline bool instanceOf(const void *p) {
    return static_cast<const T *>(p) != NULL;
}

#endif
