#include "lib/cpumap.hh"

static int logical_to_physical_[JOS_NCPU];

void cpumap_init() {
    for (int i = 0; i < JOS_NCPU; ++i)
	logical_to_physical_[i] = i;
}

int cpumap_physical_cpuid(int i) {
    return logical_to_physical_[i];
}
