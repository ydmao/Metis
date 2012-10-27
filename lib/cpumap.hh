#ifndef CPUMAP_HH_
#define CPUMAP_HH_ 1

enum { main_core = 0 };
void cpumap_init();
int cpumap_physical_cpuid(int i);

#endif
