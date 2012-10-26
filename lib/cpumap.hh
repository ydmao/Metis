#ifndef CPUMAP_HH
#define CPUMAP_HH

enum { main_core = 0 };
void cpumap_init();
int cpumap_physical_cpuid(int i);

#endif
