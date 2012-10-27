#ifndef IBS_HH_
#define IBS_HH_ 1

#include <inttypes.h>

void ibs_start(int cid);
void ibs_stop(int cid);

uint64_t ibs_read_count(int cid);
uint64_t ibs_read_latency(int cid);

#endif
