#include "threadinfo.hh"

bool threadinfo::created_ = false;
pthread_key_t threadinfo::key_;

