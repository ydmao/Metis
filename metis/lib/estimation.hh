#ifndef ESTIMATION_H
#define ESTIMATION_H

#include "mr-types.hh"

struct rate_metric {
    void update_rate() {
        if (!last_)
            rate_ = current_;
        else
            rate_ = rate_ / 2 + (current_ - last_) / 2;
        last_ = current_;
    }
    void increment() {
        ++ current_;
    }
    /* @brief: the value of current after @n more intervals */
    uint64_t future(uint64_t n) {
        return rate_ * n + current_;
    }
    uint64_t rate_;
    uint64_t last_;
    uint64_t current_;
};

union __attribute__ ((__packed__, __aligned__(JOS_CLINE))) estimation {
    struct {
        rate_metric key_;
        rate_metric pair_;
	uint64_t n_;  // number of finished task
    };
    char __pad[2 * JOS_CLINE];

    void onepair(bool newkey) {
	key_.current_ += newkey;
        ++ pair_.current_;
        if (pair_.current_ % update_interval == 0)
            key_.update_rate();
    }
    void task_finished() {
        pair_.update_rate();
        ++n_;
    }
    void inc_estimate(uint64_t *nk, uint64_t *np, int total_task) {
        uint64_t npi = pair_.future(total_task - n_);
        *nk += key_.future((npi - pair_.current_) / update_interval);
        *np += npi;
    }
    bool valid() {
        return n_;
    }
  private:
    enum { update_interval = 1000 };
};

#endif
