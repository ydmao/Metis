#ifndef ESTIMATION_H
#define ESTIMATION_H

#include "mr-types.hh"

enum { expected_keys_per_bucket = 10 };

struct estimation {
    void init() {
        bzero(m_, sizeof(m_));
    }
    void onepair(int row, int newkey) {
        mstate_type &m = m_[row];
        if (newkey)
	    m.cur_nkeys++;
        m.cur_npairs++;
        if (m.cur_npairs % estimation_interval == 0)
	    interval_passed(row);
    }
    void task_finished(int row) {
        mstate_type &m = m_[row];
        if (m.last_npairs == 0)
	    m.pair_rate = m.cur_npairs;
        else
	    m.pair_rate = m.pair_rate / 2 + (m.cur_npairs - m.last_npairs) / 2;
        m.last_npairs = m.cur_npairs;
        ++ m.nsampled;
    }
    void estimate(uint64_t &nk, uint64_t &np, int row, int ntotal) {
        mstate_type &m = m_[row];
        np = m.pair_rate * (ntotal - m.nsampled) + m.cur_npairs;
        nk = m.key_rate * (np - m.cur_npairs) / estimation_interval + m.cur_nkeys;
    }
    int get_finished(int row) {
        return m_[row].nsampled;
    }
  private:
    enum { estimation_interval = 1000 };

    union __attribute__ ((__packed__, __aligned__(JOS_CLINE))) mstate_type {
        struct {
	    uint64_t cur_nkeys;
	    uint64_t cur_npairs;
	    uint64_t last_nkeys;
	    uint64_t last_npairs;
	    uint64_t key_rate;
	    uint64_t pair_rate;
	    uint64_t nsampled;
        };
        char __pad[2 * JOS_CLINE];
    };

    void interval_passed(int row) {
        mstate_type &m = m_[row];
        if (m.last_nkeys == 0)
    	    m.key_rate = m.cur_nkeys;
        else
	    m.key_rate = m.key_rate / 2 + (m.cur_nkeys - m.last_nkeys) / 2;
        m.last_nkeys = m.cur_nkeys;
    }
    mstate_type m_[JOS_NCPU];
};

#endif
