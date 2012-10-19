#ifndef RBKTSMGR_H
#define RBKTSMGR_H

#include "mr-conf.hh"
#include "mr-types.hh"
#include "apphelper.hh"
#include "comparator.hh"
#include "psrs.hh"

template <typename T>
inline xarray<T> *merge_impl(xarray<xarray<T> > &rb, size_t subsize,
                             int ncpus, int lcpu) {
    typedef xarray<T> C;
    C *a = (C *)rb.array();
    if (!use_psrs)
        return mergesort(a, rb.size(), ncpus, lcpu,
                         comparator::final_output_pair_comp);
    C *xo = NULL;
    if (psrs<C>::main_cpu(lcpu)) {
        xo = new C;
        xo->resize(subsize);
        psrs<C>::instance()->init(xo);
    }
    psrs<C>::instance()->do_psrs(a, rb.size(), ncpus, lcpu,
                comparator::final_output_pair_comp);
    return (psrs<C>::main_cpu(lcpu)) ? xo :NULL;
}

struct reduce_bucket_manager_base {
    virtual void init(int n) = 0;
    virtual void destroy() = 0;
    virtual void set_current_reduce_task(int i) = 0;
    virtual void merge_reduced_buckets(int ncpus, int lcpu) = 0;
};

template <typename T>
struct reduce_bucket_manager : public reduce_bucket_manager_base {
    static reduce_bucket_manager *instance() {
        static reduce_bucket_manager instance;
        return &instance;
    }
    void init(int n) {
        rb_.resize(n);
        for (int i = 0; i < n; ++i)
            rb_[i].init();
        assert(pthread_key_create(&current_task_key_, NULL) == 0);
        set_current_reduce_task(0);
    }
    void destroy() {
        rb_.resize(0);
    }
    typedef xarray<T> C;
    xarray<T> *get(int p) {
        return &rb_[p];
    }
    void emit(const T &p) {
        rb_[current_task()].push_back(p);
    }
    void set_current_reduce_task(int ir) {
        assert(size_t(ir) < rb_.size());
        assert(pthread_setspecific(current_task_key_,
                                   (void *)intptr_t(ir)) == 0);
    }
    /** @brief: merge the output buckets of reduce phase, i.e. the final output.
        For psrs, the result is stored in rb_[0]; for mergesort, the result are
        spread in rb[0..(ncpus - 1)]. */
    void merge_reduced_buckets(int ncpus, int lcpu) {
        C *xo = merge_impl<T>(rb_, subsize(), ncpus, lcpu);
        shallow_free_buckets();
        if (xo) {
            rb_[lcpu].swap(*xo);
            delete xo;
        }
    }
    template <typename D>
    void transfer(int p, D *dst) {
        C *x = get(p);
        dst->data = x->array();
        dst->length = x->size();
        x->init();
    }
  private:
    int current_task() {
        return intptr_t(pthread_getspecific(current_task_key_));
    }
    void shallow_free_buckets() {
        for (size_t i = 0; i < rb_.size(); ++i)
            rb_[i].shallow_free();
    }
    size_t subsize() {
        size_t n = 0;
        for (size_t i = 0; i < rb_.size(); ++i)
            n += rb_[i].size();
        return n;
    }
    reduce_bucket_manager() {}
    xarray<C> rb_; // reduce buckets
    pthread_key_t current_task_key_;
};

#endif
