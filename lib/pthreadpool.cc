#include "thread.hh"
#include "mr-types.hh"
#include "bench.hh"
#include "cpumap.hh"
#include <assert.h>
#include <string.h>

struct  __attribute__ ((aligned(JOS_CLINE))) thread_pool_t {
    void *volatile a_;
    void *(*volatile f_) (void *);
    volatile char pending_;
    pthread_t tid_;
    volatile bool running_;

    template <typename T>
    void set_task(void *arg, T &f) {
        a_ = arg;
        f_ = f;
        mfence();
        pending_ = true;
    }

    void wait_finish() {
        while (running_)
            nop_pause();
    }

    void wait_running() {
        while (pending_)
            nop_pause();
    }
    void run_next_task() {
        while (!pending_)
            nop_pause();
        running_ = true;
        pending_ = false;
        f_(a_);
        running_ = false;
    }
};

static thread_pool_t tp_[JOS_NCPU];
static bool tp_inited_ = false;
__thread int cur_lcpu = 0;
static int main_lcpu = 0;
static int used_nlcpus = 0;

int mthread_is_mainlcpu(int lcpu) {
    return lcpu == main_lcpu;
}

void mthread_create(pthread_t * tid, int lid, void *(*start_routine) (void *),
  	            void *arg) {
    assert(tp_inited_);
    if (lid == main_lcpu)
	start_routine(arg);
    else {
        tp_[lid].wait_finish();
	tp_[lid].set_task(arg, start_routine);
        tp_[lid].wait_running();
    }
}

void mthread_join(pthread_t tid, int lid, void **exitcode) {
    tp_[lid].wait_finish();
    if (exitcode)
	*exitcode = 0;
}

static void *mthread_entry(void *args) {
    cur_lcpu = ptr2int<int>(args);
    assert(affinity_set(lcpu_to_pcpu[cur_lcpu]) == 0);
    while (true)
        tp_[cur_lcpu].run_next_task();
}

void mthread_init(int nlcpus, int mlcpu) {
    if (tp_inited_)
        return;
    cpumap_init();
    used_nlcpus = nlcpus;
    cur_lcpu = mlcpu;
    main_lcpu = mlcpu;
    assert(affinity_set(lcpu_to_pcpu[main_lcpu]) == 0);
    tp_inited_ = true;
    bzero(tp_, sizeof(tp_));
    for (int i = 0; i < used_nlcpus; ++i)
	if (i == main_lcpu)
	    tp_[i].tid_ = pthread_self();
	else
	    assert(pthread_create(&tp_[i].tid_, NULL, mthread_entry, int2ptr(i)) == 0);
}

static void *mthread_exit(void *) {
    pthread_exit(NULL);
}

void mthread_finalize(void) {
    if (!tp_inited_)
        return;
    for (int i = 0; i < used_nlcpus; ++i) {
	if (i == main_lcpu)
	    continue;
	mthread_create(NULL, i, mthread_exit, NULL);
    }
    for (int i = 0; i < used_nlcpus; ++i)
	if (i != main_lcpu)
	    pthread_join(tp_[i].tid_, NULL);
    tp_inited_ = false;
}
