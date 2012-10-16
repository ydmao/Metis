#include <assert.h>
#include <string.h>
#include "apphelper.hh"
#include "psrs.hh"
#include "bench.hh"
#include "mergesort.hh"
#include "rbktsmgr.hh"
#include "pch_kvarray.hh"
#include "pch_kvsarray.hh"
#include "pch_kvslenarray.hh"

app_arg_t the_app;

void
app_set_arg(app_arg_t * app)
{
    the_app = *app;
    if (app->atype == atype_mapreduce) {
	if (app->mapreduce.vm) {
	    assert(!app->mapreduce.reduce_func);
	    assert(!app->mapreduce.combiner);
	}
	assert(app->mapreduce.results);
	assert(!app->mapreduce.results->data);
	assert(!app->mapreduce.results->length);
        static pch_kvarray pch;
	rbkts_set_pch(&pch);
    } else if (app->atype == atype_mapgroup) {
	assert(app->mapreduce.results);
	assert(!app->mapgroup.results->data);
	assert(!app->mapgroup.results->length);
        static pch_kvslenarray pch;
	rbkts_set_pch(&pch);
    } else {
	assert(app->mapreduce.results);
	assert(!app->maponly.results->data);
	assert(!app->maponly.results->length);
        static pch_kvarray pch;
	rbkts_set_pch(&pch);
    }
}

void
app_set_final_results(void)
{
    if (the_app.atype == atype_mapgroup) {
        static pch_kvslenarray pch;
	void *arr = (void *)pch.pch_get_arr_elems(rbkts_get(0));
	uint64_t len = pch.pch_get_len(rbkts_get(0));
	the_app.mapgroup.results->data = (keyvals_len_t *) arr;
	the_app.mapgroup.results->length = len;
    } else {
        static pch_kvarray pch;
	void *arr = pch.pch_get_arr_elems(rbkts_get(0));
	uint64_t len = pch.pch_get_len(rbkts_get(0));
	the_app.mapor.results->data = (keyval_t *) arr;
	the_app.mapor.results->length = len;
    }
}
