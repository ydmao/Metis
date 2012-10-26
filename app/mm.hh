#ifndef MM_HH_
#define MM_HH_ 1
#include "application.hh"

enum { block_len = 32 };

struct mm_data_t {
    int row_num;
    int startrow;
    int startcol;
    int *matrix_A;
    int *matrix_B;
    int matrix_len;
    int *output;
};

/** @brief: Coordinates and location for each value in the matrix */
struct mm_key_t {
    int x_loc;
    int y_loc;
    int value;
};

struct mm : public map_only {
   mm(int nsplit, bool block_based) : nsplit_(nsplit), block_based_(block_based) {}
   int key_compare(const void *v1, const void *v2);
   bool split(split_t *ma, int ncores) {
       return block_based_ ? split_block(ma, ncores) : split_nonblock(ma, ncores);
   }
   void map_function(split_t *ma) {
       block_based_ ? map_function_block(ma) : map_function_nonblock(ma);
   }
   bool split_block(split_t *ma, int ncores);
   bool split_nonblock(split_t *ma, int ncores);
   virtual void map_function_block(split_t *ma);
   void map_function_nonblock(split_t *ma);
   
   int nsplit_;
   bool block_based_;
   mm_data_t d_;
};

int mm::key_compare(const void *v1, const void *v2) {
    prof_enterkcmp();
    mm_key_t *key1 = (mm_key_t *) v1;
    mm_key_t *key2 = (mm_key_t *) v2;
    int r;
    if (key1->x_loc != key2->x_loc)
        r = key1->x_loc - key2->x_loc;
    else 
        r = key1->y_loc - key2->y_loc;
    prof_leavekcmp();
    return r;
}

bool mm::split_nonblock(split_t *out, int ncores) {
    /* Make a copy of the mm_data structure */
    mm_data_t *data_out = safe_malloc<mm_data_t>();
    *data_out = d_;
    /* Check whether the various terms exist */
    if (nsplit_ == 0)
	nsplit_ = ncores * def_nsplits_per_core;
    uint64_t split_size = d_.matrix_len / nsplit_;
    assert(d_.row_num <= d_.matrix_len);
    printf("Required units is %ld\n", split_size);
    /* Reached the end of the matrix */
    if (d_.row_num >= d_.matrix_len) {
	fflush(stdout);
	free(data_out);
	return false;
    }
    /* Compute available rows */
    int available_rows = d_.matrix_len - d_.row_num;
    out->length = (split_size < size_t(available_rows)) ? split_size : available_rows;
    out->data = data_out;
    d_.row_num += out->length;
    dprintf("Allocated rows is %ld\n", out->length);
    return true;
}

/** @brief: Multiplies the allocated regions of matrix to compute partial sums */
void mm::map_function_nonblock(split_t *args) {
    int row_count = 0, x_loc, value;
    int *a_ptr, *b_ptr;
    prof_enterapp();
    assert(args && args->data);
    mm_data_t *data = (mm_data_t *)args->data;
    while (row_count < int(args->length)) {
	a_ptr = data->matrix_A + (data->row_num + row_count) * data->matrix_len;
	for (int i = 0; i < data->matrix_len; i++) {
	    b_ptr = data->matrix_B + i;
	    value = 0;
	    for (int j = 0; j < data->matrix_len; j++) {
		value += (a_ptr[j] * (*b_ptr));
		b_ptr += data->matrix_len;
	    }
	    x_loc = (data->row_num + row_count);
	    data->output[x_loc * data->matrix_len + i] = value;
	    fflush(stdout);
	}
	dprintf("%d Loop\n", data->row_num);
	row_count++;
    }
    printf("Finished Map task %d\n", data->row_num);
    fflush(stdout);
    free(data);
    prof_leaveapp();
}

/* @brief: Assign block_len elements in a row the output matrix */
bool mm::split_block(split_t *out, int ncore) {
    prof_enterapp();
    /* Make a copy of the mm_data structure */
    mm_data_t *data_out = safe_malloc<mm_data_t>();
    *data_out = d_;
    if (d_.startrow >= d_.matrix_len) {
	free(data_out);
	prof_leaveapp();
	return false;
    }
    /* Compute available rows */
    out->data = data_out;
    d_.startcol += block_len;
    if (d_.startcol > d_.matrix_len) {
	d_.startrow += block_len;
	d_.startcol = 0;
    }
    prof_leaveapp();
    return true;
}

/* Multiplies the allocated regions of matrix to compute partial sums */
void mm::map_function_block(split_t * args) {
    prof_enterapp();
    assert(args && args->data);
    mm_data_t *data = (mm_data_t *)args->data;
    dprintf("%d Start Loop \n", data->row_num);
    int i = data->startrow;
    int j = data->startcol;
    dprintf("do %d %d of %d\n", i, j, data->matrix_len);
    for (int k = 0; k < data->matrix_len; k += block_len) {
	int end_i = i + block_len;
	int end_j = j + block_len;
	int end_k = k + block_len;
	for (int a = i; a < end_i && a < data->matrix_len; a++)
	    for (int b = j; b < end_j && b < data->matrix_len; b++)
		for (int c = k; c < end_k && c < data->matrix_len; c++)
		    data->output[data->matrix_len * a + b] +=
			(data->matrix_A[data->matrix_len * a + c] *
			 data->matrix_B[data->matrix_len * c + b]);
    }
    dprintf("Finished Map task %d\n", data->row_num);
    free(data);
    prof_leaveapp();
}

inline void usage(char *fn) {
    printf("usage: %s [options]\n", fn);
    printf("options:\n");
    printf("  -p nprocs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -l : matrix dimentions. (assume squaure)\n");
}

#endif
