#ifndef APPHELPER_H
#define APPHELPER_H

#include "mr-types.hh"

extern app_arg_t the_app;
void app_set_arg(app_arg_t * app);
void app_set_final_results(void);

enum { vt_keyval = 0, vt_keyvals_len };
int app_output_pair_type();

#endif
