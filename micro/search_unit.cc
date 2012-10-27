/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#include "bsearch.hh"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

static int compare(const int *a, const int *b) {
    return *a - *b;
}

int main() {
   int x[] = {7, 8, 9, 10};
   bool found;

   int key = 11;
   assert(xsearch::lower_bound(&key, x, 4, compare, &found) == 4 && !found);

   key = 5;
   assert(xsearch::lower_bound(&key, x, 4, compare, &found) == 0 && !found);

   key = 7;
   assert(xsearch::lower_bound(&key, x, 4, compare, &found) == 0 && found);

   key = 10;
   assert(xsearch::lower_bound(&key, x, 4, compare, &found) == 3 && found);

   int y[] = {7, 8, 9, 10, 11};
   key = 12;
   assert(xsearch::lower_bound(&key, y, 5, compare, &found) == 5 && !found);

   key = 5;
   assert(xsearch::lower_bound(&key, y, 5, compare, &found) == 0 && !found);

   key = 7;
   assert(xsearch::lower_bound(&key, y, 5, compare, &found) == 0 && found);

   key = 10;
   assert(xsearch::lower_bound(&key, y, 5, compare, &found) == 3 && found);

   key = 11;
   assert(xsearch::lower_bound(&key, y, 5, compare, &found) == 4 && found);

   printf("PASS\n");
}
