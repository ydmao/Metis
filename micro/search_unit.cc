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
