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
#include "btree.hh"
#include "application.hh"
#include "test_util.hh"
#include <assert.h>
#include <iostream>
using namespace std;

struct mock_app : public map_only {
    int key_compare(const void *k1, const void *k2) {
        int64_t i1 = int64_t(k1);
        int64_t i2 = int64_t(k2);
        return i1 - i2;
    }
   
    bool split(split_t *ma, int ncore) {
        assert(0);
    }
    void map_function(split_t *ma) {
        assert(0);
    }
};

void check_tree(btree_type &bt) {
    int64_t i = 1;
    btree_type::iterator it = bt.begin();
    while (it != bt.end()) {
        CHECK_EQ(i, int64_t(it->key));
        CHECK_EQ(1, int64_t(it->size()));
        CHECK_EQ(i + 1, int64_t((*it)[0]));
        ++it;
        ++i;
    }
    assert(size_t(i) == (bt.size() + 1));
}

void check_tree_copy(btree_type &bt) {
    xarray<keyvals_t> dst;
    bt.copy(&dst);
    for (int64_t i = 1; i <= int64_t(bt.size()); ++i) {
        CHECK_EQ(i, int64_t(dst[i - 1].key));
        CHECK_EQ(1, int64_t(dst[i - 1].size()));
        CHECK_EQ(i + 1, int64_t(dst[i - 1][0]));
        ++i;
    }
}

void check_tree_copy_and_free(btree_type &bt) {
    xarray<keyvals_t> dst;
    bt.copy(&dst);
    bt.shallow_free();
    for (int64_t i = 1; i <= int64_t(bt.size()); ++i) {
        CHECK_EQ(i, int64_t(dst[i - 1].key));
        CHECK_EQ(1, int64_t(dst[i - 1].size()));
        CHECK_EQ(i + 1, int64_t(dst[i - 1][0]));
        ++i;
    }
}

void test1() {
    btree_type bt;
    bt.init();
    check_tree(bt);
    check_tree_copy(bt);
    for (int64_t i = 1; i < 1000; ++i) {
        bt.map_insert_sorted_copy_on_new((void *)i, (void *)(i + 1), 4, 0);
        check_tree(bt);
        check_tree_copy(bt);
    }
    assert(bt.size() == 999);
}

void test2() {
    btree_type bt;
    bt.init();
    check_tree(bt);
    check_tree_copy(bt);
    for (int64_t i = 1; i < 1000; ++i) {
        keyvals_t kvs;
        kvs.key = (void *)i;
        kvs.push_back((void *) (i + 1));
        bt.map_insert_sorted_new_and_raw(&kvs);
        kvs.init();
        check_tree(bt);
        check_tree_copy(bt);
        CHECK_EQ(size_t(i), bt.size());
    }
    assert(bt.size() == 999);
    check_tree_copy_and_free(bt);
}

int main(int argc, char *argv[]) {
    mock_app app;
    static_appbase::set_app(&app);
    test1();
    test2();
    cerr << "PASS" << endl;
    return 0;
}
