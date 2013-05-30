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
#ifndef BTREE_HH_
#define BTREE_HH_ 1

#include "bsearch.hh"
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

enum { order = 3 };

template <typename PARAM>
struct btnode_internal;

template <typename PARAM>
struct btnode_base {
    btnode_internal<PARAM> *parent_;
    short nk_;
    inline btnode_base() : parent_(NULL), nk_(0) {}
    virtual ~btnode_base() {}
};

template <typename PARAM>
struct btnode_leaf : public btnode_base<PARAM> {
    static const int fanout = 2 * order + 2;
    typedef typename PARAM::pair_type PAIR;
    typedef btnode_leaf<PARAM> self_type;
    typedef btnode_base<PARAM> base_type;

    typedef decltype(((PAIR *)0)->key_) key_type;
    using base_type::nk_;

    PAIR e_[fanout];
    self_type *next_;
    ~btnode_leaf() {
        for (int i = 0; i < nk_; ++i)
            e_[i].reset();
        for (int i = nk_; i < fanout; ++i)
            e_[i].init();
    }

    inline btnode_leaf() : base_type(), next_(NULL) {
        for (int i = 0; i < fanout; ++i)
            e_[i].init();
    }
    inline self_type *split() {
        auto right = new self_type;
        memcpy(right->e_, &e_[order + 1], sizeof(e_[0]) * (1 + order));
        right->nk_ = order + 1;
        nk_ = order + 1;
        auto next = next_;
        next_ = right;
        right->next_ = next;
        return right;
    }

    inline bool lower_bound(const key_type &key, int *p) {
        bool found = false;
        typename PARAM::key_comparator_type comparator;
        PAIR tmp;
        tmp.key_ = key;
        *p = xsearch::lower_bound(&tmp, e_, nk_, comparator, &found);
        return found;
    }

    inline void insert(int pos, const key_type &key, unsigned hash) {
        if (pos < nk_)
            memmove(&e_[pos + 1], &e_[pos], sizeof(e_[0]) * (nk_ - pos));
        ++ nk_;
        e_[pos].init();
        e_[pos].key_ = key;
        e_[pos].hash = hash;
    }

    inline bool need_split() const {
        return nk_ == fanout;
    }
};

template <typename PARAM>
struct btnode_internal : public btnode_base<PARAM> {
    static const int fanout = 2 * order + 2;
    typedef typename PARAM::pair_type PAIR;
    typedef btnode_internal<PARAM> self_type;
    typedef btnode_base<PARAM> base_type;

    typedef decltype(((PAIR *)0)->key_) key_type;
    using base_type::nk_;

    struct internal_pair {
        internal_pair(const key_type &k) : key_(k) {} 
        internal_pair() : key_(), v_() {} 
        key_type key_;
        base_type *v_;
    };

    internal_pair e_[fanout];
    inline btnode_internal() {
        bzero(e_, sizeof(e_));
    }
    ~btnode_internal() {}

    inline self_type *split() {
        auto nn = new self_type;
        nn->nk_ = order;
        memcpy(nn->e_, &e_[order + 1], sizeof(e_[0]) * (order + 1));
        nk_ = order;
        return nn;
    }
    inline void assign(int p, base_type *left, const key_type &key, base_type *right) {
        e_[p].v_ = left;
        e_[p].key_ = key;
        e_[p + 1].v_ = right;
    }
    inline void assign_right(int p, const key_type &key, base_type *right) {
        e_[p].key_ = key;
        e_[p + 1].v_ = right;
    }
    inline base_type *upper_bound(const key_type &key) {
        int pos = upper_bound_pos(key);
        return e_[pos].v_;
    }
    inline int upper_bound_pos(const key_type &key) {
        typename PARAM::key_comparator_type comparator;
        internal_pair tmp(key);
        return xsearch::upper_bound(&tmp, e_, nk_, comparator);
    }
    inline bool need_split() const {
        return nk_ == fanout - 1;
    }
};

template <typename PAIR_TYPE, typename KEY_COMPARE, typename KEY_COPY, typename VALUE_APPLY>
struct btree_param {
    typedef PAIR_TYPE pair_type;
    typedef KEY_COMPARE key_comparator_type;
    typedef KEY_COPY key_copy_type;
    typedef VALUE_APPLY value_apply_type;
};

template <typename PARAM>
struct btree_type {
    typedef typename PARAM::pair_type element_type;
    typedef element_type PAIR;
    typedef typename PARAM::key_copy_type key_copy_type;
    typedef typename PARAM::value_apply_type value_apply_type;

    typedef btnode_leaf<PARAM> leaf_node_type;
    typedef typename btnode_leaf<PARAM>::key_type key_type;
    typedef btnode_internal<PARAM> internal_node_type;
    typedef btnode_base<PARAM> base_node_type;

    inline void init();
    /* @brief: free the tree, but not the values */
    inline void shallow_free();
    /* @brief: insert a new key/value pair. Assertion failure if already existed */
    inline void insert(PAIR *kv);

    inline void map_insert_sorted_new_and_raw(PAIR *kv) {
        insert(kv);
    }
    /* @brief: insert key/val pair into the tree
       @return true if it is a new key */
    template <typename V>
    inline int map_insert_sorted_copy_on_new(const key_type &key, const V &val, size_t keylen, unsigned hash);

    inline size_t size() const;

    template <typename C>
    inline uint64_t transfer(C *dst);

    template <typename C>
    inline uint64_t copy(C *dst);

    /* @brief: return the number of values in the tree */
    uint64_t test_get_nvalue() {
        iterator i = begin();
        uint64_t n = 0;
        while (i != end()) {
            n += i->size();
            ++ i;
        }
        return n;
    }

    struct iterator {
        iterator() : c_(NULL), i_(0) {}
        explicit iterator(leaf_node_type *c) : c_(c), i_(0) {}
        iterator &operator=(const iterator &a) {
            c_ = a.c_;
            i_ = a.i_;
            return *this;
        }
        void operator++() {
            if (c_ && i_ + 1 == c_->nk_) {
                c_ = c_->next_;
                i_ = 0;
            } else if (c_)
                ++i_;
            else
                assert(0);
        }
        void operator++(int) {
            ++(*this);
        }
        bool operator==(const iterator &a) {
            return (!c_ && !a.c_) || (c_ == a.c_ && i_ == a.i_);
        }
        bool operator!=(const iterator &a) {
            return !(*this == a);
        }
        PAIR *operator->() {
            return &c_->e_[i_];
        }
        PAIR &operator*() {
            return c_->e_[i_];
        }
      private:
        leaf_node_type *c_;
        int i_;
    };

    iterator begin();
    iterator end();

  private:
    size_t nk_;
    short nlevel_;
    base_node_type *root_;
    template <typename C>
    uint64_t copy_traverse(C *dst, bool clear_leaf);

    /* @brief: insert @key at position @pos into leaf node @leaf,
     * and set the value of that key to empty */
    static void insert_leaf(leaf_node_type *leaf, const key_type &key, int pos, int keylen);
    static void delete_level(base_node_type *node, int level);

    leaf_node_type *first_leaf() const;

    /* @brief: insert (@key, @right) into left's parent */
    void insert_internal(const key_type &key, base_node_type *left, base_node_type *right);
    leaf_node_type *get_leaf(const key_type &key);
};

template <typename P>
void btree_type<P>::init() {
    nk_ = 0;
    nlevel_ = 0;
    root_ = NULL;
}

// left < key <= right. Right is the new sibling
template <typename P>
void btree_type<P>::insert_internal(const key_type &key, base_node_type *left, base_node_type *right) {
    auto parent = left->parent_;
    if (!parent) {
	auto newroot = new internal_node_type;
	newroot->nk_ = 1;
        newroot->assign(0, left, key, right);
	root_ = newroot;
	left->parent_ = newroot;
	right->parent_ = newroot;
	++nlevel_;
    } else {
	int ikey = parent->upper_bound_pos(key);
	// insert newkey at ikey, values at ikey + 1
	for (int i = parent->nk_ - 1; i >= ikey; i--)
	    parent->e_[i + 1].key_ = parent->e_[i].key_;
	for (int i = parent->nk_; i >= ikey + 1; i--)
	    parent->e_[i + 1].v_ = parent->e_[i].v_;
        parent->assign_right(ikey, key, right);
	++parent->nk_;
	right->parent_ = parent;
	if (parent->need_split()) {
	    key_type newkey = parent->e_[order].key_;
	    auto newparent = parent->split();
	    // push up newkey
	    insert_internal(newkey, parent, newparent);
	    // fix parent pointers
	    for (int i = 0; i < newparent->nk_ + 1; ++i)
		newparent->e_[i].v_->parent_ = newparent;
	}
    }
}

template <typename P>
btnode_leaf<P> *btree_type<P>::get_leaf(const key_type &key) {
    if (!nlevel_) {
	root_ = new leaf_node_type;
	nlevel_ = 1;
	nk_ = 0;
	return static_cast<leaf_node_type *>(root_);
    }
    auto node = root_;
    for (int i = 0; i < nlevel_ - 1; ++i)
        node = static_cast<internal_node_type *>(node)->upper_bound(key);
    return static_cast<leaf_node_type *>(node);
}

// left < splitkey <= right. Right is the new sibling
template <typename P> template <typename V>
int btree_type<P>::map_insert_sorted_copy_on_new(const key_type &k, const V &v, size_t keylen, unsigned hash) {
    auto leaf = get_leaf(k);
    int pos;
    bool found;
    if (!(found = leaf->lower_bound(k, &pos))) {
        leaf->insert(pos, key_copy_type()(k, keylen), hash);
        ++ nk_;
    }
    value_apply_type()(&leaf->e_[pos], !found, v);
    if (leaf->need_split()) {
	auto right = leaf->split();
        insert_internal(right->e_[0].key_, leaf, right);
    }
    return !found;
}

template <typename P>
void btree_type<P>::insert(PAIR *p) {
    auto leaf = get_leaf(p->key_);
    int pos;
    assert(!leaf->lower_bound(p->key_, &pos));  // must be new key
    leaf->insert(pos, p->key_, 0);  // do not copy key
    ++ nk_;
    leaf->e_[pos] = *p;
    if (leaf->need_split()) {
        auto right = leaf->split();
        insert_internal(right->e_[0].key_, leaf, right);
    }
}

template <typename P>
size_t btree_type<P>::size() const {
    return nk_;
}

template <typename P>
void btree_type<P>::delete_level(base_node_type *node, int level) {
    for (int i = 0; level > 1 && i <= node->nk_; ++i)
        delete_level(static_cast<internal_node_type *>(node)->e_[i].v_, level - 1);
    delete node;
}

template <typename P>
void btree_type<P>::shallow_free() {
    if (!nlevel_)
        return;
    delete_level(root_, nlevel_);
    init();
}

template <typename P>
typename btree_type<P>::iterator btree_type<P>::begin() {
    return iterator(first_leaf());
}

template <typename P>
typename btree_type<P>::iterator btree_type<P>::end() {
    return btree_type<P>::iterator(NULL);
}

template <typename P> template <typename C>
uint64_t btree_type<P>::copy(C *dst) {
    return copy_traverse(dst, false);
}

template <typename P> template <typename C>
uint64_t btree_type<P>::transfer(C *dst) {
    uint64_t n = copy_traverse(dst, true);
    shallow_free();
    return n;
}

template <typename P> template <typename C>
uint64_t btree_type<P>::copy_traverse(C *dst, bool clear_leaf) {
    assert(dst->size() == 0);
    if (!nlevel_)
	return 0;
    dst->resize(size());
    auto leaf = first_leaf();
    uint64_t n = 0;
    while (leaf) {
	memcpy(dst->at(n), leaf->e_, sizeof(PAIR) * leaf->nk_);
	n += leaf->nk_;
        if (clear_leaf)
            leaf->nk_ = 0;  // quickly delete all key/values from the leaf
        leaf = leaf->next_;
    }
    assert(n == nk_);
    return n;
}

template <typename P>
btnode_leaf<P> *btree_type<P>::first_leaf() const {
    if (!nk_)
        return NULL;
    auto node = root_;
    for (int i = 0; i < nlevel_ - 1; ++i)
	node = static_cast<internal_node_type *>(node)->e_[0].v_;
    return static_cast<leaf_node_type *>(node);
}
#endif
