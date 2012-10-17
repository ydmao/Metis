#ifndef XBTREE_HH_
#define XBTREE_HH_

#include "bsearch.hh"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

enum { order = 3 };

struct btnode_internal;

struct btnode_base {
    btnode_internal *parent_;
    short nk_;
    btnode_base() : parent_(NULL), nk_(0) {}
    virtual ~btnode_base() {}
};

template <typename K, typename V>
struct xpair {
    K k_;
    V v_;
};

struct keyvals_compare_functor {
    keyvals_compare_functor(key_cmp_t cmp) : cmp_(cmp) {}

    int operator()(const void *k1, const void *k2) const {
        keyvals_t *p1 = (keyvals_t *) k1;
        keyvals_t *p2 = (keyvals_t *) k2;
        return cmp_(p1->key, p2->key);
    }
  private:
    key_cmp_t cmp_;
};

struct btnode_leaf : public btnode_base {
    keyvals_t e_[2 * order + 2];
    btnode_leaf *next_;
    virtual ~btnode_leaf() {}

    btnode_leaf() : btnode_base(), next_(NULL) {
        memset(e_, 0, sizeof(e_));
    }

    btnode_leaf *split() {
        btnode_leaf *right = new btnode_leaf;
        memcpy(right->e_, &e_[order + 1], sizeof(e_[0]) * (1 + order));
        right->nk_ = order + 1;
        nk_ = order + 1;
        btnode_leaf *next = next_;
        next_ = right;
        right->next_ = next;
        return right;
    }

    int lower_bound(void *key, key_cmp_t cmp, bool *bfound) {
        keyvals_t tmp;
        tmp.key = key;
        return bsearch_eq(&tmp, e_, nk_, sizeof(e_[0]),
                          keyvals_compare_functor(cmp), bfound);
    }

    void insert(int pos, void *key, unsigned hash) {
        if (pos < nk_)
            memmove(&e_[pos + 1], &e_[pos], sizeof(e_[0]) * (nk_ - pos));
        ++ nk_;
        memset(&e_[pos], 0, sizeof(e_[pos]));
        e_[pos].key = key;
        e_[pos].hash = hash;
    }

    bool need_split() const {
        return nk_ == (order * 2 + 2);
    }
};

struct xpair_compare_functor {
    xpair_compare_functor(key_cmp_t cmp) : cmp_(cmp) {}

    int operator()(const void *k1, const void *k2) const {
        xpair<void *, btnode_base *> *p1 = (xpair<void *, btnode_base *> *) k1;
        xpair<void *, btnode_base *> *p2 = (xpair<void *, btnode_base *> *) k2;
        return cmp_(p1->k_, p2->k_);
    }
  private:
    key_cmp_t cmp_;
};

struct btnode_internal : public btnode_base {
    xpair<void *, btnode_base *> e_[2 * order + 2];
    btnode_internal() : btnode_base() {
        memset(e_, 0, sizeof(e_));
    }
    virtual ~btnode_internal() {}

    btnode_internal *split() {
        btnode_internal *nn = new btnode_internal;
        nn->nk_ = order;
        memcpy(nn->e_, &e_[order + 1], sizeof(e_[0]) * (order + 1));
        nk_ = order;
        return nn;
    }

    btnode_base *upper_bound(void *key, key_cmp_t cmp) {
        int pos = upper_bound_pos(key, cmp);
        return e_[pos].v_;
    }
    int upper_bound_pos(void *key, key_cmp_t cmp) {
        xpair<void *, btnode_base *> tmp;
        tmp.k_ = key;
        return bsearch_lar(&tmp, e_, nk_, sizeof(tmp), xpair_compare_functor(cmp));
    }
};

struct btree_type {
    static void set_key_compare(key_cmp_t kcmp);

    void init();
    /* @brief: free the tree, but not the values */
    void shallow_free();
    void insert_kvs(const keyvals_t *kvs);
    /* @brief: insert key/val pair into the tree
       @return true if it is a new key */
    int insert_kv(void *key, void *val, size_t keylen, unsigned hash);
    size_t size() const;
    uint64_t copy_kvs(keyvals_t *dst);

    /* @brief: return the number of values in the tree */
    uint64_t test_get_nvalue() {
        iterator i = begin();
        uint64_t n = 0;
        while (i != end()) {
            n += i->len;
            ++ i;
        }
        return n;
    }

    struct iterator {
        iterator() : c_(NULL), i_(0) {}
        explicit iterator(btnode_leaf *c) : c_(c), i_(0) {}
        iterator(const iterator &a) {
            c_ = a.c_;
            i_ = a.i_;
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
        keyvals_t *operator->() {
            return &c_->e_[i_];
        }
        keyvals_t &operator*() {
            return c_->e_[i_];
        }
        keyvals_t *operator&() {
            return &c_->e_[i_];
        }
      private:
        btnode_leaf *c_;
        int i_;
    };

    iterator begin();
    iterator end();

  private:
    size_t nk_;
    short nlevel_;
    btnode_base *root_;
 
    /* @brief: insert @key at position @pos into leaf node @leaf,
     * and set the value of that key to empty */
    static void insert_leaf(btnode_leaf *leaf, void *key, int pos, int keylen);
    static void delete_level(btnode_base *node, int level);

    btnode_leaf *first_leaf() const;

    /* @brief: insert (@key, @right) into left's parent */
    void insert_internal(void *key, btnode_base *left, btnode_base *right);
    btnode_leaf *get_leaf(void *key);

    static key_cmp_t keycmp_;
};

#endif
