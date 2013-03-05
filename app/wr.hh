#ifndef WR_HH
#define WR_HH

#include "application.hh"
#include "defsplitter.hh"

struct wr : public map_group {
    wr(char *d, size_t size, int nsplit) : s_(d, size, nsplit) {}
    wr(char *f, int nsplit) : s_(f, nsplit) {}

    void map_function(split_t *ma) {
        char k[1024];
        size_t klen;
        split_word sw(ma);
        while (char *index = sw.fill(k, sizeof(k), klen))
            map_emit(k, index, klen);
    }

    bool split(split_t *ma, int ncore) {
        return s_.split(ma, ncore, " \t\n\r\0");
    }

    int key_compare(const void *k1, const void *k2) {
        return strcmp((const char *)k1, (const char *)k2);
    }
    void *key_copy(void *src, size_t s) {
        char *key = safe_malloc<char>(s + 1);
        memcpy(key, src, s);
        key[s] = 0;
        return key;
    }
    void key_free(void *k) {
        free(k);
    }
  private:
    defsplitter s_;
};

inline size_t count(xarray<keyvals_len_t> *wc_vals) {
    size_t nw = 0;
    for (size_t i = 0; i < wc_vals->size(); i++)
	nw += size_t(wc_vals->at(i)->len);
    return nw;
}

inline void print_top(xarray<keyvals_len_t> *wc_vals, size_t ndisp, size_t nw) {
    printf("\nwordreverseindex: results (TOP %zd from %zu keys, %zd words):\n",
           ndisp, wc_vals->size(), nw);
    ndisp = std::min(ndisp, wc_vals->size());
    for (size_t i = 0; i < ndisp; ++i) {
	keyvals_len_t *w = wc_vals->at(i);
	printf("%15s - %d\n", (char *) w->key_, unsigned(w->len));
    }
}

#endif
