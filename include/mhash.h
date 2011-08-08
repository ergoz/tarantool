/*
 * Copyright (C) 2011 Mail.RU
 * Copyright (C) 2011 Yuriy Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/* The MIT License

   Copyright (c) 2008, by Attractive Chaos <attractivechaos@aol.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#include <stdlib.h>
#include <stdint.h>
#include <memory.h>
#include <say.h>

#define mh_cat(a, b) mh##a##_##b
#define mh_ecat(a, b) mh_cat(a, b)
#define _mh(x) mh_ecat(mh_name, x)

#define mh_unlikely(x)  __builtin_expect((x),0)

#ifndef __ac_HASH_PRIME_SIZE
#define __ac_HASH_PRIME_SIZE 31
static const uint32_t __ac_prime_list[__ac_HASH_PRIME_SIZE] = {
	3ul,		11ul,		23ul,		53ul,
	97ul,		193ul,		389ul,		769ul,
	1543ul,		3079ul,		6151ul,		12289ul,
	24593ul,	49157ul,	98317ul,	196613ul,
	393241ul,	786433ul,	1572869ul,	3145739ul,
	6291469ul,	12582917ul,	25165843ul,	50331653ul,
	100663319ul,	201326611ul,	402653189ul,	805306457ul,
	1610612741ul,	3221225473ul,	4294967291ul
};
#endif

struct _mh(pair) {
	mh_key_t key;
	mh_val_t val;
};

struct _mh(t) {
	struct _mh(pair) *p;
	char *b; /* TODO: bitset */
	uint32_t n_buckets, n_occupied, size, upper_bound;
	uint32_t prime;

	u32 resize_cnt;
	uint32_t resizing, batch;
	struct _mh(t) *shadow;
};

#define mh_free(h, i)		({ (h)->b[(i)] == 0;	})
#define mh_exist(h, i)		({ (h)->b[(i)] == 1;	})
#define mh_dirty(h, i)		({ (h)->b[(i)] == 2;	})

#define mh_setexist(h, i)	({ (h)->b[(i)] = 1;	})
#define mh_setdirty(h, i)	({ (h)->b[(i)] = 2;	})

#define mh_value(h, i)		({ (h)->p[(i)].val;	})
#define mh_size(h)		({ (h)->size; 		})
#define mh_end(h)		({ (h)->n_buckets;	})


#define slot(h, i, key_v)						\
({									\
	mh_free(h, i) || (mh_eq(h->p[i].key, key_v) && !mh_dirty(h, i)); \
})

#define slot_and_dirty(h, i, key_v)					\
({									\
	if (dirty == -1 && mh_dirty(h, i))				\
		dirty = i;						\
	slot(h, i, key_v);						\
})


#define place(h, pred, key_v)						\
({									\
	uint32_t inc, k, i, last;						\
	k = mh_hash(key_v);						\
	i = k % h->n_buckets;						\
	inc = 1 + k % (h->n_buckets - 1);				\
	last = i;							\
	for (;;) {							\
		if (pred(h, i, key_v))					\
			break;						\
		i += inc;						\
		if (i >= h->n_buckets)					\
			i -= h->n_buckets;				\
		if (mh_unlikely(i == last)) {				\
			i = h->n_buckets;				\
			break;						\
		}							\
	}								\
	i;								\
})

static inline struct _mh(t) *
_mh(init)()
{
	struct _mh(t) *h = calloc(1, sizeof(*h));
	h->shadow = calloc(1, sizeof(*h));
	h->n_buckets = 3;
	h->p = calloc(h->n_buckets, sizeof(struct _mh(pair)));
	h->b = calloc(h->n_buckets, 1);
	h->upper_bound = h->n_buckets * 0.7;
	return h;
}

static inline void
_mh(clear)(struct _mh(t) *h)
{
	free(h->p);
	h->n_buckets = 3;
	h->p = calloc(h->n_buckets, sizeof(struct _mh(pair)));
	h->upper_bound = h->n_buckets * 0.7;
}

static inline void
_mh(destroy)(struct _mh(t) *h)
{
	free(h->p);
	free(h);
}

static inline uint32_t
_mh(get)(struct _mh(t) *h, mh_key_t key)
{
	uint32_t i = place(h, slot, key);
	if (mh_free(h, i))
		return i = h->n_buckets;
	return i;
}

#if 0
static inline void
_mh(dump)(struct _mh(t) *h)
{
	printf("slots:\n");
	int k = 0;
	for(int i = 0; i < h->n_buckets; i++) {
		if (mh_dirty(h, i))
			printf("   [%i] dirty\n", i);
		else if (mh_exist(h, i)) {
			printf("   [%i] -> %i\n", i, h->p[i].key);
			k++;
		}
	}
	printf("end(%i)\n", k);
}
#endif

static inline
void _mh(resize)(struct _mh(t) *h)
{
	TIME_THIS(mh_resize);
	struct _mh(t) *s = h->shadow;
	uint32_t batch = h->batch;
	for (uint32_t o = h->resizing; o < h->n_buckets; o++) {
		if (batch-- == 0) {
			h->resizing = o;
			return;
		}
		if (!mh_exist(h, o))
			continue;
		uint32_t n = place(s, slot, h->p[o].key);
		s->p[n] = h->p[o];
		mh_setexist(s, n);
		s->n_occupied++;
	}

	free(h->p);
	free(h->b);
	s->size = h->size;
	memcpy(h, s, sizeof(*h));
	h->resize_cnt++;
	say_info("END resize of %p n_occupied:%i size:%i n_buckets:%i",
		 h, (int)h->n_occupied, (int)h->size, (int)h->n_buckets);

}

static inline void
_mh(start_resize)(struct _mh(t) *h, uint32_t buckets)
{
	if (h->resizing)
		return;
	say_info("START resize of %p n_occupied:%i size:%i n_buckets:%i",
		 h, (int)h->n_occupied, (int)h->size, (int)h->n_buckets);

	struct _mh(t) *s = h->shadow;
	if (buckets < h->n_buckets)
		buckets = h->n_buckets;
	if (buckets < h->size * 2) {
		for (int k = h->prime; k < __ac_HASH_PRIME_SIZE; k++)
			if (__ac_prime_list[k] > h->size) {
				h->prime = k + 1;
				break;
			}
	}
	h->batch = h->n_buckets / (256 * 1024);
	if (h->batch < 256) /* minimum batch is 3 */
		h->batch = 256;
	memcpy(s, h, sizeof(*h));
	s->n_buckets = __ac_prime_list[h->prime];
	s->upper_bound = s->n_buckets * 0.7;
	s->n_occupied = 0;
	s->p = malloc(s->n_buckets * sizeof(struct _mh(pair)));
	s->b = calloc(s->n_buckets, 1);
	_mh(resize)(h);
}

static inline uint32_t
_mh(put)(struct _mh(t) *h, mh_key_t key, mh_val_t val, int *ret)
{
	if (mh_unlikely(h->n_occupied >= h->upper_bound || h->resizing > 0)) {
		if (h->resizing > 0) {
			_mh(resize)(h);
			struct _mh(t) *s = h->shadow;
			uint32_t y = place(s, slot, key);
			s->p[y].key = key;
			s->p[y].val = val;
			mh_setexist(s, y);
		} else {
			_mh(start_resize)(h, 0);
		}
	}
	uint32_t dirty = -1;
	uint32_t x = place(h, slot_and_dirty, key);
	if (mh_free(h, x) || mh_dirty(h, x)) {
		if (dirty != -1) {
			x = dirty;
		} else {
			if (!mh_dirty(h, x))
				h->n_occupied++;
		}
		h->p[x].key = key;
		h->p[x].val = val;
		mh_setexist(h, x);
		h->size++;
		if (ret)
			*ret = 1;
	} else {
		h->p[x].val = val;
		if (ret)
			*ret = 0;
	}
	return x;
}

static inline void
_mh(del)(struct _mh(t) *h, uint32_t x)
{
	if (x != h->n_buckets && !mh_free(h, x)) {
		mh_setdirty(h, x);
		h->size--;

		if (mh_unlikely(h->resizing)) {
			struct _mh(t) *s = h->shadow;
			uint32_t y = place(s, slot, h->p[x].key);
			if (y != s->n_buckets && !mh_free(s, y))
				mh_setdirty(s, y);
			_mh(resize)(h);
		}
	}
}

#ifndef mh_stat
#define mh_stat(buf, h) ({					    \
                tbuf_printf(buf, "  n_buckets: %"PRIu32 CRLF        \
                            "  n_occupied: %"PRIu32 CRLF            \
                            "  size: %"PRIu32 CRLF                  \
                            "  resize_cnt: %"PRIu32 CRLF	    \
			    "  resizing: %"PRIu32 CRLF,		    \
                            h->n_buckets,                           \
                            h->n_occupied,                          \
                            h->size,                                \
                            h->resize_cnt,			    \
			    h->resizing);			    \
			})
#endif

#undef mh_dirty
#undef mh_free
#undef mh_place
#undef mh_setdirty
#undef mh_setexist
#undef mh_setvalue
#undef mh_unlikely
#undef slot
#undef slot_and_dirty
#undef mh_cat
#undef mh_ecat
#undef _mh
#undef mh_key_t
#undef mh_val_t
#undef mh_name
#undef mh_hash
#undef mh_eq
