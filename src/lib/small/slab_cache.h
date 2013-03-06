#ifndef INCLUDES_TARANTOOL_SLAB_CACHE_H
#define INCLUDES_TARANTOOL_SLAB_CACHE_H
/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include "rlist.h"

enum {
	/*
	 * Slab classes 0 to 8 are ordered, i.e. their size is
	 * a power of 2 and their address is aligned to slab size.
	 * They are obtained either using mmap(), or by splitting
	 * an mmapped() slab of higher order (buddy system).
	 */
	SLAB_ORDER_LAST = 8,
	/*
	 * The last class contains huge slabs, allocated with
	 * malloc(). This class is maintained to make life of
	 * slab_cache user easier, so that one doesn't have to
	 * worry about allocation sizes larger than SLAB_MAX_SIZE.
	 */
	SLAB_HUGE = SLAB_ORDER_LAST + 1,
	/** Binary logarithm of SLAB_MIN_SIZE. */
	SLAB_MIN_SIZE_LB = 12,
	/** Minimal size of an ordered slab, 4096 */
	SLAB_MIN_SIZE = 1 << SLAB_MIN_SIZE_LB,
	/** Maximal size of an ordered slab, 1M */
	SLAB_MAX_SIZE = SLAB_MIN_SIZE << SLAB_ORDER_LAST
};

struct slab {
	uint32_t magic;
	uint8_t order;
	/**
	 * Only used for buddy slabs. If the buddy of the current
	 * free slab is also free, both slabs are merged and
	 * a free slab of the higher order emerges.
	 */
	uint8_t in_use;
	/**
	 * Allocated size.
	 * Is different from (SLAB_MIN_SIZE << slab->order)
	 * when requested size is bigger than SLAB_MAX_SIZE
	 * (i.e. slab->order is SLAB_CLASS_LAST).
	 */
	size_t size;
	/** Next slab in class->free_slabs list, if this slab is free. */
	struct rlist next_in_class;
	/*
	 * Next slab in the list of allocated slabs. Unused
	 * if a slab has a buddy. Sic: if a slab is not allocated
	 * but is made by a split of a larger (allocated) slab,
	 * this member got to be left intact.
	 */
	struct rlist next_in_cache;
};

struct slab_class {
	struct rlist free_slabs;
};

struct slab_cache {
	/**
	 * Slabs are ordered by size, which is
	 * a multiple of two. classes[0] contains
	 * slabs of size SLAB_MIN_SIZE (order 0).
	 * classes[1] contains slabs of 2 * SLAB_MIN_SIZE,
	 * and so on.
         */
	struct slab_class classes[SLAB_ORDER_LAST + 1];
	/** All allocated slabs used in the cache. */
	struct rlist allocated_slabs;
};

void
slab_cache_create(struct slab_cache *cache);

void
slab_cache_destroy(struct slab_cache *cache);

struct slab *
slab_get(struct slab_cache *cache, size_t size);

void
slab_put(struct slab_cache *cache, struct slab *slab);

struct slab *
slab_from_ptr(void *ptr, uint8_t order);

/** Align a size. Alignment must be a power of 2 */
static inline size_t
slab_align(size_t size, size_t alignment)
{
	return (size + alignment - 1) & ~(alignment - 1);
}

/* Aligned size of slab meta. */
static inline size_t
slab_sizeof()
{
	return slab_align(sizeof(struct slab), sizeof(intptr_t));
}

#endif /* INCLUDES_TARANTOOL_SLAB_CACHE_H */
