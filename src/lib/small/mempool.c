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
#include "lib/small/mempool.h"
#include <stdlib.h>
#include <string.h>
#include "lib/small/slab_cache.h"

static inline int
mslab_cmp(struct mslab *lhs, struct mslab *rhs)
{
	/* pointer arithmetics may overflow int * range. */
	return lhs > rhs ? 1 : (lhs < rhs ? -1 : 0);
}


rb_proto(, mslab_tree_, mslab_tree_t, struct mslab)

rb_gen(, mslab_tree_, mslab_tree_t, struct mslab, node, mslab_cmp)


static inline size_t
mslab_sizeof()
{
	return slab_size_align(sizeof(struct mslab), sizeof(intptr_t));
}

static inline void
mslab_create(struct mslab *slab, uint32_t objcount, uint32_t mapsize)
{
	slab->ffi = 0;
	slab->nfree = objcount;
	/* A bit is set if a slot is free. */
	memset(slab->map, 0xFF, sizeof(slab->map[0]) * mapsize);
}

/** Beginning of object data in the slab. */
void *
mslab_offset(struct mempool *pool, struct mslab *slab)
{
	return (char *) slab + mslab_sizeof() +
		MEMPOOL_MAP_SIZEOF * pool->mapsize;
}

/** Pointer to an object from object index. */
static inline void *
mslab_obj(struct mempool *pool, struct mslab *slab, uint32_t idx)
{
	return mslab_offset(pool, slab) + idx * pool->objsize;
}

/** Object index from pointer to object */
static inline uint32_t
mslab_idx(struct mempool *pool, struct mslab *slab, void *ptr)
{
	/*
	 * @todo: consider optimizing this division with
	 * multiply-shift method described in Hacker's Delight,
	 * p. 187.
	 */
	return ((uint32_t)(ptr - mslab_offset(pool, slab)))/pool->objsize;
}

void *
mslab_alloc(struct mempool *pool, struct mslab *slab)
{
	assert(slab->nfree);
	uint32_t idx = __builtin_ffsl(slab->map[slab->ffi]);
	while (idx == 0) {
		if (slab->ffi == pool->mapsize - 1) {
			/*
			 * mslab_alloc() shouldn't be called
			 * on a full slab.
			 */
			assert(false);
			return NULL;
		}
		slab->ffi++;
		idx = __builtin_ffsl(slab->map[slab->ffi]);
	}
	/*
	 * find-first-set returns bit index starting from 1,
	 * or 0 if no bit is set. Rebase the index to offset 0.
	 */
	idx--;
	/* Mark the position as occupied. */
	slab->map[slab->ffi] ^= ((mbitmap_t) 1) << idx;
	/* If the slab is full, remove it from the rb tree. */
	if (--slab->nfree == 0)
		mslab_tree_remove(&pool->free_slabs, slab);
	/* Return the pointer at the free slot */
	return mslab_obj(pool, slab, idx + slab->ffi * MEMPOOL_MAP_BIT);
}

void
mslab_free(struct mempool *pool, struct mslab *slab, void *ptr)
{
	uint32_t idx = mslab_idx(pool, slab, ptr);
	uint32_t bit_no = idx & (MEMPOOL_MAP_BIT-1);
	idx /= MEMPOOL_MAP_BIT;
	slab->map[idx] |= ((mbitmap_t) 1) << bit_no;
	slab->nfree++;
	if (idx < slab->ffi)
		slab->ffi = idx;
	if (slab->nfree == 1) {
		/**
		 * Add this slab to the rbtree which contains partially
		 * populated slabs.
		 */
		mslab_tree_insert(&pool->free_slabs, slab);
	} else if (slab->nfree == pool->objcount) {
		/** Free the slab. */
		mslab_tree_remove(&pool->free_slabs, slab);
		if (pool->spare > slab) {
			slab_list_del(&pool->slabs, &pool->spare->slab,
				      next_in_list);
			slab_put(pool->cache, &pool->spare->slab);
			pool->spare = slab;
		 } else if (pool->spare) {
			 slab_list_del(&pool->slabs, &slab->slab,
				       next_in_list);
			 slab_put(pool->cache, &slab->slab);
		 } else {
			 pool->spare = slab;
		 }
	}
}

void
mempool_create(struct mempool *pool, struct slab_cache *cache,
	       uint32_t objsize)
{
	pool->cache = cache;
	slab_list_create(&pool->slabs);
	mslab_tree_new(&pool->free_slabs);
	pool->spare = NULL;
	pool->objsize = objsize;
	/* Keep size-induced internal fragmentation within limits. */
	size_t slab_size_min = objsize * MEMPOOL_OBJ_MIN;
	/*
	 * Calculate the amount of usable space in a slab.
	 * @note: this asserts that slab_size_min is less than
	 * SLAB_ORDER_MAX.
	 */
	pool->slab_order = slab_order(slab_size_min);
	assert(pool->slab_order <= SLAB_ORDER_LAST);
	/* Account for slab meta. */
	size_t slab_size = slab_order_size(pool->slab_order) -
		mslab_sizeof();
	/* Calculate how many objects will actually fit in a slab. */
	/*
	 * We have 'slab_size' bytes for X objects and
	 * X / 8 bits in free/used array.
	 *
	 * Therefore the formula for objcount is:
	 *
	 * X * objsize + X/8 = slab_size
	 * X = (8 * slab_size)/(8 * objsize + 1)
	 */
	size_t objcount = (CHAR_BIT * slab_size)/(CHAR_BIT * objsize + 1);
	/* How many elements of slab->map can map objcount. */
	size_t mapsize = (objcount + MEMPOOL_MAP_BIT - 1)/MEMPOOL_MAP_BIT;
	/* Adjust the result of integer division, which may be too large. */
	while (objcount * objsize + mapsize * MEMPOOL_MAP_SIZEOF > slab_size) {
		objcount--;
		mapsize = (objcount + MEMPOOL_MAP_BIT - 1)/MEMPOOL_MAP_BIT;
	}
	assert(mapsize * MEMPOOL_MAP_BIT >= objcount);
	/* The wasted memory should be under objsize */
	assert(slab_size - objcount * objsize -
	       mapsize * MEMPOOL_MAP_SIZEOF < objsize ||
	       mapsize * MEMPOOL_MAP_BIT == objcount);
	pool->objcount = objcount;
	pool->mapsize = mapsize;
}

void
mempool_destroy(struct mempool *pool)
{
	struct slab *slab, *tmp;
	rlist_foreach_entry_safe(slab, &pool->slabs.slabs,
				 next_in_list, tmp)
		slab_put(pool->cache, slab);
}

void *
mempool_alloc_nothrow(struct mempool *pool)
{
	struct mslab *slab = mslab_tree_first(&pool->free_slabs);
	if (slab == NULL) {
		if (pool->spare == NULL) {
			slab = (struct mslab *)
				slab_get_order(pool->cache, pool->slab_order);
			if (slab == NULL)
				return NULL;
			mslab_create(slab, pool->objcount, pool->mapsize);
			slab_list_add(&pool->slabs, &slab->slab,
				      next_in_list);
		} else {
			slab = pool->spare;
			pool->spare = NULL;
		}
		mslab_tree_insert(&pool->free_slabs, slab);
	}
	pool->slabs.stats.used += pool->objsize;
	return mslab_alloc(pool, slab);
}

void
mempool_free(struct mempool *pool, void *obj)
{
	struct mslab *slab = (struct mslab *)
		slab_from_ptr(obj, pool->slab_order);
	pool->slabs.stats.used -= pool->objsize;
	mslab_free(pool, slab, obj);
}
