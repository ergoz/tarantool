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
#include "slab_cache.h"
#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

static const uint32_t slab_magic = 0xeec0ffee;

/**
 * Find the nearest power of 2 size capable of containing
 * a chunk of the given size. Adjust for SLAB_MIN_SIZE and
 * SLAB_MAX_SIZE.
 */
static inline uint8_t
slab_order(size_t size)
{
	assert(size <= UINT32_MAX);
	if (size <= SLAB_MIN_SIZE)
		return 0;
	if (size > SLAB_MAX_SIZE)
		return SLAB_HUGE;

	return (uint8_t) (CHAR_BIT * sizeof(uint32_t) -
			  __builtin_clz((uint32_t) size - 1) -
			  SLAB_MIN_SIZE_LB);
}

/** Convert slab order to the mmap()ed size. */
static inline intptr_t
slab_size(uint8_t order)
{
	assert(order <= SLAB_ORDER_LAST);
	return 1 << (order + SLAB_MIN_SIZE_LB);
}

/**
 * Given a pointer allocated in a slab, get the handle
 * of the slab itself.
 */
struct slab *
slab_from_ptr(void *ptr, uint8_t order)
{
	assert(order <= SLAB_ORDER_LAST);
	intptr_t addr = (intptr_t) ptr;
	/** All memory mapped slabs are slab->size aligned. */
	struct slab *slab = (struct slab *) (addr & ~(slab_size(order) - 1));
	assert(slab->magic == slab_magic && slab->order == order);
	return slab;
}

static inline void
slab_check(struct slab *slab)
{
	(void) slab;
	assert(slab->magic == slab_magic);
	assert(slab->order <= SLAB_HUGE);
	assert(slab->order == SLAB_HUGE ||
	       (((intptr_t) slab & ~(slab_size(slab->order) - 1)) ==
		(intptr_t) slab &&
	       slab->size == slab_size(slab->order)));
}

/** Mark a slab as free. */
static inline void
slab_set_free(struct slab *slab)
{
	assert(slab->in_use == slab->order + 1);		/* Sanity. */
	slab->in_use = 0;
}

static inline void
slab_set_used(struct slab *slab)
{
	/* Not a boolean to have an extra assert. */
	slab->in_use = 1 + slab->order;
}

static inline bool
slab_is_free(struct slab *slab)
{
	return slab->in_use == 0;
}

static inline void
slab_poison(struct slab *slab)
{
	static const char poison_char = 'P';
	memset((char *) slab + slab_sizeof(), poison_char,
	       slab->size - slab_sizeof());
}

static inline void
slab_create(struct slab *slab, uint8_t order, size_t size)
{
	assert(order <= SLAB_HUGE);
	slab->magic = slab_magic;
	slab->order = order;
	slab->in_use = 0;
	slab->size = size;
}

static inline void
munmap_checked(void *addr, size_t length)
{
	if (munmap(addr, length)) {
		char buf[64];
		strerror_r(errno, buf, sizeof(buf));
		fprintf(stderr, "Error in munmap(): %s\n", buf);
		assert(false);
	}
}

static inline struct slab *
slab_mmap(uint8_t order)
{
	assert(order <= SLAB_ORDER_LAST);

	size_t size = slab_size(order);
	/*
	 * mmap twice the requested amount to be able to align
	 * the mapped address.
         */
	void *map = mmap(NULL, 2 * size,
			 PROT_READ | PROT_WRITE, MAP_PRIVATE |
			 MAP_ANONYMOUS, -1, 0);
	if (map == MAP_FAILED)
		return NULL;

	/* Align the mapped address around slab size. */
	size_t offset = (intptr_t) map & (size - 1);

	if (offset != 0) {
		/* Unmap unaligned prefix and postfix. */
		munmap_checked(map, size - offset);
		map += size - offset;
		munmap_checked(map + size, offset);
	} else {
		/* The address is returned aligned. */
		munmap_checked(map + size, size);
	}
	struct slab *slab = map;
	slab_create(slab, order, size);
	return slab;
}

static inline struct slab *
slab_buddy(struct slab *slab)
{
	assert(slab->order <= SLAB_ORDER_LAST);

	if (slab->order == SLAB_ORDER_LAST)
		return NULL;
	/* The buddy address has its respective bit negated. */
	return (void *) ((intptr_t) slab ^ slab_size(slab->order));

}

static inline struct slab *
slab_split(struct slab_cache *cache, struct slab *slab)
{
	assert(slab->order > 0);

	uint8_t new_order = slab->order - 1;
	size_t new_size = slab_size(new_order);

	slab_create(slab, new_order, new_size);
	struct slab *buddy = slab_buddy(slab);
	slab_create(buddy, new_order, new_size);
	rlist_add_entry(&cache->classes[new_order].free_slabs,
			buddy, next_in_class);
	return slab;
}

static inline struct slab *
slab_merge(struct slab *slab, struct slab *buddy)
{
	assert(slab_buddy(slab) == buddy);
	struct slab *merged = slab > buddy ? buddy : slab;
	/** Remove the buddy from the free list. */
	rlist_del_entry(buddy, next_in_class);
	merged->order++;
	merged->size = slab_size(merged->order);
	return merged;
}

void
slab_cache_create(struct slab_cache *cache)
{
	for (uint8_t i = 0; i <= SLAB_ORDER_LAST; i++)
		rlist_create(&cache->classes[i].free_slabs);
	rlist_create(&cache->allocated_slabs);
}

void
slab_cache_destroy(struct slab_cache *cache)
{
	struct rlist *slabs = &cache->allocated_slabs;
	/*
	 * allocated_slabs contains huge allocations and
	 * slabs of the largest order. All smaller slabs are
	 * obtained from larger slabs by splitting.
         */
	while (! rlist_empty(slabs)) {
		struct slab *slab = rlist_shift_entry(slabs,
						      struct slab,
						      next_in_cache);
		if (slab->order == SLAB_HUGE)
			free(slab);
		else {
			/*
			 * Don't trust slab->size or slab->order,
			 * it is wrong if the slab was reformatted
			 * for a smaller class.
		         */
			munmap_checked(slab, slab_size(SLAB_ORDER_LAST));
		}
	}
}

/**
 * Try to find a mmap()-ed region of the requested
 * order in the cache. On failure, mmap() a new
 * region, initialize and return it.
 * Returns a next-power-of-two(size) aligned address
 * for all sizes below SLAB_SIZE_MAX.
 */
struct slab *
slab_get(struct slab_cache *cache, size_t size)
{
	size += slab_sizeof();
	uint8_t order = slab_order(size);

	if (order == SLAB_HUGE) {
		struct slab *slab = (struct slab *) malloc(size);
		if (slab == NULL)
			return NULL;
		slab_create(slab, order, size);
		rlist_add_entry(&cache->allocated_slabs, slab,
				next_in_cache);
		return slab;
	}
	struct slab *slab;
	/* Search for the first available slab. If a slab
	 * of a bigger size is found, it can be split.
	 * If SLAB_ORDER_LAST is reached and there are no
	 * free slabs, allocate a new one.
	 */
	struct slab_class *class = &cache->classes[order];

	for ( ; rlist_empty(&class->free_slabs); class++) {
		if (class == cache->classes + SLAB_ORDER_LAST) {
			slab = slab_mmap(SLAB_ORDER_LAST);
			if (slab == NULL)
				return NULL;
			slab_poison(slab);
			rlist_add_entry(&cache->allocated_slabs, slab,
					next_in_cache);
			goto found;
		}
	}
	slab = rlist_shift_entry(&class->free_slabs, struct slab,
				 next_in_class);
found:
	while (slab->order != order)
		slab = slab_split(cache, slab);
	slab_set_used(slab);
	return slab;
}

/** Return a slab back to the slab cache. */
void
slab_put(struct slab_cache *cache, struct slab *slab)
{
	slab_check(slab);
	if (slab->order == SLAB_HUGE) {
		/*
		 * Free a huge slab right away, we have no
		 * further business to do with it.
		 */
		rlist_del_entry(slab, next_in_cache);
		free(slab);
		return;
	}
	/* An "ordered" slab is returned to the cache. */
	slab_set_free(slab);
	struct slab *buddy = slab_buddy(slab);
	/*
	 * The buddy slab could also have been split into a pair
	 * of smaller slabs, the first of which happens to be
	 * free. To not merge with a slab which is in fact
	 * partially occupied, first check that slab orders match.
	 */
	while (buddy && buddy->order == slab->order && slab_is_free(buddy)) {
		slab = slab_merge(slab, buddy);
		buddy = slab_buddy(slab);
	}
	slab_poison(slab);
	rlist_add_entry(&cache->classes[slab->order].free_slabs, slab,
			next_in_class);
}
