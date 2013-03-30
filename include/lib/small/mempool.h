#ifndef INCLUDES_TARANTOOL_SMALL_MEMPOOL_H
#define INCLUDES_TARANTOOL_SMALL_MEMPOOL_H
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
#include <stddef.h>
#include <stdbool.h>
#include <inttypes.h>
#include "lib/small/slab_cache.h"
#define RB_COMPACT 1
#include "third_party/rb.h"

/**
 * Pool allocator.
 *
 * Good for allocating tons of small objects of the same
 * size. Stores all objects in order-of-page-size memory blocks.
 * Each object can be freed if necessary. There is (practically)
 * no allocation overhead. Internal fragmentation may occur if
 * lots of objects are allocated, and then many of them are freed
 * in reverse-to-allocation order.
 *
 * Under the hood, uses a slab cache of mmap()-allocated pages.
 * Pages of the slab cache are never released back to the
 * operating system.
 *
 * Thread-safety
 * -------------
 * Calls to alloc() and free() on the same mempool instance must
 * be externally synchronized. Use of different instances in
 * different threads is thread-safe (but they must also
 * be based on distinct slab caches).
 *
 * Exception-safety
 * ----------------
 * The only type of failure which can occur is a failure to
 * allocate memory. In case of such error, an exception
 * (ClientError, ER_OUT_OF_RESOURCES) is raised. _nothrow()
 * version of mempool_alloc() returns NULL rather than raises
 * an error in case of failure.
 */

typedef unsigned long mbitmap_t;

enum {
	MEMPOOL_MAP_SIZEOF = sizeof(mbitmap_t),
	MEMPOOL_MAP_BIT = MEMPOOL_MAP_SIZEOF * CHAR_BIT,
	/** Mempool slab has to contain at least this many
	 * objects, to ensure that overhead of bitmaps
	 * for free/used objects is small.
	 */
	MEMPOOL_OBJ_MIN = 2 * MEMPOOL_MAP_BIT
};

/** mslab - a standard slab formatted to store mempool objects. */
struct mslab {
	struct slab slab;
	/** Index of the First bitmap element which has a Free slot. */
	uint32_t ffi;
	/** Number of available slots in the slab. */
	uint32_t nfree;
	/** Used if member of free_slabs tree. */
	rb_node(struct mslab) node;
	/**
	 * A bitmap for free used/objects in the slab.
	 * A bitmap rather than a free list is used since:
	 * - this tends to keep allocations close to the
	 *   beginning of the slab, which is better for
	 *   cache locality
	 * - it makes it possible to iterate over all
	 *   objects in a slab.
	 */
	mbitmap_t map[];
};

typedef rb_tree(struct mslab) mslab_tree_t;

/** A memory pool. */
struct mempool
{
	/** The source of empty slabs. */
	struct slab_cache *cache;
	/** All slabs. */
	struct slab_list slabs;
	/**
	 * Slabs with some amount of free space available are put
	 * into this red-black tree, which is sorted by slab
	 * address. A (partially) free slab with smallest address
	 * is chosen for allocation. This reduces internal memory
	 * fragmentation across many slabs.
	 */
	mslab_tree_t free_slabs;
	/**
	 * A completely empty slab which is not freed only to
	 * avoid the overhead of slab_cache oscillation around
	 * a single element allocation.
	 */
	struct mslab *spare;
	/**
	 * The size of an individual object. All objects
	 * allocated on the pool have the same size.
	 */
	uint32_t objsize;
	/**
	 * Mempool slabs are ordered. The order is calculated
	 * when the pool is initialized.
	 */
	uint8_t slab_order;
	/** How many objects can fit in a slab. */
	uint32_t objcount;
	/**
	 * How many bytes of the slab are reserved for
	 * slab map.
	 */
	uint32_t mapsize;
};

/** @todo: struct mempool_iterator */

/**
 * Initialize a mempool. Tell the pool the size of objects
 * it will contain.
 *
 * objsize must be >= sizeof(mbitmap_t)
 * If allocated objects must be aligned, then objsize must
 * be aligned. The start of free area in a slab is always
 * uint64_t aligned.
 *
 * @sa mempool_destroy()
 */
void
mempool_create(struct mempool *pool, struct slab_cache *cache,
	       uint32_t objsize);
/**
 * Free the memory pool and release all cached memory blocks.
 * @sa mempool_create()
 */
void
mempool_destroy(struct mempool *pool);

/** Allocate an object. */
void *
mempool_alloc_nothrow(struct mempool *pool);

/**
 * Free a single object.
 * @pre the object is allocated in this pool.
 */
void
mempool_free(struct mempool *pool, void *ptr);

/** How much memory is used by this pool. */
static inline size_t
mempool_used(struct mempool *pool)
{
	return pool->slabs.stats.used;
}


/** How much memory is held by this pool. */
static inline size_t
mempool_total(struct mempool *pool)
{
	return pool->slabs.stats.total;
}

#if defined(__OBJC__)
#include "exception.h"

static inline void *
mempool_alloc(struct mempool *pool)
{

	void *ptr = mempool_alloc_nothrow(pool);
	if (ptr == NULL)
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
			  pool->objsize, "mempool", "new slab");
	return ptr;
}

static inline void *
mempool_calloc(struct mempool *pool)
{
	return memset(mempool_alloc(pool), 0, pool->objsize);
}
#endif /* __OBJC__ */

#endif /* INCLUDES_TARANTOOL_SMALL_MEMPOOL_H */
