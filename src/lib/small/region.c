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
#include "region.h"

void *
region_alloc_slow(struct region *region, size_t size)
{
	/* The new slab must have at least this many bytes available. */
	size_t slab_min_size = size + rslab_sizeof() - slab_sizeof();

	struct rslab *slab;
	slab = (struct rslab *) slab_get(region->slab_cache, slab_min_size);
	if (slab == NULL)
		return NULL;
	slab->used = size;
	/*
	 * Sic: add the new slab to the beginning of the
	 * region, even if it is full, otherwise,
	 * region_truncate() won't work.
	 */
	rlist_add_entry(&region->slabs, slab,
			next_in_region);
	region->used += size;
	return rslab_data(slab);
}

void
region_free(struct region *region)
{
	struct rslab *slab, *tmp;
	rlist_foreach_entry_safe(slab, &region->slabs, next_in_region, tmp)
		slab_put(region->slab_cache, &slab->slab);

	rlist_create(&region->slabs);
	region->used = 0;
}

/**
 * Release all memory down to new_size; new_size has to be previously
 * obtained by calling region_used().
 */
void
region_truncate(struct region *region, size_t new_size)
{
	assert(new_size <= region->used);

	ssize_t cut_size = region->used - new_size;
	while (! rlist_empty(&region->slabs)) {
		struct rslab *slab = rlist_first_entry(&region->slabs,
						       struct rslab,
						       next_in_region);
		if (slab->used > cut_size) {
			/* This is the last slab to trim. */
			slab->used -= cut_size;
			cut_size = 0;
			break;
		}
		cut_size -= slab->used;
		/* Remove the entire slab. */
		rlist_del_entry(slab, next_in_region);
		slab_put(region->slab_cache, &slab->slab);
	}
	assert(cut_size == 0);
	region->used = new_size;
}

