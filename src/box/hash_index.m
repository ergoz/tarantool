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
#include <time.h>

#include "hash_index.h"
#include "say.h"
#include "tuple.h"
#include "exception.h"
#include "space.h"
#include "assoc.h"
#include "errinj.h"

static struct index_traits hash_index_traits = {
	.allows_partial_key = false,
};

@class HashIndex;
static inline u32
hash_hash(HashIndex *self, const void *node)
{
	return node_hash(&self->fmtdef, node);
}

static inline bool
hash_eq(HashIndex *self, const void *node_a, const void *node_b)
{
	return (node_compare(&self->fmtdef, node_a, node_b) == 0);
}

/*
 * Called only if update operation is invoked (put, delete, replace, remove)
 * and hash_eq returned true
 */
static inline bool
hash_eq_exact(HashIndex *self, const void *node_a, const void *node_b)
{
	if (self->key_def->is_unique)
		return true;

	return (node_compare_addr(&self->fmtdef, node_a, node_b) == 0);
}

#define mh_name _index
#define mh_node_t void *
#define mh_int_t u32
#define mh_hash_arg_t HashIndex *
#define mh_hash(a, arg) (hash_hash(arg, *(a)))
#define mh_eq_arg_t HashIndex *
#define mh_eq(a, b, arg) (hash_eq(arg, *(a), *(b)))
#define mh_eq_exact(a, b, arg) (hash_eq_exact(arg, *(a), *(b)))
#define TEST
#define MH_SOURCE 1
#include <mhash.h>
#undef TEST

/* {{{ HashIndex Iterators ****************************************/

struct hash_iterator {
	struct iterator base; /* Must be the first member. */
	HashIndex *index;
	struct mh_index_iterator it;
	void *key_node;
	size_t key_node_size;
	mh_int_t h_pos;
};

void
hash_iterator_free(struct iterator *ptr)
{
	assert(ptr->free == hash_iterator_free);
	struct hash_iterator *it = (struct hash_iterator *) ptr;
	index_realloc(it->key_node, 0, "iterator_key");
}

static struct tuple *
hash_iterator_next(struct iterator *ptr)
{
	assert(ptr->free == hash_iterator_free);
	struct hash_iterator *it = (struct hash_iterator *) ptr;

	mh_int_t k = mh_index_iterator_next(&it->it);

	if (k != mh_end(it->it.h)) {
		void *node = *mh_index_node(it->it.h, k);
		return node_unfold(&it->index->fmtdef, node);
	}

	return NULL;
}

struct tuple *
hash_iterator_test_ge(struct iterator *ptr)
{
	assert(ptr->free == hash_iterator_free);
	struct hash_iterator *it = (struct hash_iterator *) ptr;

	while (it->h_pos < mh_end(it->it.h)) {
		if (mh_exist(it->it.h, it->h_pos)) {
			void *node = *mh_index_node(it->it.h, it->h_pos++);
			return node_unfold(&it->index->fmtdef, node);
		}
		it->h_pos++;
	}
	return NULL;
}

static struct tuple *
hash_iterator_test_next(struct iterator *it __attribute__((unused)))
{
	return NULL;
}

static struct tuple *
hash_iterator_test_eq(struct iterator *it)
{
	it->next = hash_iterator_test_next;
	return hash_iterator_test_ge(it);
}


/* }}} */

/* {{{ HashIndex -- base class for all hashes. ********************/

@implementation HashIndex

+ (struct index_traits *) traits
{
	return &hash_index_traits;
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];

	if (self == NULL)
		return NULL;

	node_format_init(&fmtdef, space_arg, key_def_arg);
	tmp_node_size = 0;
	tmp_node = NULL;

	hash = mh_index_init();

	return self;
}

- (void) free
{
	mh_int_t k;
	mh_foreach(hash, k) {
		void *node = *mh_index_node(hash, k);
		index_realloc(node, 0, "node");
	}

	node_format_deinit(&fmtdef);
	mh_index_destroy(hash);
	index_realloc(tmp_node, 0, "key");
	tmp_node_size = 0;
	[super free];
}

- (void) reserve: (u32) n_tuples
{
	mh_index_reserve(hash, n_tuples, self, self);
}

- (void) beginBuild
{
}

- (void) buildNext: (struct tuple *)tuple
{
	[self replace: NULL :tuple :DUP_INSERT];
}

- (void) endBuild
{
}

- (void) build: (Index *) pk
{
	u32 n_tuples = [pk size];

	if (n_tuples == 0)
		return;

	[self reserve: n_tuples];

	say_info("Adding %"PRIu32 " keys to HASH index %"
		 PRIu32 "...", n_tuples, index_n(self));

	struct iterator *it = pk->position;
	struct tuple *tuple;
	[pk initIterator: it :ITER_ALL :NULL :0];

	while ((tuple = it->next(it)))
	      [self replace: NULL :tuple :DUP_INSERT];
}

- (struct tuple *) min
{
	tnt_raise(ClientError, :ER_UNSUPPORTED, "Hash index", "min()");
	return NULL;
}

- (struct tuple *) max
{
	tnt_raise(ClientError, :ER_UNSUPPORTED, "Hash index", "max()");
	return NULL;
}


- (size_t) size
{
	return mh_size(hash);
}

- (struct tuple *) findByKey: (void *) key :(int) part_count
{
	assert(key_def->is_unique);
	check_key_parts(key_def, part_count, false);

	node_foldkey(&fmtdef, &tmp_node, &tmp_node_size, key, part_count);

	mh_int_t k = mh_index_get(hash, (const void **) &tmp_node, self, self);
	if (k == mh_end(hash))
		return NULL;

	return node_unfold(&fmtdef, *mh_index_node(hash, k));
}

- (struct tuple *) replace: (struct tuple *) old_tuple
			  :(struct tuple *) new_tuple
			  :(enum dup_replace_mode) mode
{
	say_warn("replace: %p %p", old_tuple, new_tuple);

	uint32_t errcode;
	if (new_tuple) {
		void *dup_node = NULL;
		void **p_dup_node = &dup_node;

		void *new_node = NULL;
		size_t new_node_size = node_size(&fmtdef);
		new_node = index_realloc(NULL, new_node_size, "node new");

		node_fold(&fmtdef, new_node, new_tuple);

		mh_int_t pos = mh_index_replace(hash,
						(const void **) &new_node,
						&p_dup_node,
						self, self);
		ERROR_INJECT(ERRINJ_INDEX_ALLOC,
		{
			mh_index_remove(hash, (const void **) &new_node, NULL,
					self, self);
			pos = mh_end(hash);
		});

		if (pos == mh_end(hash)) {
			index_realloc(new_node, 0, "node new");

			tnt_raise(LoggedError, :ER_MEMORY_ISSUE, (ssize_t) pos,
				  "int hash", "key");
		}

		struct tuple *dup_tuple = node_unfold(&fmtdef, dup_node);
		errcode = replace_check_dup(old_tuple, dup_tuple, mode);

		if (errcode) {
			mh_index_remove(hash, (const void **) &new_node, NULL,
					self, self);
			index_realloc(new_node, 0, "node new");

			if (dup_node) {
				pos = mh_index_replace(hash, (const void **) &dup_node,
							NULL, self, self);
				if (pos == mh_end(hash)) {
					panic("Failed to allocate memory in "
					      "recover of int hash");
				}
			}
			tnt_raise(ClientError, :errcode, index_n(self));
		}

		if (dup_tuple) {
			index_realloc(dup_node, 0, "node dup");

			return dup_tuple;
		}
	}

	if (old_tuple) {
		void *old_node = NULL;
		size_t old_node_size = node_size(&fmtdef);
		old_node = alloca(old_node_size);

		node_fold(&fmtdef, old_node, old_tuple);

		void *dup_node = NULL;
		void **p_dup_node = &dup_node;

		mh_index_remove(hash, (const void **) &old_node, &p_dup_node,
				self, self);

		if (dup_node != NULL) {
			index_realloc(dup_node, 0, "index remove");
		}
	}

	return old_tuple;
}


- (struct iterator *) allocIterator
{
	struct hash_iterator *it = malloc(sizeof(*it));
	if (it == NULL) {
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, sizeof(*it),
			  "hash_index", "iterator");
	}

	memset(it, 0, sizeof(*it));
	it->base.next = hash_iterator_next;
	it->base.free = hash_iterator_free;

	return (struct iterator *) it;
}

- (void) initIterator: (struct iterator *) ptr :(enum iterator_type) type
                        :(void *) key :(int) part_count
{
	say_warn("initIterator format=%d part_count=%d",
		 fmtdef.format, part_count);

	assert(ptr->free == hash_iterator_free);
	struct hash_iterator *it = (struct hash_iterator *) ptr;

	if (part_count != 0) {
		check_key_parts(key_def, part_count,
				traits->allows_partial_key);

		node_foldkey(&fmtdef, &it->key_node, &it->key_node_size,
			      key, part_count);
	} else {
		key = NULL;
	}

	switch (type) {
	case ITER_GE:
		if (key != NULL) {
			mh_index_iterator_init_ge(hash, &it->it,
				(const void **) &it->key_node, self, self);
			break;
		}
		/* Fall through. */
	case ITER_ALL:
		mh_index_iterator_init_ge(hash, &it->it, NULL, self, self);
		break;
	case ITER_EQ:
		check_key_parts(key_def, part_count,
				traits->allows_partial_key);
#if defined(NEN_CODE)
		mh_index_iterator_init_eq(hash, &it->it,
			(const void **) &it->key_node, self, self);
#else
		it->h_pos = mh_index_get(hash, (const void **) &it->key_node, self, self);
		it->it.h = hash;
		it->base.next = hash_iterator_test_eq;
#endif
		break;
	default:
		tnt_raise(ClientError, :ER_UNSUPPORTED,
			  "Hash index", "requested iterator type");
	}

	it->index = self;
}


@end

