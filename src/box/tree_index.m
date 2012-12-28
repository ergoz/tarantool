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
#include "tree_index.h"
#include "tuple.h"
#include "space.h"
#include "exception.h"
#include "errinj.h"
#include <pickle.h>

#include <third_party/sptree.h>

/* {{{ Utilities. *************************************************/

static struct index_traits tree_index_traits = {
	.allows_partial_key = true,
};

static inline int
tree_node_compare(const void *node_a, const void *node_b,
		  TreeIndex *self)
{
	assert(node_a != NULL && node_b != NULL);

	return node_compare(&self->fmtdef, node_a, node_b);
}

static inline int
tree_node_compare_exact(const void *node_a, const void *node_b,
			TreeIndex *self)
{
	assert(node_a != NULL && node_b != NULL);

	say_warn("compare_exact>: %p %p self=%p", node_a, node_b, self);
	int r = node_compare(&self->fmtdef, node_a, node_b);
	if (r != 0 || self->key_def->is_unique)
		return r;

	return node_compare_addr(&self->fmtdef, node_a, node_b);
}

#define sptree_realloc(ptr, size) index_realloc(ptr, size, #ptr)

/*
 * Instantiate sptree definitions
 */

@class TreeIndex;
SPTREE_DEF(index, void, tree_node_compare, tree_node_compare_exact,
	   TreeIndex *, sptree_realloc);

/* {{{ TreeIndex Iterators ****************************************/

struct tree_iterator {
	struct iterator base;
	TreeIndex *index;
	struct sptree_index_iterator *iter;
	void *key_node;
	size_t key_node_size;
};

static void
tree_iterator_free(struct iterator *iterator);

static inline struct tree_iterator *
tree_iterator(struct iterator *it)
{
	assert(it->free == tree_iterator_free);
	return (struct tree_iterator *) it;
}

static void
tree_iterator_free(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	if (it->iter)
		sptree_index_iterator_free(it->iter);
	index_realloc(it, 0, "iterator");
}

static struct tuple *
tree_iterator_ge(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	const void *node = sptree_index_iterator_next(it->iter);
	return node_unfold(&it->index->fmtdef, node);
}

static struct tuple *
tree_iterator_le(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	const void *node = sptree_index_iterator_reverse_next(it->iter);
	return node_unfold(&it->index->fmtdef, node);
}

static struct tuple *
tree_iterator_eq(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	const void *node = sptree_index_iterator_next(it->iter);

	if (node != NULL &&
		tree_node_compare(it->key_node, node, it->index) == 0) {
		return node_unfold(&it->index->fmtdef, node);
	}

	return NULL;
}

static struct tuple *
tree_iterator_req(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	const void *node = sptree_index_iterator_reverse_next(it->iter);
	if (node != NULL &&
		tree_node_compare(it->key_node, node, it->index) == 0)
		return node_unfold(&it->index->fmtdef, node);

	return NULL;
}

static struct tuple *
tree_iterator_lt(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	const void *node;
	while ((node = sptree_index_iterator_reverse_next(it->iter)) != NULL) {
		if (tree_node_compare(it->key_node, node, it->index) != 0) {
			it->base.next = tree_iterator_le;
			return node_unfold(&it->index->fmtdef, node);
		}
	}

	return NULL;
}

static struct tuple *
tree_iterator_gt(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);

	const void *node;
	while ((node = sptree_index_iterator_next(it->iter)) != NULL) {
		if (tree_node_compare(it->key_node, node, it->index) != 0) {
			it->base.next = tree_iterator_ge;
			return node_unfold(&it->index->fmtdef, node);
		}
	}

	return NULL;
}

/* }}} */

/* {{{ TreeIndex -- base tree index class *************************/

@implementation TreeIndex

+ (struct index_traits *) traits
{
	return &tree_index_traits;
}

- (void) free
{
	node_format_deinit(&fmtdef);
	sptree_index_destroy(tree);
	index_realloc(tree, 0, "struct sptree");
	[super free];
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];
	if (self == NULL)
		return NULL;

	say_warn("TREE format: %d %p", index_n(self), &fmtdef);

	node_format_init(&fmtdef, space_arg, key_def_arg);
	tmp_node_size = 0;
	tmp_node = NULL;

	tree = index_realloc(NULL, sizeof(*tree), "struct sptree");
	memset(tree, 0, sizeof(*tree));
	return self;
}

- (size_t) size
{
	return tree->size;
}

- (struct tuple *) min
{
	const void *node = sptree_index_first(tree);
	return node_unfold(&fmtdef, node);

	return NULL;
}

- (struct tuple *) max
{
	const void *node = sptree_index_last(tree);
	return node_unfold(&fmtdef, node);

	return NULL;
}

- (struct tuple *) findByKey: (void *) key : (int) part_count
{
	assert(key_def->is_unique);
	check_key_parts(key_def, part_count, false);

	node_foldkey(&fmtdef, &tmp_node, &tmp_node_size, key, part_count);

	const void *node = sptree_index_find(tree, tmp_node);
	return node_unfold(&fmtdef, node);

	return NULL;
}

- (struct tuple *) replace: (struct tuple *) old_tuple
			  :(struct tuple *) new_tuple
			  :(enum dup_replace_mode) mode
{
	say_warn("replace: %p %p", old_tuple, new_tuple);
	uint32_t errcode;

	size_t nsize = node_size(&fmtdef);
	void *new_node = alloca(nsize);
	void *old_node = alloca(nsize);

	if (new_tuple) {
		void *dup_node = old_node;

		node_fold(&fmtdef, new_node, new_tuple);

		sptree_index_replace(tree, new_node, &dup_node);

		struct tuple *dup_tuple = node_unfold(&fmtdef, dup_node);
		errcode = replace_check_dup(old_tuple, dup_tuple, mode);

		if (errcode) {
			sptree_index_delete(tree, new_node);

			if (dup_node)
				sptree_index_replace(tree, dup_node, NULL);
			tnt_raise(ClientError, :errcode, index_n(self));
		}
		if (dup_tuple) {
			return dup_tuple;
		}
	}
	if (old_tuple) {
		node_fold(&fmtdef, old_node, old_tuple);
		sptree_index_delete(tree, old_node);
	}

	return old_tuple;
}

- (struct iterator *) allocIterator
{
	assert(key_def->part_count);
	struct tree_iterator *it = index_realloc(NULL,
				sizeof(struct tree_iterator), "iterator");
	memset(it, 0, sizeof(struct tree_iterator));

	it->index = self;
	it->base.free = tree_iterator_free;
	it->key_node = NULL;
	it->key_node_size = 0;

	return (struct iterator *) it;
}

- (void) initIterator: (struct iterator *) iterator
	:(enum iterator_type) type
	:(void *) key :(int) part_count
{
	struct tree_iterator *it = tree_iterator(iterator);

	if (part_count != 0) {
		assert(key != NULL);
		check_key_parts(key_def, part_count,
				traits->allows_partial_key);
	} else {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}

	/* TODO: NULL key */
	node_foldkey(&fmtdef, &it->key_node, &it->key_node_size,
		      key, part_count);

	if (iterator_type_is_reverse(type))
		sptree_index_iterator_reverse_init_set(tree, &it->iter,
						       it->key_node);
	else
		sptree_index_iterator_init_set(tree, &it->iter,
					       it->key_node);

	switch (type) {
	case ITER_EQ:
		it->base.next = tree_iterator_eq;
		break;
	case ITER_REQ:
		it->base.next = tree_iterator_req;
		break;
	case ITER_ALL:
	case ITER_GE:
		it->base.next = tree_iterator_ge;
		break;
	case ITER_GT:
		it->base.next = tree_iterator_gt;
		break;
	case ITER_LE:
		it->base.next = tree_iterator_le;
		break;
	case ITER_LT:
		it->base.next = tree_iterator_lt;
		break;
	default:
		tnt_raise(ClientError, :ER_UNSUPPORTED,
			  "Tree index", "requested iterator type");
	}
}

- (void) beginBuild
{
	assert(index_is_primary(self));

	tree->size = 0;
	tree->max_size = 64;

	size_t sz = tree->max_size * node_size(&fmtdef);
	tree->members = index_realloc(tree->members, sz, "tree->members");
}

- (void) buildNext: (struct tuple *) tuple
{
	size_t nsize = node_size(&fmtdef);
	if (tree->size == tree->max_size) {
		tree->max_size *= 2;

		size_t sz = tree->max_size * nsize;
		tree->members = index_realloc(tree->members, sz, "tree->members");
	}

	void *new_node = ((char *) tree->members + tree->size * nsize);
	node_fold(&fmtdef, new_node, tuple);

	tree->size++;
}

- (void) endBuild
{
	assert(index_is_primary(self));

	u32 n_tuples = tree->size;
	u32 estimated_tuples = tree->max_size;
	void *nodes = tree->members;

	sptree_index_init(tree, node_size(&fmtdef),
			  nodes, n_tuples, estimated_tuples, self);
}

- (void) build: (Index *) pk
{
	u32 n_tuples = [pk size];
	u32 estimated_tuples = n_tuples * 1.2;
	size_t nsize = node_size(&fmtdef);

	void *nodes = NULL;
	say_warn("n_tuples: %u", n_tuples);
	if (n_tuples) {
		/*
		 * Allocate a little extra to avoid
		 * unnecessary realloc() when more data is
		 * inserted.
		*/
		size_t sz = estimated_tuples * nsize;
		nodes = index_realloc(nodes, sz, "tree->members");
	}

	struct iterator *it = pk->position;
	[pk initIterator: it :ITER_ALL :NULL :0];

	struct tuple *tuple;


	say_warn("build tree type: %d, part_count %d is linear %d", fmtdef.format, fmtdef.key_def->part_count, fmtdef.is_linear);
	for (u32 i = 0; (tuple = it->next(it)) != NULL; ++i) {
		void *node = ((char *) nodes + i * nsize);
		node_fold(&fmtdef, node, tuple);
		say_warn("build: tuple: %p node: %p self: %p", tuple, node, self);
	}

	if (n_tuples) {
		say_info("Sorting %"PRIu32 " keys in index %" PRIu32 "...", n_tuples,
			 index_n(self));
	}

	/* If n_tuples == 0 then estimated_tuples = 0, elem == NULL, tree is empty */
	sptree_index_init(tree, nsize,
			  nodes, n_tuples, estimated_tuples, self);
}

@end

/* }}} */
