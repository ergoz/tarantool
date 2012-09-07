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
#include "index.h"
#include "say.h"
#include "tuple.h"
#include "pickle.h"
#include <palloc.h>
#include "exception.h"
#include "space.h"

#include <third_party/murmur_hash2.c>
#include <third_party/sptree.h>

#include <stdio.h>

static struct index_traits index_traits = {
	.allows_partial_key = true,
};

static struct index_traits hash_index_traits = {
	.allows_partial_key = false,
};

/* {{{ Utilities. *************************************************/

/**
 * Unsigned 32-bit int comparison.
 */
static inline int
u32_cmp(u32 a, u32 b)
{
	return a < b ? -1 : (a > b);
}

/**
 * Unsigned 64-bit int comparison.
 */
static inline int
u64_cmp(u64 a, u64 b)
{
	return a < b ? -1 : (a > b);
}

/**
 * Tuple address comparison.
 */
static inline int
ta_cmp(const struct tuple *tuple_a, const struct tuple *tuple_b)
{
	if (!tuple_a)
		return 0;
	if (!tuple_b)
		return 0;
	return tuple_a < tuple_b ? -1 : (tuple_a > tuple_b);
}

static struct tuple *
iterator_next_equal(struct iterator *it __attribute__((unused)))
{
	return NULL;
}

static struct tuple *
iterator_first_equal(struct iterator *it)
{
	it->next_equal = iterator_next_equal;
	return it->next(it);
}

/* }}} */

/* {{{ Search helper. *********************************************/

/**
 * Helper struct to pass additional info to functions that perform
 * tuple comparison or hashing during search in an index.
 *
 * The "index" field is always set to the Index instance the search
 * is performed on.
 *
 * The "key_data" and "part_count" fields are set when comparison or
 * hashing is to be done against a key rather than a tuple.
 */
struct index_search_helper
{
	Index *index;
	u8 *data;
	int part_count;
};

/* Tuple search initializer. */
#define INDEX_SEARCH_HELPER(idx)					\
	(struct index_search_helper) {					\
		.index = (idx), .data = NULL, .part_count = -1,		\
	}

/* Key search Initializer. */
#define INDEX_KEY_SEARCH_HELPER(idx, key, cnt)				\
	(struct index_search_helper) {					\
		.index = (idx), .data = (key), .part_count = (cnt),	\
	}

/**
 * The key data accompanied by the required metadata. The key data goes
 * either from a tuple or from an externally supplied key.
 *
 * The location if key parts within the "data" area differs in these two
 * cases. The metadata fields define the actual location of the key parts.
 */
struct index_search_data
{
	struct key_part *parts;
	struct field_desc *field_desc;
       	const void *data;
	u32 part_count;
};

static void
index_search_set_key_data(struct index_search_data *data,
			  struct index_search_helper *helper)
{
	data->data = helper->data;
	data->part_count = helper->part_count;
	data->field_desc = helper->index->field_desc;
	data->parts = helper->index->parts;
}

static void
index_search_set_tuple_data(struct index_search_data *data,
			    struct index_search_helper *helper,
			    const struct tuple *tuple)
{
	data->data = tuple->data;
	data->part_count = helper->index->key_def->part_count;
	data->field_desc = helper->index->space->field_desc;
	data->parts = helper->index->key_def->parts;
}

static uint32_t
index_search_hash(struct index_search_helper *helper,
		  const struct tuple *tuple)
{
	struct index_search_data d;
	if (helper->part_count >= 0) {
		assert(tuple == NULL);
		index_search_set_key_data(&d, helper);
	} else {
		index_search_set_tuple_data(&d, helper, tuple);
	}

	uint32_t h = 13;
	for (int part = 0; part < d.part_count; part++) {
		int field = d.parts[part].fieldno;
		const u8 *data = d.data + d.field_desc[field].disp;
		if (d.field_desc[field].base) {
			data += space_get_base_offset(helper->index->space, tuple,
						      d.field_desc[field].base);
		}
		u32 len = load_varint32((void**) &data);
		h = MurmurHash2(data, len, h);
	}

	return h;
}

static int
index_search_equal(struct index_search_helper *helper,
		   const struct tuple *tuple_a,
		   const struct tuple *tuple_b)
{
	struct index_search_data da;
	struct index_search_data db;
	if (helper->part_count >= 0) {
		assert(tuple_a == NULL);
		index_search_set_key_data(&da, helper);
	} else {
		index_search_set_tuple_data(&da, helper, tuple_a);
	}
	index_search_set_tuple_data(&db, helper, tuple_b);

	if (da.part_count != db.part_count)
		return 0;

	for (int part = 0; part < da.part_count; part++) {
		int field_a = da.parts[part].fieldno;
		int field_b = db.parts[part].fieldno;

		const u8 *data_a = da.data + da.field_desc[field_a].disp;
		if (da.field_desc[field_a].base) {
			data_a += space_get_base_offset(helper->index->space, tuple_a,
							da.field_desc[field_a].base);
		}

		const u8 *data_b = db.data + db.field_desc[field_b].disp;
		if (db.field_desc[field_b].base) {
			data_b += space_get_base_offset(helper->index->space, tuple_b,
							db.field_desc[field_b].base);
		}

		u32 len_a = load_varint32((void**) &data_a);
		u32 len_b = load_varint32((void**) &data_b);
		if (len_a != len_b)
			return 0;

		if (memcmp(data_a, data_b, len_a) != 0)
			return 0;
	}

	return 1;
}

static int
index_search_compare(struct index_search_helper *helper,
		     const struct tuple *tuple_a,
		     const struct tuple *tuple_b)
{
	struct index_search_data da;
	struct index_search_data db;
	if (helper->part_count >= 0) {
		assert(tuple_a == NULL);
		index_search_set_key_data(&da, helper);
	} else {
		index_search_set_tuple_data(&da, helper, tuple_a);
	}
	index_search_set_tuple_data(&db, helper, tuple_b);

	int part_count = MIN(da.part_count, db.part_count);
	for (int part = 0; part < part_count; part++) {
		int field_a = da.parts[part].fieldno;
		int field_b = db.parts[part].fieldno;

		const u8 *data_a = da.data + da.field_desc[field_a].disp;
		if (da.field_desc[field_a].base) {
			data_a += space_get_base_offset(helper->index->space, tuple_a,
							da.field_desc[field_a].base);
		}

		const u8 *data_b = db.data + db.field_desc[field_b].disp;
		if (db.field_desc[field_b].base) {
			data_b += space_get_base_offset(helper->index->space, tuple_b,
							db.field_desc[field_b].base);
		}

		u32 len_a = load_varint32((void**) &data_a);
		u32 len_b = load_varint32((void**) &data_b);

		int cmp;
		if (da.field_desc[field_a].type == NUM) {
			cmp = u32_cmp(*((u32 *) data_a), *((u32 *) data_b));
		} else if (da.field_desc[field_a].type == NUM64) {
			cmp = u64_cmp(*((u64 *) data_a), *((u64 *) data_b));
		} else {
			cmp = memcmp(data_a, data_b, MIN(len_a, len_b));
			if (cmp == 0) {
				cmp = ((int) len_a) - ((int) len_b);
			}
		}
		if (cmp) {
			return cmp;
		}
	}

	return 0;
}

/* }}} */

/* {{{ Concrete index class declarations. *************************/

/**
 * Instantiate hash table definitions.
 */
#define mh_name _tuple_table
#define mh_arg_t struct index_search_helper *
#define mh_val_t struct tuple *
#define mh_hash(x, k) index_search_hash((x), (k))
#define mh_eq(x, ka, kb) index_search_equal((x), (ka), (kb))
#define MH_SOURCE 1
#include <mhash-val.h>

/**
 * Instantiate sptree definitions.
 */
SPTREE_DEF(index, realloc);

@interface HashIndex: Index {
	struct mh_tuple_table_t hash;
}
- (void) reserve: (u32) n_tuples;
@end

@interface TreeIndex: Index {
	sptree_index tree;
};
@end

/* }}} */

/* {{{ Index -- base class for all indexes. ***********************/

@implementation Index

static void
index_adjust_key(Index *index, const void *key, int part_count)
{
	const u8 *data = key;
	for (int field = 1; field < part_count; field++) {
		u32 len = load_varint32((void **) &data);
		data += len;
		index->field_desc[field].disp = data - ((u8 *) key);
	}
}

+ (struct index_traits *) traits
{
	return &index_traits;
}

+ (Index *) alloc: (enum index_type) type
	 :(struct key_def *) key_def
	 :(struct space *) space
{
	(void) key_def;
	(void) space;

	switch (type) {
	case HASH:
		return [HashIndex alloc];
	case TREE:
		return [TreeIndex alloc];
	default:
		break;
	}
	panic("unsupported index type");
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init];
	if (self) {
		traits = [object_getClass(self) traits];
		key_def = key_def_arg;
		space = space_arg;
		position = [self allocIterator];

		parts = malloc(sizeof(struct key_part) * key_def->part_count);
		if (parts == NULL)
			abort();

		field_desc = malloc(sizeof(struct field_desc)
				    * key_def->part_count);
		if (field_desc == NULL)
			abort();

		for (int i = 0; i < key_def->part_count; i++) {
			parts[i].type = key_def->parts[i].type;
			parts[i].fieldno = i;

			field_desc[i].type = key_def->parts[i].type;
			field_desc[i].base = 0;
			field_desc[i].disp = 0;
		}
	}
	return self;
}

- (void) free
{
	position->free(position);
	free(parts);
	free(field_desc);
	[super free];
}

- (void) beginBuild
{
	[self subclassResponsibility: _cmd];
}

- (void) buildNext: (struct tuple *)tuple
{
	(void) tuple;
	[self subclassResponsibility: _cmd];
}

- (void) endBuild
{
	[self subclassResponsibility: _cmd];
}

- (void) build: (Index *) pk
{
	(void) pk;
	[self subclassResponsibility: _cmd];
}

- (size_t) size
{
	[self subclassResponsibility: _cmd];
	return 0;
}

- (struct tuple *) min
{
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (struct tuple *) max
{
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (struct tuple *) findByKey: (void *) key :(int) part_count
{
	space_check_key(self, key, part_count, false);
	return [self findUnsafe: key :part_count];
}

- (struct tuple *) findUnsafe: (void *) key :(int) part_count
{
	(void) key;
	(void) part_count;
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (struct tuple *) findByTuple: (struct tuple *) pattern
{
	(void) pattern;
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (void) remove: (struct tuple *) tuple
{
	(void) tuple;
	[self subclassResponsibility: _cmd];
}

- (void) replace: (struct tuple *) old_tuple
	:(struct tuple *) new_tuple
{
	(void) old_tuple;
	(void) new_tuple;
	[self subclassResponsibility: _cmd];
}

- (struct iterator *) allocIterator
{
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (void) initIterator: (struct iterator *) iterator :(enum iterator_type) type
{
	(void) iterator;
	(void) type;
	[self subclassResponsibility: _cmd];
}

- (void) initIteratorByKey: (struct iterator *) iterator :(enum iterator_type) type
                        :(void *) key :(int) part_count
{
	space_check_key(self, key, part_count, traits->allows_partial_key);
	[self initIteratorUnsafe: iterator :type :key :part_count];
}

- (void) initIteratorUnsafe: (struct iterator *) iterator :(enum iterator_type) type
                        :(void *) key :(int) part_count
{
	(void) iterator;
	(void) type;
	(void) key;
	(void) part_count;
	[self subclassResponsibility: _cmd];
}

@end

/* }}} */

/* {{{ HashIndex. *************************************************/

struct hash_iterator {
	struct iterator base; /* Must be the first member. */
	HashIndex *index;
	mh_int_t h_pos;
};


@implementation HashIndex

static struct hash_iterator *
hash_iterator(struct iterator *it)
{
	return (struct hash_iterator *) it;
}

struct tuple *
hash_iterator_next(struct iterator *iterator)
{
	assert(iterator->next == hash_iterator_next);
	struct hash_iterator *it = hash_iterator(iterator);

	while (it->h_pos != mh_end(&it->index->hash)) {
		if (mh_exist((&it->index->hash), it->h_pos))
			return mh_value(&it->index->hash, it->h_pos++);
		it->h_pos++;
	}
	return NULL;
}

void
hash_iterator_free(struct iterator *iterator)
{
	assert(iterator->next == hash_iterator_next);
	free(iterator);
}

+ (struct index_traits *) traits
{
	return &hash_index_traits;
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];
	if (self) {
		mh_tuple_table_init(&hash);
	}
	return self;
}

- (void) free
{
	mh_tuple_table_destroy(&hash);
	[super free];
}

- (void) reserve: (u32) n_tuples
{
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	mh_tuple_table_reserve(&helper, &hash, n_tuples);
}

- (void) beginBuild
{
}

- (void) buildNext: (struct tuple *)tuple
{
	[self replace: NULL :tuple];
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
	[pk initIterator: it :ITER_FORWARD];

	while ((tuple = it->next(it)))
	      [self replace: NULL :tuple];
}

- (size_t) size
{
	return mh_size(&hash);
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

- (struct tuple *) findUnsafe: (void *) key :(int) part_count
{
	struct index_search_helper helper =
		INDEX_KEY_SEARCH_HELPER(self, key, part_count);
	index_adjust_key(self, key, part_count);
	mh_int_t k = mh_tuple_table_get(&helper, &hash, NULL);
	struct tuple *ret = NULL;
	if (k != mh_end(&hash))
		ret = mh_value(&hash, k);
	return ret;
}

- (struct tuple *) findByTuple: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	mh_int_t k = mh_tuple_table_get(&helper, &hash, tuple);
	struct tuple *ret = NULL;
	if (k != mh_end(&hash))
		ret = mh_value(&hash, k);
	return ret;
}

- (void) remove: (struct tuple *) tuple
{
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	mh_int_t k = mh_tuple_table_get(&helper, &hash, tuple);
	if (k != mh_end(&hash))
		mh_tuple_table_del(&helper, &hash, k);
}

- (void) replace: (struct tuple *) old_tuple
	:(struct tuple *) new_tuple
{
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	if (old_tuple != NULL) {
		mh_int_t k = mh_tuple_table_get(&helper, &hash, old_tuple);
		if (k != mh_end(&hash))
			mh_tuple_table_del(&helper, &hash, k);
	}
	mh_tuple_table_put(&helper, &hash, new_tuple, NULL);
}

- (void) initIterator: (struct iterator *) iterator :(enum iterator_type) type
{
	assert(iterator->next == hash_iterator_next);
	struct hash_iterator *it = hash_iterator(iterator);

	if (type == ITER_REVERSE)
		tnt_raise(IllegalParams, :"hash iterator is forward only");

	it->base.next_equal = 0; /* Should not be used if not positioned. */
	it->h_pos = mh_begin(&hash);
	it->index = self;
}

- (void) initIteratorUnsafe: (struct iterator *) iterator
			   :(enum iterator_type) type
			   :(void *) key :(int) part_count
{
	if (type == ITER_REVERSE)
		tnt_raise(IllegalParams, :"hash iterator is forward only");

	assert(iterator->next == hash_iterator_next);
	struct hash_iterator *it = hash_iterator(iterator);

	it->base.next_equal = iterator_first_equal;

	struct index_search_helper helper =
		INDEX_KEY_SEARCH_HELPER(self, key, part_count);
	index_adjust_key(self, key, part_count);
	it->h_pos = mh_tuple_table_get(&helper, &hash, NULL);
	it->index = self;
}

- (struct iterator *) allocIterator
{
	struct hash_iterator *it = malloc(sizeof(struct hash_iterator));
	if (it) {
		memset(it, 0, sizeof(struct hash_iterator));
		it->base.next = hash_iterator_next;
		it->base.free = hash_iterator_free;
	}
	return (struct iterator *) it;
}

@end

/* }}} */

/* {{{ TreeIndex. *************************************************/

struct tree_iterator {
	struct iterator base;
	struct index_search_helper helper;
	struct sptree_index_iterator *iter;
};

static inline struct tree_iterator *
tree_iterator(struct iterator *it)
{
	return (struct tree_iterator *) it;
}

@implementation TreeIndex

static int
tree_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	struct tuple *const *tuple_a = node_a;
	struct tuple *const *tuple_b = node_b;
	struct index_search_helper *helper = arg;
	return index_search_compare(helper, tuple_a ? *tuple_a : NULL, *tuple_b);
}

static int
tree_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	struct tuple *const *tuple_a = node_a;
	struct tuple *const *tuple_b = node_b;
	struct index_search_helper *helper = arg;
	int r = index_search_compare(helper, tuple_a ? *tuple_a : NULL, *tuple_b);
	if (r == 0) {
		r = ta_cmp(tuple_a ? *tuple_a : NULL, *tuple_b);
	}
	return r;
}

static struct tuple *
tree_iterator_next(struct iterator *iterator)
{
	assert(iterator->next == tree_iterator_next);
	struct tree_iterator *it = tree_iterator(iterator);
	struct tuple **tuplep = sptree_index_iterator_next(it->iter);
	return tuplep != NULL ? *tuplep : NULL;
}

static struct tuple *
tree_iterator_reverse_next(struct iterator *iterator)
{
	assert(iterator->next == tree_iterator_reverse_next);
	struct tree_iterator *it = tree_iterator(iterator);
	struct tuple **tuplep = sptree_index_iterator_reverse_next(it->iter);
	return tuplep != NULL ? *tuplep : NULL;
}

static struct tuple *
tree_iterator_next_equal(struct iterator *iterator)
{
	assert(iterator->next == tree_iterator_next);
	struct tree_iterator *it = tree_iterator(iterator);
	struct tuple **tuplep = sptree_index_iterator_next(it->iter);
	if (tuplep != NULL) {
		TreeIndex *index = (TreeIndex *) it->helper.index;
		if (index->tree.compare(NULL, tuplep, &it->helper) == 0) {
			return *tuplep;
		}
	}
	return NULL;
}

static struct tuple *
tree_iterator_reverse_next_equal(struct iterator *iterator)
{
	assert(iterator->next == tree_iterator_reverse_next);
	struct tree_iterator *it = tree_iterator(iterator);
	struct tuple **tuplep = sptree_index_iterator_reverse_next(it->iter);
	if (tuplep != NULL) {
		TreeIndex *index = (TreeIndex *) it->helper.index;
		if (index->tree.compare(NULL, tuplep, &it->helper) == 0) {
			return *tuplep;
		}
	}
	return NULL;
}

static void
tree_iterator_free(struct iterator *iterator)
{
	assert(iterator->free == tree_iterator_free);
	struct tree_iterator *it = tree_iterator(iterator);
	if (it->iter)
		sptree_index_iterator_free(it->iter);

	free(it);
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];
	if (self) {
		memset(&tree, 0, sizeof tree);
	}
	return self;
}

- (void) free
{
	sptree_index_destroy(&tree);
	[super free];
}

- (size_t) size
{
	return tree.size;
}

- (struct tuple *) min
{
	struct tuple **tuplep = sptree_index_first(&tree);
	return tuplep != NULL ? *tuplep : NULL;
}

- (struct tuple *) max
{
	struct tuple **tuplep = sptree_index_last(&tree);
	return tuplep != NULL ? *tuplep : NULL;
}

- (struct tuple *) findUnsafe: (void *) key : (int) part_count
{
	struct index_search_helper helper =
		INDEX_KEY_SEARCH_HELPER(self, key, part_count);
	index_adjust_key(self, key, part_count);
	struct tuple **tuplep = sptree_index_find(&tree, NULL, &helper);
	return tuplep != NULL ? *tuplep : NULL;
}

- (struct tuple *) findByTuple: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	struct tuple **tuplep = sptree_index_find(&tree, &tuple, &helper);
	return tuplep != NULL ? *tuplep : NULL;
}

- (void) remove: (struct tuple *) tuple
{
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	sptree_index_delete(&tree, &tuple, &helper);
}

- (void) replace: (struct tuple *) old_tuple
		: (struct tuple *) new_tuple
{
	/* TODO: review this check */
	if (new_tuple->field_count < key_def->max_fieldno)
		tnt_raise(ClientError, :ER_NO_SUCH_FIELD,
			  key_def->max_fieldno);

	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	if (old_tuple) {
		sptree_index_delete(&tree, &old_tuple, &helper);
	}
	sptree_index_insert(&tree, &new_tuple, &helper);
}

- (struct iterator *) allocIterator
{
	struct tree_iterator *it = malloc(sizeof(struct tree_iterator));
	if (it) {
		memset(it, 0, sizeof(struct tree_iterator));
		it->helper.index = self;
		it->base.free = tree_iterator_free;
	}
	return (struct iterator *) it;
}

- (void) initIterator: (struct iterator *) iterator :(enum iterator_type) type
{
	[self initIteratorUnsafe: iterator :type :NULL :0];
}

- (void) initIteratorUnsafe: (struct iterator *) iterator
			   :(enum iterator_type) type
			   :(void *) key
			   :(int) part_count
{
	assert(iterator->free == tree_iterator_free);
	struct tree_iterator *it = tree_iterator(iterator);
	it->helper = INDEX_KEY_SEARCH_HELPER(self, key, part_count);
	index_adjust_key(self, key, part_count);
	if (type == ITER_FORWARD) {
		it->base.next = tree_iterator_next;
		it->base.next_equal = tree_iterator_next_equal;
		sptree_index_iterator_init_set(&tree, &it->iter,
					       NULL, &it->helper);
	} else if (type == ITER_REVERSE) {
		it->base.next = tree_iterator_reverse_next;
		it->base.next_equal = tree_iterator_reverse_next_equal;
		sptree_index_iterator_reverse_init_set(&tree, &it->iter,
						       NULL, &it->helper);
	}
}

- (void) beginBuild
{
	assert(index_is_primary(self));

	tree.size = 0;
	tree.max_size = 64;

	size_t sz = tree.max_size * sizeof(struct tuple *);
	tree.members = malloc(sz);
	if (tree.members == NULL) {
		panic("malloc(): failed to allocate %"PRI_SZ" bytes", sz);
	}
}

- (void) buildNext: (struct tuple *) tuple
{
	if (tree.size == tree.max_size) {
		tree.max_size *= 2;

		size_t sz = tree.max_size * sizeof(struct tuple *);
		tree.members = realloc(tree.members, sz);
		if (tree.members == NULL) {
			panic("malloc(): failed to allocate %"PRI_SZ" bytes", sz);
		}
	}

	((struct tuple **) tree.members)[tree.size] = tuple;
	tree.size++;
}

- (void) endBuild
{
	assert(index_is_primary(self));

	u32 n_tuples = tree.size;
	u32 estimated_tuples = tree.max_size;
	void *nodes = tree.members;

	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	sptree_index_init(&tree,
			  sizeof(struct tuple *), nodes, n_tuples,
			  estimated_tuples, tree_node_cmp, tree_node_cmp,
			  &helper);
}

- (void) build: (Index *) pk
{
	u32 n_tuples = [pk size];
	u32 estimated_tuples = n_tuples * 1.2;

	struct tuple **nodes = NULL;
	if (n_tuples) {
		/*
		 * Allocate a little extra to avoid
		 * unnecessary realloc() when more data is
		 * inserted.
		*/
		size_t sz = estimated_tuples * sizeof(struct tuple *);
		nodes = malloc(sz);
		if (nodes == NULL) {
			panic("malloc(): failed to allocate %"PRI_SZ" bytes", sz);
		}
	}

	struct iterator *it = pk->position;
	[pk initIterator: it :ITER_FORWARD];

	struct tuple *tuple;
	for (u32 i = 0; (tuple = it->next(it)) != NULL; ++i) {
		nodes[i] = tuple;
	}

	if (n_tuples) {
		say_info("Sorting %"PRIu32 " keys in index %" PRIu32 "...", n_tuples,
			 index_n(self));
	}

	/* If n_tuples == 0 then estimated_tuples = 0, elem == NULL, tree is empty */
	struct index_search_helper helper = INDEX_SEARCH_HELPER(self);
	sptree_index_init(&tree,
			  sizeof(struct tuple *), nodes, n_tuples,
			  estimated_tuples, tree_node_cmp,
			  key_def->is_unique ? tree_node_cmp : tree_dup_node_cmp,
			  &helper);
}

@end

/* }}} */
