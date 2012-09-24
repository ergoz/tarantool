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

/* Pseudo-tuple indicating a key.  */
#define SEARCH_KEY ((struct tuple *) 0)

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

/* {{{ Index search routines. *************************************/

@class BaseIndex;

/**
 * The data relevant to the search request over a given index. It consists
 * of the specific key data accompanied by the pertinent metadata. The key
 * data goes either from a tuple or from a genuine externally supplied key.
 *
 * The location if key parts within the "data" area differs in these two
 * cases. The metadata fields define the actual location of the key parts.
 */
struct search_data
{
	/* The entire key data. */
       	const u8 *data;

	/* Description of the key. */
	struct key_part *parts;

	/* The tuple the key data goes from. May be NULL if the data goes
	   from a genuine key. The tuple is needed to access its base offset
	   table. */
	const struct tuple *tuple;

	/* The offset data needed for non-linear field access. */
	u32 *offset_table;
};

/**
 * Helper struct to pass additional info to functions that perform
 * tuple comparison or hashing during search in an index.
 *
 * The "index" field is always set to the Index instance the search
 * is performed on.
 *
 * The "data" and "part_count" fields are set when comparison or
 * hashing is to be done against a key rather than a tuple.
 */
struct search_helper
{
	/* The index the search is done on. */
	BaseIndex *index;

	/* The number of key parts. */
	u32 part_count;

	/* Comparison left-hand data. */
	struct search_data a;
	/* Comparison right-hand data. */
	struct search_data b;
};

@interface BaseIndex: Index
{
@public
	/* Metadata for keys. */
	struct key_part *key_parts;

	/* Search helper utilized for index maintenance
	   rather than tuple lookup. */
	struct search_helper common_helper;
}
@end

/* Key search initializer. */
#define KEY_SEARCH_HELPER(idx, cnt)					\
	(struct search_helper) {					\
		.index = (idx),						\
		.part_count = MIN((cnt), (idx)->key_def->part_count),	\
	}

/* Tuple search initializer. */
#define SEARCH_HELPER(idx)						\
	(struct search_helper) {					\
		.index = (idx),						\
		.part_count = (idx)->key_def->part_count,		\
	}

/* Offset table initializer. */
#define SEARCH_OFFSET_TABLE(idx)					\
	((idx)->key_def->needs_offset_table				\
	 ? alloca((idx)->key_def->part_count * sizeof(uint32_t))	\
	 : NULL)

#define DEFINE_KEY_SEARCH_DATA(v, idx, key, cnt)			\
	struct search_helper v = KEY_SEARCH_HELPER(idx, cnt);		\
	v.a.offset_table = NULL;					\
	v.b.offset_table = SEARCH_OFFSET_TABLE(idx);			\
	search_set_key(&v.a, &v, key);					\
	search_set_tuple_meta(&v.b, &v);

#define DEFINE_SEARCH_DATA(v, idx, tuple)				\
	struct search_helper v = SEARCH_HELPER(idx);			\
	v.a.offset_table = SEARCH_OFFSET_TABLE(idx);			\
	v.b.offset_table = SEARCH_OFFSET_TABLE(idx);			\
	search_set_tuple_meta(&v.a, &v);				\
	search_set_tuple_data(&v.a, &v, tuple);				\
	search_set_tuple_meta(&v.b, &v);

#define DEFINE_SEARCH_DATA_NOTUPLE(v, idx)				\
	struct search_helper v = SEARCH_HELPER(idx);			\
	v.a.offset_table = SEARCH_OFFSET_TABLE(idx);			\
	v.b.offset_table = SEARCH_OFFSET_TABLE(idx);			\
	search_set_tuple_meta(&v.a, &v);				\
	search_set_tuple_meta(&v.b, &v);

static void
search_set_key(struct search_data *data,
	       struct search_helper *helper,
	       const void *key)
{
	data->data = key;
	data->tuple = SEARCH_KEY;
	data->parts = helper->index->key_parts;
}

static void
search_set_tuple_meta(struct search_data *data,
		      struct search_helper *helper)
{
	data->parts = helper->index->key_def->parts;
}

static void
search_set_tuple_data(struct search_data *data,
		      struct search_helper *helper,
		      const struct tuple *tuple)
{
	data->data = tuple->data;
	data->tuple = tuple;

	if (data->offset_table == NULL)
		return;

	struct space *space = helper->index->space;
	struct key_def *def = helper->index->key_def;
	for (int f = 0; f < def->max_fieldno; f++) {
		int p = def->cmp_order[f];
		if (p == -1)
			continue;

		if (space->field_desc[f].base >= 0) {
			u32 offset = space->field_desc[f].disp;
			if (space->field_desc[f].base > 0) {
				offset += space_get_base_offset(
					tuple,
					space->field_desc[f].base);
			}
			data->offset_table[p] = offset;
			continue;
		}

		u32 offset = 0;
		int prev_f = f - 1;
		assert(prev_f >= 0);
		if (space->field_desc[prev_f].base >= 0) {
			offset = space->field_desc[prev_f].disp;
			if (space->field_desc[prev_f].base > 0) {
				offset += space_get_base_offset(
					tuple,
					space->field_desc[prev_f].base);
			}
		} else {
			int prev_p = def->cmp_order[prev_f];
			assert(prev_p >= 0);
			offset = data->offset_table[prev_p];
		}

		const u8 *ptr = tuple->data + offset;
	        u32 size = load_varint32((void**) &ptr);
		data->offset_table[p] = ptr - tuple->data + size;
	}
}

static inline const u8 *
search_get_part(struct search_data *data, int part, const u8* prev_data_end)
{
	switch(data->parts[part].offset_code) {
	case BASE_DISP:
		return (data->data + data->parts[part].disp
			+ space_get_base_offset(data->tuple,
						data->parts[part].base));
	case DISP_ONLY:
		return (data->data + data->parts[part].disp);
	case OFFSET_TABLE:
		return (data->data + data->offset_table[part]);
	case NEXT_FIELD:
		return prev_data_end;
	}
	panic("bad offset code");
}

static uint32_t
search_hash(struct search_helper *x, const struct tuple *tuple)
{
	/* Prepare data for hashing. */
	if (x->a.tuple != tuple) {
		/* This call is made on behalf of the index build
		   or rehash procedure. */
		x = &x->index->common_helper;
		search_set_tuple_data(&x->a, x, tuple);
	}

	/* Initial hash value. */
	uint32_t h = 13;

	/* Hash data part by part. */
	assert(x->part_count > 0);
	const u8 *adata = search_get_part(&x->a, 0, NULL);
	for (int part = 0;;) {
		if (x->a.parts[part].type == NUM) {
			assert(*adata == 4);
			u32 a32 = *((u32 *) (adata + 1));
			h = (h << 9) ^ (h >> 23) ^ a32;
			if (++part == x->part_count)
				return h;
			adata = search_get_part(&x->a, part, adata + 1 + 4);
		} else if (x->a.parts[part].type == NUM64) {
			assert(*adata == 8);
			u64 a64 = *((u64 *) (adata + 1));
			h = (h << 9) ^ (h >> 23) ^ (uint32_t) (a64);
			h = (h << 9) ^ (h >> 23) ^ (uint32_t) (a64 >> 32);
			if (++part == x->part_count)
				return h;
			adata = search_get_part(&x->a, part, adata + 1 + 8);
		} else {
			int asize = load_varint32((void**) &adata);
			h = MurmurHash2(adata, asize, h);
			if (++part == x->part_count)
				return h;
			adata = search_get_part(&x->a, part, adata + asize);
		}
	}
}

static int
search_equal(struct search_helper *x,
	     const struct tuple *tuple_a,
	     const struct tuple *tuple_b)
{
	/* Prepare data for comparison. */
	if (x->a.tuple != tuple_a) {
		/* This call is made on behalf of the index build
		   or rehash procedure. */
		x = &x->index->common_helper;
		search_set_tuple_data(&x->a, x, tuple_a);
	}
	search_set_tuple_data(&x->b, x, tuple_b);

	/* Compare data part by part. */
	assert(x->part_count > 0);
	const u8 *adata = search_get_part(&x->a, 0, NULL);
	const u8 *bdata = search_get_part(&x->b, 0, NULL);
	for (int part = 0;;) {
		assert(x->a.parts[part].type == x->b.parts[part].type);
		if (x->a.parts[part].type == NUM) {
			assert(*adata == 4);
			assert(*bdata == 4);
			if (*((u32 *) (adata + 1)) != *((u32 *) (bdata + 1)))
				return 0;
			if (++part == x->part_count)
				return 1;
			adata = search_get_part(&x->a, part, adata + 1 + 4);
			bdata = search_get_part(&x->b, part, bdata + 1 + 4);
		} else if (x->a.parts[part].type == NUM64) {
			assert(*adata == 8);
			assert(*bdata == 8);
			if (*((u64 *) (adata + 1)) != *((u64 *) (bdata + 1)))
				return 0;
			if (++part == x->part_count)
				return 1;
			adata = search_get_part(&x->a, part, adata + 1 + 8);
			bdata = search_get_part(&x->b, part, bdata + 1 + 8);
		} else {
			int asize = load_varint32((void**) &adata);
			int bsize = load_varint32((void**) &bdata);
			if (asize != bsize)
				return 0;
			if (memcmp(adata, bdata, asize) != 0)
				return 0;
			if (++part == x->part_count)
				return 1;
			adata = search_get_part(&x->a, part, adata + asize);
			bdata = search_get_part(&x->b, part, bdata + asize);
		}
	}
}

static int
search_compare(struct search_helper *x,
	       const struct tuple *tuple_a,
	       const struct tuple *tuple_b)
{
	/* Prepare data for comparison. */
	if (x->a.tuple != tuple_a) {
		/* This call is made on behalf of the index build
		   or rebalance procedure. */
		x = &x->index->common_helper;
		search_set_tuple_data(&x->a, x, tuple_a);
	} else if (x->part_count == 0) {
		/* This must be a search by an empty key, which
		   matches every tuple. */
		assert(tuple_a == SEARCH_KEY);
		return 0;
	}
	search_set_tuple_data(&x->b, x, tuple_b);

	/* Compare data part by part. */
	assert(x->part_count > 0);
	const u8 *adata = search_get_part(&x->a, 0, NULL);
	const u8 *bdata = search_get_part(&x->b, 0, NULL);
	for (int part = 0;;) {
		assert(x->a.parts[part].type == x->b.parts[part].type);
		if (x->a.parts[part].type == NUM) {
			assert(*adata == 4);
			assert(*bdata == 4);
			int r = u32_cmp(*((u32 *) (adata + 1)),
					*((u32 *) (bdata + 1)));
			if (r || ++part == x->part_count)
				return r;
			adata = search_get_part(&x->a, part, adata + 1 + 4);
			bdata = search_get_part(&x->b, part, bdata + 1 + 4);
		} else if (x->a.parts[part].type == NUM64) {
			assert(*adata == 8);
			assert(*bdata == 8);
			int r = u64_cmp(*((u64 *) (adata + 1)),
					*((u64 *) (bdata + 1)));
			if (r || ++part == x->part_count)
				return r;
			adata = search_get_part(&x->a, part, adata + 1 + 8);
			bdata = search_get_part(&x->b, part, bdata + 1 + 8);
		} else {
			int asize = load_varint32((void**) &adata);
			int bsize = load_varint32((void**) &bdata);
			if (asize < bsize) {
				int r = memcmp(adata, bdata, asize);
				return r ? r : -1;
			} else if (asize > bsize) {
				int r = memcmp(adata, bdata, bsize);
				return r ? r : +1;
			} 
			int r = memcmp(adata, bdata, asize);
			if (r || ++part == x->part_count)
				return r;
			adata = search_get_part(&x->a, part, adata + asize);
			bdata = search_get_part(&x->b, part, bdata + asize);
		}
	}
}

/* }}} */

/* {{{ Concrete index class declarations. *************************/

/**
 * Instantiate hash table definitions.
 */
#define mh_name _tuple_table
#define mh_arg_t struct search_helper *
#define mh_val_t struct tuple *
#define mh_hash(x, k) search_hash((x), (k))
#define mh_eq(x, ka, kb) search_equal((x), (ka), (kb))
#define MH_SOURCE 1
#include <mhash-val.h>

/**
 * Instantiate sptree definitions.
 */
SPTREE_DEF(index, realloc);

@interface HashIndex: BaseIndex {
	struct mh_tuple_table_t hash;
}
- (void) reserve: (u32) n_tuples;
@end

@interface TreeIndex: BaseIndex {
	sptree_index tree;
};
@end

/* }}} */

/* {{{ Index -- abstract index class. *****************************/

@implementation Index

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
	}
	return self;
}

- (void) free
{
	position->free(position);
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

/* {{{ BaseIndex -- base class for all indexes.  ******************/

@implementation BaseIndex

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];
	if (self) {

		/* Initialize key metadata. */
		size_t sz = sizeof(struct key_part) * key_def->part_count;
		key_parts = malloc(sz);
		if (key_parts == NULL)
			panic("malloc(): failed to allocate %"PRI_SZ" bytes", sz);

		for (int i = 0; i < key_def->part_count; i++) {
			key_parts[i].type = key_def->parts[i].type;
			key_parts[i].fieldno = i;
			if (i == 0) {
				key_parts[i].offset_code = DISP_ONLY;
				key_parts[i].base = 0;
			} else {
				key_parts[i].offset_code = NEXT_FIELD;
				key_parts[i].base = -1;
			}
			key_parts[i].disp = 0;
		}

		/* Initialize common search helper. */
		common_helper.index = self;
		common_helper.part_count = key_def->part_count;
		if (key_def->needs_offset_table) {
			sz = key_def->part_count * sizeof(uint32_t);
			common_helper.a.offset_table = malloc(sz);
			common_helper.b.offset_table = malloc(sz);
			if (common_helper.a.offset_table == NULL
			    || common_helper.b.offset_table == NULL)
				panic("malloc(): failed to allocate %"PRI_SZ" bytes", sz);
		} else {
			common_helper.a.offset_table = NULL;
			common_helper.b.offset_table = NULL;
		}
		search_set_tuple_meta(&common_helper.a, &common_helper);
		search_set_tuple_meta(&common_helper.b, &common_helper);
	}
	return self;
}

- (void) free
{
	if (key_def->needs_offset_table) {
		free(common_helper.a.offset_table);
		free(common_helper.b.offset_table);
	}

	free(key_parts);
	[super free];
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
	mh_tuple_table_reserve(&common_helper, &hash, n_tuples);
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
	DEFINE_KEY_SEARCH_DATA(helper, self, key, part_count);

	mh_int_t k = mh_tuple_table_get(&helper, &hash, SEARCH_KEY);
	struct tuple *ret = NULL;
	if (k != mh_end(&hash))
		ret = mh_value(&hash, k);
	return ret;
}

- (struct tuple *) findByTuple: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);

	DEFINE_SEARCH_DATA(helper, self, tuple);

	mh_int_t k = mh_tuple_table_get(&helper, &hash, tuple);
	struct tuple *ret = NULL;
	if (k != mh_end(&hash))
		ret = mh_value(&hash, k);
	return ret;
}

- (void) remove: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);

	DEFINE_SEARCH_DATA(helper, self, tuple);

	mh_int_t k = mh_tuple_table_get(&helper, &hash, tuple);
	if (k != mh_end(&hash))
		mh_tuple_table_del(&helper, &hash, k);
}

- (void) replace: (struct tuple *) old_tuple
	:(struct tuple *) new_tuple
{
	assert((new_tuple->flags & IN_SPACE) != 0);

	DEFINE_SEARCH_DATA_NOTUPLE(helper, self);

	if (old_tuple != NULL) {
		assert((old_tuple->flags & IN_SPACE) != 0);

		search_set_tuple_data(&helper.a, &helper, old_tuple);
		mh_int_t k = mh_tuple_table_get(&helper, &hash, old_tuple);
		if (k != mh_end(&hash))
			mh_tuple_table_del(&helper, &hash, k);
	}

	search_set_tuple_data(&helper.a, &helper, new_tuple);
	mh_int_t pos = mh_tuple_table_put(&helper, &hash, new_tuple, NULL);
	if (pos == mh_end(&hash))
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, (ssize_t) pos,
			  "hash", "key");
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

	DEFINE_KEY_SEARCH_DATA(helper, self, key, part_count);

	it->h_pos = mh_tuple_table_get(&helper, &hash, SEARCH_KEY);
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
	struct sptree_index_iterator *iter;
	TreeIndex *index;
	void *key;
	int part_count;
};

static inline struct tree_iterator *
tree_iterator(struct iterator *it)
{
	return (struct tree_iterator *) it;
}

static void
tree_iterator_set_key(struct tree_iterator *it,
		      void *key, int part_count)
{
	/* Find the key size. */
	u8 *data = key;
	for (int part = 0; part < part_count; part++) {
		u32 size = load_varint32((void**) &data);
		data += size;
	}
	u32 key_size = data - (u8 *) key;

	/* Copy the key data, */
	if (key_size > 0) {
		it->key = malloc(key_size);
		if (it->key == NULL)
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE, key_size,
				  "tree index", "key");

		memcpy(it->key, key, key_size);
		it->part_count = part_count;
	}
}

@implementation TreeIndex

static int
tree_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	struct search_helper *helper = arg;
	struct tuple *const *tuple_a = node_a;
	struct tuple *const *tuple_b = node_b;
	return search_compare(helper, *tuple_a, *tuple_b);
}

static int
tree_dup_node_cmp(const void *node_a, const void *node_b, void *arg)
{
	struct tuple *const *tuple_a = node_a;
	struct tuple *const *tuple_b = node_b;
	if (*tuple_a == *tuple_b)
		return 0;

	struct search_helper *helper = arg;
	int r = search_compare(helper, *tuple_a, *tuple_b);
	if (r != 0)
		return r;

	return *tuple_a < *tuple_b ? -1 : 1;
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
		DEFINE_KEY_SEARCH_DATA(helper, it->index, it->key, it->part_count);

		struct tuple *pseudo = SEARCH_KEY;
		if (it->index->tree.compare(&pseudo, tuplep, &helper) == 0) {
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
		DEFINE_KEY_SEARCH_DATA(helper, it->index, it->key, it->part_count);

		struct tuple *pseudo = SEARCH_KEY;
		if (it->index->tree.compare(&pseudo, tuplep, &helper) == 0) {
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
	if (it->key != NULL)
		free(it->key);
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
	DEFINE_KEY_SEARCH_DATA(helper, self, key, part_count);

	struct tuple *pseudo = SEARCH_KEY;
	struct tuple **tuplep = sptree_index_find(&tree, &pseudo, &helper);
	return tuplep != NULL ? *tuplep : NULL;
}

- (struct tuple *) findByTuple: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);

	DEFINE_SEARCH_DATA(helper, self, tuple);

	struct tuple **tuplep = sptree_index_find(&tree, &tuple, &helper);
	return tuplep != NULL ? *tuplep : NULL;
}

- (void) remove: (struct tuple *) tuple
{
	assert((tuple->flags & IN_SPACE) != 0);

	DEFINE_SEARCH_DATA(helper, self, tuple);

	sptree_index_delete(&tree, &tuple, &helper);
}

- (void) replace: (struct tuple *) old_tuple
		: (struct tuple *) new_tuple
{
	assert((new_tuple->flags & IN_SPACE) != 0);

	/* TODO: review this check */
	if (new_tuple->field_count < key_def->max_fieldno)
		tnt_raise(ClientError, :ER_NO_SUCH_FIELD,
			  key_def->max_fieldno);

	DEFINE_SEARCH_DATA_NOTUPLE(helper, self);

	if (old_tuple) {
		assert((old_tuple->flags & IN_SPACE) != 0);
		search_set_tuple_data(&helper.a, &helper, old_tuple);
		sptree_index_delete(&tree, &old_tuple, &helper);
	}

	search_set_tuple_data(&helper.a, &helper, new_tuple);
	sptree_index_insert(&tree, &new_tuple, &helper);
}

- (struct iterator *) allocIterator
{
	struct tree_iterator *it = malloc(sizeof(struct tree_iterator));
	if (it) {
		memset(it, 0, sizeof(struct tree_iterator));
		it->index = self;
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

	DEFINE_KEY_SEARCH_DATA(helper, self, key, part_count);
	tree_iterator_set_key(it, key, part_count);

	struct tuple *pseudo = SEARCH_KEY;
	if (type == ITER_FORWARD) {
		it->base.next = tree_iterator_next;
		it->base.next_equal = tree_iterator_next_equal;
		sptree_index_iterator_init_set(&tree, &it->iter,
					       &pseudo, &helper);
	} else if (type == ITER_REVERSE) {
		it->base.next = tree_iterator_reverse_next;
		it->base.next_equal = tree_iterator_reverse_next_equal;
		sptree_index_iterator_reverse_init_set(&tree, &it->iter,
						       &pseudo, &helper);
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

	sptree_index_init(&tree,
			  sizeof(struct tuple *), nodes, n_tuples,
			  estimated_tuples, tree_node_cmp, tree_node_cmp,
			  &common_helper);
}

- (void) build: (Index *) pk
{
	u32 n_tuples = [pk size];
	u32 estimated_tuples = n_tuples * 1.2;

	struct tuple **nodes = NULL;
	if (n_tuples) {
		/*
		 * Allocate a little extra to avoid unnecessary
		 * realloc() when more data is inserted.
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
	sptree_index_init(&tree,
			  sizeof(struct tuple *), nodes, n_tuples,
			  estimated_tuples, tree_node_cmp,
			  key_def->is_unique ? tree_node_cmp : tree_dup_node_cmp,
			  &common_helper);
}

@end

/* }}} */
