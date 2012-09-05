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
#include "tree.h"
#include "say.h"
#include "tuple.h"
#include "pickle.h"
#include <palloc.h>
#include "exception.h"
#include "space.h"

#include <stdio.h>

static struct index_traits index_traits = {
	.allows_partial_key = true,
};

static struct index_traits hash_index_traits = {
	.allows_partial_key = false,
};

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

static void
check_key_parts(struct key_def *key_def,
		int part_count, bool partial_key_allowed)
{
	if (part_count > key_def->part_count)
		tnt_raise(ClientError, :ER_KEY_PART_COUNT,
			  part_count, key_def->part_count);
	if (!partial_key_allowed && part_count < key_def->part_count)
		tnt_raise(ClientError, :ER_EXACT_MATCH,
			  part_count, key_def->part_count);

}

/* {{{ Index -- base class for all indexes. ********************/

@interface HashIndex: Index {
	/* Hash table impl. */
	struct mh_tuple_table_t *hash;

	/* Shared temporary description of key data. */
	struct tuple *key_tuple;
}

- (void) reserve: (u32) n_tuples;
@end

@implementation Index

@class HashIndex;
@class TreeIndex;

+ (struct index_traits *) traits
{
	return &index_traits;
}

+ (Index *) alloc: (enum index_type) type
	 :(struct key_def *) key_def
	 :(struct space *) space
{
	switch (type) {
	case HASH:
		return [HashIndex alloc];
	case TREE:
		return [TreeIndex alloc: key_def :space];
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
	check_key_parts(key_def, part_count, false);
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
	check_key_parts(key_def, part_count, traits->allows_partial_key);
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

/* {{{ HashIndex -- base class for all hashes. ********************/

#include <third_party/murmur_hash2.c>

static inline struct index_key *
tuple_key(struct tuple *tuple)
{
	return (struct index_key *) tuple->data;
}

static struct tuple *
create_key_tuple(struct key_def *key_def)
{
	struct tuple *tuple = tuple_alloc(sizeof(struct index_key), NULL);
	tuple->flags |= KEY_TUPLE;

	struct index_key *index_key = tuple_key(tuple);
	index_key->data = NULL;
	index_key->part_count = key_def->part_count;

	index_key->parts = malloc(sizeof(struct key_part)
				  * key_def->part_count);
	if (index_key->parts == NULL)
		abort();

	index_key->part_desc = malloc(sizeof(struct field_desc)
				      * key_def->part_count);
	if (index_key->part_desc == NULL)
		abort();

	for (int i = 0; i < key_def->part_count; i++) {
		index_key->part_desc[i].type = key_def->parts[i].type;
		index_key->part_desc[i].base = 0;
		index_key->part_desc[i].disp = 0;

		index_key->parts[i].type = key_def->parts[i].type;
		index_key->parts[i].fieldno = i;
	}

	return tuple;
}

static void
free_key_tuple(struct tuple *tuple)
{
	struct index_key *index_key = tuple_key(tuple);
	free(index_key->part_desc);
	tuple_free(tuple);
}

static void
set_key_tuple_data(struct tuple *tuple, void *key, int part_count)
{
	struct index_key *index_key = tuple_key(tuple);
	assert(index_key->part_count == part_count);
	index_key->data = key;

	for (int part = 1; part < part_count; part++) {
		u32 len = load_varint32((void**) &key);
		key += len;
		index_key->part_desc[part].disp = key - index_key->data;
	}
}

static void
set_hash_data(struct index_key *index_key, Index *index, struct tuple *tuple)
{
	if ((tuple->flags & KEY_TUPLE) != 0) {
		*index_key = *tuple_key(tuple);
	} else {
		index_key->data = tuple->data;
		index_key->part_count = index->key_def->part_count;
		index_key->part_desc = index->space->field_desc;
		index_key->parts = index->key_def->parts;
	}
}

static void
check_tuple(HashIndex *index, struct tuple *tuple)
{
	struct index_key index_key;
	set_hash_data(&index_key, index, tuple);

	for (int part = 0; part < index_key.part_count; part++) {
		int field = index_key.parts[part].fieldno;
		u8 *data = index_key.data + index_key.part_desc[field].disp;
		if (index_key.part_desc[field].base) {
			assert((tuple->flags & IN_SPACE) != 0);
			assert((tuple->flags & KEY_TUPLE) == 0);
			data += space_get_base_offset(index->space, tuple,
						      index_key.part_desc[field].base);
		}
		u32 len = load_varint32((void**) &data);
		if (index_key.part_desc[field].type == NUM) {
			if (len != 4)
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
		} else if (index_key.part_desc[field].type == NUM64) {
			if (len != 8)
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		}
	}
}

static uint32_t
hash_tuple(HashIndex *index, struct tuple *tuple)
{
	struct index_key index_key;
	set_hash_data(&index_key, index, tuple);

	uint32_t h = 13;
	for (int part = 0; part < index_key.part_count; part++) {
		int field = index_key.parts[part].fieldno;
		u8 *data = index_key.data + index_key.part_desc[field].disp;
		if (index_key.part_desc[field].base) {
			assert((tuple->flags & IN_SPACE) != 0);
			assert((tuple->flags & KEY_TUPLE) == 0);
			data += space_get_base_offset(index->space, tuple,
						      index_key.part_desc[field].base);
		}
		u32 len = load_varint32((void**) &data);
		h = MurmurHash2(data, len, h);
	}

	return h;
}

static int
hash_tuple_eq(HashIndex *index, struct tuple *tuple_a, struct tuple *tuple_b)
{
	struct index_key index_key_a;
	struct index_key index_key_b;
	set_hash_data(&index_key_a, index, tuple_a);
	set_hash_data(&index_key_b, index, tuple_b);

	if (index_key_a.part_count != index_key_b.part_count)
		return 0;

	for (int part = 0; part < index_key_a.part_count; part++) {
		int field_a = index_key_a.parts[part].fieldno;
		int field_b = index_key_b.parts[part].fieldno;

		u8 *data_a = index_key_a.data + index_key_a.part_desc[field_a].disp;
		if (index_key_a.part_desc[field_a].base) {
			assert((tuple_a->flags & IN_SPACE) != 0);
			assert((tuple_a->flags & KEY_TUPLE) == 0);
			data_a += space_get_base_offset(index->space, tuple_a,
							index_key_a.part_desc[field_a].base);
		}

		u8 *data_b = index_key_b.data + index_key_b.part_desc[field_b].disp;
		if (index_key_b.part_desc[field_b].base) {
			assert((tuple_b->flags & IN_SPACE) != 0);
			assert((tuple_b->flags & KEY_TUPLE) == 0);
			data_b += space_get_base_offset(index->space, tuple_b,
							index_key_b.part_desc[field_b].base);
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

#define mh_name _tuple_table
#define mh_ext_t HashIndex *
#define mh_val_t struct tuple *
#define mh_hash(x, v) hash_tuple(x, v)
#define mh_eq(x, a, b) hash_tuple_eq(x, (a), (b))
#define MH_SOURCE 1
#include <mhash-val.h>

struct hash_iterator {
	struct iterator base; /* Must be the first member. */
	struct mh_tuple_table_t *hash;
	mh_int_t h_pos;
};

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

	while (it->h_pos != mh_end(it->hash)) {
		if (mh_exist(it->hash, it->h_pos))
			return mh_value(it->hash, it->h_pos++);
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


@implementation HashIndex

+ (struct index_traits *) traits
{
	return &hash_index_traits;
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init: key_def_arg :space_arg];
	if (self) {
		hash = mh_tuple_table_init();
		hash->ext = self;
		key_tuple = create_key_tuple(key_def);
	}
	return self;
}

- (void) free
{
	free_key_tuple(key_tuple);
	mh_tuple_table_destroy(hash);
	[super free];
}

- (void) reserve: (u32) n_tuples
{
	mh_tuple_table_reserve(hash, n_tuples);
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
	return mh_size(hash);
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
	set_key_tuple_data(key_tuple, key, part_count);
	check_tuple(self, key_tuple);
	mh_int_t k = mh_tuple_table_get(hash, key_tuple);
	struct tuple *ret = NULL;
	if (k != mh_end(hash))
		ret = mh_value(hash, k);
	return ret;
}

- (struct tuple *) findByTuple: (struct tuple *) tuple
{
	check_tuple(self, tuple);
	mh_int_t k = mh_tuple_table_get(hash, tuple);
	struct tuple *ret = NULL;
	if (k != mh_end(hash))
		ret = mh_value(hash, k);
	return ret;
}

- (void) remove: (struct tuple *) tuple
{
	check_tuple(self, tuple);
	mh_int_t k = mh_tuple_table_get(hash, tuple);
	if (k != mh_end(hash))
		mh_tuple_table_del(hash, k);
}

- (void) replace: (struct tuple *) old_tuple
	:(struct tuple *) new_tuple
{
	if (old_tuple != NULL) {
		check_tuple(self, old_tuple);
		mh_int_t k = mh_tuple_table_get(hash, old_tuple);
		if (k != mh_end(hash))
			mh_tuple_table_del(hash, k);
	}
	mh_tuple_table_put(hash, new_tuple, NULL);
}

- (void) initIterator: (struct iterator *) iterator :(enum iterator_type) type
{
	assert(iterator->next == hash_iterator_next);
	struct hash_iterator *it = hash_iterator(iterator);

	if (type == ITER_REVERSE)
		tnt_raise(IllegalParams, :"hash iterator is forward only");

	it->base.next_equal = 0; /* Should not be used if not positioned. */
	it->h_pos = mh_begin(hash);
	it->hash = hash;
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

	set_key_tuple_data(key_tuple, key, part_count);
	check_tuple(self, key_tuple);
	it->h_pos = mh_tuple_table_get(hash, key_tuple);

	it->hash = hash;
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
