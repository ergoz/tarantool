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
#include "hash_index.h"
#include "tree_index.h"
#include "bitset_index.h"
#include "tuple.h"
#include "say.h"
#include "exception.h"
#include "space.h"
#include "pickle.h"
#include "palloc.h"
#include "fiber.h"

static struct index_traits index_traits = {
	.allows_partial_key = false,
};

const char *field_data_type_strs[] = {"UNKNOWN", "NUM", "NUM64", "STR", "\0"};
STRS(index_type, INDEX_TYPE);
STRS(iterator_type, ITERATOR_TYPE);

/* {{{ Utilities. **********************************************/

/**
 * Check if replacement of an old tuple with a new one is
 * allowed.
 */
uint32_t
replace_check_dup(struct tuple *old_tuple,
		  struct tuple *dup_tuple,
		  enum dup_replace_mode mode)
{
	if (dup_tuple == NULL) {
		if (mode == DUP_REPLACE) {
			/*
			 * dup_replace_mode is DUP_REPLACE, and
			 * a tuple with the same key is not found.
			 */
			return ER_TUPLE_NOT_FOUND;
		}
	} else { /* dup_tuple != NULL */
		if (dup_tuple != old_tuple &&
		    (old_tuple != NULL || mode == DUP_INSERT)) {
			/*
			 * There is a duplicate of new_tuple,
			 * and it's not old_tuple: we can't
			 * possibly delete more than one tuple
			 * at once.
			 */
			return ER_TUPLE_FOUND;
		}
	}
	return 0;
}

void
index_validate_key(Index *self, enum iterator_type type, const void *key,
		   u32 part_count)
{
	if (part_count == 0 && type == ITER_ALL) {
		assert(key == NULL);
		return;
	}

	if (part_count > self->key_def->part_count)
		tnt_raise(ClientError, :ER_KEY_PART_COUNT,
			  self->key_def->part_count, part_count);

	if ((!self->traits->allows_partial_key) &&
			part_count < self->key_def->part_count) {
		tnt_raise(ClientError, :ER_EXACT_MATCH,
			  self->key_def->part_count, part_count);
	}

	const void *key_data = key;
	for (u32 part = 0; part < part_count; part++) {
		u32 field_size = load_varint32(&key_data);

		u32 field_no = self->key_def->parts[part].fieldno;

		assert (field_no < self->space->max_fieldno);
		enum field_data_type field_type =
				self->space->field_types[field_no];

		if (field_type == NUM && field_size != sizeof(u32))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");

		if (field_type == NUM64 && field_size != sizeof(u64))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");

		key_data += field_size;
	}
}

uint32_t
tuple_write_key_size(const struct tuple *tuple, const struct key_def *key_def)
{
	uint32_t size = 0;
	for (uint32_t part = 0; part < key_def->part_count; part++) {
		uint32_t field_no = key_def->parts[part].fieldno;
		const void *field = tuple_field(tuple, field_no);
		uint32_t field_size = load_varint32(&field);
		size += field_size + varint32_sizeof(field_size);
	}

	return size;
}

void *
tuple_write_key(const struct tuple *tuple, const struct key_def *key_def,
	       void *buf)
{
	uint8_t *begin = buf;
	uint8_t *cur = begin;
	for (uint32_t part = 0; part < key_def->part_count; part++) {
		uint32_t field_no = key_def->parts[part].fieldno;
		const void *field = tuple_field(tuple, field_no);
		uint32_t field_size = load_varint32(&field);
		cur = save_varint32(cur, field_size);
		memcpy(cur, field, field_size);
		cur += field_size;
	}

	return cur;
}

static int
tuple_compare_field(const void *field_a, const void *field_b,
			  enum field_data_type type)
{
	uint32_t field_size_a = load_varint32(&field_a);
	uint32_t field_size_b = load_varint32(&field_b);

	switch (type) {
	case NUM:
	{
		assert (field_size_a == sizeof(uint32_t));
		assert (field_size_b == sizeof(uint32_t));
		uint32_t a = *(uint32_t *) field_a;
		uint32_t b = *(uint32_t *) field_b;
		return a < b ? -1 : (a > b);
	}
	case NUM64:
	{
		assert (field_size_a == sizeof(uint64_t));
		assert (field_size_b == sizeof(uint64_t));
		uint64_t a = *(uint64_t *) field_a;
		uint64_t b = *(uint64_t *) field_b;
		return a < b ? -1 : (a > b);
	}
	case STRING:
	{
		int cmp = memcmp(field_a, field_b, field_size_a);
		if (cmp != 0)
			return cmp;

		if (field_size_a > field_size_b) {
			return 1;
		} else if (field_size_a < field_size_b){
			return -1;
		} else {
			return 0;
		}
	}
	default:
		assert (false);
	}
}

int
tuple_compare(const struct tuple *tuple_a, const struct tuple *tuple_b,
	      const struct key_def *key_def)
{
	for (uint32_t part = 0; part < key_def->part_count; part++) {
		uint32_t field_no = key_def->parts[part].fieldno;
		const void *field_a = tuple_field(tuple_a, field_no);
		const void *field_b = tuple_field(tuple_b, field_no);

		int r = tuple_compare_field(field_a, field_b,
					    key_def->parts[part].type);

		if (r != 0) {
			return r;
		}
	}

	return 0;
}

int
tuple_compare_dup(const struct tuple *tuple_a, const struct tuple *tuple_b,
		  const struct key_def *key_def)
{
	int r = tuple_compare(tuple_a, tuple_b, key_def);
	if (r != 0) {
		return r;
	}

	return tuple_a < tuple_b ? -1 : (tuple_a > tuple_b);
}


int
tuple_compare_with_key(const struct tuple *tuple_a, const void *key,
		       uint32_t part_count, const struct key_def *key_def)
{
	part_count = MIN(part_count, key_def->part_count);
	for (uint32_t part = 0; part < part_count; part++) {
		uint32_t field_no = key_def->parts[part].fieldno;
		const void *field_a = tuple_field(tuple_a, field_no);

		int r = tuple_compare_field(field_a, key,
					    key_def->parts[part].type);
		uint32_t key_size = load_varint32(&key);
		key = (uint8_t *) key + key_size;

		if (r != 0) {
			return r;
		}
	}

	return 0;
}

/* }}} */

/* {{{ Index -- base class for all indexes. ********************/

@implementation Index

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
		return [HashIndex alloc: key_def :space];
	case TREE:
		return [TreeIndex alloc];
	case BITSET:
		return [BitsetIndex alloc];
	default:
		assert(false);
	}

	return NULL;
}

- (id) init: (struct key_def *) key_def_arg :(struct space *) space_arg
{
	self = [super init];
	if (self == NULL)
		return NULL;

	traits = [object_getClass(self) traits];
	key_def = key_def_arg;
	space = space_arg;
	position = [self allocIterator];

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

- (struct tuple *) random: (u32) rnd
{
	(void) rnd;
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (struct tuple *) findByKey: (const void *) key :(u32) part_count
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

- (struct tuple *) replace: (struct tuple *) old_tuple
			  : (struct tuple *) new_tuple
			  : (enum dup_replace_mode) mode
{
	(void) old_tuple;
	(void) new_tuple;
	(void) mode;
	[self subclassResponsibility: _cmd];
	return NULL;
}

- (struct iterator *) allocIterator
{
	[self subclassResponsibility: _cmd];
	return NULL;
}


- (void) initIterator: (struct iterator *) iterator
	:(enum iterator_type) type
	:(void *) key :(u32) part_count
{
	(void) iterator;
	(void) type;
	(void) key;
	(void) part_count;
	[self subclassResponsibility: _cmd];
}

@end

/* }}} */
