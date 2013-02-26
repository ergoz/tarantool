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
#include "space.h"
#include <stdlib.h>
#include <string.h>
#include <cfg/tarantool_box_cfg.h>
#include <cfg/warning.h>
#include <tarantool.h>
#include <lua/init.h>
#include <exception.h>
#include "tuple.h"
#include <pickle.h>
#include <palloc.h>
#include <assoc.h>
#include <fiber.h>
#include <tbuf.h>

#include <box/box.h>
#include "port.h"
#include "request.h"
#include "box_lua_space.h"

bool secondary_indexes_enabled = false;
bool primary_indexes_enabled = false;

/*
 * Space meta
 */

struct space_meta_field_def {
	u32 field_no;
	enum field_data_type field_type;
};

struct space_meta {
	u32 space_no;
	char name[BOX_SPACE_NAME_MAXLEN];
	u32 arity;
	u32 flags;
	u32 max_fieldno;
	u32 field_defs_count;
	struct space_meta_field_def field_defs[0];
};

static u32
space_meta_calc_load_size_v1(const void *d, u32 field_count);

static u32
space_meta_calc_save_size_v1(const struct space_meta *meta);

static const void *
space_meta_load_v1(struct space_meta *meta, const void *d, u32 field_count);

static void *
space_meta_save_v1(void *d, u32 *p_part_count, const struct space_meta *meta);

/*
 * Index meta
 */

struct index_meta_part {
	u32 field_no;
};

struct index_meta {
	u32 index_no;
	u32 space_no;
	char name[BOX_INDEX_NAME_MAXLEN];
	enum index_type type;
	bool is_unique;
	u32 max_fieldno;
	u32 part_count;
	struct index_meta_part parts[0];
};

static u32
index_meta_calc_load_size_v1(const void *d, u32 field_count);

static const void *
index_meta_load_v1(struct index_meta *meta, const void *d, u32 field_count);

static const void *
index_meta_load_v1(struct index_meta *meta, const void *d, u32 field_count);

static void *
index_meta_save_v1(void *d, u32 *p_part_count,const struct index_meta *meta);

/*
 * Cache
 */

static struct mh_i32ptr_t *spaces_by_no;

static struct space *
space_cache_new(void);

static void
space_cache_delete(struct space *space);

static void
space_cache_update_do_index(struct space *space, struct index_meta *meta);

static void
space_cache_update_do_space(struct space *space, struct space_meta *meta);

static void
space_cache_invalidate(struct space *space);

static void
key_def_create(struct key_def *key_def, const struct index_meta *meta,
	       const enum field_data_type *field_types);

static void
key_def_destroy(struct key_def *key_def);

/*
 * System spaces
 */

static void
space_super_create(void);

/*
 * Utils
 */
#define raise_meta_error(fmt, ...) ({					\
	enum { MSG_BUF_SIZE = 1024 };					\
									\
	char msg_buf[MSG_BUF_SIZE];					\
	snprintf(msg_buf, MSG_BUF_SIZE-1, fmt, ##__VA_ARGS__);		\
	msg_buf[MSG_BUF_SIZE-1] = 0;					\
									\
	tnt_raise(IllegalParams, :msg_buf);				\
})

static struct space *
space_cache_new(void)
{
	struct space *sp = calloc(1, sizeof(*sp));
	if (unlikely(sp == NULL)) {
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, sizeof(*sp),
			  "space", "space");
	}

	sp->no = UINT32_MAX; /* set some invalid id, space_no = 0 is valid */

	rlist_create(&sp->before_triggers);

	return sp;
}

static void
space_cache_delete(struct space *space)
{
	if (space == NULL)
		return;

	space_cache_invalidate(space);

	struct mh_i32ptr_node_t no_node = { .key = space->no };
	mh_i32ptr_remove(spaces_by_no, &no_node, NULL, NULL);

	for (u32 index_no = 0 ; index_no < BOX_INDEX_MAX; index_no++) {
		if (space->index[index_no] == NULL)
			continue;

		key_def_destroy(&space->key_defs[index_no]);

		[space->index[index_no] free];
		space->index[index_no] = NULL;
	}

	struct space_trigger *trigger;
	rlist_foreach_entry(trigger, &space->before_triggers, link) {
		assert(trigger->free != NULL);
		trigger->free(trigger);
	}

	free(space->field_types);
	free(space);
}

static u32
space_meta_calc_load_size_v1(const void *d, u32 field_count)
{
	if ((field_count - 4) % 2 != 0) {
		raise_meta_error("invalid field count");
	}

	(void) d;

	u32 field_def_count = (field_count - 4) / 2;
	return sizeof(struct space_meta) + field_def_count *
			sizeof(struct space_meta_field_def);
}

static u32
space_meta_calc_save_size_v1(const struct space_meta *meta)
{
	const void *name = meta->name;
	u32 name_len = load_varint32(&name);

	return name_len + varint32_sizeof(name_len) +
			(3 + 2 * meta->field_defs_count) *
			(varint32_sizeof(sizeof(u32)) + sizeof(u32));
}

static const void *
space_meta_load_v1(struct space_meta *meta,
		   const void *d, u32 field_count)
{
	if ((field_count - 4) % 2 != 0) {
		raise_meta_error("invalid field count");
	}

	const void *name = NULL;
	u32 name_len;
	d = load_field_u32(d, &meta->space_no);
	d = load_field_str(d, &name, &name_len);
	d = load_field_u32(d, &meta->arity);
	d = load_field_u32(d, &meta->flags);

	if (meta->space_no >= BOX_SPACE_MAX) {
		raise_meta_error("invalid space_no");
	}

	if (name_len + varint32_sizeof(name_len) + 1 >=
			BOX_SPACE_NAME_MAXLEN) {
		raise_meta_error("name is too long");
	}

	assert(name_len + varint32_sizeof(name_len) < BOX_SPACE_NAME_MAXLEN);
	memset(meta->name, 0, sizeof(meta->name));
	save_field_str(meta->name, name, name_len);

	meta->max_fieldno = 0;
	meta->field_defs_count = (field_count - 4) / 2;
	for (u32 i = 0; i < meta->field_defs_count; i++) {
		/* field no */
		u32 field_no;
		d = load_field_u32(d, &field_no);

		/* field type */
		u32 field_type;
		d = load_field_u32(d, &field_type);

		if (meta->max_fieldno < field_no + 1) {
			meta->max_fieldno = field_no + 1;
		}

		meta->field_defs[i].field_no = field_no;
		meta->field_defs[i].field_type = field_type;
	}

	/*
	 * Check type definitions
	 */
	enum field_data_type *new_types = p0alloc(fiber->gc_pool,
			meta->max_fieldno * sizeof(*new_types));

	for (u32 i = 0; i < meta->field_defs_count; i++) {
		if (meta->field_defs[i].field_no >= BOX_FIELD_MAX) {
			raise_meta_error("field_def=%u, invalid field_no", i);
		}
		if (meta->field_defs[i].field_type == UNKNOWN ||
		    meta->field_defs[i].field_type >= field_data_type_MAX) {
			raise_meta_error("field_def=%u invalid field_type", i);
		}

		if (new_types[meta->field_defs[i].field_no] != UNKNOWN) {
			raise_meta_error("field_def=%u duplicate field_no", i);
		}

		new_types[meta->field_defs[i].field_no] =
				meta->field_defs[i].field_type;
	}

	return d;
}

static void *
space_meta_save_v1(void *d, u32 *p_part_count, const struct space_meta *meta)
{
	const void *name = meta->name;
	u32 name_len = load_varint32(&name);

	d = save_field_u32(d, meta->space_no);
	d = save_field_str(d, name, name_len);
	d = save_field_u32(d, meta->arity);
	d = save_field_u32(d, meta->flags);
	for (u32 i = 0; i < meta->field_defs_count; i++) {
		d = save_field_u32(d, meta->field_defs[i].field_no);
		d = save_field_u32(d, meta->field_defs[i].field_type);
	}

	*p_part_count = 4 + 2 * meta->field_defs_count;

	return d;
}

static struct space_meta *
space_meta_from_tuple(const struct tuple *tuple)
{
	struct space_meta *meta;
	const void *d = tuple->data;
	u32 m_size = space_meta_calc_load_size_v1(d, tuple->field_count);
	meta = p0alloc(fiber->gc_pool, m_size);
	d = space_meta_load_v1(meta, d, tuple->field_count);
	assert (d = tuple->data + tuple->bsize);
	return meta;
}

static struct tuple *
space_meta_to_tuple(const struct space_meta *meta)
{
	u32 size = space_meta_calc_save_size_v1(meta);
	struct tuple *tuple = tuple_alloc(size);
	void *d = tuple->data;
	d = space_meta_save_v1(d, &tuple->field_count, meta);
	assert(d = tuple->data + tuple->bsize);
	return tuple;
}

static u32
index_meta_calc_load_size_v1(const void *d, u32 field_count)
{
	(void) d;

	u32 fields_count = (field_count - 5);
	return sizeof(struct index_meta) + fields_count * sizeof(u32);
}

static u32
index_meta_calc_save_size_v1(const struct index_meta *meta)
{
	const void *name = meta->name;
	u32 name_len = load_varint32(&name);

	return name_len + varint32_sizeof(name_len) +
			4 * (varint32_sizeof(sizeof(u32)) + sizeof(u32))+
			meta->part_count * sizeof(*meta->parts);
}

static const void *
index_meta_load_v1(struct index_meta *meta, const void *d, u32 field_count)
{
	const void *name = NULL;
	u32 name_len;
	d = load_field_u32(d, &meta->space_no);
	d = load_field_u32(d, &meta->index_no);
	d = load_field_str(d, &name, &name_len);
	u32 type = 0;
	d = load_field_u32(d, &type);
	meta->type = type;
	u32 is_unique;
	d = load_field_u32(d, &is_unique);
	meta->is_unique = (is_unique != 0);

	if (meta->space_no >= BOX_SPACE_MAX) {
		raise_meta_error("invalid space_no");
	}

	if (meta->index_no >= BOX_INDEX_MAX) {
		raise_meta_error("invalid index_no");
	}

	if (name_len + varint32_sizeof(name_len) + 1 >=
			BOX_INDEX_NAME_MAXLEN) {
		raise_meta_error("name is too long");
	}

	assert(name_len + varint32_sizeof(name_len) < BOX_SPACE_NAME_MAXLEN);
	memset(meta->name, 0, sizeof(meta->name));
	save_field_str(meta->name, name, name_len);

	meta->max_fieldno = 0;
	meta->part_count = (field_count - 5);
	for (u32 part = 0; part < meta->part_count; part++) {
		/* field no */
		u32 field_no;
		d = load_field_u32(d, &field_no);

		if (meta->max_fieldno < field_no + 1) {
			meta->max_fieldno = field_no + 1;
		}

		meta->parts[part].field_no = field_no;
	}

	if (meta->part_count == 0) {
		raise_meta_error("at least one field is needed");
	}

	return d;
}

static void *
index_meta_save_v1(void *d, u32 *p_field_count, const struct index_meta *meta)
{
	const void *name = meta->name;
	u32 name_len = load_varint32(&name);

	d = save_field_u32(d, meta->space_no);
	d = save_field_u32(d, meta->index_no);
	d = save_field_str(d, name, name_len);
	d = save_field_u32(d, meta->type);
	d = save_field_u32(d, meta->is_unique);
	for (u32 i = 0; i < meta->part_count; i++) {
		d = save_field_u32(d, meta->parts[i].field_no);
	}

	*p_field_count = 5 + meta->part_count;

	return d;
}

static struct index_meta *
index_meta_from_tuple(const struct tuple *tuple)
{
	struct index_meta *meta;
	const void *d = tuple->data;
	u32 m_size = index_meta_calc_load_size_v1(d, tuple->field_count);
	meta = p0alloc(fiber->gc_pool, m_size);
	d = index_meta_load_v1(meta, d, tuple->field_count);
	assert (d = tuple->data + tuple->bsize);
	return meta;
}

static struct tuple *
index_meta_to_tuple(const struct index_meta *meta)
{
	u32 size = index_meta_calc_save_size_v1(meta);
	struct tuple *tuple = tuple_alloc(size);
	void *d = tuple->data;
	d = index_meta_save_v1(d, &tuple->field_count, meta);
	assert(d = tuple->data + tuple->bsize);
	return tuple;
}


static void
space_systrigger_free(struct space_trigger *trigger)
{
	free(trigger);
}

static void
space_sysspace_check_types(const struct index_meta *index_m,
			   const enum field_data_type *types, u32 max_fieldno)
{
	for (u32 part = 0; part < index_m->part_count; part++){
		u32 field_no = index_m->parts[part].field_no;
		if (field_no >= max_fieldno || types[field_no] == UNKNOWN) {
			raise_meta_error("pk=%u: field %u is not defined as "
					  "required by index %u",
					  index_m->space_no,
					  index_m->parts[part].field_no,
					  index_m->index_no);
		}
	}
}

static struct tuple *
space_sysspace_check_replace(struct space_trigger *tr, struct space *sysspace,
			     struct tuple *old_tuple, struct tuple *new_tuple)
{
	(void) tr;
	(void) sysspace;
	assert (new_tuple != NULL);

	/*
	 * Parse new space meta
	 */
	struct space_meta *new_space_m = space_meta_from_tuple(new_tuple);

	/*
	 * Check that meta for super spaces is not changing
	 */
	if (new_space_m->space_no == BOX_SYSSPACE_NO ||
	    new_space_m->space_no == BOX_SYSINDEX_NO) {
		raise_meta_error("pk=%u: cannot change system spaces",
				 new_space_m->space_no);
	}

	if (old_tuple == NULL)
		return new_tuple;

	/*
	 * Parse old space meta
	 */
	struct space_meta *old_m = space_meta_from_tuple(old_tuple);
	assert (old_m->space_no == new_space_m->space_no);

	/*
	 * Check that all indexes fields are defined
	 */
	enum field_data_type *new_types = p0alloc(fiber->gc_pool,
			new_space_m->max_fieldno * sizeof(*new_types));
	for (u32 i = 0; i < new_space_m->field_defs_count; i++) {
		new_types[new_space_m->field_defs[i].field_no] =
				new_space_m->field_defs[i].field_type;
	}

	char space_key[varint32_sizeof(BOX_SPACE_MAX) + sizeof(u32)];
	save_field_u32(space_key, new_space_m->space_no);
	struct space *sysindex = space_find_by_no(BOX_SYSINDEX_NO);
	Index *sysindex_pk = index_find_by_no(sysindex, 0);
	struct iterator *it = [sysindex_pk allocIterator];
	@try {
		[sysindex_pk initIterator: it :ITER_EQ :space_key :1];

		struct tuple *index_tuple = NULL;
		while ( (index_tuple = it->next(it)) != NULL) {
			struct index_meta *index_m =
					index_meta_from_tuple(index_tuple);

			space_sysspace_check_types(index_m, new_types,
						   new_space_m->max_fieldno);
		}
	} @finally {
		it->free(it);
	}

	/*
	 * Try to find a cache entry for this space
	 */
	struct space *space = NULL;
	bool has_data = false;
	const struct mh_i32ptr_node_t node = { .key = old_m->space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &node, NULL, NULL);
	if (k != mh_end(spaces_by_no)) {
		space = mh_i32ptr_node(spaces_by_no, k)->val;
		has_data = space->index[0] != NULL &&
			[space->index[0] size] > 0;
		space_cache_invalidate(space);
	}

	if (!has_data)
		return new_tuple;

	assert (has_data);

	/*
	 * Check arity
	 */
	if (new_space_m->arity != 0 && new_space_m->arity != old_m->arity) {
		raise_meta_error("pk=%u: "
				 "arity can only be changed to 0, "
				 "because space is not empty",
				 new_space_m->space_no);
	}

	/*
	 * Check flags
	 */
	if (new_space_m->flags != old_m->flags) {
		raise_meta_error("pk=%u: "
				 "flags are read-only, "
				 "because space is not empty",
				 new_space_m->space_no);
	}

	/*
	 * Check that field definitions are not changing
	 */
	if (new_space_m->field_defs_count != old_m->field_defs_count) {
		raise_meta_error("pk=%u: "
				 "field defs are read-only, "
				 "because space is not empty",
				 new_space_m->space_no);
	}

	for (u32 i = 0; i < old_m->field_defs_count; i++) {
		u32 field_no = old_m->field_defs[i].field_no;

		if (new_types[field_no] != old_m->field_defs[i].field_type) {
			raise_meta_error("pk=%u: "
					 "field defs are read-only, "
					 "because space is not empty",
					 new_space_m->space_no);
		}
	}

	return new_tuple;
}

static struct tuple *
space_sysspace_check_delete(struct space_trigger *tr, struct space *sysspace,
			    struct tuple *old_tuple) {
	assert (old_tuple != NULL);
	(void) tr;
	(void) sysspace;

	/*
	 * Parse old space meta
	 */
	struct space_meta *old_m = space_meta_from_tuple(old_tuple);

	/*
	 * Check that meta for super spaces is not changing
	 */
	if (old_m->space_no == BOX_SYSSPACE_NO ||
	    old_m->space_no == BOX_SYSINDEX_NO) {
		raise_meta_error("pk=%u: cannot change system spaces",
				 old_m->space_no);
	}

	/*
	 * Try to find a cache entry for this space
	 */
	struct space *space = NULL;
	bool has_data = false;
	const struct mh_i32ptr_node_t node = { .key = old_m->space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &node, NULL, NULL);
	if (k != mh_end(spaces_by_no)) {
		space = mh_i32ptr_node(spaces_by_no, k)->val;
		has_data = space->index[0] != NULL &&
			[space->index[0] size] > 0;
	}

	/*
	 * Check that the space is empty
	 */
	if (has_data) {
		raise_meta_error("pk=%u: cannot remove,"
				 "because space is not empty",
				 old_m->space_no);
	}

	/*
	 * Check that the space does not have indexes
	 */
	char space_key[varint32_sizeof(BOX_SPACE_MAX) + sizeof(u32)];
	save_field_u32(space_key, old_m->space_no);
	struct space *sysindex = space_find_by_no(BOX_SYSINDEX_NO);
	Index *sysindex_pk = index_find_by_no(sysindex, 0);
	struct iterator *it = [sysindex_pk allocIterator];
	@try {
		[sysindex_pk initIterator: it :ITER_EQ :space_key :1];

		if (it->next(it) != NULL) {
			raise_meta_error("pk=%u: "
					 "cannot remove, because "
					 "space has indexes",
					 old_m->space_no);
		}
	} @finally {
		it->free(it);
	}

	/*
	 * Remove the cache entry
	 */
	if (space != NULL) {
		space_cache_delete(space);
	}

	return NULL;
}

static struct tuple *
space_sysspace_check(struct space_trigger *trigger, struct space *sysspace,
		     struct tuple *old_tuple, struct tuple *new_tuple)
{
	assert (old_tuple != NULL || new_tuple != NULL);

	if (new_tuple != NULL) {
		return space_sysspace_check_replace(trigger, sysspace,
						    old_tuple, new_tuple);
	} else /* old_tuple != NULL */ {
		return space_sysspace_check_delete(trigger, sysspace,
						   old_tuple);
	}

	return NULL;
}

static struct tuple *
space_sysindex_check_replace(struct space_trigger *tr, struct space *sysindex,
			     struct tuple *old_tuple, struct tuple *new_tuple)
{
	assert (new_tuple != NULL);
	(void) tr;
	(void) sysindex;

	struct space *sysspace = space_find_by_no(BOX_SYSSPACE_NO);
	Index *sysspace_pk = index_find_by_no(sysspace, 0);

	/*
	 * Parse new index meta
	 */
	struct index_meta *new_index_m = index_meta_from_tuple(new_tuple);

	/*
	 * Check that meta for super spaces is not changing
	 */
	if (new_index_m->space_no == BOX_SYSSPACE_NO ||
	    new_index_m->space_no == BOX_SYSINDEX_NO) {
		raise_meta_error("pk=%u,%u: cannot change system spaces",
				 new_index_m->space_no, new_index_m->index_no);
	}

	/*
	 * Check that space exist
	 */
	char space_key[varint32_sizeof(BOX_SPACE_MAX) + sizeof(u32)];
	save_field_u32(space_key, new_index_m->space_no);
	struct tuple *space_tuple = [sysspace_pk findByKey :space_key :1];
	if (space_tuple == NULL) {
		raise_meta_error("pk=%u,%u: space does not exist",
				  new_index_m->space_no, new_index_m->index_no);
	}

	/*
	 * Check index parameters
	 */
	if (new_index_m->type >= index_type_MAX) {
		raise_meta_error("pk=%u,%u: invalid index type",
				 new_index_m->space_no, new_index_m->index_no);
	}

	if (new_index_m->index_no == 0 && !new_index_m->is_unique) {
		raise_meta_error("pk=%u,%u: primary key must be unique",
				 new_index_m->space_no, new_index_m->index_no);
	}

	if (new_index_m->part_count > 1 &&
	    (new_index_m->type == HASH /* || new_index_m->type == BITSET */)) {
		raise_meta_error("pk=%u,%u: this index is single-part only",
				 new_index_m->space_no, new_index_m->index_no);
	}

	if (!new_index_m->is_unique &&
	    (new_index_m->type == HASH /* || new_index_m->type == BITSET */)) {
		raise_meta_error("pk=%u,%u: this index is unique only",
				 new_index_m->space_no, new_index_m->index_no);
	}

	/*
	 * Parse space meta
	 */
	struct space_meta *space_m = space_meta_from_tuple(space_tuple);

	/*
	 * Check that all indexed fields are defined
	 */
	enum field_data_type *types = p0alloc(fiber->gc_pool,
			space_m->max_fieldno * sizeof(*types));
	for (u32 i = 0; i < space_m->field_defs_count; i++) {
		types[space_m->field_defs[i].field_no] =
				space_m->field_defs[i].field_type;
	}
	space_sysspace_check_types(new_index_m, types, space_m->max_fieldno);

	/*
	 * Try to find a cache entry for this space
	 */
	struct space *space = NULL;
	bool has_data = false;
	const struct mh_i32ptr_node_t node = { .key = new_index_m->space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &node, NULL, NULL);
	if (k != mh_end(spaces_by_no)) {
		space = mh_i32ptr_node(spaces_by_no, k)->val;
		has_data = space->index[0] != NULL &&
			[space->index[0] size] > 0;

		space_cache_invalidate(space);
	}

	if (!has_data)
		return new_tuple;

	if (old_tuple == NULL) {
		raise_meta_error("pk=%u,%u: cannot add the index, "
				 "because space is not empty",
				 new_index_m->space_no, new_index_m->index_no);
	}

	/*
	 * Parse old index meta
	 */
	struct index_meta *old_index_m = index_meta_from_tuple(old_tuple);

	assert (old_index_m->index_no == new_index_m->index_no);
	assert (old_index_m->space_no == new_index_m->space_no);
	(void) old_index_m;

	bool key_def_changed = false;
	if (old_index_m->type == new_index_m->type &&
	    old_index_m->is_unique == new_index_m->is_unique &&
	    old_index_m->part_count == new_index_m->part_count) {

		for (u32 part = 0; part < new_index_m->part_count; part++) {
			if (old_index_m->parts[part].field_no !=
			    new_index_m->parts[part].field_no) {
				key_def_changed = true;
				break;
			}
		}

	} else {
		key_def_changed = true;
	}

	if (key_def_changed) {
		raise_meta_error("pk=%u,%u: cannot change the index, "
				 "because space is not empty",
				 new_index_m->space_no, new_index_m->index_no);
	}

	return new_tuple;
}

static struct tuple *
space_sysindex_check_delete(struct space_trigger *tr, struct space *sysindex,
			    struct tuple *old_tuple)
{
	assert (old_tuple != NULL);
	(void) tr;
	(void) sysindex;

	/*
	 * Parse old index meta
	 */
	struct index_meta *old_index_m = index_meta_from_tuple(old_tuple);

	/*
	 * Check that meta for super spaces is not changing
	 */
	if (old_index_m->space_no == BOX_SYSSPACE_NO ||
	    old_index_m->space_no == BOX_SYSINDEX_NO) {
		raise_meta_error("pk=%u,%u: cannot change system spaces",
				 old_index_m->space_no, old_index_m->index_no);
	}

	/*
	 * Try to find a cache entry for this space
	 */
	struct space *space = NULL;
	bool space_has_data = false;
	const struct mh_i32ptr_node_t node = { .key = old_index_m->space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &node, NULL, NULL);
	if (k != mh_end(spaces_by_no)) {
		space = mh_i32ptr_node(spaces_by_no, k)->val;
		space_has_data = space->index[0] != NULL &&
				[space->index[0] size] > 0;
	}

	if (space_has_data) {
		raise_meta_error("pk=%u,%u: cannot change the index, "
				 "because space is not empty",
				 old_index_m->space_no, old_index_m->index_no);
	}

	/*
	 * Invalidate cache
	 */
	if (space != NULL) {
		space_cache_delete(space);
	}

	return NULL;
}

static struct tuple *
space_sysindex_check(struct space_trigger *trigger, struct space *sysindex,
		     struct tuple *old_tuple, struct tuple *new_tuple)
{
	assert (old_tuple != NULL || new_tuple != NULL);

	if (new_tuple != NULL) {
		return space_sysindex_check_replace(trigger, sysindex,
						   old_tuple, new_tuple);
	} else /* old_tuple != NULL */ {
		return space_sysindex_check_delete(trigger, sysindex,
						   old_tuple);
	}

	return NULL;
}

static void
space_super_create(void)
{
	/*
	 * sys_space
	 */
	struct space_meta *sysspace_meta = p0alloc(fiber->gc_pool,
				sizeof(*sysspace_meta) + 4 *
				sizeof(*sysspace_meta->field_defs));
	sysspace_meta->space_no = BOX_SYSSPACE_NO;
	save_field_str(sysspace_meta->name, BOX_SYSSPACE_NAME,
		       strlen(BOX_SYSSPACE_NAME));
	sysspace_meta->arity = 0;
	sysspace_meta->flags = 0;
	sysspace_meta->max_fieldno = 4;
	sysspace_meta->field_defs_count = 4;
	sysspace_meta->field_defs[0].field_no = 0;
	sysspace_meta->field_defs[0].field_type = NUM;
	sysspace_meta->field_defs[1].field_no = 1;
	sysspace_meta->field_defs[1].field_type = STRING;
	sysspace_meta->field_defs[2].field_no = 2;
	sysspace_meta->field_defs[2].field_type = NUM;
	sysspace_meta->field_defs[3].field_no = 3;
	sysspace_meta->field_defs[3].field_type = NUM;

	struct index_meta *sysspace_idx_meta_0 = p0alloc(fiber->gc_pool,
				sizeof(*sysspace_idx_meta_0) + 1 *
				sizeof(*sysspace_idx_meta_0->parts));

	sysspace_idx_meta_0->space_no = BOX_SYSSPACE_NO;
	sysspace_idx_meta_0->index_no = 0;
	save_field_str(sysspace_idx_meta_0->name, "pk", strlen("pk"));
	sysspace_idx_meta_0->type = TREE;
	sysspace_idx_meta_0->is_unique = true;
	sysspace_idx_meta_0->max_fieldno = 1;
	sysspace_idx_meta_0->part_count = 1;
	sysspace_idx_meta_0->parts[0].field_no = 0;

	struct index_meta *sysspace_idx_meta_1 = p0alloc(fiber->gc_pool,
				sizeof(*sysspace_idx_meta_1) + 1 *
				sizeof(*sysspace_idx_meta_1->parts));

	sysspace_idx_meta_1->space_no = BOX_SYSSPACE_NO;
	sysspace_idx_meta_1->index_no = 1;
	save_field_str(sysspace_idx_meta_1->name, "name", strlen("name"));
	sysspace_idx_meta_1->type = TREE;
	sysspace_idx_meta_1->is_unique = true;
	sysspace_idx_meta_1->max_fieldno = 2;
	sysspace_idx_meta_1->part_count = 1;
	sysspace_idx_meta_1->parts[0].field_no = 1;

	/*
	 * sys_index
	 */
	struct space_meta *sysindex_meta = p0alloc(fiber->gc_pool,
				sizeof(*sysindex_meta) + 5 *
				sizeof(*sysindex_meta->field_defs));
	sysindex_meta->space_no = BOX_SYSINDEX_NO;
	save_field_str(sysindex_meta->name, BOX_SYSINDEX_NAME,
		       strlen(BOX_SYSINDEX_NAME));
	sysindex_meta->arity = 0;
	sysindex_meta->max_fieldno = 5;
	sysindex_meta->flags = 0;
	sysindex_meta->field_defs_count = 5;
	sysindex_meta->field_defs[0].field_no = 0;
	sysindex_meta->field_defs[0].field_type = NUM;
	sysindex_meta->field_defs[1].field_no = 1;
	sysindex_meta->field_defs[1].field_type = NUM;
	sysindex_meta->field_defs[2].field_no = 2;
	sysindex_meta->field_defs[2].field_type = STRING;
	sysindex_meta->field_defs[3].field_no = 3;
	sysindex_meta->field_defs[3].field_type = NUM;
	sysindex_meta->field_defs[4].field_no = 4;
	sysindex_meta->field_defs[4].field_type = NUM;

	struct index_meta *sysindex_idx_meta_0 = p0alloc(fiber->gc_pool,
				sizeof(*sysindex_idx_meta_0) + 2 *
				sizeof(*sysindex_idx_meta_0->parts));

	sysindex_idx_meta_0->space_no = BOX_SYSINDEX_NO;
	sysindex_idx_meta_0->index_no = 0;
	save_field_str(sysindex_idx_meta_0->name, "pk", strlen("pk"));
	sysindex_idx_meta_0->type = TREE;
	sysindex_idx_meta_0->is_unique = true;
	sysindex_idx_meta_0->max_fieldno = 2;
	sysindex_idx_meta_0->part_count = 2;
	sysindex_idx_meta_0->parts[0].field_no = 0;
	sysindex_idx_meta_0->parts[1].field_no = 1;

	struct index_meta *sysindex_idx_meta_1 = p0alloc(fiber->gc_pool,
				sizeof(*sysindex_idx_meta_1) + 2 *
				sizeof(*sysindex_idx_meta_1->parts));

	sysindex_idx_meta_1->space_no = BOX_SYSINDEX_NO;
	sysindex_idx_meta_1->index_no = 1;
	save_field_str(sysindex_idx_meta_1->name, "name", strlen("name"));
	sysindex_idx_meta_1->type = TREE;
	sysindex_idx_meta_1->is_unique = true;
	sysindex_idx_meta_1->max_fieldno = 3;
	sysindex_idx_meta_1->part_count = 2;
	sysindex_idx_meta_1->parts[0].field_no = 0;
	sysindex_idx_meta_1->parts[0].field_no = 2;

	struct space *sysspace = NULL;
	struct space *sysindex = NULL;
	struct tuple *sysspace_tuple = NULL;
	struct tuple *sysspace_idx_tuple_0 = NULL;
	struct tuple *sysspace_idx_tuple_1 = NULL;
	struct tuple *sysindex_tuple = NULL;
	struct tuple *sysindex_idx_tuple_0 = NULL;
	struct tuple *sysindex_idx_tuple_1 = NULL;

	@try {
		sysspace = space_cache_new();
		space_cache_update_do_space(sysspace, sysspace_meta);
		space_cache_update_do_index(sysspace, sysspace_idx_meta_0);
		space_cache_update_do_index(sysspace, sysspace_idx_meta_1);

		struct space_trigger *check;
		check = calloc(1, sizeof(*check));
		if (check == NULL) {
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
			sizeof(*check), "space", "check_trigger");
		}
		check->free = space_systrigger_free;
		check->replace = space_sysspace_check;
		rlist_add_entry(&sysspace->before_triggers, check, link);

		sysindex = space_cache_new();
		space_cache_update_do_space(sysindex, sysindex_meta);
		space_cache_update_do_index(sysindex, sysindex_idx_meta_0);
		space_cache_update_do_index(sysindex, sysindex_idx_meta_1);

		check = calloc(1, sizeof(*check));
		if (check == NULL) {
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
			sizeof(*check), "space", "check_trigger");
		}
		check->free = space_systrigger_free;
		check->replace = space_sysindex_check;
		rlist_add_entry(&sysindex->before_triggers, check, link);

		sysspace_tuple = space_meta_to_tuple(sysspace_meta);
		sysspace_idx_tuple_0 = index_meta_to_tuple(sysspace_idx_meta_0);
		sysspace_idx_tuple_1 = index_meta_to_tuple(sysspace_idx_meta_1);

		sysindex_tuple = space_meta_to_tuple(sysindex_meta);
		sysindex_idx_tuple_0 = index_meta_to_tuple(sysindex_idx_meta_0);
		sysindex_idx_tuple_1 = index_meta_to_tuple(sysindex_idx_meta_1);

		assert (sysspace->index[0] != NULL);
		[sysspace->index[0] beginBuild];
		@try {
			[sysspace->index[0] buildNext: sysspace_tuple];
			[sysspace->index[0] buildNext: sysindex_tuple];
		} @finally {
			[sysspace->index[0] endBuild];
		}

		assert (sysindex->index[0] != NULL);
		[sysindex->index[0] beginBuild];
		@try {
			[sysindex->index[0] buildNext: sysspace_idx_tuple_0];
			[sysindex->index[0] buildNext: sysspace_idx_tuple_1];
			[sysindex->index[0] buildNext: sysindex_idx_tuple_0];
			[sysindex->index[0] buildNext: sysindex_idx_tuple_1];
		} @finally {
			[sysindex->index[0] endBuild];
		}

		tuple_ref(sysspace_tuple, 1);
		tuple_ref(sysspace_idx_tuple_0, 1);
		tuple_ref(sysspace_idx_tuple_1, 1);
		tuple_ref(sysindex_tuple, 1);
		tuple_ref(sysindex_idx_tuple_0, 1);
		tuple_ref(sysindex_idx_tuple_1, 1);

		mh_int_t pos;
		struct mh_i32ptr_node_t no_node;
		no_node.key = sysspace->no;
		no_node.val = sysspace;
		pos = mh_i32ptr_replace(spaces_by_no, &no_node, NULL, NULL, NULL);
		if (pos == mh_end(spaces_by_no)) {
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
				  sizeof(no_node), "spaces_by_no", "space");
		}

		no_node.key = sysindex->no;
		no_node.val = sysindex;
		pos = mh_i32ptr_replace(spaces_by_no, &no_node, NULL, NULL, NULL);
		if (pos == mh_end(spaces_by_no)) {
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
				  sizeof(no_node), "spaces_by_no", "space");
		}

	} @catch(tnt_Exception *) {
		tuple_free(sysspace_tuple);
		tuple_free(sysspace_idx_tuple_0);
		tuple_free(sysspace_idx_tuple_1);
		tuple_free(sysindex_tuple);
		tuple_free(sysindex_idx_tuple_0);
		tuple_free(sysindex_idx_tuple_1);

		space_cache_delete(sysspace);
		space_cache_delete(sysindex);
		@throw;
	}

	sysspace->is_valid = true;
	sysindex->is_valid = true;
}

static void
key_def_create(struct key_def *key_def, const struct index_meta *meta,
	       const enum field_data_type *field_types)
{
	size_t sz = 0;

	memset(key_def, 0, sizeof(*key_def));

	key_def->type = meta->type;
	key_def->is_unique = meta->is_unique;

	key_def->part_count = meta->part_count;
	sz = sizeof(*key_def->parts) * key_def->part_count;
	key_def->parts = malloc(sz);
	if (key_def->parts == NULL)
		goto error_1;

	key_def->max_fieldno = meta->max_fieldno;

	sz = key_def->max_fieldno * sizeof(u32);
	key_def->cmp_order = malloc(sz);
	if (key_def->cmp_order == NULL)
		goto error_2;

	for (u32 field_no = 0; field_no < key_def->max_fieldno; field_no++) {
		key_def->cmp_order[field_no] = BOX_FIELD_MAX;
	}

	for (u32 part = 0; part < key_def->part_count; part++) {
		u32 field_no = meta->parts[part].field_no;
		key_def->parts[part].fieldno = field_no;
		assert(field_no < key_def->max_fieldno);
		key_def->parts[part].type = field_types[field_no];
		assert(key_def->parts[part].type != UNKNOWN);
		key_def->cmp_order[field_no] = part;
	}

	return;

error_2:
	free(key_def->parts);
error_1:
	key_def->parts = NULL;
	key_def->cmp_order = NULL;
	tnt_raise(LoggedError, :ER_MEMORY_ISSUE, sz, "space", "index");
}

static void
key_def_destroy(struct key_def *key_def)
{
	free(key_def->parts);
	free(key_def->cmp_order);
	key_def->parts = NULL;
	key_def->cmp_order = NULL;
}

static void
space_cache_invalidate(struct space *sp)
{
	sp->is_valid = false;
	if (tarantool_L == NULL)
		return;

	box_lua_space_cache_clear(tarantool_L, sp);
}

static void
space_cache_update_do_index(struct space *space, struct index_meta *meta)
{
	bool is_empty = (space->index[0] == NULL) || space_size(space) == 0;

	assert(meta->index_no < BOX_INDEX_MAX);
	assert(meta->part_count > 0);

	if (!is_empty) {
		assert (space->no == meta->space_no);
	}

	/* Check if key_def was changed */
	struct key_def *key_def = &space->key_defs[meta->index_no];
	bool key_def_changed = false;
	if (space->index[meta->index_no] == NULL) {
		key_def_changed = true;
	} else if (key_def->type != meta->type ||
		   key_def->is_unique != meta->is_unique ||
		   key_def->part_count != meta->part_count) {
		key_def_changed = true;
	} else {
		for (u32 part = 0; part < meta->part_count; part++) {
			u32 field_no = meta->parts[part].field_no;
			if (key_def->parts[part].fieldno != field_no ||
			    key_def->parts[part].type !=
					space->field_types[field_no]) {
				key_def_changed = true;
				break;
			}
		}
	}

	if (!key_def_changed) {
		/* Update the other param which do not affect key_def */
		Index *index = space->index[meta->index_no];
		assert (index != NULL);

		const void *name = meta->name;
		u32 name_len = load_varint32(&name);
		save_field_str(index->name, name, name_len);
		return;
	}

	if (!is_empty) {
		raise_meta_error("pk=%u, %u: "
				 "key definition is read-only because"
				 "space is not empty",
				 meta->space_no, meta->index_no);
	}

	assert (space->index[meta->index_no] == NULL);
#if 0
	/* Drop the old index */
	if (space->index[meta->index_no] != NULL) {
		struct key_def *key_def = &space->key_defs[meta->index_no];
		key_def_destroy(key_def);

		[space->index[meta->index_no] free];
		space->index[meta->index_no] = NULL;
		return;
	}
#endif

	/* Create a new one */
	assert (space->index[meta->index_no] == NULL);
	key_def_create(&space->key_defs[meta->index_no], meta,
		       space->field_types);
	@try {
		space->index[meta->index_no] =
			[[Index alloc :key_def->type :key_def :space]
			  init :key_def :space];
		assert (space->index[meta->index_no] != NULL);

		space->index[meta->index_no]->no = meta->index_no;
		const void *name = meta->name;
		u32 name_len = load_varint32(&name);
		save_field_str(space->index[meta->index_no]->name, name,
			       name_len);

		/*
		 * If primary indexes is not enabled then the index will be
		 * created by box_init/recovery_row procedures.
		 * Otherwise, just create an empty index.
		 */
		if (primary_indexes_enabled) {
			[space->index[meta->index_no] beginBuild];
			[space->index[meta->index_no] endBuild];
		}
	} @catch(tnt_Exception *) {
		key_def_destroy(&space->key_defs[meta->index_no]);
		if (space->index[meta->index_no] != NULL) {
			[space->index[meta->index_no] free];
			space->index[meta->index_no] = NULL;
		}
		@throw;
	}
}

static void
space_cache_update_do_space(struct space *space, struct space_meta *meta)
{
	bool is_empty = (space->index[0] == NULL) || space_size(space) == 0;

	if (!is_empty) {
		assert (space->no = meta->space_no);
	}

	space->no = meta->space_no;
	const void *name = meta->name;
	u32 name_len = load_varint32(&name);
	save_field_str(space->name, name, name_len);

	assert(is_empty || meta->arity == 0 || space->arity == meta->arity);
	space->arity = meta->arity;
	space->flags = meta->flags;

	assert(is_empty || meta->max_fieldno <= space->max_fieldno);
	if (!is_empty) {
		for (u32 i = 0; i < meta->field_defs_count; i++) {
			u32 field_no = meta->field_defs[i].field_no;
			assert(space->field_types[field_no] ==
			       meta->field_defs[field_no].field_type);
		}
	}

	space->max_fieldno = meta->max_fieldno;

	/* Allocate field_types */
	free(space->field_types);
	space->field_types = NULL;
	size_t sz = meta->max_fieldno * sizeof(*space->field_types);
	space->field_types = malloc(sz);
	if (space->field_types == NULL) {
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, sz,
			  "space", "field types");
	}
	memset(space->field_types, 0, sizeof(sz));

	/* Update field types */
	for (u32 i = 0; i < meta->field_defs_count; i++) {
		space->field_types[meta->field_defs[i].field_no] =
				meta->field_defs[i].field_type;
	}
}

static struct space *
space_cache_update(struct tuple *new_space_tuple)
{
	/*
	 * Parse new space meta
	 */
	struct space_meta *new_space_m = space_meta_from_tuple(new_space_tuple);

	say_info("Space %u: begin updating configuration...",
		 new_space_m->space_no);

	/*
	 * Try to get an existing cache entry or create a new
	 */
	struct space *space = NULL;
	struct mh_i32ptr_node_t no_node = { .key = new_space_m->space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &no_node, NULL, NULL);
	if (k == mh_end(spaces_by_no)) {
		space = space_cache_new();
		space->no = new_space_m->space_no;
		no_node.val = space;

		mh_int_t pos;
		pos = mh_i32ptr_replace(spaces_by_no, &no_node, NULL, NULL, NULL);
		if (pos == mh_end(spaces_by_no)) {
			space_cache_delete(space);
			tnt_raise(LoggedError, :ER_MEMORY_ISSUE,
				  sizeof(no_node), "spaces_by_no", "space");
		}
	} else {
		space = mh_i32ptr_node(spaces_by_no, k)->val;
	}

	assert (space != NULL);
	assert (!space->is_valid);

	/*
	 * Update space itself
	 */
	space_cache_update_do_space(space, new_space_m);

	/*
	 * Create/update indexes
	 */
	/* If space is empty then just drop all indexes in the cache */
	bool has_data = space->index[0] != NULL && [space->index[0] size] > 0;
	if (!has_data) {
		for (u32 i = 0; i < BOX_INDEX_MAX; i++) {
			if (space->index[i] == NULL)
				continue;

			key_def_destroy(&space->key_defs[i]);
			[space->index[i] free];
			space->index[i] = NULL;
		}
	}

	/* Get all indexes from sysindex and update one by one */
	char space_key[varint32_sizeof(BOX_SPACE_MAX) + sizeof(u32)];
	save_field_u32(space_key, space->no);
	struct space *sysspace_index = space_find_by_no(BOX_SYSINDEX_NO);
	Index *index = index_find_by_no(sysspace_index, 0);
	struct iterator *it = [index allocIterator];
	@try {
		[index initIterator: it :ITER_EQ :space_key :1];

		struct tuple *index_tuple = NULL;
		struct index_meta *index_meta;
		while ((index_tuple = it->next(it))) {
			/*
			 * Parse index meta
			 */
			index_meta = index_meta_from_tuple(index_tuple);

			/*
			 * Create/update the index
			 */
			space_cache_update_do_index(space, index_meta);
		}
	} @finally {
		it->free(it);
	}

	/*
	 * Perform post checks
	 */

	/* Check if pk is configured*/
	if (space->index[0] == NULL) {
		raise_meta_error("primary key is not configured");
	}

	/* Check if pk is unique */
	if (!space->key_defs[0].is_unique) {
		raise_meta_error("primary key must be unique");
	}

	/* Cache entry is valid */
	space->is_valid = true;

	say_info("Space %u: end updating configuration", space->no);

	return space;
}

static struct space *
space_cache_miss(u32 space_no)
{
	/*
	 * Try to get metadata by space no
	 */
	struct tuple *new_space_tuple = NULL;

	struct space *sysspace_space = space_find_by_no(BOX_SYSSPACE_NO);

	char space_key[varint32_sizeof(BOX_SPACE_MAX) + sizeof(u32)];
	save_field_u32(space_key, space_no);
	Index *index = index_find_by_no(sysspace_space, 0);
	struct iterator *it = [index allocIterator];
	@try {
		[index initIterator: it :ITER_EQ :space_key :1];
		new_space_tuple = it->next(it);
	} @finally {
		it->free(it);
	}

	if (new_space_tuple == NULL) {
		char name_buf[11];
		sprintf(name_buf, "%u", space_no);
		tnt_raise(ClientError, :ER_NO_SUCH_SPACE, name_buf);
	}

	return space_cache_update(new_space_tuple);
}

/* return space by its number */
struct space *
space_find_by_no(u32 space_no)
{
	const struct mh_i32ptr_node_t node = { .key = space_no };
	mh_int_t k = mh_i32ptr_get(spaces_by_no, &node, NULL, NULL);
	if (likely(k != mh_end(spaces_by_no))) {
		struct space *space = mh_i32ptr_node(spaces_by_no, k)->val;
		if (likely(space->is_valid))
			return space;
	}

	assert (space_no != BOX_SYSSPACE_NO);
	assert (space_no != BOX_SYSINDEX_NO);
	return space_cache_miss(space_no);
}

struct space *
space_find_by_name(const void *name)
{
	struct space *sysspace_space = space_find_by_no(BOX_SYSSPACE_NO);

	/* Try to lookup the space metadata by space name */
	Index *index = index_find_by_no(sysspace_space, 1);
	struct iterator *it = index->position;
	/* TODO: const void * problem is fixed in master */
	[index initIterator: it :ITER_EQ :(void *) name:1];
	struct tuple *new_space_tuple = it->next(it);
	if (new_space_tuple == NULL) {
		const void *name2 = name;
		u32 name_len = load_varint32(&name2);
		char name_buf[BOX_SPACE_NAME_MAXLEN];
		memcpy(name_buf, name2, name_len);
		name_buf[name_len] = 0;
		tnt_raise(ClientError, :ER_NO_SUCH_SPACE, name_buf);
	}

	const void *d = new_space_tuple->data;
	u32 space_no;
	load_field_u32(d, &space_no);
	return space_find_by_no(space_no);
}

Index *
index_find_by_no(struct space *sp, u32 index_no)
{
	if (index_no >= BOX_INDEX_MAX || sp->index[index_no] == NULL)
		tnt_raise(LoggedError, :ER_NO_SUCH_INDEX, index_no,
			  space_n(sp));
	return sp->index[index_no];
}

size_t
space_size(struct space *sp)
{
	return [index_find_by_no(sp, 0) size];
}

struct tuple *
space_replace(struct space *sp, struct tuple *old_tuple,
	      struct tuple *new_tuple, enum dup_replace_mode mode)
{
	u32 i = 0;

	@try {
		/* Update the primary key */
		Index *pk = index_find_by_no(sp, 0);
		assert(pk->key_def->is_unique);
		/*
		 * If old_tuple is not NULL, the index
		 * has to find and delete it, or raise an
		 * error.
		 */
		old_tuple = [pk replace: old_tuple :new_tuple :mode];

		assert(old_tuple || new_tuple);

		if (!secondary_indexes_enabled)
			return old_tuple;

		/* Update secondary keys */
		for (i = i + 1; i < BOX_INDEX_MAX; i++) {
			if (sp->index[i] == NULL)
				continue;
			Index *index = sp->index[i];
			[index replace: old_tuple :new_tuple :DUP_INSERT];
		}
		return old_tuple;
	} @catch (tnt_Exception *e) {
		/* Rollback all changes */
		for (; i > 0; i--) {
			Index *index = sp->index[i-1];
			[index replace: new_tuple: old_tuple: DUP_INSERT];
		}
		@throw;
	}
}

void
space_validate_tuple(struct space *sp, struct tuple *new_tuple)
{
	/* Check to see if the tuple has a sufficient number of fields. */
	if (new_tuple->field_count < sp->max_fieldno)
		tnt_raise(IllegalParams,
			  :"tuple must have all indexed fields");

	if (sp->arity > 0 && sp->arity != new_tuple->field_count)
		tnt_raise(IllegalParams,
			  :"tuple field count must match space cardinality");

	/* Sweep through the tuple and check the field sizes. */
	const u8 *data = new_tuple->data;
	for (u32 f = 0; f < sp->max_fieldno; ++f) {
		/* Get the size of the current field and advance. */
		u32 len = load_varint32((const void **) &data);
		data += len;
		/*
		 * Check fixed size fields (NUM and NUM64) and
		 * skip undefined size fields (STRING and UNKNOWN).
		 */
		if (sp->field_types[f] == NUM) {
			if (len != sizeof(u32))
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE,
					  "u32");
		} else if (sp->field_types[f] == NUM64) {
			if (len != sizeof(u64))
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE,
					  "u64");
		}
	}
}

void
space_free(void)
{
	mh_int_t i;
	mh_foreach(spaces_by_no, i) {
		struct space *space = mh_i32ptr_node(spaces_by_no, i)->val;
		space_cache_delete(space);
	}
}

/**
 * @brief Create a new meta tuple based on confetti space configuration
 */
static struct tuple *
space_config_convert_space(tarantool_cfg_space *cfg_space, u32 space_no)
{
	/* TODO: create space_meta first */

	u32 max_fieldno = 0;
	for (u32 i = 0; cfg_space->index[i] != NULL; ++i) {
		typeof(cfg_space->index[i]) cfg_index = cfg_space->index[i];

		/* Calculate key part count and maximal field number. */
		for (u32 part = 0; cfg_index->key_field[part] != NULL; ++part) {
			typeof(cfg_index->key_field[part]) cfg_key =
					cfg_index->key_field[part];

			if (cfg_key->fieldno == -1) {
				/* last filled key reached */
				break;
			}

			max_fieldno = MAX(max_fieldno, cfg_key->fieldno + 1);
		}
	}

	assert(max_fieldno > 0);

	enum field_data_type *field_types =
			palloc(fiber->gc_pool, max_fieldno * sizeof(u32));
	memset(field_types, 0, max_fieldno * sizeof(u32));

	u32 defined_fieldno = 0;
	for (u32 i = 0; cfg_space->index[i] != NULL; ++i) {
		typeof(cfg_space->index[i]) cfg_index = cfg_space->index[i];

		/* Calculate key part count and maximal field number. */
		for (u32 part = 0; cfg_index->key_field[part] != NULL; ++part) {
			typeof(cfg_index->key_field[part]) cfg_key =
					cfg_index->key_field[part];

			if (cfg_key->fieldno == -1) {
				/* last filled key reached */
				break;
			}

			enum field_data_type t =  STR2ENUM(field_data_type,
							   cfg_key->type);
			if (field_types[cfg_key->fieldno] == t)
				continue;

			assert (field_types[cfg_key->fieldno] == UNKNOWN);
			field_types[cfg_key->fieldno] = t;
			defined_fieldno++;
		}
	}

	char name_buf[11];
	sprintf(name_buf, "%u", space_no);
	size_t name_len = strlen(name_buf);

	size_t bsize = varint32_sizeof(name_len) + name_len +
		       (varint32_sizeof(sizeof(u32)) + sizeof(u32)) *
		       (3 + 2 * defined_fieldno);

	struct tuple *tuple = tuple_alloc(bsize);
	assert (tuple != NULL);
	tuple->field_count = 1 + 3 + 2 * defined_fieldno;

	void *d = tuple->data;
	/* space_no */
	d = save_field_u32(d, space_no);

	/* name */
	d = save_field_str(d, name_buf, name_len);

	/* arity */
	u32 arity = (cfg_space->cardinality != -1) ? cfg_space->cardinality : 0;
	d = save_field_u32(d, arity);

	u32 flags = 0;
	d = save_field_u32(d, flags);

	for (u32 fieldno = 0; fieldno < max_fieldno; fieldno++) {
		u32 type = field_types[fieldno];
		if (type == UNKNOWN)
			continue;

		d = save_field_u32(d, fieldno);
		d = save_field_u32(d, type);
		defined_fieldno--;
	}

	assert (defined_fieldno == 0);
	assert (tuple->data + tuple->bsize == d);

#if defined(DEBUG)
	struct tbuf *out = tbuf_new(fiber->gc_pool);
	tuple_print(out, tuple->field_count, tuple->data);
	say_debug("Space %u meta: %.*s",
		  space_no, (int) out->size, tbuf_str(out));
#endif /* defined(DEBUG) */

	return tuple;
}

/**
 * @brief Create a new meta tuple based on confetti index configuration
 */
static struct tuple *
space_config_convert_index(tarantool_cfg_space_index *cfg_index,
			   u32 space_no, u32 index_no)
{
	/* TODO: create index_meta first */

	u32 defined_fieldno = 0;
	/* Calculate key part count and maximal field number. */
	for (u32 part = 0; cfg_index->key_field[part] != NULL; ++part) {
		typeof(cfg_index->key_field[part]) cfg_key =
				cfg_index->key_field[part];

		if (cfg_key->fieldno == -1) {
			/* last filled key reached */
			break;
		}

		defined_fieldno++;
	}

	assert(defined_fieldno > 0);

	char name_buf[11];
	if (index_no != 0) {
		sprintf(name_buf, "%u", index_no);
	} else {
		strcpy(name_buf, "pk");
	}
	size_t name_len = strlen(name_buf);

	size_t bsize = varint32_sizeof(name_len) + name_len +
		       (varint32_sizeof(sizeof(u32)) + sizeof(u32)) *
		       (4 + defined_fieldno);

	struct tuple *tuple = tuple_alloc(bsize);
	assert (tuple != NULL);
	tuple->field_count = 1 + 4 + defined_fieldno;

	void *d = tuple->data;
	/* space_no */
	d = save_field_u32(d, space_no);

	/* index_no */
	d = save_field_u32(d, index_no);

	/* name */
	d = save_field_str(d, name_buf, name_len);

	/* type */
	u32 type = STR2ENUM(index_type, cfg_index->type);
	assert (type < index_type_MAX);
	d = save_field_u32(d, type);

	/* unique */
	u32 unique = (cfg_index->unique) ? 1 : 0;
	d = save_field_u32(d, unique);

	for (u32 part = 0; cfg_index->key_field[part] != NULL; ++part) {
		typeof(cfg_index->key_field[part]) cfg_key =
				cfg_index->key_field[part];

		if (cfg_key->fieldno == -1) {
			/* last filled key reached */
			break;
		}

		u32 fieldno = cfg_key->fieldno;
		assert (fieldno < BOX_FIELD_MAX);
		d = save_field_u32(d, fieldno);

		defined_fieldno--;
	}

	assert (defined_fieldno == 0);
	assert (tuple->data + tuple->bsize == d);

#if defined(DEBUG)
	struct tbuf *out = tbuf_new(fiber->gc_pool);
	tuple_print(out, tuple->field_count, tuple->data);
	say_debug("Space %u index %u meta: %.*s",
		  space_no, index_no, (int) out->size, tbuf_str(out));
#endif /* defined(DEBUG) */

	return tuple;
}

static struct tuple *
space_config_convert_space_memcached(u32 memcached_space)
{
	/* TODO: create space_meta first */

	const char *name = "memcached";
	u32 name_len = strlen(name);

	u32 defined_fieldno = 3;
	size_t bsize = varint32_sizeof(name_len) + name_len +
		       (varint32_sizeof(sizeof(u32)) + sizeof(u32)) *
		       (3 + 2 * defined_fieldno);

	struct tuple *tuple = tuple_alloc(bsize);
	assert (tuple != NULL);
	tuple->field_count = 1 + 3 + 2 * defined_fieldno;

	void *d = tuple->data;
	/* space_no */
	d = save_field_u32(d, memcached_space);

	/* name */
	d = save_field_str(d, name, name_len);

	/* arity */
	u32 arity = 4;
	d = save_field_u32(d, arity);

	/* flags */
	u32 flags = 0;
	d = save_field_u32(d, flags);

	u32 field_no = 0;
	u32 field_type = STRING;
	d = save_field_u32(d, field_no);
	d = save_field_u32(d, field_type);

	field_no = 2;
	field_type = STRING;
	d = save_field_u32(d, field_no);
	d = save_field_u32(d, field_type);

	field_no = 3;
	field_type = STRING;
	d = save_field_u32(d, field_no);
	d = save_field_u32(d, field_type);

	assert (tuple->data + tuple->bsize == d);
#if defined(DEBUG)
	struct tbuf *out = tbuf_new(fiber->gc_pool);
	tuple_print(out, tuple->field_count, tuple->data);
	say_debug("Space %u meta: %.*s",
		  memcached_space, (int) out->size, tbuf_str(out));
#endif /* defined(DEBUG) */

	return tuple;
}


static struct tuple *
space_config_convert_index_memcached(u32 memcached_space)
{
	/* TODO: create index_meta first */

	static const char *name = "pk";

	u32 name_len = strlen(name);

	u32 defined_fieldno = 1;
	size_t bsize = varint32_sizeof(name_len) + name_len +
		       (varint32_sizeof(sizeof(u32)) + sizeof(u32)) *
		       (4 + defined_fieldno);

	struct tuple *tuple = tuple_alloc(bsize);
	assert (tuple != NULL);
	tuple->field_count = 1 + 4 + defined_fieldno;

	void *d = tuple->data;
	/* space_no */
	d = save_field_u32(d, memcached_space);

	/* index_no */
	u32 index_no = 0;
	d = save_field_u32(d, index_no);

	/* name */
	d = save_field_str(d, name, name_len);

	/* type */
	u32 type = HASH;
	d = save_field_u32(d, type);

	/* is_unique */
	u32 is_unique = 1;
	d = save_field_u32(d, is_unique);

	u32 field_no = 0;
	d = save_field_u32(d, field_no);

	assert (tuple->data + tuple->bsize == d);
#if defined(DEBUG)
	struct tbuf *out = tbuf_new(fiber->gc_pool);
	tuple_print(out, tuple->field_count, tuple->data);
	say_debug("Space %u index %u meta: %.*s",
		  memcached_space, 0, (int) out->size, tbuf_str(out));
#endif /* defined(DEBUG) */

	return tuple;
}

static void
space_config_convert(void)
{
	struct space *sysspace_space = space_find_by_no(BOX_SYSSPACE_NO);
	struct space *sysspace_index = space_find_by_no(BOX_SYSINDEX_NO);

	Index *sysspace_pk = index_find_by_no(sysspace_space, 0);
	Index *sysindex_pk = index_find_by_no(sysspace_index, 0);

	/* exit if no spaces are configured */
	if (cfg.space == NULL) {
		return;
	}

	out_warning(0,
		    "Starting from version 1.4.9 space configuration is "
		    "stored in %s and %s meta spaces and 'space' "
		    "section in tarantool.cfg is no longer necessary.\n"

		    "Your current space configuration was automatically "
		    "imported at startup. Please save a snapshot and remove "
		    "'space' section from the configuration file to remove "
		    "this warning. ",
		    BOX_SYSSPACE_NAME, BOX_SYSINDEX_NAME);

	struct tuple *meta = NULL;
	@try {
		/* fill box spaces */
		for (u32 i = 0; cfg.space[i] != NULL; ++i) {
			tarantool_cfg_space *cfg_space = cfg.space[i];

			if (!CNF_STRUCT_DEFINED(cfg_space) || !cfg_space->enabled)
				continue;

			assert(cfg.memcached_port == 0 || i != cfg.memcached_space);
			meta = space_config_convert_space(cfg_space, i);
			tuple_ref(meta, 1);
			[sysspace_pk replace :NULL :meta :DUP_INSERT];
			meta = NULL;

			for (u32 j = 0; cfg_space->index[j] != NULL; ++j) {
				meta = space_config_convert_index(
						cfg_space->index[j], i, j);
				tuple_ref(meta, 1);
				[sysindex_pk replace :NULL: meta: DUP_INSERT];
				meta = NULL;
			}
		}

		if (cfg.memcached_port != 0) {
			meta = space_config_convert_space_memcached(
						cfg.memcached_space);
			[sysspace_pk replace :NULL :meta :DUP_INSERT];
			tuple_ref(meta, 1);
			meta = NULL;

			meta = space_config_convert_index_memcached(
						cfg.memcached_space);
			[sysindex_pk replace :NULL :meta :DUP_INSERT];
			tuple_ref(meta, 1);
			meta = NULL;
		}
	} @catch(tnt_Exception *e) {
		if (meta != NULL)  {
			tuple_free(meta);
		}

		@throw;
	}

#if 0
	/* Init config spaces */
	for (u32 i = 0; cfg.space[i] != NULL; ++i) {
		if (!CNF_STRUCT_DEFINED(cfg.space[i]) || !cfg.space[i]->enabled)
			continue;

		space_find_by_no(i);
	}
#endif
}

void
space_init(void)
{
	spaces_by_no = mh_i32ptr_new();

	/* configure system spaces */
	space_super_create();

	/* cconvert configuration */
	space_config_convert();
}

void
build_secondary_indexes(void)
{
	assert(primary_indexes_enabled == true);
	assert(secondary_indexes_enabled == false);

	mh_int_t i;
	mh_foreach(spaces_by_no, i) {
		u32 space_no = mh_i32ptr_node(spaces_by_no, i)->key;

		say_info("Space %u: begin building secondary keys...",
			 space_no);
		struct space *space = space_find_by_no(space_no);
		assert (space->is_valid);

		Index *pk = index_find_by_no(space, 0);
		for (u32 j = 1; j < BOX_INDEX_MAX; j++) {
			if (space->index[j] == NULL)
				continue;

			Index *index = index_find_by_no(space, j);
			[index build: pk];
		}

		say_info("Space %u: end building secondary keys",
			 space->no);
	}

	/* enable secondary indexes now */
	secondary_indexes_enabled = true;
}

int
check_spaces(struct tarantool_cfg *conf)
{
	/* exit if no spaces are configured */
	if (conf->space == NULL) {
		return 0;
	}

	for (size_t i = 0; conf->space[i] != NULL; ++i) {
		typeof(conf->space[i]) space = conf->space[i];

		if (i >= BOX_SPACE_MAX) {
			out_warning(0, "(space = %zu) invalid id, (maximum=%u)",
				    i, BOX_SPACE_MAX);
			return -1;
		}

		if (!CNF_STRUCT_DEFINED(space)) {
			/* space undefined, skip it */
			continue;
		}

		if (!space->enabled) {
			/* space disabled, skip it */
			continue;
		}

		if (conf->memcached_port && i == conf->memcached_space) {
			out_warning(0, "Space %zu is already used as "
				    "memcached_space.", i);
			return -1;
		}

		/* at least one index in space must be defined
		 * */
		if (space->index == NULL) {
			out_warning(0, "(space = %zu) "
				    "at least one index must be defined", i);
			return -1;
		}

		if (space->estimated_rows != 0) {
			out_warning(0, "Space %zu: estimated_rows is ignored",
				    i);
		}

		u32 max_key_fieldno = 0;

		/* check spaces indexes */
		for (size_t j = 0; space->index[j] != NULL; ++j) {
			typeof(space->index[j]) index = space->index[j];
			u32 key_part_count = 0;
			enum index_type index_type;

			/* check index bound */
			if (j >= BOX_INDEX_MAX) {
				/* maximum index in space reached */
				out_warning(0, "(space = %zu index = %zu) "
					    "too many indexed (%u maximum)", i, j, BOX_INDEX_MAX);
				return -1;
			}

			/* at least one key in index must be defined */
			if (index->key_field == NULL) {
				out_warning(0, "(space = %zu index = %zu) "
					    "at least one field must be defined", i, j);
				return -1;
			}

			/* check unique property */
			if (index->unique == -1) {
				/* unique property undefined */
				out_warning(0, "(space = %zu index = %zu) "
					    "unique property is undefined", i, j);
			}

			for (size_t k = 0; index->key_field[k] != NULL; ++k) {
				typeof(index->key_field[k]) key = index->key_field[k];

				if (key->fieldno == -1) {
					/* last key reached */
					break;
				}

				if (key->fieldno >= BOX_FIELD_MAX) {
					/* maximum index in space reached */
					out_warning(0, "(space = %zu index = %zu) "
						    "invalid field number (%u maximum)",
						    i, j, BOX_FIELD_MAX);
					return -1;
				}

				/* key must has valid type */
				if (STR2ENUM(field_data_type, key->type) == field_data_type_MAX) {
					out_warning(0, "(space = %zu index = %zu) "
						    "unknown field data type: `%s'", i, j, key->type);
					return -1;
				}

				if (max_key_fieldno < key->fieldno + 1) {
					max_key_fieldno = key->fieldno + 1;
				}

				++key_part_count;
			}

			/* Check key part count. */
			if (key_part_count == 0) {
				out_warning(0, "(space = %zu index = %zu) "
					    "at least one field must be defined", i, j);
				return -1;
			}

			index_type = STR2ENUM(index_type, index->type);

			/* check index type */
			if (index_type == index_type_MAX) {
				out_warning(0, "(space = %zu index = %zu) "
					    "unknown index type '%s'", i, j, index->type);
				return -1;
			}

			/* First index must be unique. */
			if (j == 0 && index->unique == false) {
				out_warning(0, "(space = %zu) space first index must be unique", i);
				return -1;
			}

			switch (index_type) {
			case HASH:
				/* check hash index */
				/* hash index must has single-field key */
				if (key_part_count != 1) {
					out_warning(0, "(space = %zu index = %zu) "
						    "hash index must has a single-field key", i, j);
					return -1;
				}
				/* hash index must be unique */
				if (!index->unique) {
					out_warning(0, "(space = %zu index = %zu) "
						    "hash index must be unique", i, j);
					return -1;
				}
				break;
			case TREE:
				/* extra check for tree index not needed */
				break;
			default:
				assert(false);
			}
		}

		/* Check for index field type conflicts */
		if (max_key_fieldno > 0) {
			char *types = alloca(max_key_fieldno);
			memset(types, 0, max_key_fieldno);
			for (size_t j = 0; space->index[j] != NULL; ++j) {
				typeof(space->index[j]) index = space->index[j];
				for (size_t k = 0; index->key_field[k] != NULL; ++k) {
					typeof(index->key_field[k]) key = index->key_field[k];
					if (key->fieldno == -1)
						break;

					u32 f = key->fieldno;
					enum field_data_type t = STR2ENUM(field_data_type, key->type);
					assert(t != field_data_type_MAX);
					if (types[f] != t) {
						if (types[f] == UNKNOWN) {
							types[f] = t;
						} else {
							out_warning(0, "(space = %zu fieldno = %zu) "
								    "index field type mismatch", i, f);
							return -1;
						}
					}
				}

			}
		}
	}

	return 0;
}

