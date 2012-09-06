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
#include <exception.h>
#include "tuple.h"
#include <pickle.h>
#include <palloc.h>

const char *index_type_strs[] = { "HASH", "TREE", "\0" };
const char *field_data_type_strs[] = {"NUM", "NUM64", "STR", "\0"};

struct space *spaces = NULL;

bool primary_indexes_enabled = false;
bool secondary_indexes_enabled = false;

/** Free a key definition. */
static void
key_free(struct key_def *key_def)
{
	free(key_def->parts);
	free(key_def->cmp_order);
}

void
space_replace(struct space *sp, struct tuple *old_tuple,
	      struct tuple *new_tuple)
{
	int n = index_count(sp);
	for (int i = 0; i < n; i++) {
		Index *index = sp->index[i];
		[index replace: old_tuple :new_tuple];
	}
}

/**
 * Check the key for validness.
 */
void
space_check_key(Index *index, void *key, int part_count,
		bool partial_key_allowed)
{
	struct key_def *key_def = index->key_def;

	/* Check to see if the key has a valid number of parts. */
	if (part_count > key_def->part_count)
		tnt_raise(ClientError, :ER_KEY_PART_COUNT,
			  part_count, key_def->part_count);
	if (!partial_key_allowed && part_count < key_def->part_count)
		tnt_raise(ClientError, :ER_EXACT_MATCH,
			  part_count, key_def->part_count);

	/* Sweep through the key and check the field sizes. */
	for (int part = 0; part < part_count; part++) {
		u32 len = load_varint32((void**) &key);
		key += len;

		if (key_def->parts[part].type == NUM) {
			if (len != 4)
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
		} else if (key_def->parts[part].type == NUM64) {
			if (len != 8)
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		}
	}
}

/**
 * Check the tuple to see if it has the required fields.
 */
void
space_check_tuple(struct space *space, struct tuple *tuple)
{
	/* Check to see if the tuple has a valid number of fields. */
	if (tuple->field_count < space->max_fieldno)
		tnt_raise(IllegalParams, :"tuple must have all indexed fields");
	if (space->arity > 0 && space->arity != tuple->field_count)
		tnt_raise(IllegalParams, :"tuple field count must match space cardinality");

	/* Sweep through the tuple and check the field sizes. */
	u8 *data = tuple->data;
	for (int field = 0; field < space->max_fieldno; ++field) {
		/* Get the size of the current field and advance. */
		u32 len = load_varint32((void **) &data);
		data += len;

		/* Check fixed size fields (NUM and NUM64) and skip undefined
		   size fields (STRING and UNKNOWN). */
		if (space->field_desc[field].type == NUM) {
			if (len != sizeof(u32))
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
		} else if (space->field_desc[field].type == NUM64) {
			if (len != sizeof(u64))
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		}
	}
}

/**
 * Check the tuple against the constraints (so far only uniqueness).
 */
void
space_check_constraints(struct space *space, struct tuple *old_tuple,
			struct tuple *new_tuple)
{
	int n = index_count(space);

	/* Check key uniqueness. Only secondary indexes are validated here. */
	for (int i = 1; i < n; ++i) {
		Index *index = space->index[i];
		if (index->key_def->is_unique) {
			struct tuple *tuple = [index findByTuple: new_tuple];
			if (tuple != NULL && tuple != old_tuple)
				tnt_raise(ClientError, :ER_INDEX_VIOLATION);
		}
	}
}

void
space_adjust(struct space *space, struct tuple *tuple)
{
	int base_count = space->base_count;
	if (base_count == 0)
		return;

	u8 *next = tuple->data;
	for (int i = 0; i < space->max_fieldno; i++) {
		u8 *data = next;
		u32 len = load_varint32((void**) &next);
		next += len;

		if (space->field_desc[i].type == UNKNOWN)
			continue;
		if (space->field_desc[i].base == 0)
			continue;
		if (space->field_desc[i].disp != 0)
			continue;

		space_set_base_offset(space, tuple,
				      space->field_desc[i].base,
				      data - tuple->data);

		if (--base_count == 0)
			break;
	}
}

void
space_remove(struct space *sp, struct tuple *tuple)
{
	int n = index_count(sp);
	for (int i = 0; i < n; i++) {
		Index *index = sp->index[i];
		[index remove: tuple];
	}
}

void
space_free(void)
{
	int i;
	for (i = 0 ; i < BOX_SPACE_MAX ; i++) {
		if (!spaces[i].enabled)
			continue;

		int j;
		for (j = 0 ; j < spaces[i].key_count; j++) {
			Index *index = spaces[i].index[j];
			[index free];
			key_free(&spaces[i].key_defs[j]);
		}

		free(spaces[i].key_defs);
		free(spaces[i].field_desc);
	}
}

static void
key_init(struct key_def *def, struct tarantool_cfg_space_index *cfg_index)
{
	def->max_fieldno = 0;
	def->part_count = 0;

	/* Calculate key part count and maximal field number. */
	for (int k = 0; cfg_index->key_field[k] != NULL; ++k) {
		typeof(cfg_index->key_field[k]) cfg_key = cfg_index->key_field[k];

		if (cfg_key->fieldno == -1) {
			/* last filled key reached */
			break;
		}

		def->max_fieldno = MAX(def->max_fieldno, cfg_key->fieldno);
		def->part_count++;
	}

	/* init def array */
	def->parts = malloc(sizeof(struct key_part) * def->part_count);
	if (def->parts == NULL) {
		panic("can't allocate def parts array for index");
	}

	/* init compare order array */
	def->max_fieldno++;
	def->cmp_order = malloc(def->max_fieldno * sizeof(u32));
	if (def->cmp_order == NULL) {
		panic("can't allocate def cmp_order array for index");
	}
	memset(def->cmp_order, -1, def->max_fieldno * sizeof(u32));

	/* fill fields and compare order */
	for (int k = 0; cfg_index->key_field[k] != NULL; ++k) {
		typeof(cfg_index->key_field[k]) cfg_key = cfg_index->key_field[k];

		if (cfg_key->fieldno == -1) {
			/* last filled key reached */
			break;
		}

		/* fill keys */
		def->parts[k].fieldno = cfg_key->fieldno;
		def->parts[k].type = STR2ENUM(field_data_type, cfg_key->type);
		/* fill compare order */
		def->cmp_order[cfg_key->fieldno] = k;
	}
	def->is_unique = cfg_index->unique;
}

/**
 * Extract all available field info from keys
 *
 * @param space		space to extract field info for
 * @param key_count	the number of keys
 * @param key_defs	key description array
 */
static void
space_init_field_types(struct space *space)
{
	int i, max_fieldno;
	int key_count = space->key_count;
	struct key_def *key_defs = space->key_defs;

	/* find max max field no */
	max_fieldno = 0;
	for (i = 0; i < key_count; i++) {
		max_fieldno = MAX(max_fieldno, key_defs[i].max_fieldno);
	}

	/* alloc & init field type info */
	space->max_fieldno = max_fieldno;
	space->field_desc = malloc(max_fieldno * sizeof(struct field_desc));
	for (i = 0; i < max_fieldno; i++) {
		space->field_desc[i].type = UNKNOWN;
	}

	/* extract field type info */
	for (i = 0; i < key_count; i++) {
		struct key_def *def = &key_defs[i];
		for (int pi = 0; pi < def->part_count; pi++) {
			struct key_part *part = &def->parts[pi];
			assert(part->fieldno < max_fieldno);
			space->field_desc[part->fieldno].type = part->type;
		}
	}

#ifndef NDEBUG
	/* validate field type info */
	for (i = 0; i < key_count; i++) {
		struct key_def *def = &key_defs[i];
		for (int pi = 0; pi < def->part_count; pi++) {
			struct key_part *part = &def->parts[pi];
			assert(space->field_desc[part->fieldno].type == part->type);
		}
	}
#endif

	/* init field offset info */
	int current_base = 0;
	int current_disp = 0;
	int known_offset = 1;
	for (i = 0; i < max_fieldno; i++) {
		if (space->field_desc[i].type == UNKNOWN) {
			known_offset = 0;
			continue;
		}

		if (!known_offset) {
			current_base = ++space->base_count;
			current_disp = 0;
			known_offset = 1;
		}

		space->field_desc[i].base = current_base;
		space->field_desc[i].disp = current_disp;

		fprintf(stderr, "space %d field %d base/disp: %d/%d\n",
			space_n(space), i, current_base, current_disp);

		/* On a fixed length field account for the appropriate
		   varint length code and for the actual data length */
		if (space->field_desc[i].type == NUM) {
			current_disp += 1 + 4;
		} else if (space->field_desc[i].type == NUM64) {
			current_disp += 1 + 8;
		} else {
			/* TODO: check individual indexes to see if the field
			   is a part of dense sequences in all of the indexes
			   that include it and that for indexes that does not
			   include it there is a later point where a new base
			   can be made or there are no further fields at all. 
			   In such cases a new base for the following fields
			   is not really needed. */
			known_offset = 0;
		}
	}

	fprintf(stderr, "space %d base_count: %d\n", space_n(space), space->base_count);
}

static void
space_config()
{
	/* exit if no spaces are configured */
	if (cfg.space == NULL) {
		return;
	}

	/* fill box spaces */
	for (int i = 0; cfg.space[i] != NULL; ++i) {
		tarantool_cfg_space *cfg_space = cfg.space[i];

		if (!CNF_STRUCT_DEFINED(cfg_space) || !cfg_space->enabled)
			continue;

		assert(cfg.memcached_port == 0 || i != cfg.memcached_space);

		spaces[i].enabled = true;
		spaces[i].arity = cfg_space->cardinality;

		/*
		 * Collect key/field info. We need aggregate
		 * information on all keys before we can create
		 * indexes.
		 */
		spaces[i].key_count = 0;
		for (int j = 0; cfg_space->index[j] != NULL; ++j) {
			++spaces[i].key_count;
		}

		spaces[i].key_defs = malloc(spaces[i].key_count *
					    sizeof(struct key_def));
		if (spaces[i].key_defs == NULL) {
			panic("can't allocate key def array");
		}
		for (int j = 0; cfg_space->index[j] != NULL; ++j) {
			typeof(cfg_space->index[j]) cfg_index = cfg_space->index[j];
			key_init(&spaces[i].key_defs[j], cfg_index);
		}
		space_init_field_types(&spaces[i]);

		/* fill space indexes */
		for (int j = 0; cfg_space->index[j] != NULL; ++j) {
			typeof(cfg_space->index[j]) cfg_index = cfg_space->index[j];
			enum index_type type = STR2ENUM(index_type, cfg_index->type);
			struct key_def *key_def = &spaces[i].key_defs[j];
			Index *index = [Index alloc: type :key_def :&spaces[i]];
			[index init: key_def :&spaces[i]];
			spaces[i].index[j] = index;
		}
		say_info("space %i successfully configured", i);
	}
}

void
space_init(void)
{
	/* Allocate and initialize space memory. */
	spaces = p0alloc(eter_pool, sizeof(struct space) * BOX_SPACE_MAX);

	/* configure regular spaces */
	space_config();
}

void
begin_build_primary_indexes(void)
{
	assert(primary_indexes_enabled == false);
	for (u32 n = 0; n < BOX_SPACE_MAX; ++n) {
		if (spaces[n].enabled == false)
			continue;

		Index *pk = spaces[n].index[0];
		[pk beginBuild];
	}
}

void
end_build_primary_indexes(void)
{
	for (u32 n = 0; n < BOX_SPACE_MAX; ++n) {
		if (spaces[n].enabled == false)
			continue;

		Index *pk = spaces[n].index[0];
		[pk endBuild];
	}
	primary_indexes_enabled = true;
}

void
build_secondary_indexes(void)
{
	assert(primary_indexes_enabled == true);
	assert(secondary_indexes_enabled == false);

	for (u32 n = 0; n < BOX_SPACE_MAX; ++n) {
		if (spaces[n].enabled == false)
			continue;
		if (spaces[n].key_count <= 1)
			continue; /* no secondary keys */

		say_info("Building secondary keys in space %" PRIu32 "...", n);

		Index *pk = spaces[n].index[0];
		for (int i = 1; i < spaces[n].key_count; i++) {
			Index *index = spaces[n].index[i];
			[index build: pk];
		}

		say_info("Space %"PRIu32": done", n);
	}

	/* enable secondary indexes now */
	secondary_indexes_enabled = true;
}

i32
check_spaces(struct tarantool_cfg *conf)
{
	/* exit if no spaces are configured */
	if (conf->space == NULL) {
		return 0;
	}

	for (size_t i = 0; conf->space[i] != NULL; ++i) {
		typeof(conf->space[i]) space = conf->space[i];

		if (!CNF_STRUCT_DEFINED(space)) {
			/* space undefined, skip it */
			continue;
		}

		if (!space->enabled) {
			/* space disabled, skip it */
			continue;
		}

		/* check space bound */
		if (i >= BOX_SPACE_MAX) {
			/* maximum space is reached */
			out_warning(0, "(space = %zu) "
				    "too many spaces (%i maximum)", i, space);
			return -1;
		}

		if (conf->memcached_port && i == conf->memcached_space) {
			out_warning(0, "Space %i is already used as "
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

		int max_key_fieldno = -1;

		/* check spaces indexes */
		for (size_t j = 0; space->index[j] != NULL; ++j) {
			typeof(space->index[j]) index = space->index[j];
			u32 key_part_count = 0;
			enum index_type index_type;

			/* check index bound */
			if (j >= BOX_INDEX_MAX) {
				/* maximum index in space reached */
				out_warning(0, "(space = %zu index = %zu) "
					    "too many indexed (%i maximum)", i, j, BOX_INDEX_MAX);
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

				/* key must has valid type */
				if (STR2ENUM(field_data_type, key->type) == field_data_type_MAX) {
					out_warning(0, "(space = %zu index = %zu) "
						    "unknown field data type: `%s'", i, j, key->type);
					return -1;
				}

				if (max_key_fieldno < key->fieldno) {
					max_key_fieldno = key->fieldno;
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
		if (max_key_fieldno >= 0) {
			char *types = alloca(max_key_fieldno + 1);
			memset(types, UNKNOWN, max_key_fieldno + 1);
			for (size_t j = 0; space->index[j] != NULL; ++j) {
				typeof(space->index[j]) index = space->index[j];
				for (size_t k = 0; index->key_field[k] != NULL; ++k) {
					typeof(index->key_field[k]) key = index->key_field[k];
					int f = key->fieldno;
					if (f == -1) {
						break;
					}
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

