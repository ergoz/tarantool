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
#include "box_lua.h"
#include "lua/init.h"
#include <fiber.h>
#include "box/box.h"
#include "request.h"
#include "txn.h"

#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
#include "lj_obj.h"
#include "lj_ctype.h"
#include "lj_cdata.h"
#include "lj_cconv.h"

#include "pickle.h"
#include "tuple.h"
#include "space.h"
#include "port.h"
#include "tbuf.h"

/* contents of box.lua */
extern const char box_lua[];

/**
 * All box connections share the same Lua state. We use
 * Lua coroutines (lua_newthread()) to have multiple
 * procedures running at the same time.
 */
static lua_State *root_L;

/*
 * Functions, exported in box_lua.h should have prefix
 * "box_lua_"; functions, available in Lua "box"
 * should start with "lbox_".
 */

/** {{{ box.tuple Lua library
 *
 * To avoid extra copying between Lua memory and garbage-collected
 * tuple memory, provide a Lua userdata object 'box.tuple'.  This
 * object refers to a tuple instance in the slab allocator, and
 * allows accessing it using Lua primitives (array subscription,
 * iteration, etc.). When Lua object is garbage-collected,
 * tuple reference counter in the slab allocator is decreased,
 * allowing the tuple to be eventually garbage collected in
 * the slab allocator.
 */

static const char *tuplelib_name = "box.tuple";

static void
lbox_pushtuple(struct lua_State *L, struct tuple *tuple);

static struct tuple *
lua_totuple(struct lua_State *L, int index);

static inline struct tuple *
lua_checktuple(struct lua_State *L, int narg)
{
	struct tuple *t = *(void **) luaL_checkudata(L, narg, tuplelib_name);
	assert(t->refs);
	return t;
}

struct tuple *
lua_istuple(struct lua_State *L, int narg)
{
	if (lua_getmetatable(L, narg) == 0)
		return NULL;
	luaL_getmetatable(L, tuplelib_name);
	struct tuple *tuple = 0;
	if (lua_equal(L, -1, -2))
		tuple = * (void **) lua_touserdata(L, narg);
	lua_pop(L, 2);
	return tuple;
}

static int
lbox_tuple_new(lua_State *L)
{
	int argc = lua_gettop(L);
	if (argc < 1)
		luaL_error(L, "tuple.new(): bad arguments");
	struct tuple *tuple = lua_totuple(L, 1);
	lbox_pushtuple(L, tuple);
	return 1;
}

static int
lbox_tuple_gc(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	tuple_ref(tuple, -1);
	return 0;
}

static int
lbox_tuple_len(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	lua_pushnumber(L, tuple->field_count);
	return 1;
}

static int
lbox_tuple_slice(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	int argc = lua_gettop(L) - 1;
	int start, end;

	/*
	 * Prepare the range. The second argument is optional.
	 * If the end is beyond tuple size, adjust it.
	 * If no arguments, or start > end, return an error.
	 */
	if (argc == 0 || argc > 2)
		luaL_error(L, "tuple.slice(): bad arguments");
	start = lua_tointeger(L, 2);
	if (start < 0)
		start += tuple->field_count;
	if (argc == 2) {
		end = lua_tointeger(L, 3);
		if (end < 0)
			end += tuple->field_count;
		else if (end > tuple->field_count)
			end = tuple->field_count;
	} else {
		end = tuple->field_count;
	}
	if (end <= start)
		luaL_error(L, "tuple.slice(): start must be less than end");

	const void *field = tuple->data;
	u32 fieldno = 0;
	u32 stop = end - 1;

	while (field < (const void *) tuple->data + tuple->bsize) {
		size_t len = load_varint32(&field);
		if (fieldno >= start) {
			lua_pushlstring(L, field, len);
			if (fieldno == stop)
				break;
		}
		field += len;
		fieldno += 1;
	}
	return end - start;
}


/**
 * Given a tuple, range of fields to remove (start and end field
 * numbers), and a list of fields to paste, calculate the size of
 * the resulting tuple.
 *
 * @param L      lua stack, contains a list of arguments to paste
 * @param start  offset in the lua stack where paste arguments start
 * @param tuple  old tuple
 * @param offset cut field offset
 * @param len    how many fields to cut
 * @param[out]   sizes of the left and right part
 *
 * @return size of the new tuple
*/
static size_t
transform_calculate(struct lua_State *L, struct tuple *tuple,
		    int start, int argc, int offset, int len,
		    size_t lr[2])
{
	/* calculate size of the new tuple */
	const void *tuple_end = tuple->data + tuple->bsize;
	const void *tuple_field = tuple->data;

	lr[0] = tuple_range_size(&tuple_field, tuple_end, offset);

	/* calculate sizes of supplied fields */
	size_t mid = 0;
	for (int i = start ; i <= argc ; i++) {
		switch (lua_type(L, i)) {
		case LUA_TNUMBER:
			mid += varint32_sizeof(sizeof(u32)) + sizeof(u32);
			break;
		case LUA_TCDATA:
			mid += varint32_sizeof(sizeof(u64)) + sizeof(u64);
			break;
		case LUA_TSTRING: {
			size_t field_size = lua_objlen(L, i);
			mid += varint32_sizeof(field_size) + field_size;
			break;
		}
		default:
			luaL_error(L, "tuple.transform(): unsupported field type '%s'",
				   lua_typename(L, lua_type(L, i)));
			break;
		}
	}

	/* calculate size of the removed fields */
	tuple_range_size(&tuple_field, tuple_end, len);

	/* calculate last part of the tuple fields */
	lr[1] = tuple_end - tuple_field;

	return lr[0] + mid + lr[1];
}

/**
 * Tuple transforming function.
 *
 * Remove the fields designated by 'offset' and 'len' from an tuple,
 * and replace them with the elements of supplied data fields,
 * if any.
 *
 * Function returns newly allocated tuple.
 * It does not change any parent tuple data.
 */
static int
lbox_tuple_transform(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	int argc = lua_gettop(L);
	if (argc < 3)
		luaL_error(L, "tuple.transform(): bad arguments");
	int offset = lua_tointeger(L, 2);
	int len = lua_tointeger(L, 3);

	/* validate offset and len */
	if (offset < 0) {
		if (-offset > tuple->field_count)
			luaL_error(L, "tuple.transform(): offset is out of bound");
		offset += tuple->field_count;
	} else if (offset > tuple->field_count) {
		offset = tuple->field_count;
	}
	if (len < 0)
		luaL_error(L, "tuple.transform(): len is negative");
	if (len > tuple->field_count - offset)
		len = tuple->field_count - offset;

	/* calculate size of the new tuple */
	size_t lr[2]; /* left and right part sizes */
	size_t size = transform_calculate(L, tuple, 4, argc, offset, len, lr);

	/* allocate new tuple */
	struct tuple *dest = tuple_alloc(size);
	dest->field_count = (tuple->field_count - len) + (argc - 3);

	/* construct tuple */
	memcpy(dest->data, tuple->data, lr[0]);
	void *ptr = dest->data + lr[0];
	for (int i = 4; i <= argc; i++) {
		switch (lua_type(L, i)) {
		case LUA_TNUMBER: {
			u32 v = lua_tonumber(L, i);
			ptr = pack_lstr(ptr, &v, sizeof(v));
			break;
		}
		case LUA_TCDATA: {
			u64 v = tarantool_lua_tointeger64(L, i);
			ptr = pack_lstr(ptr, &v, sizeof(v));
			break;
		}
		case LUA_TSTRING: {
			size_t field_size = 0;
			const char *v = luaL_checklstring(L, i, &field_size);
			ptr = pack_lstr(ptr, v, field_size);
			break;
		}
		default:
			/* default type check is done in transform_calculate()
			 * function */
			break;
		}
	}
	memcpy(ptr, tuple_field(tuple, offset + len), lr[1]);

	lbox_pushtuple(L, dest);
	return 1;
}

/*
 * Tuple find function.
 *
 * Find each or one tuple field according to the specified key.
 *
 * Function returns indexes of the tuple fields that match
 * key criteria.
 *
 */
static int
tuple_find(struct lua_State *L, struct tuple *tuple, size_t offset,
	   const char *key, size_t key_size,
	   bool all)
{
	int top = lua_gettop(L);
	int idx = offset;
	if (idx >= tuple->field_count)
		return 0;
	const void *field = tuple_field(tuple, idx);
	while (field < (const void *) tuple->data + tuple->bsize) {
		size_t len = load_varint32(&field);
		if (len == key_size && (memcmp(field, key, len) == 0)) {
			lua_pushinteger(L, idx);
			if (!all)
				break;
		}
		field += len;
		idx++;
	}
	return lua_gettop(L) - top;
}

static int
lbox_tuple_find_do(struct lua_State *L, bool all)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	int argc = lua_gettop(L);
	size_t offset = 0;
	switch (argc - 1) {
	case 1: break;
	case 2:
		offset = lua_tointeger(L, 2);
		break;
	default:
		luaL_error(L, "tuple.find(): bad arguments");
	}
	size_t key_size = 0;
	const char *key = NULL;
	u32 u32v;
	u64 u64v;
	switch (lua_type(L, argc)) {
	case LUA_TNUMBER:
		u32v = lua_tonumber(L, argc);
		key_size = sizeof(u32);
		key = (const char*)&u32v;
		break;
	case LUA_TCDATA:
		u64v = tarantool_lua_tointeger64(L, argc);
		key_size = sizeof(u64);
		key = (const char*)&u64v;
		break;
	case LUA_TSTRING:
		key = luaL_checklstring(L, argc, &key_size);
		break;
	default:
		luaL_error(L, "tuple.find(): bad field type");
	}
	return tuple_find(L, tuple, offset, key, key_size, all);
}

static int
lbox_tuple_find(struct lua_State *L)
{
	return lbox_tuple_find_do(L, false);
}

static int
lbox_tuple_findall(struct lua_State *L)
{
	return lbox_tuple_find_do(L, true);
}

static int
lbox_tuple_unpack(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	const void *field = tuple->data;

	while (field < (const void *) tuple->data + tuple->bsize) {
		size_t len = load_varint32(&field);
		lua_pushlstring(L, field, len);
		field += len;
	}
	assert(lua_gettop(L) == tuple->field_count + 1);
	return tuple->field_count;
}

static int
lbox_tuple_totable(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	lua_newtable(L);
	int index = 1;
	const void *field = tuple->data;
	while (field < (const void *) tuple->data + tuple->bsize) {
		size_t len = load_varint32(&field);
		lua_pushnumber(L, index++);
		lua_pushlstring(L, field, len);
		lua_rawset(L, -3);
		field += len;
	}
	return 1;
}

/**
 * Implementation of tuple __index metamethod.
 *
 * Provides operator [] access to individual fields for integer
 * indexes, as well as searches and invokes metatable methods
 * for strings.
 */
static int
lbox_tuple_index(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	/* For integer indexes, implement [] operator */
	if (lua_isnumber(L, 2)) {
		int i = luaL_checkint(L, 2);
		if (i >= tuple->field_count)
			luaL_error(L, "%s: index %d is out of bounds (0..%d)",
				   tuplelib_name, i, tuple->field_count-1);
		const void *field = tuple_field(tuple, i);
		u32 len = load_varint32(&field);
		lua_pushlstring(L, field, len);
		return 1;
	}
	/* If we got a string, try to find a method for it. */
	const char *sz = luaL_checkstring(L, 2);
	lua_getmetatable(L, 1);
	lua_getfield(L, -1, sz);
	return 1;
}

static int
lbox_tuple_tostring(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	/* @todo: print the tuple */
	struct tbuf *tbuf = tbuf_new(fiber->gc_pool);
	tuple_print(tbuf, tuple->field_count, tuple->data);
	lua_pushlstring(L, tbuf->data, tbuf->size);
	return 1;
}

static void
lbox_pushtuple(struct lua_State *L, struct tuple *tuple)
{
	if (tuple) {
		void **ptr = lua_newuserdata(L, sizeof(void *));
		luaL_getmetatable(L, tuplelib_name);
		lua_setmetatable(L, -2);
		*ptr = tuple;
		tuple_ref(tuple, 1);
	} else {
		lua_pushnil(L);
	}
}

/**
 * Sequential access to tuple fields. Since tuple is a list-like
 * structure, iterating over tuple fields is faster
 * than accessing fields using an index.
 */
static int
lbox_tuple_next(struct lua_State *L)
{
	struct tuple *tuple = lua_checktuple(L, 1);
	int argc = lua_gettop(L) - 1;
	const void *field = NULL;
	size_t len;

	if (argc == 0 || (argc == 1 && lua_type(L, 2) == LUA_TNIL))
		field = tuple->data;
	else if (argc == 1 && lua_islightuserdata(L, 2))
		field = lua_touserdata(L, 2);
	else
		luaL_error(L, "tuple.next(): bad arguments");

	(void)field;
	assert(field >= (const void *) tuple->data);
	if (field < (const void *) tuple->data + tuple->bsize) {
		len = load_varint32(&field);
		lua_pushlightuserdata(L, (void *) field + len);
		lua_pushlstring(L, field, len);
		return 2;
	}
	lua_pushnil(L);
	return  1;
}

/** Iterator over tuple fields. Adapt lbox_tuple_next
 * to Lua iteration conventions.
 */
static int
lbox_tuple_pairs(struct lua_State *L)
{
	lua_pushcfunction(L, lbox_tuple_next);
	lua_pushvalue(L, -2); /* tuple */
	lua_pushnil(L);
	return 3;
}

static const struct luaL_reg lbox_tuple_meta[] = {
	{"__gc", lbox_tuple_gc},
	{"__len", lbox_tuple_len},
	{"__index", lbox_tuple_index},
	{"__tostring", lbox_tuple_tostring},
	{"next", lbox_tuple_next},
	{"pairs", lbox_tuple_pairs},
	{"slice", lbox_tuple_slice},
	{"transform", lbox_tuple_transform},
	{"find", lbox_tuple_find},
	{"findall", lbox_tuple_findall},
	{"unpack", lbox_tuple_unpack},
	{"totable", lbox_tuple_totable},
	{NULL, NULL}
};

static const struct luaL_reg lbox_tuplelib[] = {
	{"new", lbox_tuple_new},
	{NULL, NULL}
};

/* }}} */

/** {{{ box.index Lua library: access to spaces and indexes
 */

static const char *indexlib_name = "box.index";
static const char *iteratorlib_name = "box.index.iterator";

static struct iterator *
lbox_checkiterator(struct lua_State *L, int i)
{
	struct iterator **it = luaL_checkudata(L, i, iteratorlib_name);
	assert(it != NULL);
	return *it;
}

static void
lbox_pushiterator(struct lua_State *L, Index *index,
                  struct iterator *it, enum iterator_type type,
                  const void *key, size_t size, int part_count)
{
	struct {
		struct iterator *it;
		char key[];
	} *holder = lua_newuserdata(L, sizeof(void *) + size);
	luaL_getmetatable(L, iteratorlib_name);
	lua_setmetatable(L, -2);

	holder->it = it;
	memcpy(holder->key, key, size);

	[index initIterator: it :type :(key ? holder->key : NULL)
		:part_count];
}

static int
lbox_iterator_gc(struct lua_State *L)
{
	struct iterator *it = lbox_checkiterator(L, -1);
	it->free(it);
	return 0;
}

static Index *
lua_checkindex(struct lua_State *L, int i)
{
	Index **index = luaL_checkudata(L, i, indexlib_name);
	assert(index != NULL);
	return *index;
}

static int
lbox_index_new(struct lua_State *L)
{
	int n = luaL_checkint(L, 1); /* get space id */
	int idx = luaL_checkint(L, 2); /* get index id in */
	/* locate the appropriate index */
	struct space *sp = space_find(n);
	Index *index = index_find(sp, idx);

	/* create a userdata object */
	void **ptr = lua_newuserdata(L, sizeof(void *));
	*ptr = index;
	/* set userdata object metatable to indexlib */
	luaL_getmetatable(L, indexlib_name);
	lua_setmetatable(L, -2);

	return 1;
}

static int
lbox_index_tostring(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	lua_pushfstring(L, "index %d in space %d",
			index_n(index), space_n(index->space));
	return 1;
}

static int
lbox_index_len(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	lua_pushinteger(L, [index size]);
	return 1;
}

static int
lbox_index_part_count(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	lua_pushinteger(L, index->key_def->part_count);
	return 1;
}

static int
lbox_index_min(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	lbox_pushtuple(L, [index min]);
	return 1;
}

static int
lbox_index_max(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	lbox_pushtuple(L, [index max]);
	return 1;
}

static int
lbox_index_random(struct lua_State *L)
{
	if (lua_gettop(L) != 2 || lua_isnil(L, 2))
		luaL_error(L, "Usage: index:random((uint32) rnd)");

	Index *index = lua_checkindex(L, 1);
	u32 rnd = lua_tointeger(L, 2);
	lbox_pushtuple(L, [index random: rnd]);
	return 1;
}

/**
 * Convert an element on Lua stack to a part of an index
 * key.
 *
 * Lua type system has strings, numbers, booleans, tables,
 * userdata objects. Tarantool indexes only support 32/64-bit
 * integers and strings.
 *
 * Instead of considering each Tarantool <-> Lua type pair,
 * here we follow the approach similar to one in lbox_pack()
 * (see tarantool_lua.m):
 *
 * Lua numbers are converted to 32 or 64 bit integers,
 * if key part is integer. In all other cases,
 * Lua types are converted to Lua strings, and these
 * strings are used as key parts.
 */

void append_key_part(struct lua_State *L, int i,
		     struct tbuf *tbuf, enum field_data_type type)
{
	const char *str;
	size_t size;
	u32 v_u32;
	u64 v_u64;

	if (lua_type(L, i) == LUA_TNUMBER) {
		if (type == NUM64) {
			v_u64 = (u64) lua_tonumber(L, i);
			str = (char *) &v_u64;
			size = sizeof(u64);
		} else {
			v_u32 = (u32) lua_tointeger(L, i);
			str = (char *) &v_u32;
			size = sizeof(u32);
		}
	} else {
		str = luaL_checklstring(L, i, &size);
	}
	char varint_buf[sizeof(u32) + 1];
	size_t pack_len = (pack_varint32(varint_buf, size) -
			   (void *) varint_buf);
	tbuf_append(tbuf, varint_buf, pack_len);
	tbuf_append(tbuf, str, size);
}

/*
 * Lua iterator over a Taratnool/Box index.
 *
 *	(iteration_state, tuple) = index.next(index, [params])
 *
 * When [params] are absent or nil
 * returns a pointer to a new ALL iterator and
 * to the first tuple (or nil, if the index is
 * empty).
 *
 * When [params] is a userdata,
 * i.e. we're inside an iteration loop, retrieves
 * the next tuple from the iterator.
 *
 * Otherwise, [params] can be used to seed
 * a new iterator with iterator type and
 * type-specific arguments. For exaple,
 * for GE iterator, a list of Lua scalars
 * cann follow the box.index.GE: this will
 * start iteration from the offset specified by
 * the given (multipart) key.
 *
 * @return Returns an iterator object, either created
 *         or taken from Lua stack.
 */

static inline struct iterator *
lbox_create_iterator(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	int argc = lua_gettop(L);

	size_t allocated_size = palloc_allocated(fiber->gc_pool);

	/* Create a new iterator. */
	enum iterator_type type = ITER_ALL;
	u32 field_count = 0;
	const void *key = NULL;
	u32 key_size = 0;
	if (argc == 1 || (argc == 2 && lua_type(L, 2) == LUA_TNIL)) {
		/*
		 * Nothing or nil on top of the stack,
		 * iteration over entire range from the
		 * beginning (ITER_ALL).
		 */
	} else {
		 type = luaL_checkint(L, 2);
		 if (type >= iterator_type_MAX)
			 luaL_error(L, "unknown iterator type: %d", type);
		 /* What else do we have on the stack? */
		 if (argc == 2 || (argc == 3 && lua_type(L, 3) == LUA_TNIL)) {
			 /* Nothing */
		 } else if (argc == 3 && lua_type(L, 3) == LUA_TUSERDATA) {
			/* Tuple. */
			struct tuple *tuple = lua_checktuple(L, 2);
			field_count = tuple->field_count;
			key = tuple->data;
			key_size = tuple->bsize;
		} else {
			/* Single or multi- part key. */
			field_count = argc - 2;
			struct tbuf *data = tbuf_new(fiber->gc_pool);
			for (u32 i = 0; i < field_count; i++) {
				enum field_data_type type = UNKNOWN;
				if (i < index->key_def->part_count) {
					type = index->key_def->parts[i].type;
				}
				append_key_part(L, i + 3, data, type);
			}
			key = data->data;
			key_size = data->size;
		}
		/*
		 * We allow partially specified keys for TREE
		 * indexes. HASH indexes can only use single-part
		 * keys.
		*/
		if (field_count > index->key_def->part_count)
			luaL_error(L, "Key part count %d"
				   " is greater than index part count %d",
				   field_count, index->key_def->part_count);
	}
	struct iterator *it = [index allocIterator];
	lbox_pushiterator(L, index, it, type, key, key_size,
	                  field_count);

	/* truncate memory used by key construction */
	ptruncate(fiber->gc_pool, allocated_size);
	return it;
}

/**
 * Lua-style next() function, for use in pairs().
 * @example:
 * for k, v in box.space[0].index[0].idx.next, box.space[0].index[0].idx, nil do
 *	print(v)
 * end
 */
static int
lbox_index_next(struct lua_State *L)
{
	int argc = lua_gettop(L);
	struct iterator *it = NULL;
	if (argc == 2 && lua_type(L, 2) == LUA_TUSERDATA) {
		/*
		 * Apart from the index itself, we have only one
		 * other argument, and it's a userdata: must be
		 * iteration state created before.
		 */
		it = lbox_checkiterator(L, 2);
	} else {
		it = lbox_create_iterator(L);
	}
	struct tuple *tuple = it->next(it);
	/* If tuple is NULL, pushes nil as end indicator. */
	lbox_pushtuple(L, tuple);
	return tuple ? 2 : 1;
}

/** iterator() closure function. */
static int
lbox_index_iterator_closure(struct lua_State *L)
{
	/* Extract closure arguments. */
	struct iterator *it = lbox_checkiterator(L, lua_upvalueindex(1));

	struct tuple *tuple = it->next(it);

	/* If tuple is NULL, push nil as end indicator. */
	lbox_pushtuple(L, tuple);
	return 1;
}

/**
 * @brief Create iterator closure over a Taratnool/Box index.
 * @example lua it = box.space[0].index[0]:iterator(box.index.GE, 1);
 *   print(it(), it()).
 * @param L lua stack
 * @see http://www.lua.org/pil/7.1.html
 * @return number of return values put on the stack
 */
static int
lbox_index_iterator(struct lua_State *L)
{
	/* Create iterator and push it onto the stack. */
	(void) lbox_create_iterator(L);
	lua_pushcclosure(L, &lbox_index_iterator_closure, 1);
	return 1;
}


/**
 * Lua index subtree count function.
 * Iterate over an index, count the number of tuples which equal the
 * provided search criteria. The argument can either point to a
 * tuple, a key, or one or more key parts. Returns the number of matched
 * tuples.
 */
static int
lbox_index_count(struct lua_State *L)
{
	Index *index = lua_checkindex(L, 1);
	int argc = lua_gettop(L) - 1;
	if (argc == 0)
		luaL_error(L, "index.count(): one or more arguments expected");

	/* preparing single or multi-part key */
	size_t allocated_size = palloc_allocated(fiber->gc_pool);
	const void *key;
	u32 key_part_count;
	if (argc == 1 && lua_type(L, 2) == LUA_TUSERDATA) {
		/* Searching by tuple. */
		struct tuple *tuple = lua_checktuple(L, 2);
		key = tuple->data;
		key_part_count = tuple->field_count;
	} else {
		/* Single or multi- part key. */
		key_part_count = argc;
		struct tbuf *data = tbuf_new(fiber->gc_pool);
		for (u32 i = 0; i < argc; ++i) {
			enum field_data_type type = UNKNOWN;
			if (i < index->key_def->part_count) {
				type = index->key_def->parts[i].type;
			}
			append_key_part(L, i + 2, data, type);
		}
		key = data->data;
	}
	u32 count = 0;
	/* preparing index iterator */
	struct iterator *it = index->position;
	[index initIterator: it :ITER_EQ :key :key_part_count];
	/* iterating over the index and counting tuples */
	struct tuple *tuple;
	while ((tuple = it->next(it)) != NULL)
		count++;

	/* truncate memory used by key construction */
	ptruncate(fiber->gc_pool, allocated_size);

	/* returning subtree size */
	lua_pushnumber(L, count);
	return 1;
}

static const struct luaL_reg lbox_index_meta[] = {
	{"__tostring", lbox_index_tostring},
	{"__len", lbox_index_len},
	{"part_count", lbox_index_part_count},
	{"min", lbox_index_min},
	{"max", lbox_index_max},
	{"random", lbox_index_random},
	{"next", lbox_index_next},
	{"iterator", lbox_index_iterator},
	{"count", lbox_index_count},
	{NULL, NULL}
};

static const struct luaL_reg indexlib [] = {
	{"new", lbox_index_new},
	{NULL, NULL}
};

static const struct luaL_reg lbox_iterator_meta[] = {
	{"__gc", lbox_iterator_gc},
	{NULL, NULL}
};

/* }}} */

/** {{{ Lua I/O: facilities to intercept box output
 * and push into Lua stack.
 */

struct port_lua
{
	struct port_vtab *vtab;
	struct lua_State *L;
};

static inline struct port_lua *
port_lua(struct port *port) { return (struct port_lua *) port; }

/*
 * For addU32/dupU32 do nothing -- the only u32 Box can give
 * us is tuple count, and we don't need it, since we intercept
 * everything into Lua stack first.
 * @sa iov_add_multret
 */

static void
port_lua_add_tuple(struct port *port, struct tuple *tuple,
		   u32 flags __attribute__((unused)))
{
	lua_State *L = port_lua(port)->L;
	@try {
		lbox_pushtuple(L, tuple);
	} @catch (...) {
		tnt_raise(ClientError, :ER_PROC_LUA, lua_tostring(L, -1));
	}
}

struct port_vtab port_lua_vtab = {
	port_lua_add_tuple,
	port_null_eof,
};

static struct port *
port_lua_create(struct lua_State *L)
{
	struct port_lua *port = palloc(fiber->gc_pool, sizeof(struct port_lua));
	port->vtab = &port_lua_vtab;
	port->L = L;
	return (struct port *) port;
}

/**
 * Convert a Lua table to a tuple with as little
 * overhead as possible.
 */
static struct tuple *
lua_table_to_tuple(struct lua_State *L, int index)
{
	u32 field_count = 0;
	u32 tuple_len = 0;

	size_t field_len;

	/** First go: calculate tuple length. */
	lua_pushnil(L);  /* first key */
	while (lua_next(L, index) != 0) {
		++field_count;

		switch (lua_type(L, -1)) {
		case LUA_TNUMBER:
		{
			uint64_t n = lua_tonumber(L, -1);
			field_len = n > UINT32_MAX ? sizeof(uint64_t) : sizeof(uint32_t);
			break;
		}
		case LUA_TCDATA:
		{
			/* Check if we can convert. */
			(void) tarantool_lua_tointeger64(L, -1);
			field_len = sizeof(u64);
			break;
		}
		case LUA_TSTRING:
		{
			(void) lua_tolstring(L, -1, &field_len);
			break;
		}
		default:
			tnt_raise(ClientError, :ER_PROC_RET,
				  lua_typename(L, lua_type(L, -1)));
			break;
		}
		tuple_len += field_len + varint32_sizeof(field_len);
		lua_pop(L, 1);
	}
	struct tuple *tuple = tuple_alloc(tuple_len);
	/*
	 * Important: from here and on if there is an exception,
	 * the tuple is leaked.
	 */
	tuple->field_count = field_count;
	void *pos = tuple->data;

	/* Second go: store data in the tuple. */

	lua_pushnil(L);  /* first key */
	while (lua_next(L, index) != 0) {
		switch (lua_type(L, -1)) {
		case LUA_TNUMBER:
		{
			uint64_t n = lua_tonumber(L, -1);
			if (n > UINT32_MAX) {
				pos = pack_lstr(pos, &n, sizeof(n));
			} else {
				uint32_t n32 = (uint32_t) n;
				pos = pack_lstr(pos, &n32, sizeof(n32));
			}
			break;
		}
		case LUA_TCDATA:
		{
			uint64_t n = tarantool_lua_tointeger64(L, -1);
			pos = pack_lstr(pos, &n, sizeof(n));
			break;
		}
		case LUA_TSTRING:
		{
			const char *field = lua_tolstring(L, -1, &field_len);
			pos = pack_lstr(pos, field, field_len);
			break;
		}
		default:
			assert(false);
			break;
		}
		lua_pop(L, 1);
	}
	return tuple;
}

static struct tuple*
lua_totuple(struct lua_State *L, int index)
{
	int type = lua_type(L, index);
	struct tuple *tuple;
	switch (type) {
	case LUA_TTABLE:
	{
		tuple = lua_table_to_tuple(L, index);
		break;
	}
	case LUA_TNUMBER:
	{
		size_t len = sizeof(u32);
		u32 num = lua_tointeger(L, index);
		tuple = tuple_alloc(len + varint32_sizeof(len));
		tuple->field_count = 1;
		pack_lstr(tuple->data, &num, len);
		break;
	}
	case LUA_TCDATA:
	{
		u64 num = tarantool_lua_tointeger64(L, index);
		size_t len = sizeof(u64);
		tuple = tuple_alloc(len + varint32_sizeof(len));
		tuple->field_count = 1;
		pack_lstr(tuple->data, &num, len);
		break;
	}
	case LUA_TSTRING:
	{
		size_t len;
		const char *str = lua_tolstring(L, index, &len);
		tuple = tuple_alloc(len + varint32_sizeof(len));
		tuple->field_count = 1;
		pack_lstr(tuple->data, str, len);
		break;
	}
	case LUA_TNIL:
	case LUA_TBOOLEAN:
	{
		const char *str = tarantool_lua_tostring(L, index);
		size_t len = strlen(str);
		tuple = tuple_alloc(len + varint32_sizeof(len));
		tuple->field_count = 1;
		pack_lstr(tuple->data, str, len);
		break;
	}
	case LUA_TUSERDATA:
	{
		tuple = lua_istuple(L, index);
		if (tuple)
			break;
	}
	default:
		/*
		 * LUA_TNONE, LUA_TTABLE, LUA_THREAD, LUA_TFUNCTION
		 */
		tnt_raise(ClientError, :ER_PROC_RET, lua_typename(L, type));
		break;
	}
	return tuple;
}

static void
port_add_lua_ret(struct port *port, struct lua_State *L, int index)
{
	struct tuple *tuple = lua_totuple(L, index);
	@try {
		port_add_tuple(port, tuple, BOX_RETURN_TUPLE);
	} @finally {
		if (tuple->refs == 0)
			tuple_free(tuple);
	}
}

/**
 * Add all elements from Lua stack to fiber iov.
 *
 * To allow clients to understand a complex return from
 * a procedure, we are compatible with SELECT protocol,
 * and return the number of return values first, and
 * then each return value as a tuple.
 */
static void
port_add_lua_multret(struct port *port, struct lua_State *L)
{
	int nargs = lua_gettop(L);
	for (int i = 1; i <= nargs; ++i)
		port_add_lua_ret(port, L, i);
}

/* }}} */

/**
 * The main extension provided to Lua by Tarantool/Box --
 * ability to call INSERT/UPDATE/SELECT/DELETE from within
 * a Lua procedure.
 *
 * This is a low-level API, and it expects
 * all arguments to be packed in accordance
 * with the binary protocol format (iproto
 * header excluded).
 *
 * Signature:
 * box.process(op_code, request)
 */
static int
lbox_process(lua_State *L)
{
	u32 op = lua_tointeger(L, 1); /* Get the first arg. */
	size_t sz;
	const void *req = luaL_checklstring(L, 2, &sz); /* Second arg. */
/*         if (op == CALL) { */
		/*
		 * We should not be doing a CALL from within a CALL.
		 * To invoke one stored procedure from another, one must
		 * do it in Lua directly. This deals with
		 * infinite recursion, stack overflow and such.
		 */
/*                 return luaL_error(L, "box.process(CALL, ...) is not allowed"); */
/*         } */
	int top = lua_gettop(L); /* to know how much is added by rw_callback */

	size_t allocated_size = palloc_allocated(fiber->gc_pool);
	struct port *port_lua = port_lua_create(L);
	@try {
		box_process(port_lua, op, req, sz);
	} @finally {
		/*
		 * This only works as long as port_lua doesn't
		 * use fiber->cleanup and fiber->gc_pool.
		 */
		ptruncate(fiber->gc_pool, allocated_size);
	}
	return lua_gettop(L) - top;
}

static int
lbox_raise(lua_State *L)
{
	if (lua_gettop(L) != 2)
		luaL_error(L, "box.raise(): bad arguments");
	uint32_t code = lua_tointeger(L, 1);
	if (!code)
		luaL_error(L, "box.raise(): unknown error code");
	const char *str = lua_tostring(L, 2);
	tnt_raise(ClientError, :code :str);
	return 0;
}

/**
 * A helper to find a Lua function by name and put it
 * on top of the stack.
 */
static void
box_lua_find(lua_State *L, const char *name, const char *name_end)
{
	int index = LUA_GLOBALSINDEX;
	const char *start = name, *end;

	while ((end = memchr(start, '.', name_end - start))) {
		lua_checkstack(L, 3);
		lua_pushlstring(L, start, end - start);
		lua_gettable(L, index);
		if (! lua_istable(L, -1))
			tnt_raise(ClientError, :ER_NO_SUCH_PROC,
				  name_end - name, name);
		start = end + 1; /* next piece of a.b.c */
		index = lua_gettop(L); /* top of the stack */
	}
	lua_pushlstring(L, start, name_end - start);
	lua_gettable(L, index);
	if (! lua_isfunction(L, -1)) {
		/* lua_call or lua_gettable would raise a type error
		 * for us, but our own message is more verbose. */
		tnt_raise(ClientError, :ER_NO_SUCH_PROC,
			  name_end - name, name);
	}
	/* setting stack that it would contain only
	 * the function pointer. */
	if (index != LUA_GLOBALSINDEX) {
		lua_replace(L, 1);
		lua_settop(L, 1);
	}
}

/**
 * Invoke a Lua stored procedure from the binary protocol
 * (implementation of 'CALL' command code).
 */
void
box_lua_execute(struct request *request, struct port *port)
{
	const void **reqpos = &request->data;
	const void *reqend = request->data + request->len;
	lua_State *L = lua_newthread(root_L);
	int coro_ref = luaL_ref(root_L, LUA_REGISTRYINDEX);
	/* Request flags: not used. */
	(void) (pick_u32(reqpos, reqend));
	@try {
		u32 field_len;
		/* proc name */
		const void *field = pick_field_str(reqpos, reqend, &field_len);
		box_lua_find(L, field, field + field_len);
		/* Push the rest of args (a tuple). */
		u32 nargs = pick_u32(reqpos, reqend);
		luaL_checkstack(L, nargs, "call: out of stack");
		for (int i = 0; i < nargs; i++) {
			field = pick_field_str(reqpos, reqend, &field_len);
			lua_pushlstring(L, field, field_len);
		}
		lua_call(L, nargs, LUA_MULTRET);
		/* Send results of the called procedure to the client. */
		port_add_lua_multret(port, L);
	} @catch (tnt_Exception *e) {
		@throw;
	} @catch (...) {
		tnt_raise(ClientError, :ER_PROC_LUA, lua_tostring(L, -1));
	} @finally {
		/*
		 * Allow the used coro to be garbage collected.
		 * @todo: cache and reuse it instead.
		 */
		luaL_unref(root_L, LUA_REGISTRYINDEX, coro_ref);
	}
}

static void
box_index_init_iterator_types(struct lua_State *L, int idx)
{
	for (int i = 0; i < iterator_type_MAX; i++) {
		assert(strncmp(iterator_type_strs[i], "ITER_", 5) == 0);
		lua_pushnumber(L, i);
		/* cut ITER_ prefix from enum name */
		lua_setfield(L, idx, iterator_type_strs[i] + 5);
	}
}

/**
 * Pack our BER integer into luaL_Buffer
 */
static void
luaL_addvarint32(luaL_Buffer *b, u32 value)
{
	char buf[sizeof(u32)+1];
	char *bufend = pack_varint32(buf, value);
	luaL_addlstring(b, buf, bufend - buf);
}

/**
 * Convert box.pack() format specifier to Tarantool
 * binary protocol UPDATE opcode
 */
static char format_to_opcode(char format)
{
	switch (format) {
	case '=': return 0;
	case '+': return 1;
	case '&': return 2;
	case '^': return 3;
	case '|': return 4;
	case ':': return 5;
	case '#': return 6;
	case '!': return 7;
	case '-': return 8;
	default: return format;
	}
}

/**
 * Counterpart to @a format_to_opcode
 */
static char opcode_to_format(char opcode)
{
	switch (opcode) {
	case 0: return '=';
	case 1: return '+';
	case 2: return '&';
	case 3: return '^';
	case 4: return '|';
	case 5: return ':';
	case 6: return '#';
	case 7: return '!';
	case 8: return '-';
	default: return opcode;
	}
}

static int
luaL_packsize(struct lua_State *L, int index)
{
	switch (lua_type(L, index)) {
	case LUA_TNUMBER:
	case LUA_TCDATA:
	case LUA_TSTRING:
		return 1;
	case LUA_TUSERDATA:
	{
		struct tuple *t = lua_istuple(L, index);
		if (t == NULL)
			luaL_error(L, "box.pack: unsupported type");
		return t->field_count;
	}
	case LUA_TTABLE:
	{
		int size = 0;
		lua_pushnil(L);
		while (lua_next(L, index) != 0) {
			/* Sic: use absolute index. */
			size += luaL_packsize(L, lua_gettop(L));
			lua_pop(L, 1);
		}
		return size;
	}
	default:
		luaL_error(L, "box.pack: unsupported type");
	}
	return 0;
}

static void
luaL_packvalue(struct lua_State *L, luaL_Buffer *b, int index)
{
	size_t size;
	const char *str;
	u32 u32buf;
	u64 u64buf;
	switch (lua_type(L, index)) {
	case LUA_TNUMBER:
		u64buf = lua_tonumber(L, index);
		if (u64buf > UINT32_MAX) {
			str = (char *) &u64buf;
			size = sizeof(u64);
		} else {
			u32buf = (u32) u64buf;
			str = (char *) &u32buf;
			size = sizeof(u32);
		}
		break;
	case LUA_TCDATA:
		u64buf = tarantool_lua_tointeger64(L, index);
		str = (char *) &u64buf;
		size = sizeof(u64);
		break;
	case LUA_TSTRING:
		str = luaL_checklstring(L, index, &size);
		break;
	case LUA_TUSERDATA:
	{
		struct tuple *tu = lua_istuple(L, index);
		if (tu == NULL)
			luaL_error(L, "box.pack: unsupported type");
		luaL_addlstring(b, (char*)tu->data, tu->bsize);
		return;
	}
	case LUA_TTABLE:
	{
		lua_pushnil(L);
		while (lua_next(L, index) != 0) {
			/* Sic: use absolute index. */
			luaL_packvalue(L, b, lua_gettop(L));
			lua_pop(L, 1);
		}
		return;
	}
	default:
		luaL_error(L, "box.pack: unsupported type");
		break;
	}
	luaL_addvarint32(b, size);
	luaL_addlstring(b, str, size);
}

static void
luaL_packstack(struct lua_State *L, luaL_Buffer *b, int first, int last)
{
	int size = 0;
	/* sic: if arg_count is 0, first > last */
	for (int i = first; i <= last; ++i)
		size += luaL_packsize(L, i);
	luaL_addlstring(b, (char *) &size, sizeof(size));
	for (int i = first; i <= last; ++i)
		luaL_packvalue(L, b, i);
}


/**
 * To use Tarantool/Box binary protocol primitives from Lua, we
 * need a way to pack Lua variables into a binary representation.
 * We do it by exporting a helper function
 *
 * box.pack(format, args...)
 *
 * which takes the format, which is very similar to Perl 'pack'
 * format, and a list of arguments, and returns a binary string
 * which has the arguments packed according to the format.
 *
 * For example, a typical SELECT packet packs in Lua like this:
 *
 * pkt = box.pack("iiiiiip", -- pack format
 *                         0, -- space id
 *                         0, -- index id
 *                         0, -- offset
 *                         2^32, -- limit
 *                         1, -- number of SELECT arguments
 *                         1, -- tuple cardinality
 *                         key); -- the key to use for SELECT
 *
 * @sa doc/box-protocol.txt, binary protocol description
 * @todo: implement box.unpack(format, str), for testing purposes
 */
static int
lbox_pack(struct lua_State *L)
{
	luaL_Buffer b;
	const char *format = luaL_checkstring(L, 1);
	/* first arg comes second */
	int i = 2;
	int nargs = lua_gettop(L);
	u16 u16buf;
	u32 u32buf;
	u64 u64buf;
	size_t size;
	const char *str;

	luaL_buffinit(L, &b);

	while (*format) {
		if (i > nargs)
			luaL_error(L, "box.pack: argument count does not match "
				   "the format");
		switch (*format) {
		case 'B':
		case 'b':
			/* signed and unsigned 8-bit integers */
			u32buf = lua_tointeger(L, i);
			if (u32buf > 0xff)
				luaL_error(L, "box.pack: argument too big for "
					   "8-bit integer");
			luaL_addchar(&b, (char) u32buf);
			break;
		case 'S':
		case 's':
			/* signed and unsigned 8-bit integers */
			u32buf = lua_tointeger(L, i);
			if (u32buf > 0xffff)
				luaL_error(L, "box.pack: argument too big for "
					   "16-bit integer");
			u16buf = (u16) u32buf;
			luaL_addlstring(&b, (char *) &u16buf, sizeof(u16));
			break;
		case 'I':
		case 'i':
			/* signed and unsigned 32-bit integers */
			u32buf = lua_tointeger(L, i);
			luaL_addlstring(&b, (char *) &u32buf, sizeof(u32));
			break;
		case 'L':
		case 'l':
			/* signed and unsigned 64-bit integers */
			u64buf = tarantool_lua_tointeger64(L, i);
			luaL_addlstring(&b, (char *) &u64buf, sizeof(u64));
			break;
		case 'w':
			/* Perl 'pack' BER-encoded integer */
			luaL_addvarint32(&b, lua_tointeger(L, i));
			break;
		case 'A':
		case 'a':
			/* A sequence of bytes */
			str = luaL_checklstring(L, i, &size);
			luaL_addlstring(&b, str, size);
			break;
		case 'P':
		case 'p':
			luaL_packvalue(L, &b, i);
			break;
		case 'V':
		{
			int arg_count = luaL_checkint(L, i);
			if (i + arg_count > nargs)
				luaL_error(L, "box.pack: argument count does not match "
					   "the format");
			luaL_packstack(L, &b, i + 1, i + arg_count);
			i += arg_count;
			break;
		}
		case '=':
			/* update tuple set foo = bar */
		case '+':
			/* set field += val */
		case '-':
			/* set field -= val */
		case '&':
			/* set field & =val */
		case '|':
			/* set field |= val */
		case '^':
			/* set field ^= val */
		case ':':
			/* splice */
		case '#':
			/* delete field */
		case '!':
			/* insert field */
			/* field no */
			u32buf = (u32) lua_tointeger(L, i);
			luaL_addlstring(&b, (char *) &u32buf, sizeof(u32));
			luaL_addchar(&b, format_to_opcode(*format));
			break;
		default:
			luaL_error(L, "box.pack: unsupported pack "
				   "format specifier '%c'", *format);
		}
		i++;
		format++;
	}
	luaL_pushresult(&b);
	return 1;
}

static int
lbox_unpack(struct lua_State *L)
{
	size_t format_size = 0;
	const char *format = luaL_checklstring(L, 1, &format_size);
	const char *f = format;

	size_t str_size = 0;
	const void *str =  luaL_checklstring(L, 2, &str_size);
	const void *end = str + str_size;
	const void *s = str;

	int i = 0;

	char charbuf;
	u8  u8buf;
	u16 u16buf;
	u32 u32buf;

#define CHECK_SIZE(cur) if (unlikely((cur) >= end)) {	                \
	luaL_error(L, "box.unpack('%c'): got %d bytes (expected: %d+)",	\
		   *f, (int) (end - str), (int) 1 + ((cur) - str));	\
}
	while (*f) {
		switch (*f) {
		case 'b':
			CHECK_SIZE(s);
			u8buf = *(u8 *) s;
			lua_pushnumber(L, u8buf);
			s++;
			break;
		case 's':
			CHECK_SIZE(s + 1);
			u16buf = *(u16 *) s;
			lua_pushnumber(L, u16buf);
			s += 2;
			break;
		case 'i':
			CHECK_SIZE(s + 3);
			u32buf = *(u32 *) s;
			lua_pushnumber(L, u32buf);
			s += 4;
			break;
		case 'l':
			CHECK_SIZE(s + 7);
			luaL_pushnumber64(L, *(uint64_t*) s);
			s += 8;
			break;
		case 'w':
			/* pick_varint32 throws exception on error. */
			u32buf = pick_varint32(&s, end);
			lua_pushnumber(L, u32buf);
			break;
		case 'a':
		case 'A':
			lua_pushlstring(L, s, end - s);
			s = end;
			break;
		case 'P':
		case 'p':
			/* pick_varint32 throws exception on error. */
			u32buf = pick_varint32(&s, end);
			CHECK_SIZE(s + u32buf - 1);
			lua_pushlstring(L, (const char *) s, u32buf);
			s += u32buf;
			break;
		case '=':
			/* update tuple set foo = bar */
		case '+':
			/* set field += val */
		case '-':
			/* set field -= val */
		case '&':
			/* set field & =val */
		case '|':
			/* set field |= val */
		case '^':
			/* set field ^= val */
		case ':':
			/* splice */
		case '#':
			/* delete field */
		case '!':
			/* insert field */
			CHECK_SIZE(s + 4);

			/* field no */
			u32buf = *(u32 *) s;

			/* opcode */
			charbuf = *(char *) (s + 4);
			charbuf = opcode_to_format(charbuf);
			if (charbuf != *f) {
				luaL_error(L, "box.unpack('%s'): "
					   "unexpected opcode: "
					   "offset %d, expected '%c',"
					   "found '%c'",
					   format, s - str, *f, charbuf);
			}

			lua_pushnumber(L, u32buf);
			s += 5;
			break;
		default:
			luaL_error(L, "box.unpack: unsupported "
				   "format specifier '%c'", *f);
		}
		i++;
		f++;
	}

	assert(s <= end);

	if (s != end) {
		luaL_error(L, "box.unpack('%s'): too many bytes: "
			   "unpacked %d, total %d",
			   format, s - str, str_size);
	}

	return i;

#undef CHECK_SIZE
}

static const struct luaL_reg boxlib[] = {
	{"process", lbox_process},
	{"raise", lbox_raise},
	{"pack", lbox_pack},
	{"unpack", lbox_unpack},
	{NULL, NULL}
};

void
mod_lua_init(struct lua_State *L)
{
	/* box, box.tuple */
	tarantool_lua_register_type(L, tuplelib_name, lbox_tuple_meta);
	luaL_register(L, tuplelib_name, lbox_tuplelib);
	lua_pop(L, 1);
	luaL_register(L, "box", boxlib);
	lua_pop(L, 1);
	/* box.index */
	tarantool_lua_register_type(L, indexlib_name, lbox_index_meta);
	luaL_register(L, "box.index", indexlib);
	box_index_init_iterator_types(L, -2);
	lua_pop(L, 1);
	tarantool_lua_register_type(L, iteratorlib_name, lbox_iterator_meta);
	/* Load box.lua */
	if (luaL_dostring(L, box_lua))
		panic("Error loading box.lua: %s", lua_tostring(L, -1));

	assert(lua_gettop(L) == 0);

	root_L = L;
}
