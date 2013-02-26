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
#include "pickle.h"
#include "box_lua_space.h"
#include "space.h"
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
#include <say.h>
#include <tbuf.h>
#include <fiber.h>
#include <lua/init.h>

static int
lbox_spaces_index(struct lua_State *L);
static int
lbox_pushspace(struct lua_State *L, struct space *space);

static const char *lbox_spaces_name = "box.spaces";

static const struct luaL_reg lbox_spaces_meta[] = {
	{"__index", lbox_spaces_index},
	{NULL, NULL}
};

/**
 * Make a single space available in Lua,
 * via box.space[] array.
 *
 * @return A new table representing a space on top of the Lua
 * stack.
 */
static int
lbox_pushspace(struct lua_State *L, struct space *space)
{
	lua_newtable(L);

	/* space.cardinality */
	lua_pushstring(L, "cardinality");
	lua_pushnumber(L, space->arity);
	lua_settable(L, -3);

	/* space.n */
	lua_pushstring(L, "n");
	lua_pushnumber(L, space->no);
	lua_settable(L, -3);

	/* space name */
	lua_pushstring(L, "name");
	assert (space->name != NULL);
	const void *name = space->name;
	u32 name_len = load_varint32(&name);
	lua_pushlstring(L, name, name_len);
	lua_settable(L, -3);

	/* all exists spaces are enabled */
	lua_pushstring(L, "enabled");
	lua_pushboolean(L, 1);
	lua_settable(L, -3);

	/* legacy field */
	lua_pushstring(L, "estimated_rows");
	lua_pushnumber(L, 0);
	lua_settable(L, -3);

	/* space.index */
	lua_pushstring(L, "index");
	lua_newtable(L);
	/*
	 * Fill space.index table with
	 * all defined indexes.
	 */
	for (int i = 0; i < BOX_INDEX_MAX; i++) {
		if (space->index[i] == NULL)
			continue;

		const void *name = space->index[i]->name;
		assert (name != NULL);
		u32 name_len = load_varint32(&name);
		lua_pushinteger(L, space->index[i]->no);

		lua_newtable(L);		/* space.index[i] */

		/* index_no */
		lua_pushstring(L, "n");
		lua_pushnumber(L, space->index[i]->no);
		lua_settable(L, -3);

		/* index name */
		lua_pushstring(L, "name");
		lua_pushlstring(L, name, name_len);
		lua_settable(L, -3);

		lua_pushstring(L, "unique");
		lua_pushboolean(L, space->key_defs[i].is_unique);
		lua_settable(L, -3);

		lua_pushstring(L, "type");

		lua_pushstring(L, index_type_strs[space->key_defs[i].type]);
		lua_settable(L, -3);

		lua_pushstring(L, "key_field");
		lua_newtable(L);

		for (u32 j = 0; j < space->key_defs[i].part_count; j++) {
			lua_pushnumber(L, j);
			lua_newtable(L);

			lua_pushstring(L, "type");
			switch (space->key_defs[i].parts[j].type) {
			case NUM:
				lua_pushstring(L, "NUM");
				break;
			case NUM64:
				lua_pushstring(L, "NUM64");
				break;
			case STRING:
				lua_pushstring(L, "STR");
				break;
			default:
				lua_pushstring(L, "UNKNOWN");
				break;
			}
			lua_settable(L, -3);

			lua_pushstring(L, "fieldno");
			lua_pushnumber(L, space->key_defs[i].parts[j].fieldno);
			lua_settable(L, -3);

			lua_settable(L, -3);
		}

		lua_settable(L, -3);	/* space[i].key_field */

		lua_pushlstring(L, name, name_len);
		lua_pushvalue(L, -2);
		lua_settable(L, -5);	/* space[i] */
		lua_settable(L, -3);	/* space[name] */
	}
	lua_settable(L, -3);	/* push space.index */

	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	lua_pushstring(L, "bless_space");
	lua_gettable(L, -2);

	lua_pushvalue(L, -3);			/* box, bless, space */
	lua_call(L, 1, 0);
	lua_pop(L, 1);	/* cleanup stack */

	return 1;
}

static int
lbox_spaces_index(struct lua_State *L)
{
	struct space *sp = NULL;
	if (lua_type(L, 2) == LUA_TNUMBER) {
		lua_Integer req = lua_tointeger(L, 2);
		if (req >= 0 && req < UINT_MAX) {
			sp = space_find_by_no((u32) req);
		}
	} else {
		size_t len = 0;
		const char *req = lua_tolstring(L, 2, &len);
		struct tbuf *buf = tbuf_new(fiber->gc_pool);
		write_varint32(buf, len);
		tbuf_append(buf, req, len);
		sp = space_find_by_name(tbuf_str(buf));
	}

	if (sp == NULL) {
		lua_pushnil(L);
		return 1;
	}

	lbox_pushspace(L, sp);
	lua_pushvalue(L, -1);
	lua_rawseti(L, 1, sp->no);

	const void *name = sp->name;
	u32 name_len = load_varint32(&name);
	lua_pushlstring(L, name, name_len);
	lua_pushvalue(L, -2);
	lua_rawset(L, 1);

	return 1;
}

void
box_lua_space_cache_clear(struct lua_State *L, struct space *sp)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	if (!lua_istable(L, -1))
		return;

	lua_getfield(L, -1, "space");
	if (!lua_istable(L, -1))
		return;

	lua_pushnil(L);
	lua_rawseti(L, -2, sp->no);

	const void *name;
	u32 name_len;
	load_field_str(sp->name, &name, &name_len);
	if (name_len == 0)
		return;

	lua_pushlstring(L, name, name_len);
	lua_pushnil(L);
	lua_rawset(L, -3);
}

void
box_lua_load_cfg(struct lua_State *L)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	if (!lua_istable(L, -1))
		return;

	lua_newtable(L);
	lua_setfield(L, -2, "space");

	lua_getfield(L, -1, "space");
	tarantool_lua_register_type(L, lbox_spaces_name, lbox_spaces_meta);
	luaL_register(L, lbox_spaces_name, lbox_spaces_meta);

	lua_setmetatable(L, -2);
	lua_pop(L, 1);
}
