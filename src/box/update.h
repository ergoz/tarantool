#ifndef TARANTOOL_BOX_UPDATE_H_INCLUDED
#define TARANTOOL_BOX_UPDATE_H_INCLUDED
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
#include <third_party/queue.h>
#include <util.h>
#include <rope.h>
#include <stdbool.h>

enum {
	/** A limit on how many operations a single UPDATE can have. */
	BOX_UPDATE_OP_CNT_MAX = 4000
};

/** UPDATE operation codes. */
#define UPDATE_OP_CODES(_)   \
	_(UPDATE_OP_SET, 0)      \
	_(UPDATE_OP_ADD, 1)      \
	_(UPDATE_OP_AND, 2)      \
	_(UPDATE_OP_XOR, 3)      \
	_(UPDATE_OP_OR, 4)       \
	_(UPDATE_OP_SPLICE, 5)   \
	_(UPDATE_OP_DELETE, 6)   \
	_(UPDATE_OP_INSERT, 7)   \
	_(UPDATE_OP_SUBTRACT, 8) \
	_(UPDATE_OP_MAX, 10)

ENUM(update_op_codes, UPDATE_OP_CODES);

/** Argument of SET operation. */
struct op_set_arg {
	u32 length;
	const void *value;
};

/** Argument of ADD, AND, XOR, OR operations. */
struct op_arith_arg {
	u32 val_size;
	union {
		i32 i32_val;
		i64 i64_val;
	};
};

/** Argument of SPLICE. */
struct op_splice_arg {
	i32 offset;        /** splice position */
	i32 cut_length;    /** cut this many bytes. */
	const void *paste; /** paste what? */
	i32 paste_length;  /** paste this many bytes. */
	/** Offset of the tail in the old field */
	i32 tail_offset;
	/** Size of the tail. */
	i32 tail_length;
};

union update_op_arg {
	struct op_set_arg set;
	struct op_arith_arg arith;
	struct op_splice_arg splice;
};

struct update_op;

typedef void (*init_op_func)(struct rope *rope, struct update_op *op);
typedef void (*do_op_func)(union update_op_arg *arg, const void *in, void *out);

/** A set of functions and properties to initialize and do an op. */
struct update_op_meta {
	init_op_func init_op;
	do_op_func do_op;
	bool works_in_place;
};

/** A single UPDATE operation. */
struct update_op {
	STAILQ_ENTRY(update_op) next;
	struct update_op_meta *meta;
	union update_op_arg arg;
	u32 field_no;
	u32 new_field_len;
	u8 opcode;
};

size_t
update_calc_new_tuple_length(struct rope *rope);

struct rope *
update_create_rope(struct update_op *op, struct update_op *op_end,
                   struct tuple *tuple);

void
update_do_ops(struct rope *rope, struct tuple *new_tuple);

struct update_op *
update_read_ops(const void **reqpos, const void *reqend, u32 op_cnt);

#endif
