
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
#include "tuple.h"
#include "update.h"
#include "request.h"
#include "txn.h"
#include "index.h"
#include "space.h"
#include "port.h"
#include "box_lua.h"
#include <errinj.h>
#include <pickle.h>
#include <fiber.h>
#include <rope.h>

STRS(update_op_codes, UPDATE_OP_CODES);

/**
 * UPDATE request implementation.
 *
 * UPDATE request is represented by a sequence of operations, each
 * working with a single field. There also are operations which
 * add or remove fields. More than one operation on the same field
 * is allowed.
 *
 * Supported field change operations are: SET, ADD, bitwise AND,
 * XOR and OR, SPLICE.
 *
 * Supported tuple change operations are: SET (when SET field_no
 * == last_field_no + 1), DELETE, INSERT, PUSH and POP.
 * If the number of fields in a tuple is altered by an operation,
 * field index of all following operation is evaluated against the
 * new tuple.
 *
 * Despite the allowed complexity, a typical use case for UPDATE
 * is when the operation count is much less than field count in
 * a tuple.
 *
 * With the common case in mind, UPDATE tries to minimize
 * the amount of unnecessary temporary tuple copies.
 *
 * First, operations are parsed and initialized. Then, the
 * resulting tuple length is calculated. A new tuple is allocated.
 * Finally, operations are applied sequentially, each copying data
 * from the old tuple to the new tuple.
 *
 * With this approach, cost of UPDATE is proportional to O(tuple
 * length) + O(C * log C), where C is the number of operations in
 * the request, and data is copied from the old tuple to the new
 * one only once.
 *
 * There are two special cases in this general scheme, which
 * are handled as follows:
 *
 * 1) As long as INSERT, DELETE, PUSH and POP change the relative
 * field order, an auxiliary data structure is necessary to look
 * up fields in the "old" tuple by field number. Such field
 * index is built on demand, using "rope" data structure.
 *
 * A rope is a binary tree designed to store long strings built
 * from pieces. Each tree node points to a substring of a large
 * string. In our case, each rope node points at a range of
 * fields, initially in the old tuple, and then, as fields are
 * added and deleted by UPDATE, in the "current" tuple.
 * Note, that the tuple itself is not materialized: when
 * operations which affect field count are initialized, the rope
 * is updated to reflect the new field order.
 * In particular, if a field is deleted by an operation,
 * it disappears from the rope and all subsequent operations
 * on this field number instead affect the field following the
 * one.
 *
 * 2) Multiple operations can occur on the same field, and not all
 * operations, by design, can work correctly "in place".
 * For example, SET(4, "aaaaaa") followed by SPLICE(4, 0, 5, 0, * ""),
 * results in zero increase of total tuple length, but requires
 * space to store SET results. To make sure we never go beyond
 * allocated memory, the main loop may allocate a temporary buffer
 * to store intermediate operation results.
 */

STAILQ_HEAD(op_list, update_op);

/**
 * We can have more than one operation on the same field.
 * A descriptor of one changed field.
 */
struct update_field {
	/** UPDATE operations against the first field in the range. */
	struct op_list ops;
	/** Points at start of field *data* in the old tuple. */
	const void *old;
	/** End of the old field. */
	const void *tail;
	/**
	 * Length of the "tail" in the old tuple from end
	 * of old data to the beginning of the field in the
	 * next update_field structure.
         */
	u32 tail_len;
};

static void
update_field_init(struct update_field *field,
		  const void *old, u32 old_len, u32 tail_len)
{
	STAILQ_INIT(&field->ops);
	field->old = old;
	field->tail = old + old_len;
	field->tail_len = tail_len;
}

static inline u32
update_field_len(struct update_field *f)
{
	struct update_op *last = STAILQ_LAST(&f->ops, update_op, next);
	return last? last->new_field_len : f->tail - f->old;
}

static inline void
op_check_field_no(u32 field_no, u32 field_max)
{
	if (field_no > field_max)
		tnt_raise(ClientError, :ER_NO_SUCH_FIELD, field_no);
}

static inline void
op_adjust_field_no(struct update_op *op, u32 field_max)
{
	if (op->field_no == UINT32_MAX)
		op->field_no = field_max;
	else
		op_check_field_no(op->field_no, field_max);
}


static void
do_update_op_set(struct op_set_arg *arg, const void *in __attribute__((unused)),
		 void *out)
{
	memcpy(out, arg->value, arg->length);
}

static void
do_update_op_add(struct op_arith_arg *arg, const void *in, void *out)
{
	if (arg->val_size == sizeof(i32))
		*(i32 *)out = *(i32 *)in + arg->i32_val;
	else
		*(i64 *)out = *(i64 *)in + arg->i64_val;
}

static void
do_update_op_subtract(struct op_arith_arg *arg, const void *in, void *out)
{
	if (arg->val_size == sizeof(i32))
		*(i32 *)out = *(i32 *)in - arg->i32_val;
	else
		*(i64 *)out = *(i64 *)in - arg->i64_val;
}

static void
do_update_op_and(struct op_arith_arg *arg, const void *in, void *out)
{
	if (arg->val_size == sizeof(i32))
		*(i32 *)out = *(i32 *)in & arg->i32_val;
	else
		*(i64 *)out = *(i64 *)in & arg->i64_val;
}

static void
do_update_op_xor(struct op_arith_arg *arg, const void *in, void *out)
{
	if (arg->val_size == sizeof(i32))
		*(i32 *)out = *(i32 *)in ^ arg->i32_val;
	else
		*(i64 *)out = *(i64 *)in ^ arg->i64_val;
}

static void
do_update_op_or(struct op_arith_arg *arg, const void *in, void *out)
{
	if (arg->val_size == sizeof(i32))
		*(i32 *)out = *(i32 *)in | arg->i32_val;
	else
		*(i64 *)out = *(i64 *)in | arg->i64_val;
}

static void
do_update_op_splice(struct op_splice_arg *arg, const void *in, void *out)
{
	memcpy(out, in, arg->offset); /* copy field head. */
	out += arg->offset;
	memcpy(out, arg->paste, arg->paste_length); /* copy the paste */
	out += arg->paste_length;
	memcpy(out, in + arg->tail_offset, arg->tail_length); /* copy tail */
}

static void
do_update_op_insert(struct op_set_arg *arg,
		    const void *in __attribute__((unused)),
		    void *out)
{
	memcpy(out, arg->value, arg->length);
}

static void
init_update_op_insert(struct rope *rope, struct update_op *op)
{
	op_adjust_field_no(op, rope_size(rope));
	struct update_field *field = palloc(fiber->gc_pool,
					    sizeof(struct update_field));
	update_field_init(field, op->arg.set.value, op->arg.set.length, 0);
	rope_insert(rope, op->field_no, field, 1);
}

static void
init_update_op_set(struct rope *rope, struct update_op *op)
{
	if (op->field_no < rope_size(rope)) {
		struct update_field *field = rope_extract(rope,
							  op->field_no);
		/* Skip all previous ops. */
		STAILQ_INIT(&field->ops);
		STAILQ_INSERT_TAIL(&field->ops, op, next);
		op->new_field_len = op->arg.set.length;
	} else {
		init_update_op_insert(rope, op);
	}
}

static void
init_update_op_delete(struct rope *rope, struct update_op *op)
{
	op_adjust_field_no(op, rope_size(rope) - 1);
	rope_erase(rope, op->field_no);
}

static void
init_update_op_arith(struct rope *rope, struct update_op *op)
{
	op_check_field_no(op->field_no, rope_size(rope) - 1);

	struct update_field *field = rope_extract(rope, op->field_no);
	struct op_arith_arg *arg = &op->arg.arith;
	u32 field_len = update_field_len(field);

	switch (field_len) {
	case sizeof(i32):
		/* 32-bit operation */

		/* Check the operand type. */
		if (op->arg.set.length != sizeof(i32))
			tnt_raise(ClientError, :ER_ARG_TYPE,
				  "32-bit int");

		arg->i32_val = *(i32 *)op->arg.set.value;
		break;
	case sizeof(i64):
		/* 64-bit operation */
		switch (op->arg.set.length) {
		case sizeof(i32):
			/* 32-bit operand */
			/* cast 32-bit operand to 64-bit */
			arg->i64_val = *(i32 *)op->arg.set.value;
			break;
		case sizeof(i64):
			/* 64-bit operand */
			arg->i64_val = *(i64 *)op->arg.set.value;
			break;
		default:
			tnt_raise(ClientError, :ER_ARG_TYPE,
				  "32-bit or 64-bit int");
		}
		break;
	default:
		tnt_raise(ClientError, :ER_FIELD_TYPE,
			  "32-bit or 64-bit int");
	}
	STAILQ_INSERT_TAIL(&field->ops, op, next);
	arg->val_size = op->new_field_len = field_len;
}

static void
init_update_op_splice(struct rope *rope, struct update_op *op)
{
	op_check_field_no(op->field_no, rope_size(rope) - 1);
	struct update_field *field = rope_extract(rope, op->field_no);

	u32 field_len = update_field_len(field);

	struct op_splice_arg *arg = &op->arg.splice;
	const void *value = op->arg.set.value;
	const void *end = value + op->arg.set.length;

	/* Read the offset. */
	arg->offset = pick_field_u32(&value, end);
	if (arg->offset < 0) {
		if (-arg->offset > field_len)
			tnt_raise(ClientError, :ER_SPLICE,
				  "offset is out of bound");
		arg->offset += field_len;
	} else if (arg->offset > field_len) {
		arg->offset = field_len;
	}
	assert(arg->offset >= 0 && arg->offset <= field_len);

	/* Read the cut length. */
	arg->cut_length = pick_field_u32(&value, end);
	if (arg->cut_length < 0) {
		if (-arg->cut_length > (field_len - arg->offset))
			arg->cut_length = 0;
		else
			arg->cut_length += field_len - arg->offset;
	} else if (arg->cut_length > field_len - arg->offset) {
		arg->cut_length = field_len - arg->offset;
	}

	/* Read the paste. */
	arg->paste = pick_field_str(&value, end, (u32 *) &arg->paste_length);

	/* Fill tail part */
	arg->tail_offset = arg->offset + arg->cut_length;
	arg->tail_length = field_len - arg->tail_offset;

	/* Check that the operands are fully read. */
	if (value != end)
		tnt_raise(IllegalParams, :"field splice format error");

	/* Record the new field length. */
	op->new_field_len = arg->offset + arg->paste_length + arg->tail_length;
	STAILQ_INSERT_TAIL(&field->ops, op, next);
}

static struct update_op_meta
update_op_meta[UPDATE_OP_MAX + 1] =
{
	{ init_update_op_set,    (do_op_func) do_update_op_set,      true  },
	{ init_update_op_arith,  (do_op_func) do_update_op_add,      true  },
	{ init_update_op_arith,  (do_op_func) do_update_op_and,      true  },
	{ init_update_op_arith,  (do_op_func) do_update_op_xor,      true  },
	{ init_update_op_arith,  (do_op_func) do_update_op_or,       true  },
	{ init_update_op_splice, (do_op_func) do_update_op_splice,   false },
	{ init_update_op_delete, (do_op_func) NULL,                  true  },
	{ init_update_op_insert, (do_op_func) do_update_op_insert,   true  },
	{ init_update_op_arith,  (do_op_func) do_update_op_subtract, true  }
};

static void *
rope_alloc(void *ctx, size_t size)
{
	return palloc(ctx, size);
}

/** Free rope node - do nothing, since we use a pool allocator. */
static void
rope_free(void *ctx __attribute__((unused)), void *mem __attribute__((unused)))
{}

/** Split a range of fields in two, allocating update_field
 * context for the new range.
 */
static void *
update_field_split(void *data, size_t size __attribute__((unused)),
		   size_t offset)
{
	struct update_field *prev = data;

	struct update_field *next = palloc(fiber->gc_pool,
					   sizeof(struct update_field));
	assert(offset > 0 && prev->tail_len > 0);

	const void *field = prev->tail;
	const void *end = field + prev->tail_len;

	prev->tail_len = tuple_range_size(&field, end, offset - 1);
	u32 field_len = load_varint32(&field);

	update_field_init(next, field, field_len,
			  end - field - field_len);
	return next;
}

/**
 * We found a tuple to do the update on. Prepare and optimize
 * the operations.
 */
struct rope *
update_create_rope(struct update_op *op, struct update_op *op_end,
                   struct tuple *tuple)
{
	struct rope *rope = rope_new(update_field_split,
				     rope_alloc, rope_free,
				     fiber->gc_pool);

	/* Initialize the rope with the old tuple. */

	struct update_field *first = palloc(fiber->gc_pool,
					    sizeof(struct update_field));
	const void *field = tuple->data;
	const void *end = tuple->data + tuple->bsize;
	u32 field_len = load_varint32(&field);
	update_field_init(first, field, field_len, end - field - field_len);

	rope_append(rope, first, tuple->field_count);

	for (; op < op_end; op++)
		op->meta->init_op(rope, op);

	return rope;
}

size_t
update_calc_new_tuple_length(struct rope *rope)
{
	u32 new_tuple_len = 0;
	struct rope_iter it;
	struct rope_node *node;

	rope_iter_create(&it, rope);
	for (node = rope_iter_start(&it); node; node = rope_iter_next(&it)) {
		struct update_field *field = rope_leaf_data(node);
		u32 field_len = update_field_len(field);
		new_tuple_len += (varint32_sizeof(field_len) +
				  field_len + field->tail_len);
	}

	if (new_tuple_len == 0)
		tnt_raise(ClientError, :ER_TUPLE_IS_EMPTY);

	return new_tuple_len;
}

void
update_do_ops(struct rope *rope, struct tuple *new_tuple)
{
	void *new_data = new_tuple->data;
	void *new_data_end = new_data + new_tuple->bsize;

	new_tuple->field_count = 0;

	struct rope_iter it;
	struct rope_node *node;

	rope_iter_create(&it, rope);
	for (node = rope_iter_start(&it); node; node = rope_iter_next(&it)) {

		struct update_field *field = rope_leaf_data(node);
		u32 field_count = rope_leaf_size(node);
		u32 field_len = update_field_len(field);

		new_data = pack_varint32(new_data, field_len);

		const void *old_field = field->old;
		void *new_field = (STAILQ_EMPTY(&field->ops) ?
				   (void*) old_field : new_data);
		struct update_op *op;
		STAILQ_FOREACH(op, &field->ops, next) {
			/*
			 * Pre-allocate a temporary buffer when the
			 * subject operation requires it, i.e.:
			 * - op overwrites data while reading it thus
			 *   can't work with in == out (SPLICE)
			 * - op result doesn't fit into the new tuple
			 *   (can happen when a big SET is then
			 *   shrunk by a SPLICE).
			 */
			if ((old_field == new_field &&
			     !op->meta->works_in_place) ||
			    /*
			     * Sic: this predicate must function even if
			     * new_field != new_data.
			     */
			    new_data + op->new_field_len > new_data_end) {
				/*
				 * Since we don't know which of the two
				 * conditions above got us here, simply
				 * palloc a *new* buffer of sufficient
				 * size.
				 */
				new_field = palloc(fiber->gc_pool,
						   op->new_field_len);
			}
			op->meta->do_op(&op->arg, old_field, new_field);
			/* Next op uses previous op output as its input. */
			old_field = new_field;
		}
		/*
		 * Make sure op results end up in the tuple, copy
		 * tail_len from the old tuple.
		*/
		if (new_field != new_data)
			memcpy(new_data, new_field, field_len);
		new_data += field_len;
		assert(field->tail_len == 0 || field_count > 1);
		if (field_count > 1) {
			memcpy(new_data, field->tail, field->tail_len);
			new_data += field->tail_len;
		}
		new_tuple->field_count += field_count;
	}
}

struct update_op *
update_read_ops(const void **reqpos, const void *reqend, u32 op_cnt)
{
	if (op_cnt > BOX_UPDATE_OP_CNT_MAX)
		tnt_raise(IllegalParams, :"too many operations for update");
	if (op_cnt == 0)
		tnt_raise(IllegalParams, :"no operations for update");
	/* Read update operations.  */
	struct update_op *ops = palloc(fiber->gc_pool, op_cnt *
				       sizeof(struct update_op));
	struct update_op *op = ops, *ops_end = ops + op_cnt;
	for (; op < ops_end; op++) {
		/* Read operation */
		op->field_no = pick_u32(reqpos, reqend);
		op->opcode = pick_u8(reqpos, reqend);

		if (op->opcode >= UPDATE_OP_MAX)
			tnt_raise(ClientError, :ER_UNKNOWN_UPDATE_OP);
		op->meta = &update_op_meta[op->opcode];

		op->arg.set.value = pick_field(reqpos, reqend);
		op->arg.set.length = load_varint32(&op->arg.set.value);
	}
	/* Check the remainder length, the request must be fully read. */
	if (*reqpos != reqend)
		tnt_raise(IllegalParams, :"can't unpack request");
	return ops;
}
