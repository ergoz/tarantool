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

#include <pickle.h>
#include <tbuf.h>
#include <palloc.h>
#include <exception.h>

static struct tuple_format *tuple_formats[UINT16_MAX];
static uint16_t tuple_format_id_last;

void
tuple_init(void)
{
	tuple_format_id_last = 0;
	memset(tuple_formats, 0, sizeof(tuple_formats));
}

void
tuple_free(void)
{
	for (uint16_t format_id = 0; format_id < UINT16_MAX; format_id++) {
		if (tuple_formats[format_id] == NULL)
			continue;

		tuple_formats[format_id]->delete_cb(tuple_formats[format_id]);
		tuple_formats[format_id] = NULL;
	}
}

void
tuple_format_register(struct tuple_format *fmt)
{
	if (tuple_format_id_last >= UINT16_MAX) {
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, sizeof(*fmt),
			  "tuple_format", "register");
	}

	fmt->format_id = tuple_format_id_last++;
	tuple_formats[fmt->format_id] = fmt;
}

const struct tuple_format *
tuple_format(const struct tuple *tuple)
{
	if (tuple_formats[tuple->format_id] != NULL)
		return tuple_formats[tuple->format_id];

	return NULL;
}

extern inline void
tuple_delete(struct tuple *tuple);

extern inline struct tuple *
tuple_read(const struct tuple_format *fmt, const void *buf, uint32_t size);

extern inline uint32_t
tuple_write_size(const struct tuple *tuple);

extern inline void *
tuple_write(const struct tuple *tuple, void *buf);

extern inline uint32_t
tuple_writev(const struct tuple *tuple, struct obuf *obuf);

extern inline const void *
tuple_field(const struct tuple *tuple, uint32_t field_no);

extern inline struct tuple_iterator *
tuple_iterator_new(const struct tuple *tuple, uint32_t field_no,
		   void *(*realloc)(void *ptr, size_t size));

extern inline void
tuple_iterator_delete(struct tuple_iterator *it);

extern inline const void *
tuple_iterator_next(struct tuple_iterator *it);

/**
 * Add count to tuple's reference counter.
 * When the counter goes down to 0, the tuple is destroyed.
 *
 * @pre tuple->refs + count >= 0
 */
void
tuple_ref(struct tuple *tuple, int count)
{
	assert(tuple->refs + count >= 0);
	tuple->refs += count;

	if (tuple->refs == 0)
		tuple_delete(tuple);
}

/** print field to tbuf */
static void
print_field(struct tbuf *buf, const void *f)
{
	uint32_t size = load_varint32(&f);
	switch (size) {
	case 2:
		tbuf_printf(buf, "%hu", *(u16 *)f);
		break;
	case 4:
		tbuf_printf(buf, "%u", *(u32 *)f);
		break;
	case 8:
		tbuf_printf(buf, "%"PRIu64, *(u64 *)f);
		break;
	default:
		tbuf_printf(buf, "'");
		while (size-- > 0) {
			if (0x20 <= *(u8 *)f && *(u8 *)f < 0x7f)
				tbuf_printf(buf, "%c", *(u8 *)f++);
			else
				tbuf_printf(buf, "\\x%02X", *(u8 *)f++);
		}
		tbuf_printf(buf, "'");
		break;
	}
}

/**
 * Print a tuple in yaml-compatible mode to tbuf:
 * key: { value, value, value }
 */
void
tuple_print(struct tbuf *buf, const struct tuple *tuple)
{
	if (tuple->field_count == 0) {
		tbuf_printf(buf, "'': {}");
		return;
	}

	struct tuple_iterator *it = tuple_iterator_new(tuple, 0, prealloc);
	const void *f = tuple_iterator_next(it);
	print_field(buf, f);
	tbuf_printf(buf, ": {");

	for (u32 i = 1; i < tuple->field_count; i++) {
		f = tuple_iterator_next(it);
		print_field(buf, f);
		if (likely(i + 1 < tuple->field_count))
		tbuf_printf(buf, ", ");
	}

	tbuf_printf(buf, "}");
}

/**
 * Print a tuple in yaml-compatible mode to tbuf:
 * key: { value, value, value }
 */
void
tuple_print_fields(struct tbuf *buf, u32 field_count, const void *f)
{
	if (field_count == 0) {
		tbuf_printf(buf, "'': {}");
		return;
	}

	print_field(buf, f);
	tbuf_printf(buf, ": {");
	u32 field_size = load_varint32(&f);
	f = (u8 *)f + field_size;

	for (u32 i = 1; i < field_count; i++) {
		print_field(buf, f);
		if (likely(i + 1 < field_count))
			tbuf_printf(buf, ", ");

		field_size = load_varint32(&f);
		f = (u8 *)f + field_size;
	}
	tbuf_printf(buf, "}");
}
