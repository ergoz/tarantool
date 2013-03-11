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

/**
 * In-memory tuple format.
 *
 * In-memory tuple format stores all fields entirely in the memory.
 * The tuple format consists of three parts:
 *  1. tuple_header - base format header, see @link struct tuple @endlink;
 *  2. tuple_mem_meta - metadata, see @link struct tuple_meta @endlink;
 *  3. tuple_fields[(varint32(field_size), field_data)] - tuple fields within
 *  iproto-compatible format.
 *
 * Meta contains the size of tuple_fields part (in bytes) and offsets for all
 * indexed fields. Indexed fields can be accessed very quickly using just
 * three lookups in memory:
 *  1. Get a position from the tuple_format_mem->offset_map by field_no;
 *  2. Get an offset from tuple_mem_meta->offsets table by the position;
 *  3. Get a field from the tuple by the offset.
 * The access to other fields requires at most |m - n| iteration where `n` is
 * a required field number and `m` is a nearest indexed field number.
 * An offset for field #0 is never stored.
 */

#include "tuple_mem.h"

#include <util.h>
#include <iobuf.h>
#include <pickle.h>
#include <salloc.h>

static void
tuple_mem_delete(const struct tuple_format *fmt, struct tuple *tuple);

static struct tuple *
tuple_mem_read(const struct tuple_format *fmt, const void *buf, uint32_t size);

static uint32_t
tuple_mem_write_size(const struct tuple_format *fmt, const struct tuple *tuple);

static void *
tuple_mem_write(const struct tuple_format *fmt, const struct tuple *tuple,
		void *buf);

static uint32_t
tuple_mem_writev(const struct tuple_format *fmt,
		 const struct tuple *tuple, struct obuf *obuf);

static const void *
tuple_mem_field(const struct tuple_format *fmt, const struct tuple *tuple,
		uint32_t field_no);

static struct tuple_iterator *
tuple_mem_iterator_new(const struct tuple_format *fmt_base,
		       const struct tuple *tuple, uint32_t field_no,
		       void *(*realloc)(void *ptr, size_t size));

static void
tuple_mem_iterator_delete(const struct tuple_format *fmt_base,
			  struct tuple_iterator *it_base);

static const void *
tuple_mem_iterator_next(const struct tuple_format *fmt_base,
			struct tuple_iterator *it_base);

/**
 * @brief In-memory tuple format
 */
struct tuple_format_mem {
	/** Base format instance */
	struct tuple_format base;
	/** Size of tuple_mem_meta->offsets table */
	uint32_t tuple_offsets_size;
	/**
	 * Map (field_no -> pos in the offsets table that stored in each tuple).
	 * If offset_map[field_no] == UINT32_MAX then an offset is not saved.
	 * Othewise, tuple_meta->offsets[offset_map[field_no]] is an offset in
	 * bytes starting from the tuple_fields part.
	 **/
	uint32_t offset_map_size;
	uint32_t offset_map[0];
};

/**
 * @brief Metadata that stored in each tuple
 */
struct tuple_mem_meta {
	/** Size of the fields part in bytes */
	uint32_t bsize;
	/**
	 * Saved fields offsets (indexed by field_no-1). An offset for
	 * field #0 is not stored.
	 */
	uint32_t offsets[0];
} __attribute__((packed));

static uint32_t
tuple_mem_meta_size(const struct tuple_format_mem *fmt)
{
	return sizeof(struct tuple_mem_meta) +
			sizeof(uint32_t) * fmt->tuple_offsets_size;
}

struct tuple_iterator_mem {
	struct tuple_iterator base;
	const void *cur;
	const void *end;
};

struct tuple_format *
tuple_format_mem_new(uint32_t indexed_fields_count, uint32_t *indexed_fields)
{
	uint32_t offset_map_size = 0;
	for (uint32_t f = 0; f < indexed_fields_count; f++) {
		offset_map_size = MAX(offset_map_size, indexed_fields[f] + 1);
	}

	struct tuple_format_mem *fmt;
	size_t fmt_size = sizeof(*fmt) +
			    offset_map_size * sizeof(*fmt->offset_map);

	fmt = calloc(1, fmt_size);
	if (fmt == NULL) {
		tnt_raise(LoggedError, :ER_MEMORY_ISSUE, fmt_size,
			  "tuple_format", "tuple_format");
	}

	fmt->base.delete_cb = tuple_format_mem_delete;
	fmt->base.tuple_delete_cb = tuple_mem_delete;
	fmt->base.tuple_read_cb = tuple_mem_read;
	fmt->base.tuple_write_size_cb = tuple_mem_write_size;
	fmt->base.tuple_write_cb = tuple_mem_write;
	fmt->base.tuple_writev_cb = tuple_mem_writev;
	fmt->base.tuple_field_cb = tuple_mem_field;

	fmt->base.tuple_iterator_new_cb = tuple_mem_iterator_new;
	fmt->base.tuple_iterator_delete_cb = tuple_mem_iterator_delete;
	fmt->base.tuple_iterator_next_cb = tuple_mem_iterator_next;

	fmt->offset_map_size = offset_map_size;
	for (uint32_t i = 0; i < fmt->offset_map_size; i++) {
		fmt->offset_map[i] = UINT32_MAX;
	}

	fmt->tuple_offsets_size = 0;
	for (uint32_t f = 0; f < indexed_fields_count; f++) {
		uint32_t field_no = indexed_fields[f];
		if (field_no == 0) {
			/* don't index the first field */
			continue;
		}
		fmt->offset_map[field_no] = fmt->tuple_offsets_size++;
	}

	return (struct tuple_format *) fmt;
}

void
tuple_format_mem_delete(struct tuple_format *fmt_base)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	(void) fmt_base;

	free(fmt_base);
}

void
tuple_mem_delete(const struct tuple_format *fmt_base, struct tuple *tuple)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	(void) fmt_base;

	say_debug("tuple_delete(%p)", tuple);
	assert(tuple->refs == 0);
	sfree(tuple);
}

struct tuple *
tuple_mem_read(const struct tuple_format *fmt_base, const void *buf,
		      uint32_t size)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	struct tuple_format_mem *fmt = (struct tuple_format_mem *) fmt_base;

	uint32_t tuple_size = tuple_mem_meta_size(fmt) + size;

	struct tuple *tuple = salloc(sizeof(*tuple) + tuple_size, "tuple");
	say_debug("tuple_new(%u) = %p", tuple_size, tuple);
	tuple->refs = 0;
	tuple->format_id = fmt_base->format_id;

	struct tuple_mem_meta *meta =
			(struct tuple_mem_meta *) tuple->fmtdata;

	uint8_t *data = tuple->fmtdata + tuple_mem_meta_size(fmt);
	const uint8_t *begin = (const uint8_t *) buf;
	const uint8_t *cur = begin;
	const uint8_t *end = begin + size;

	@try {
		tuple->field_count = 0;

		while (cur < end) {
			const uint8_t *s = cur;
			u32 field_size = load_varint32_s((const void **) &cur,
							 (end - cur));
			cur += field_size;

			if ((cur - begin) > size) {
				tnt_raise(IllegalParams, :"invalid tuple size");
			}

			if (tuple->field_count < fmt->offset_map_size &&
			    fmt->offset_map[tuple->field_count] != UINT32_MAX){
				uint32_t idx = fmt->offset_map[
					       tuple->field_count];
				assert (idx < fmt->tuple_offsets_size);
				meta->offsets[idx] = (uint32_t) (s - begin);
			}

			tuple->field_count++;
		}

		if ( (end - begin) != size) {
			tnt_raise(IllegalParams, :"invalid tuple size");
		}

		if (tuple->field_count < fmt->offset_map_size) {
			tnt_raise(IllegalParams,
				  :"tuple must have all indexed fields");
		}
	} @catch(tnt_Exception *) {
		sfree(tuple);
		@throw;
	}

	meta->bsize = (uint32_t) (end - begin);
	memcpy(data, begin, size);

	return tuple;
}

uint32_t
tuple_mem_write_size(const struct tuple_format *fmt_base,
			    const struct tuple *tuple)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	(void) fmt_base;

	const struct tuple_mem_meta *meta =
			(const struct tuple_mem_meta *) tuple->fmtdata;

	return meta->bsize;
}

void *
tuple_mem_write(const struct tuple_format *fmt_base,
		       const struct tuple *tuple, void *buf)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);

	struct tuple_format_mem *fmt = (struct tuple_format_mem *) fmt_base;

	const struct tuple_mem_meta *meta =
			(const struct tuple_mem_meta *) tuple->fmtdata;

	const uint8_t *data = tuple->fmtdata + tuple_mem_meta_size(fmt);
	memcpy(buf, data, meta->bsize);

	return (uint8_t *) buf + meta->bsize;
}

uint32_t
tuple_mem_writev(const struct tuple_format *fmt_base,
			const struct tuple *tuple, struct obuf *obuf)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	struct tuple_format_mem *fmt = (struct tuple_format_mem *) fmt_base;

	const struct tuple_mem_meta *meta =
			(const struct tuple_mem_meta *) tuple->fmtdata;

	const uint8_t *data = tuple->fmtdata + tuple_mem_meta_size(fmt);
	obuf_dup(obuf, data, meta->bsize);
	return meta->bsize;
}

const void *
tuple_mem_field(const struct tuple_format *fmt_base,
		       const struct tuple *tuple, uint32_t field_no)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	struct tuple_format_mem *fmt = (struct tuple_format_mem *) fmt_base;

	if (field_no >= tuple->field_count ) {
		tnt_raise(IllegalParams, :"invalid tuple field");
	}

	const struct tuple_mem_meta *meta =
			(const struct tuple_mem_meta *) tuple->fmtdata;

	const uint8_t *cur = tuple->fmtdata + tuple_mem_meta_size(fmt);
	const uint8_t *end = cur + meta->bsize;

	if (field_no == 0)
		return cur;

	if (field_no < fmt->offset_map_size &&
	    fmt->offset_map[field_no] != UINT32_MAX) {
		uint32_t idx = fmt->offset_map[field_no];
		assert (idx < fmt->tuple_offsets_size);
		/* Field is indexed => Get field data using saved offset. */
		return cur + meta->offsets[idx];
	}

	/* Field is not indexed => Iterate starting from the last indexed
	 * field. */
	uint32_t cur_field_no = 0;
	if (fmt->offset_map_size > 1) {
		uint32_t cur_field_no2 = MIN(field_no, fmt->offset_map_size-1);
		for (; cur_field_no2 > 0; cur_field_no2--) {
			uint32_t idx = fmt->offset_map[cur_field_no2];
			if (idx != UINT32_MAX) {
				cur_field_no = cur_field_no2;
				assert (idx < fmt->tuple_offsets_size);
				cur = cur + meta->offsets[idx];
				break;
			}
		}
	}

	for (;cur_field_no < field_no; cur_field_no++) {
		u32 field_size = load_varint32_s((const void **) &cur,
						 (end - cur));
		cur += field_size;
	}

	assert (cur < end);
	return cur;
}

struct tuple_iterator *
tuple_mem_iterator_new(const struct tuple_format *fmt_base,
			      const struct tuple *tuple, uint32_t field_no,
			      void *(*realloc)(void *ptr, size_t size))
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	assert (tuple->format_id == fmt_base->format_id);

	struct tuple_mem_meta *meta =
			(struct tuple_mem_meta *) tuple->fmtdata;

	struct tuple_iterator_mem *it = realloc(NULL, sizeof(*it));
	assert (it != NULL);
	it->base.realloc = realloc;
	it->base.tuple = tuple;
	it->cur = tuple_mem_field(fmt_base, tuple, field_no);
	it->end = it->cur + meta->bsize;

	return &it->base;
}

void
tuple_mem_iterator_delete(const struct tuple_format *fmt_base,
				 struct tuple_iterator *it_base)
{
	(void) fmt_base;
	it_base->realloc(it_base, 0);
}

const void *
tuple_mem_iterator_next(const struct tuple_format *fmt_base,
			       struct tuple_iterator *it_base)
{
	assert (fmt_base->tuple_delete_cb == tuple_mem_delete);
	assert (it_base->tuple->format_id == fmt_base->format_id);

	struct tuple_iterator_mem *it = (struct tuple_iterator_mem *) it_base;

	if (it->cur >= it->end)
		return NULL;

	const void *result = it->cur;
	u32 field_size = load_varint32_s(&it->cur, (it->end - it->cur));
	it->cur = (uint8_t *) it->cur + field_size;
	return result;
}
