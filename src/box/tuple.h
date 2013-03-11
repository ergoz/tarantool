#ifndef TARANTOOL_BOX_TUPLE_H_INCLUDED
#define TARANTOOL_BOX_TUPLE_H_INCLUDED
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

#include <stddef.h>
#include <stdint.h>

#include <util.h> /* assert() */

struct tuple;
struct tuple_format;
struct tuple_iterator;
struct obuf;
struct tbuf;

/**
 * @cond false
 * Tuple format methods
 */
typedef void
(*tuple_format_delete_cb_t)(struct tuple_format *fmt);

typedef void
(*tuple_delete_cb_t)(const struct tuple_format *fmt, struct tuple *tuple);

typedef struct tuple *
(*tuple_read_cb_t)(const struct tuple_format *fmt, const void *buf,
		   uint32_t size);

typedef uint32_t
(*tuple_write_size_cb_t)(const struct tuple_format *fmt,
			 const struct tuple *tuple);

typedef void *
(*tuple_write_cb_t)(const struct tuple_format *fmt, const struct tuple *tuple,
		    void *buf);

typedef uint32_t
(*tuple_writev_cb_t)(const struct tuple_format *fmt, const struct tuple *tuple,
		     struct obuf *obuf);

typedef const void *
(*tuple_field_cb_t)(const struct tuple_format *fmt, const struct tuple *tuple,
		    uint32_t field_no);

typedef struct tuple_iterator *
(*tuple_iterator_new_cb_t)(const struct tuple_format *fmt,
			   const struct tuple *tuple, uint32_t field_no,
			   void *(*realloc)(void *ptr, size_t size));

typedef void
(*tuple_iterator_delete_cb_t)(const struct tuple_format *fmt,
			      struct tuple_iterator *it);

typedef const void *
(*tuple_iterator_next_cb_t)(const struct tuple_format *fmt,
			    struct tuple_iterator *it);
/** @endcond **/

/**
 * @brief Tuple format interface
 */
struct tuple_format {
	/** @cond false **/
	uint16_t format_id;

	tuple_format_delete_cb_t delete_cb;
	tuple_delete_cb_t tuple_delete_cb;
	tuple_read_cb_t tuple_read_cb;
	tuple_write_size_cb_t tuple_write_size_cb;
	tuple_write_cb_t tuple_write_cb;
	tuple_writev_cb_t tuple_writev_cb;
	tuple_field_cb_t tuple_field_cb;

	tuple_iterator_new_cb_t tuple_iterator_new_cb;
	tuple_iterator_delete_cb_t tuple_iterator_delete_cb;
	tuple_iterator_next_cb_t tuple_iterator_next_cb;
	/** @endcond */
};

/**
 * @brief Initialize tuple library
 */
void
tuple_init(void);

/**
 * @brief Deinitialize tuple library
 */
void
tuple_free(void);

/**
 * @brief Register tuple format \a fmt
 * @param fmt tuple format
 */
void
tuple_format_register(struct tuple_format *fmt);

/**
 * An atom of Tarantool/Box storage.
 */
struct tuple
{
	/** reference counter */
	uint16_t refs;
	/** tuple format id */
	uint16_t format_id;
	/** number of fields in the variable part. */
	uint32_t field_count;
	/** format-specified data. */
	uint8_t fmtdata[0];
} __attribute__((packed));

/**
 * Tuple Iterator
 */
struct tuple_iterator {
	const struct tuple *tuple;
	void *(*realloc)(void *ptr, size_t size);
};

/**
 * Change tuple reference counter. If it has reached zero, free the tuple.
 *
 * @pre tuple->refs + count >= 0
 */
void
tuple_ref(struct tuple *tuple, int count);

/**
 * @brief Return tuple format instance
 * @param tuple tuple
 * @return tuple format instance
 */
const struct tuple_format *
tuple_format(const struct tuple *tuple);

/**
 * @brief Destroy and free tuple
 * @param tuple tuple
 */
inline void
tuple_delete(struct tuple *tuple)
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_delete_cb(format, tuple);
}

/**
 * @brief Allocate and create a new tuple from fields stored in iproto format
 * {(varint32(field_size), field_data)}.
 * @param fmt tuple_format
 * @param buf buffer
 * @param size buffer size
 * @return new tuple
 */
inline struct tuple *
tuple_read(const struct tuple_format *fmt, const void *buf, uint32_t size)
{
	return fmt->tuple_read_cb(fmt, buf, size);
}

/**
 * @brief Calculate the size of tuple fields representation in iproto format.
 * @param tuple tuple
 * @return size in bytes
 */
inline uint32_t
tuple_write_size(const struct tuple *tuple)
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_write_size_cb(format, tuple);
}

/**
 * @brief Save \a tuple fields in iproto format.
 * @param tuple tuple
 * @param buf buffer
 * @return the pointer to last + 1 position
 * @pre Size of buffer must be >= tuple_write_size(tuple)
 */
inline void *
tuple_write(const struct tuple *tuple, void *buf)
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_write_cb(format, tuple, buf);
}

/**
 * @brief Save \a tuple fields in iproto format.
 * @param tuple tuple
 * @param obuf output buffer
 * @return the number of bytes written
 * @see tuple_write
 */
inline uint32_t
tuple_writev(const struct tuple *tuple, struct obuf *obuf)
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_writev_cb(format, tuple, obuf);
}

/**
 * @brief Get a field from tuple.
 * @param tuple tuple
 * @param field_no field number
 * @return the pointer to the field prefixed with varint32 size.
 * The pointer is valid until next method call.
 */
inline const void *
tuple_field(const struct tuple *tuple, uint32_t field_no)
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_field_cb(format, tuple, field_no);
}

/**
 * @brief Allocate and create a new tuple iterator.
 * @param tuple tuple
 * @param field_no start field
 * @return a new tuple iterator
 */
inline struct tuple_iterator *
tuple_iterator_new(const struct tuple *tuple, uint32_t field_no,
		   void *(*realloc)(void *ptr, size_t size))
{
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_iterator_new_cb(format, tuple, field_no, realloc);
}

/**
 * @brief Destroy and free tuple iterator.
 * @param it tuple iterator
 */
inline void
tuple_iterator_delete(struct tuple_iterator *it)
{
	assert (it->tuple != NULL);
	const struct tuple *tuple = it->tuple;
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	format->tuple_iterator_delete_cb(format, it);
}

/**
 * @brief Return next field.
 * @param it tuple iterator
 * @retval NULL if \a it does not have more tuples
 * @retval next tuple otherwise
 */
inline const void *
tuple_iterator_next(struct tuple_iterator *it)
{
	assert (it->tuple != NULL);
	const struct tuple *tuple = it->tuple;
	const struct tuple_format *format = tuple_format(tuple);
	assert (format != NULL);
	return format->tuple_iterator_next_cb(format, it);
}

/**
 * @brief Print tuple to tbuf
 * @param buf tbuf
 * @param tuple tuple
 */
void
tuple_print(struct tbuf *buf, const struct tuple *tuple);

void
tuple_print_fields(struct tbuf *buf, u32 field_count, const void *f);

#endif /* TARANTOOL_BOX_TUPLE_H_INCLUDED */

