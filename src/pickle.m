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
#include <tbuf.h>
#include <fiber.h>
#include <iproto.h>		/* for err codes */
#include "say.h"

/* caller must ensure that there is space in target */
u8 *
save_varint32(u8 *target, u32 value)
{
	if (value >= (1 << 7)) {
		if (value >= (1 << 14)) {
			if (value >= (1 << 21)) {
				if (value >= (1 << 28))
					*(target++) = (u8)(value >> 28) | 0x80;
				*(target++) = (u8)(value >> 21) | 0x80;
			}
			*(target++) = (u8)((value >> 14) | 0x80);
		}
		*(target++) = (u8)((value >> 7) | 0x80);
	}
	*(target++) = (u8)((value) & 0x7F);

	return target;
}

inline static void
append_byte(struct tbuf *b, u8 byte)
{
	*((u8 *)b->data + b->size) = byte;
	b->size++;
}

void
write_varint32(struct tbuf *b, u32 value)
{
	tbuf_ensure(b, 5);
	if (value >= (1 << 7)) {
		if (value >= (1 << 14)) {
			if (value >= (1 << 21)) {
				if (value >= (1 << 28))
					append_byte(b, (u8)(value >> 28) | 0x80);
				append_byte(b, (u8)(value >> 21) | 0x80);
			}
			append_byte(b, (u8)((value >> 14) | 0x80));
		}
		append_byte(b, (u8)((value >> 7) | 0x80));
	}
	append_byte(b, (u8)((value) & 0x7F));
}

#define read_u(bits)									\
	u##bits read_u##bits(struct tbuf *b)						\
	{										\
		if (b->size < (bits)/8)							\
			tnt_raise(IllegalParams, :"packet too short (expected "#bits" bits)");\
		u##bits r = *(u##bits *)b->data;					\
		b->capacity -= (bits)/8;							\
		b->size -= (bits)/8;							\
		b->data += (bits)/8;							\
		return r;								\
	}

read_u(8)
read_u(16)
read_u(32)
read_u(64)

u32
read_varint32(struct tbuf *buf)
{
	const void *b = buf->data;
	u32 ret = load_varint32_s(&b, buf->size);

	size_t read = (b - buf->data);
	buf->data += read;
	buf->capacity -= read;
	buf->size -= read;

	return ret;
}

u32
pick_u32(void *data, void **rest)
{
	u32 *b = data;
	if (rest != NULL)
		*rest = b + 1;
	return *b;
}

void *
read_field(struct tbuf *buf)
{
	void *p = buf->data;
	u32 data_len = read_varint32(buf);

	if (data_len > buf->size)
		tnt_raise(IllegalParams, :"packet too short (expected a field)");

	buf->capacity -= data_len;
	buf->size -= data_len;
	buf->data += data_len;
	return p;
}

void *
read_str(struct tbuf *buf, u32 size)
{
	void *p = buf->data;

	if (size > buf->size)
		tnt_raise(IllegalParams, :"packet too short (expected a string)");

	buf->capacity -= size;
	buf->size -= size;
	buf->data += size;
	return p;
}

u32
valid_tuple(struct tbuf *buf, u32 field_count)
{
	void *data = buf->data;
	u32 r, size = buf->size;

	for (int i = 0; i < field_count; i++)
		read_field(buf);

	r = size - buf->size;
	buf->data = data;
	buf->size = size;
	return r;
}

size_t
varint32_sizeof(u32 value)
{
	if (value < (1 << 7))
		return 1;
	if (value < (1 << 14))
		return 2;
	if (value < (1 << 21))
		return 3;
	if (value < (1 << 28))
		return 4;
	return 5;
}

const void *
load_field_str(const void *data, const void **p_val, u32 *p_size)
{
	u32 len = load_varint32(&data);

	if (p_size != NULL && p_val != NULL) {
		*p_size =  len;
		*p_val = data;
	}

	return (const char *) data + len;
}

const void *
load_field_u32(const void *data, u32 *p_val)
{
	u32 len = load_varint32(&data);
	if (len != sizeof(u32)) {
		tnt_raise(IllegalParams, :"invalid field size");
	}

	if (p_val != NULL) {
		*p_val = *(u32 *) data;
	}

	return (const char *) data + sizeof(u32);
}

const void *
load_field_u64(const void *data, u64 *p_val)
{
	u64 len = load_varint32(&data);
	if (len != sizeof(u32)) {
		tnt_raise(IllegalParams, :"invalid field size");
	}

	if (p_val != NULL) {
		*p_val = *(u64 *) data;
	}

	return (const char *) data + sizeof(u64);
}

void *
save_field_str(void *buf, const void *data, u32 len)
{
	buf = save_varint32(buf, len);
	memcpy(buf, data, len);
	buf += len;

	return buf;
}

void *
save_field_u32(void *buf, u32 val)
{
	return save_field_str(buf, &val, sizeof(val));
}

void *
save_field_u64(void *buf, u64 val)
{
	return save_field_str(buf, &val, sizeof(val));
}

