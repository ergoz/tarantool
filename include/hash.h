#ifndef TARANTOOL_HASH_H_INCLUDED
#define TARANTOOL_HASH_H_INCLUDED

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
 *
 * This source is based on MurmurHash3.cpp code found in smhasher (2012-10-20):
 * http://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp
 *
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to MurmurHash3.cpp code.
 */

#include <util.h>

/**
 * @file
 * @brief Hash functions.
 *
 * This file contains two types of hash functions:
 * * regular hash - 32-bit result (@a hash32_XXX methods)
 * * progressive hash -  32-bit result (@a phash32_XXX methods)
 *
 * The regular hash should be used if source data is located in one
 * contiguous memory chunk.
 *
 * Progressive hash should be used if source data is received chunk by chunk
 * (i.e. it placing all hashing data to one contiguous memory chunk would
 * take a large overhead). See @link phash32_begin @endlink for examples.
 *
 * Current implementation is based on Murmur3A 32-bit hash.
 *
 * @note The API is not guarantees that @a phash methods will return same
 * values as @a hash methods. Specialized methods for numbers (e.g. _u32, _u64)
 * must return same values as generalized _chunk method. However, specialized
 * versions might be faster than _chunk method for this type of data.
 *
 * Please use @a salt to protect you methods from hash-collision attacks.
 *
 * All methods are thread-safe.
 *
 * @see http://code.google.com/p/smhasher/
 */

/** @cond false **/
struct phash {
	u32 partial_hash;
	u32 carry;
};
/** @endcond **/

/**
 * @brief Represents the internal state of the progressive hash.
 * @note Please do not use @a "struct phash" directly, use this typedef instead.
 * In future versions this typedef can be replaced with other definition.
 */
typedef struct phash phash_t;

/** @cond false **/

/** @todo Move these methods to bit.h **/

/**
 * @brief Rotate x left by r bits
 * @param x
 * @param r
 * @return x rotated left by r bits
 */
static inline
u32
bit_rotl_u32(u32 x, int r)
{
	/* gcc recognises this code and generates a rotate instruction */
	return (((u32) x << r) | ((u32) x >> (32 - r)));
}

/**
 * @brief Rotate x right by r bits
 * @param x
 * @param r
 * @return x rotated right by r bits
 * @todo Move this method to bit.h
 */
static inline
u32
bit_rotr_u32(u32 x, int r)
{
	/* gcc recognises this code and generates a rotate instruction */
	return (((u32) x >> r) | ((u32) x << (32 - r)));
}

/**
 * @copydoc bit_rotl_u32
 */
static inline
u32
bit_rotl_u64(u64 x, int r)
{
	/* gcc recognises this code and generates a rotate instruction */
	return (((u64) x << r) | ((u64) x >> (64 - r)));
}

/**
 * @copydoc bit_rotr_u32
 */
static inline
u32
bit_rotr_u64(u64 x, int r)
{
	/* gcc recognises this code and generates a rotate instruction */
	return (((u64) x >> r) | ((u64) x << (64 - r)));
}

/**
 * @brief Function returns a byte order swapped integer.
 * On big endian systems, the number is converted to little endian byte order.
 * On little endian systems, the number is converted to big endian byte order.
 * @param x
 * @return x with swapped bytes
 * @todo Move to this method bit.h
 */
static inline
u32
bswap_u32(u32 x) {
	return __builtin_bswap32(x);
}

/*-----------------------------------------------------------------------------
 * Endianess, misalignment capabilities and util macros
 *
 * The following 3 macros are defined in this section. The other macros defined
 * are only needed to help derive these 3.
 *
 * READ_UINT32(x)   Read a little endian unsigned 32-bit int
 * UNALIGNED_SAFE   Defined if READ_UINT32 works on non-word boundaries
 */

#if defined(__i386__) || defined(__x86_64__)
#define __HASH_UNALIGNED_SAFE 1
#endif

/* Now find best way we can to READ_UINT32 */
#if defined(HAVE_BYTE_ORDER_BIG_ENDIAN)
#define __HASH_READ_UINT32(ptr)   (bswap_u32(*((u32*)(ptr))))
#else
/* CPU endian matches murmurhash algorithm, so read 32-bit word directly */
#define __HASH_READ_UINT32(ptr)   (*((u32*)(ptr)))
#endif

/*-----------------------------------------------------------------------------
 * Core murmurhash algorithm macros */

#define __HASH_C1  (0xcc9e2d51)
#define __HASH_C2  (0x1b873593)

/* This is the main processing body of the algorithm. It operates
 * on each full 32-bits of input. */
#define __HASH_DOBLOCK(h1, k1) do{ \
	k1 *= __HASH_C1; \
	k1 = bit_rotl_u32(k1,15); \
	k1 *= __HASH_C2; \
	h1 ^= k1; \
	h1 = bit_rotl_u32(h1,13); \
	h1 = h1*5+0xe6546b64; \
    }while(0)


/* Append unaligned bytes to carry, forcing hash churn if we have 4 bytes */
/* cnt=bytes to process, h1=name of h1 var, c=carry, n=bytes in c, ptr/len=payload */
#define __HASH_DOBYTES(cnt, h1, c, n, ptr, len) do{ \
    int _i = cnt; \
    while(_i--) { \
	c = c>>8 | *ptr++<<24; \
	n++; len--; \
	if(n==4) { \
	    __HASH_DOBLOCK(h1, c); \
	    n = 0; \
	} \
    } }while(0)

/** @endcond **/
/*---------------------------------------------------------------------------*/

/**
 * @brief Initialize progressive hash.
 *
 * Workflow example:
 * @code
 * phash_t phash;
 * phash32_begin(&phash, GENERATED_MAGIC_NUMBER);
 * phash32_append_u32(&phash, 1);
 * phash32_append_u64(&phash, 2L);
 * phash32_append_chunk(&phash, str, strlen(str));
 * u32 val = phash32_end(&phash);
 * @endcode
 * @param phash progressive hash
 * @param salt an initial value of hash function (sometimes also called seed)
 */
static inline
void
phash32_begin(phash_t *phash, u32 salt)
{
	phash->partial_hash = salt;
	phash->carry = 0;
}

/**
 * @brief Appends memory chunk @a data with size @a size to progressive hash
 * @a phash.
 * @param phash progressive hash
 * @param data pointer to a memory chunk
 * @param size size of @a data memory chunk
 */
static inline
void
phash32_append_chunk(phash_t *phash, const void *data, size_t size)
{
	u32 h1 = phash->partial_hash;
	u32 c = phash->carry;

	const u8 *ptr = (u8*) data;
	const u8 *end;

	/* Extract carry count from low 2 bits of c value */
	int n = c & 3;

#if defined(__HASH_UNALIGNED_SAFE)
	/* This CPU handles unaligned word access */

	/* Consume any carry bytes */
	int i = (4-n) & 3;
	if(i && i <= (int) size) {
		__HASH_DOBYTES(i, h1, c, n, ptr, size);
	}

	/* Process 32-bit chunks */
	end = ptr + size/4*4;
	for( ; ptr < end ; ptr+=4) {
		u32 k1 = __HASH_READ_UINT32(ptr);
		__HASH_DOBLOCK(h1, k1);
	}

#else /*UNALIGNED_SAFE*/
	/* This CPU does not handle unaligned word access */

	/* Consume enough so that the next data byte is word aligned */
	int i = -(long)ptr & 3;
	if(i && i <= (int) size) {
		__HASH_DOBYTES(i, h1, c, n, ptr, size);
	}

	/* We're now aligned. Process in aligned blocks.
	 * Specialise for each possible carry count */
	end = ptr + size/4*4;
	switch(n) { /* how many bytes in c */
	case 0: /* c=[----]  w=[3210]  b=[3210]=w            c'=[----] */
		for( ; ptr < end ; ptr+=4) {
			u32 k1 = __HASH_READ_UINT32(ptr);
			__HASH_DOBLOCK(h1, k1);
		}
		break;
	case 1: /* c=[0---]  w=[4321]  b=[3210]=c>>24|w<<8   c'=[4---] */
		for( ; ptr < end ; ptr+=4) {
			u32 k1 = c>>24;
			c = __HASH_READ_UINT32(ptr);
			k1 |= c<<8;
			__HASH_DOBLOCK(h1, k1);
		}
		break;
	case 2: /* c=[10--]  w=[5432]  b=[3210]=c>>16|w<<16  c'=[54--] */
		for( ; ptr < end ; ptr+=4) {
			u32 k1 = c>>16;
			c = __HASH_READ_UINT32(ptr);
			k1 |= c<<16;
			__HASH_DOBLOCK(h1, k1);
		}
		break;
	case 3: /* c=[210-]  w=[6543]  b=[3210]=c>>8|w<<24   c'=[654-] */
		for( ; ptr < end ; ptr+=4) {
			u32 k1 = c>>8;
			c = __HASH_READ_UINT32(ptr);
			k1 |= c<<24;
			__HASH_DOBLOCK(h1, k1);
		}
	}
#endif /*UNALIGNED_SAFE*/

	size -= (size/4)*4;
	/* Advance over whole 32-bit chunks, possibly leaving 1..3 bytes */
	if (size > 0) {
		/* Append any remaining bytes into carry */
		__HASH_DOBYTES(size, h1, c, n, ptr, size);
	}

	/* Copy out new running hash and carry */
	phash->partial_hash = h1;
	phash->carry = (c & ~0xff) | n;
}


/**
 * @brief Appends 32-bit number @a data to progressive hash @a phash
 * @param phash progressive hash
 * @param data 32-bit number
 */
static inline
void
phash32_append_u32(phash_t *phash, u32 data)
{
	u32 h1 = phash->partial_hash;
	u32 c = phash->carry;

	/* Extract carry count from low 2 bits of c value */
	const int n = c & 3;

	data = __HASH_READ_UINT32(&data);

	switch(n) {
	case 1:
		c = (c >> 8) | (data << 24);
		c = (c >> 8) | (data >> 8) << 24;
		c = (c >> 8) | (data >> 16) << 24;
		__HASH_DOBLOCK(h1, c);
		c = (c >> 8);
		phash->partial_hash = h1;
		phash->carry = (c & ~0xff) | n;
		return;
	case 2:
		c = (c >> 8) | (data << 24);
		c = (c >> 8) | (data >> 8) << 24;
		__HASH_DOBLOCK(h1, c);
		c = (c >> 8);
		c = (c >> 8);
		phash->partial_hash = h1;
		phash->carry = (c & ~0xff) | n;
		return;
	case 3:
		c = (c >> 8) | (data << 24);
		__HASH_DOBLOCK(h1, c);
		c = (c >> 8) | (data >> 8) << 24;
		c = (c >> 8) | (data >> 16) << 24;
		c = (c >> 8);
		phash->partial_hash = h1;
		phash->carry = (c & ~0xff) | n;
		return;
	case 0:
		__HASH_DOBLOCK(h1, data);
		phash->partial_hash = h1;
		phash->carry = (c & ~0xff) | n;
		return;
	}
}


/**
 * @brief Appends 64-bit number @a data to progressive hash @a phash
 * @param phash progressive hash
 * @param data 64-bit number
 */
static inline
void
phash32_append_u64(phash_t *phash, u64 data)
{
	u32 data1 = (u32) data;
	u32 data2 = *(((u32 *) &data) + 1);
	phash32_append_u32(phash, data1);
	phash32_append_u32(phash, data2);
}


/**
 * @brief Finalize @a phash and return generated hash value
 * @param phash progressive hash
 * @return generated hash value
 */
static inline
u32
phash32_end(phash_t *phash)
{
	u32 h = phash->partial_hash;
	u32 carry = phash->carry;

	u32 k1;
	int n = carry & 3;
	if(n) {
		k1 = carry >> (4-n)*8;
		k1 *= __HASH_C1;
		k1 = bit_rotl_u32(k1,15);
		k1 *= __HASH_C2; h ^= k1;
	}

	/* this part is not compatible with the original murmur3 hash */
	/* h ^= total_length; */

	/* fmix */
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;
}


/**
 * @brief Calculate hash value of 32-bit number @a data
 * @param data 32-bit number
 * @param salt an initial value of hash function (sometimes also called seed)
 * @return generated hash value
 */
static inline
u32
hash32_u32(u32 data, u32 salt)
{
#if defined(HASH_USE_MURMUR_FOR_U32)
	u32 h = salt;
	__HASH_DOBLOCK(h, data);

	/* this part is not compatible with the original murmur3 hash */
	/* h ^= total_length; */

	/* fmix */
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;

#else /* !defined(HASH_USE_MURMUR_FOR_U32) */
	return bit_rotr_u32(data, salt % 32);
#endif
}

/**
 * @brief Calculate hash value of 64-bit number @a data
 * @param data 64-bit number
 * @param salt an initial value of hash function (sometimes also called seed)
 * @return generated hash value
 */
static inline
u32
hash32_u64(u64 data, u32 salt)
{
#if defined(HASH_USE_MURMUR_FOR_U64)
	phash_t phash;
	phash32_begin(&phash, salt);
	phash32_append_u64(&phash, data);
	return phash32_end(&phash);
#else /* !defined(HASH_USE_MURMUR_FOR_U64) */
	return bit_rotr_u64(data, salt % 64);
	/* ORIGINAL VERSION: (a)>>33^(a)^(a)<<11) */
#endif
}

/**
 * @brief Calculate hash value of memory chunk @a data with size @a size
 * @param data pointer to a memory chunk
 * @param size size of @a data memory chunk
 * @return generated hash value
 */
static inline
u32
hash32_chunk(const void *data, size_t size, u32 salt)
{
	phash_t phash;
	phash32_begin(&phash, salt);
	phash32_append_chunk(&phash, data, size);
	return phash32_end(&phash);
}

#undef __HASH_UNALIGNED_SAFE
#undef __HASH_READ_UINT32
#undef __HASH_C1
#undef __HASH_C2
#undef __HASH_DOBLOCK
#undef __HASH_DOBYTES

#endif /* TARANTOOL_HASH_H_INCLUDED */
