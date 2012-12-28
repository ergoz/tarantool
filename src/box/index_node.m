#include "index_node.h"
#include "space.h"
#include "tuple.h"
#include "pickle.h"
#include "hash.h"

static void
node_realloc_ensure(void **p_ptr, size_t *p_ptr_size, size_t size)
{
	if (*p_ptr_size >= size)
		return;

	*p_ptr = index_realloc(*p_ptr, size, "key");
	*p_ptr_size = size;
}

/* {{{ Tree internal data types. **********************************/

/**
 * Tree types
 *
 * There are four specialized kinds of tree indexes optimized for different
 * combinations of index fields.
 *
 * In the most general case tuples consist of variable length fields and the
 * index uses a sparsely distributed subset of these fields. So to determine
 * the field location in the tuple it is required to scan all the preceding
 * fields. To avoid such scans on each access to a tree node we use the SPARSE
 * tree index structure. It pre-computes on the per-tuple basis the required
 * field offsets (or immediate field values for NUMs) and stores them in the
 * corresponding tree node.
 *
 * In case the index fields form a dense sequence it is possible to find
 * each successive field location based on the previous field location. So it
 * is only required to find the offset of the first index field in the tuple
 * and store it in the tree node. In this case we use the DENSE tree index
 * structure.
 *
 * In case the index consists of only one small field it is cheaper to
 * store the field value immediately in the node rather than store the field
 * offset. In this case we use the NUM32 tree index structure.
 *
 * In case the first field offset in a dense sequence is constant there is no
 * need to store any extra data in the node. For instance, the first index
 * field may be the first field in the tuple so the offset is always zero or
 * all the preceding fields may be fixed-size NUMs so the offset is a non-zero
 * constant. In this case we use the FIXED tree structure.
 *
 * Note that there may be fields with unknown types. In particular, if a field
 * is not used by any index then it doesn't have to be typed. So in many cases
 * we cannot actually determine if the fields preceding to the index are fixed
 * size or not. Therefore we may miss the opportunity to use this optimization
 * in such cases.
 */


/**
 * Representation of a STR field within a sparse tree index.

 * Depending on the STR length we keep either the offset of the field within
 * the tuple or a copy of the field. Specifically, if the STR length is less
 * than or equal to 7 then the length is stored in the "length" field while
 * the copy of the STR data in the "data" field. Otherwise the STR offset in
 * the tuple is stored in the "offset" field. The "length" field in this case
 * is set to 0xFF. The actual length has to be read from the tuple.
 */
struct sparse_str
{
	union
	{
		u8 data[7];
		u32 offset;
	};
	u8 length;
} __attribute__((packed));

#define BIG_LENGTH 0xff

/**
 * Reprsentation of a tuple field within a sparse tree index.
 *
 * For all NUMs and short STRs it keeps a copy of the field, for long STRs
 * it keeps the offset of the field in the tuple.
 */
union sparse_part {
	u32 num32;
	u64 num64;
	struct sparse_str str;
};

#define _SIZEOF_SPARSE_PARTS(part_count) \
	(sizeof(union sparse_part) * (part_count))

#define SIZEOF_SPARSE_PARTS(def) _SIZEOF_SPARSE_PARTS((def)->part_count)

/**
 * Tree nodes for different tree types
 */

struct sparse_node {
	struct tuple *tuple;
	union sparse_part parts[];
} __attribute__((packed));

struct dense_node {
	struct tuple *tuple;
	u32 offset;
} __attribute__((packed));

struct num32_node {
	struct tuple *tuple;
	u32 value;
} __attribute__((packed));

struct fixed_node {
	struct tuple *tuple;
} __attribute__((packed));

/**
 * Representation of data for key search. The data corresponds to some
 * struct key_def. The part_count field from struct key_data may be less
 * than or equal to the part_count field from the struct key_def. Thus
 * the search data may be partially specified.
 *
 * For simplicity sake the key search data uses sparse_part internally
 * regardless of the target kind of tree because there is little benefit
 * of having the most compact representation of transient search data.
 */
struct key_data
{
	u8 *data;
	int part_count;
	union sparse_part parts[];
};

/* }}} */

/* {{{ Tree auxiliary functions. **********************************/

/**
 * Find if the field has fixed offset.
 */
static int
find_fixed_offset(struct space *space, int fieldno, int skip)
{
	int i = skip;
	int offset = 0;

	while (i < fieldno) {
		/* if the field is unknown give up on it */
		if (i >= space_max_fieldno(space) || space_field_type(space, i) == UNKNOWN) {
			return -1;
		}

		/* On a fixed length field account for the appropiate
		   varint length code and for the actual data length */
		if (space_field_type(space, i) == NUM) {
			offset += 1 + 4;
		} else if (space_field_type(space, i) == NUM64) {
			offset += 1 + 8;
		}
		/* On a variable length field give up */
		else {
			return -1;
		}

		++i;
	}

	return offset;
}

/**
 * Find the first index field.
 */
static u32
find_first_field(struct key_def *key_def)
{
	for (int field = 0; field < key_def->max_fieldno; ++field) {
		int part = key_def->cmp_order[field];
		if (part != -1) {
			return field;
		}
	}
	panic("index field not found");
}

/**
 * Find the appropriate tree type for a given key.
 */
static enum node_format
find_node_format(struct space *space, struct key_def *key_def)
{
	int dense = 1;
	int fixed = 1;

	/* Scan for the first tuple field used by the index */
	int field = find_first_field(key_def);
	if (find_fixed_offset(space, field, 0) < 0) {
		fixed = 0;
	}

	/* Check that there are no gaps after the first field */
	for (; field < key_def->max_fieldno; ++field) {
		int part = key_def->cmp_order[field];
		if (part == -1) {
			dense = 0;
			break;
		}
	}

	/* Return the appropriate type */
	if (!dense) {
		return NODE_FORMAT_SPARSE;
	} else if (fixed) {
		return NODE_FORMAT_FIXED;
	} else if (key_def->part_count == 1 && key_def->parts[0].type == NUM) {
		return NODE_FORMAT_NUM32;
	} else {
		return NODE_FORMAT_DENSE;
	}
}

/**
 * Check if key parts make a linear sequence of fields.
 */
static bool
key_is_linear(struct key_def *key_def)
{
	if (key_def->part_count > 1) {
		int prev = key_def->parts[0].fieldno;
		for (int i = 1; i < key_def->part_count; ++i) {
			int next = key_def->parts[i].fieldno;
			if (next != (prev + 1)) {
				return false;
			}
			prev = next;
		}
	}
	return true;
}

/******************************************************************************/
/* node_fold/unfold_sparse
 ******************************************************************************/

static inline void
node_fold_sparse(const struct node_format_def *fmtdef, void *node,
		 struct tuple *tuple) {
	struct key_def *key_def = fmtdef->key_def;
	struct sparse_node *node_x = (struct sparse_node *) node;
	node_x->tuple = tuple;

	u8 *part_data = tuple->data;

	union sparse_part *parts = node_x->parts;
	memset(parts, 0, sizeof(parts[0]) * key_def->part_count);

	for (int field = 0; field < key_def->max_fieldno; ++field) {
		assert(field < tuple->field_count);

		u8 *data = part_data;
		u32 len = load_varint32((void**) &data);

		int part = key_def->cmp_order[field];
		if (part != -1) {
			if (key_def->parts[part].type == NUM) {
				if (len != sizeof parts[part].num32) {
					tnt_raise(IllegalParams, :"key is not u32");
				}
				memcpy(&parts[part].num32, data, len);
			} else if (key_def->parts[part].type == NUM64) {
				if (len != sizeof parts[part].num64) {
					tnt_raise(IllegalParams, :"key is not u64");
				}
				memcpy(&parts[part].num64, data, len);
			} else if (len <= sizeof(parts[part].str.data)) {
				parts[part].str.length = len;
				memcpy(parts[part].str.data, data, len);
			} else {
				parts[part].str.length = BIG_LENGTH;
				parts[part].str.offset = (u32) (part_data - tuple->data);
			}
		}

		part_data = data + len;
	}
}

static inline struct tuple *
node_unfold_sparse(const struct node_format_def *fmtdef, const void *node)  {
	(void) fmtdef;
	const struct sparse_node *node_x = node;
	return node_x ? node_x->tuple : NULL;
}

/******************************************************************************/
/* node_fold/unfold_dense
 ******************************************************************************/

static inline void
node_fold_dense(const struct node_format_def *fmtdef, void *node, struct tuple *tuple) {
	struct key_def *key_def = fmtdef->key_def;
	struct dense_node *node_x = (struct dense_node *) node;
	node_x->tuple = tuple;
	node_x->offset = 0;

	u32 max_fieldno = MIN(key_def->max_fieldno, tuple->field_count);

	u8 *tuple_data = tuple->data;

	for (int field = 0; field < max_fieldno; ++field) {

		u8 *data = tuple_data;
		u32 len = load_varint32((void**) &data);

		int part = key_def->cmp_order[field];
		u32 tuple_bsize = (tuple_data - tuple->data);
		say_warn("folddense: field = %u part=%d %u", field, part, tuple_bsize);

		if (part != -1) {
			node_x->offset = (u32) (tuple_data - tuple->data);
			return;
		}

		tuple_data = data + len;
	}
}

static inline struct tuple *
node_unfold_dense(const struct node_format_def *fmtdef, const void *node)
{
	(void) fmtdef;
	const struct dense_node *node_x = node;
	return node_x ? node_x->tuple : NULL;
}

/******************************************************************************/
/* node_fold/unfold_fixed
 ******************************************************************************/

static inline void
node_fold_fixed(const struct node_format_def *fmtdef, void *node,
		struct tuple *tuple)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	(void) fmtdef;

	struct fixed_node *node_x = (struct fixed_node *) node;
	node_x->tuple = tuple;
}

static inline struct tuple *
node_unfold_fixed(const struct node_format_def *fmtdef, const void *node)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	(void) fmtdef;

	const struct fixed_node *node_x = (const struct fixed_node *) node;
	return node_x ? node_x->tuple : NULL;
}

/******************************************************************************/
/* node_fold/unfold_num32
 ******************************************************************************/

static inline void
node_fold_num32(const struct node_format_def *fmtdef, void *node, struct tuple *tuple)
{
	struct key_def *key_def = fmtdef->key_def;
	struct num32_node *node_x = (struct num32_node *) node;
	node_x->tuple = tuple;
	u8 *tuple_data = tuple->data;

	for (int field = 0; field < key_def->max_fieldno; ++field) {
		assert(field < tuple->field_count);

		u8 *data = tuple_data;
		u32 len = load_varint32((void**) &data);

		int part = key_def->cmp_order[field];
		if (part != -1) {
			if (len != sizeof(u32))
				tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
			node_x->value = *(u32 *) data;
			return;
		}

		tuple_data = data + len;
	}

	panic("index field not found");
}

static inline struct tuple *
node_unfold_num32(const struct node_format_def *fmtdef, const void *node)
{
	(void) fmtdef;
	const struct num32_node *node_x = node;
	return node_x ? node_x->tuple : NULL;
}

/******************************************************************************/
/* node_compare functions
 ******************************************************************************/

/**
 * Unsigned 32-bit int comparison.
 */
static inline int
u32_cmp(u32 a, u32 b)
{
	return a < b ? -1 : (a > b);
}

/**
 * Unsigned 64-bit int comparison.
 */
static inline int
u64_cmp(u64 a, u64 b)
{
	return a < b ? -1 : (a > b);
}

static inline int
str_cmp(const u8 *ad, u32 al, const u8 *bd, u32 bl)
{
	int cmp = memcmp(ad, bd, MIN(al, bl));
	if (cmp == 0) {
		cmp = (int) al - (int) bl;
	}

	say_warn("str_cmp: %.*s %.*s => %d", al, ad, bl, bd,
		 cmp);

	return cmp;
}

/* {{{ node_compare_sparse ****************************************************/

static inline int
sparse_part_compare(enum field_data_type type,
		    const u8 *data_a, union sparse_part part_a,
		    const u8 *data_b, union sparse_part part_b)
{
	if (type == NUM) {
		return u32_cmp(part_a.num32, part_b.num32);
	} else if (type == NUM64) {
		return u64_cmp(part_a.num64, part_b.num64);
	} else {
		int cmp;
		const u8 *ad, *bd;
		u32 al = part_a.str.length;
		u32 bl = part_b.str.length;
		if (al == BIG_LENGTH) {
			ad = data_a + part_a.str.offset;
			al = load_varint32((void **) &ad);
		} else {
			assert(al <= sizeof(part_a.str.data));
			ad = part_a.str.data;
		}
		if (bl == BIG_LENGTH) {
			bd = data_b + part_b.str.offset;
			bl = load_varint32((void **) &bd);
		} else {
			assert(bl <= sizeof(part_b.str.data));
			bd = part_b.str.data;
		}

		cmp = memcmp(ad, bd, MIN(al, bl));
		if (cmp == 0) {
			cmp = (int) al - (int) bl;
		}

		return cmp;
	}
}

static inline int
node_compare_sparse(const struct node_format_def *fmtdef,
		    const void *node_a, const void *node_b)
{
	const struct sparse_node *node_xa = node_a;
	const struct sparse_node *node_xb = node_b;

	const struct tuple *tuple_a = node_xa->tuple;
	const struct tuple *tuple_b = node_xb->tuple;

	for (int part = 0; part < fmtdef->key_def->part_count; ++part) {
		int r = sparse_part_compare(fmtdef->key_def->parts[part].type,
					    tuple_a->data, node_xa->parts[part],
					    tuple_b->data, node_xb->parts[part]);
		if (r) {
			return r;
		}
	}

	return 0;
}

/* node_compare_sparse }}} ****************************************************/

/* {{{ node_compare_dense *****************************************************/

static inline int
dense_part_compare(enum field_data_type type,
		   const u8 *ad, u32 al,
		   const u8 *bd, u32 bl)
{
	switch (type) {
	case NUM:
		if (al != sizeof(u32) || bl != sizeof(u32))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		return u32_cmp(*(u32 *) ad, *(u32 *) bd);
	case NUM64:
		if (al != sizeof(u64) || bl != sizeof(u64))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		return u64_cmp(*(u64 *) ad, *(u64 *) bd);
	case STRING:
		return str_cmp(ad, al, bd, bl);
	default:
		assert(false);
	}

	return 0;
}

static inline int
single_node_compare(struct key_def *key_def,
		    u32 first_field,
		    struct tuple *tuple_a, u32 offset_a,
		    struct tuple *tuple_b, u32 offset_b)
{
	(void) first_field;

	say_warn("single_node_comapre: field_no %u field_count = %u offset_a %u offset_b %u",
		 key_def->parts[0].fieldno,
		 tuple_a->field_count, offset_a, offset_b);

	if (unlikely(key_def->parts[0].fieldno >= tuple_a->field_count ||
	    key_def->parts[0].fieldno >= tuple_b->field_count)) {
		return 0;
	}

	u8 *ad = tuple_a->data + offset_a;
	u8 *bd = tuple_b->data + offset_b;
	u32 al = load_varint32((void**) &ad);
	u32 bl = load_varint32((void**) &bd);

	return dense_part_compare(key_def->parts[0].type,
				  ad, al, bd, bl);
}

static inline int
calc_min_part_count(u32 first_field, int part_count,
		    u32 field_count_a, u32 field_count_b)
{
	if (field_count_a < first_field ||
	    field_count_b < first_field)
		return 0;

	if ((first_field + part_count) > field_count_a) {
		part_count = field_count_a - first_field;
	}

	if ((first_field + part_count) > field_count_b) {
		part_count = field_count_b - first_field;
	}

	return part_count;
}

static inline int
linear_node_compare(struct key_def *key_def,
		    u32 first_field,
		    struct tuple *tuple_a, u32 offset_a,
		    struct tuple *tuple_b, u32 offset_b)
{
	(void) first_field;

	int part_count = calc_min_part_count(first_field, key_def->part_count,
		tuple_a->field_count, tuple_b->field_count);

	say_warn("tuple_a->field_count: %u", tuple_a->field_count);
	say_warn("tuple_b->field_count: %u", tuple_b->field_count);
	say_warn("first_field: %u", first_field);
	say_warn("key_def->part_count: %u", key_def->part_count);

	say_warn("Part_count: %d", part_count);
	if (part_count <= 0)
		return 0;

	/* Compare key parts. */
	u8 *ad = tuple_a->data + offset_a;
	u8 *bd = tuple_b->data + offset_b;
	for (int part = 0; part < part_count; ++part) {
		u32 al = load_varint32((void**) &ad);
		u32 bl = load_varint32((void**) &bd);
		say_warn("dense_part_compare: %d",
			 key_def->parts[part].fieldno);
		int r = dense_part_compare(key_def->parts[part].type,
					   ad, al, bd, bl);
		if (r) {
			return r;
		}
		ad += al;
		bd += bl;
	}
	return 0;
}

static inline int
dense_node_compare(struct key_def *key_def, u32 first_field,
		   struct tuple *tuple_a, u32 offset_a,
		   struct tuple *tuple_b, u32 offset_b)
{
	int part_count = calc_min_part_count(first_field, key_def->part_count,
		tuple_a->field_count, tuple_b->field_count);

	say_warn("dense_node_compare: %d", part_count);

	/* Allocate space for offsets. */
	u32 *off_a = alloca(2 * part_count * sizeof(u32));
	u32 *off_b = off_a + part_count;

	/* Find field offsets. */
	off_a[0] = offset_a;
	off_b[0] = offset_b;
	if (part_count > 1) {
		u8 *ad = tuple_a->data + offset_a;
		u8 *bd = tuple_b->data + offset_b;
		for (int i = 1; i < part_count; ++i) {
			u32 al = load_varint32((void**) &ad);
			u32 bl = load_varint32((void**) &bd);
			ad += al;
			bd += bl;
			off_a[i] = ad - tuple_a->data;
			off_b[i] = bd - tuple_b->data;
		}
	}

	/* Compare key parts. */
	for (int part = 0; part < part_count; ++part) {
		int field = key_def->parts[part].fieldno;
		u8 *ad = tuple_a->data + off_a[field - first_field];
		u8 *bd = tuple_b->data + off_b[field - first_field];
		u32 al = load_varint32((void *) &ad);
		u32 bl = load_varint32((void *) &bd);
		int r = dense_part_compare(key_def->parts[part].type,
					   ad, al, bd, bl);
		if (r) {
			return r;
		}
	}
	return 0;
}

static inline int
node_compare_dense_single(const struct node_format_def *fmtdef,
			 const void *node_a, const void *node_b)
{
	say_warn("node_compare_dense_single");

	assert(fmtdef->format == NODE_FORMAT_DENSE);
	assert(fmtdef->key_def->part_count == 1);

	const struct dense_node *node_xa = node_a;
	const struct dense_node *node_xb = node_b;

	return single_node_compare(fmtdef->key_def, fmtdef->first_field,
				   node_xa->tuple, node_xa->offset,
				   node_xb->tuple, node_xb->offset);
}

static inline int
node_compare_dense_linear(const struct node_format_def *fmtdef,
			  const void *node_a, const void *node_b)
{
	say_warn("node_compare_dense_linear: %p %p", node_a, node_b);

	assert(fmtdef->format == NODE_FORMAT_DENSE);
	assert(fmtdef->is_linear);

	const struct dense_node *node_xa = node_a;
	const struct dense_node *node_xb = node_b;

	say_warn("node_compare_dense_linear: %p %p", node_xa->tuple, node_xb->tuple);

	return linear_node_compare(fmtdef->key_def, fmtdef->first_field,
				   node_xa->tuple, node_xa->offset,
				   node_xb->tuple, node_xb->offset);
}

static inline int
node_compare_dense(const struct node_format_def *fmtdef,
		   const void *node_a, const void *node_b)
{
	say_warn("node_compare_dense");

	assert(fmtdef->format == NODE_FORMAT_DENSE);

	const struct dense_node *node_xa = node_a;
	const struct dense_node *node_xb = node_b;

	return dense_node_compare(fmtdef->key_def, fmtdef->first_field,
				  node_xa->tuple, node_xa->offset,
				  node_xb->tuple, node_xb->offset);
}

/* node_compare_dense }}} *****************************************************/

/* {{{ node_compare_fixed *****************************************************/

static inline int
node_compare_fixed_single(const struct node_format_def *fmtdef,
		   const void *node_a, const void *node_b) {
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	assert(fmtdef->key_def->part_count == 1);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	struct tuple *tuple_b = node_unfold_fixed(fmtdef, node_b);

	say_warn("Comapre fixed single: %p %p (%u) %p %p (%u)", node_a, tuple_a, tuple_a->field_count, node_b, tuple_b, tuple_b->field_count);
	return single_node_compare(fmtdef->key_def, fmtdef->first_field,
				   tuple_a, fmtdef->first_offset,
				   tuple_b, fmtdef->first_offset);
}

static inline int
node_compare_fixed_linear(const struct node_format_def *fmtdef,
			  const void *node_a, const void *node_b)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	assert(fmtdef->is_linear);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	struct tuple *tuple_b = node_unfold_fixed(fmtdef, node_b);

	return linear_node_compare(fmtdef->key_def, fmtdef->first_field,
				   tuple_a, fmtdef->first_offset,
				   tuple_b, fmtdef->first_offset);
}

static inline int
node_compare_fixed(const struct node_format_def *fmtdef,
		   const void *node_a, const void *node_b)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	struct tuple *tuple_b = node_unfold_fixed(fmtdef, node_b);

	return dense_node_compare(fmtdef->key_def, fmtdef->first_field,
				  tuple_a, fmtdef->first_offset,
				  tuple_b, fmtdef->first_offset);
}

/* node_compare_fixed }}} *****************************************************/

/* {{{ node_compare_num32 *****************************************************/

static inline int
node_compare_num32(const struct node_format_def *fmtdef,
		   const void *node_a, const void *node_b)
{
	(void) fmtdef;
	const struct num32_node *node_xa = node_a;
	const struct num32_node *node_xb = node_b;

	if (unlikely(node_xa->tuple == NULL || node_xa->tuple == NULL)) {
		/* one of these tuples is key and part_count == 0 */
		return 0;
	}

	return u32_cmp(node_xa->value, node_xb->value);
}

/* node_compare_num32 }}} *****************************************************/

/******************************************************************************/
/* node_hash functions
 ******************************************************************************/

/* {{{ node_hash_sparse *******************************************************/

static inline u32
node_hash_sparse(const struct node_format_def *fmtdef, const void *node_a)
{
	const struct sparse_node *node_xa = node_a;

	phash_t phash;
	phash32_begin(&phash, 0);

	const u8 *ad;
	u32 al;
	for (int part = 0; part < fmtdef->key_def->part_count; ++part) {
		switch(fmtdef->key_def->parts[part].type) {
		case NUM:
			phash32_append_u32(&phash, node_xa->parts[part].num32);
			break;
		case NUM64:
			phash32_append_u64(&phash, node_xa->parts[part].num64);
			break;
		case STRING:
			al = node_xa->parts[part].str.length;
			if (al == BIG_LENGTH) {
				ad = node_xa->tuple->data +
						node_xa->parts[part].str.offset;
				al = load_varint32((void **) &ad);
			} else {
				assert(al <= sizeof(node_xa->parts[part].str.data));
				ad = node_xa->parts[part].str.data;
			}

			phash32_append_chunk(&phash, ad, al);
			break;
		default:
			assert(false);
		}
	}

	return phash32_end(&phash);
}

/* node_hash_sparse }}} *******************************************************/

/* {{{ node_hash_dense ********************************************************/

static inline u32
single_node_hash(struct key_def *key_def, u32 first_field,
		 struct tuple *tuple_a, u32 offset_a)
{
	(void) first_field;

	assert(first_field + key_def->part_count <= tuple_a->field_count);
	assert(key_def->part_count == 1);

	u8 *ad = tuple_a->data + offset_a;
	u32 al = load_varint32((void**) &ad);

	switch(key_def->parts[0].type) {
	case NUM:
		if (al != sizeof(u32))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
		return hash32_u32(*(u32 *) ad, 0);
	case NUM64:
		if (al != sizeof(u64))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		return hash32_u64(*(u64 *) ad, 0);
	case STRING:
		return hash32_chunk(ad, al, 0);
	default:
		assert(false);
	}

	return 0;
}

static inline void
dense_node_hash_part(phash_t *phash, enum field_data_type type,
		     const u8 *ad, u32 al) {
	switch(type) {
	case NUM:
		if (al != sizeof(u32))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u32");
		phash32_append_u32(phash, *(u32 *) ad);
		break;
	case NUM64:
		if (al != sizeof(u64))
			tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
		phash32_append_u64(phash, *(u64 *) ad);
		break;
	case STRING:
		phash32_append_chunk(phash, ad, al);
		break;
	default:
		assert(false);
	}
}

static inline u32
linear_node_hash(struct key_def *key_def,
		    u32 first_field  __attribute__((unused)),
		    struct tuple *tuple_a, u32 offset_a)
{
	int part_count = key_def->part_count;
	assert(first_field + part_count <= tuple_a->field_count);

	phash_t phash;
	phash32_begin(&phash, 0);

	/* Compare key parts. */
	u8 *ad = tuple_a->data + offset_a;
	for (int part = 0; part < part_count; ++part) {
		u32 al = load_varint32((void**) &ad);
		dense_node_hash_part(&phash, key_def->parts[part].type, ad, al);
		ad += al;
	}

	return phash32_end(&phash);
}

static inline u32
dense_node_hash(struct key_def *key_def, u32 first_field,
		struct tuple *tuple_a, u32 offset_a)
{
	int part_count = key_def->part_count;
	assert(first_field + part_count <= tuple_a->field_count);

	/* Allocate space for offsets. */
	u32 *off_a = alloca(1 * part_count * sizeof(u32));

	/* Find field offsets. */
	off_a[0] = offset_a;
	if (part_count > 1) {
		u8 *ad = tuple_a->data + offset_a;
		for (int i = 1; i < part_count; ++i) {
			u32 al = load_varint32((void**) &ad);
			ad += al;
			off_a[i] = ad - tuple_a->data;
		}
	}

	phash_t phash;
	phash32_begin(&phash, 0);

	/* Compare key parts. */
	for (int part = 0; part < part_count; ++part) {
		int field = key_def->parts[part].fieldno;
		u8 *ad = tuple_a->data + off_a[field - first_field];
		u32 al = load_varint32((void *) &ad);
		dense_node_hash_part(&phash, key_def->parts[part].type, ad, al);
	}

	return phash32_end(&phash);
}

static inline u32
node_hash_dense_single(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_DENSE);
	assert(fmtdef->key_def->part_count == 1);

	const struct dense_node *node_xa = node_a;
	return single_node_hash(fmtdef->key_def, fmtdef->first_field,
				node_xa->tuple, node_xa->offset);
}

static inline u32
node_hash_dense_linear(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_DENSE);
	assert(fmtdef->is_linear);

	const struct dense_node *node_xa = node_a;
	return linear_node_hash(fmtdef->key_def, fmtdef->first_field,
				node_xa->tuple, node_xa->offset);
}

static inline u32
node_hash_dense(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_DENSE);

	const struct dense_node *node_xa = node_a;
	return dense_node_hash(fmtdef->key_def, fmtdef->first_field,
			       node_xa->tuple, node_xa->offset);
}

/* node_hash_dense }}} ********************************************************/

/* {{{ node_hash_fixed ********************************************************/

static inline u32
node_hash_fixed_single(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	assert(fmtdef->key_def->part_count == 1);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	return single_node_hash(fmtdef->key_def, fmtdef->first_field,
				tuple_a, fmtdef->first_offset);
}

static inline u32
node_hash_fixed_linear(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	assert(fmtdef->is_linear);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	return linear_node_hash(fmtdef->key_def, fmtdef->first_field,
				tuple_a, fmtdef->first_offset);
}

static inline u32
node_hash_fixed(const struct node_format_def *fmtdef, const void *node_a)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);

	struct tuple *tuple_a = node_unfold_fixed(fmtdef, node_a);
	return dense_node_hash(fmtdef->key_def, fmtdef->first_field,
			       tuple_a, fmtdef->first_offset);
}

/* node_hash_fixed }}} ********************************************************/

/* {{ node_hash_num32 *********************************************************/

static inline u32
node_hash_num32(const struct node_format_def *fmtdef, const void *node_a)
{
	const struct num32_node *node_xa = (const struct num32_node *) node_a;
	return hash32_u32(node_xa->value, fmtdef->hash_salt);
}

/* node_hash_num32 }}} ********************************************************/

/******************************************************************************/
/* node_foldkey functions
 ******************************************************************************/

/* {{{ node_foldkey_num32 ****************************************************/

static inline size_t
node_foldkey_num32(const struct node_format_def *fmtdef,
		    void **p_node, size_t *p_node_size,
		    const void *key, int part_count)
{
	assert(fmtdef->format == NODE_FORMAT_NUM32);
	assert(fmtdef->key_def->part_count == 1);
	assert(p_node != NULL);
	(void) part_count;

	if (part_count <= 0) {
		node_realloc_ensure(p_node, p_node_size, fmtdef->node_size);
		struct num32_node *node = *p_node;
		node->value = 0;
		node->tuple = NULL;
		return fmtdef->node_size;
	}

	assert(key != NULL);

	node_realloc_ensure(p_node, p_node_size, fmtdef->node_size);

	struct num32_node *node = *p_node;
	u32 field_size = load_varint32((void *) &key);
	if (field_size != sizeof(u32))
		tnt_raise(ClientError, :ER_KEY_FIELD_TYPE, "u64");
	(void) field_size;

	node->value = *(u32 *) key;
	node->tuple = (void *) 1; /* arbitraty non-zero value */

	return fmtdef->node_size;
}

/* node_foldkey_num32 }}} ****************************************************/

/* {{{ node_foldkey_fixed ****************************************************/

#if 0
static inline size_t
node_foldkey_fixed_single(const struct node_format_def *fmtdef,
			void **p_node, size_t *p_node_size,
			const void *key, int part_count)
{
	assert(fmtdef->format == NODE_FORMAT_FIXED);
	assert(fmtdef->key_def->part_count == 1);
	assert(p_node != NULL);

	if (part_count <= 0) {
		size_t node_size = fmtdef->node_size + sizeof(struct tuple);
		node_realloc_ensure(p_node, p_node_size, node_size);
		struct tuple *tuple = (struct tuple *)
				((char *) *p_node + fmtdef->node_size);

		tuple->bsize = 0;
		tuple->field_count = 0;

		return node_size;
	}

	assert(part_count >= 0 && key != NULL);

	const void *key2 = key;
	u32 key_size = load_varint32((void *) &key2);
	key_size += (key2 - key);

	size_t node_size = sizeof(struct fixed_node) + sizeof(struct tuple) +
			fmtdef->first_offset + key_size;
	node_realloc_ensure(p_node, p_node_size, node_size);

	struct fixed_node *node_x = (struct fixed_node *) *p_node;
	node_x->tuple = (struct tuple *)
			((char *) *p_node + sizeof(struct fixed_node));

	node_x->tuple->bsize = key_size;

	node_x->tuple->field_count = fmtdef->first_field + 1;
	memcpy(node_x->tuple->data, key, key_size);


	return node_size;
}

#endif

/* {{{ node_foldkey_generic ***************************************************/

static inline size_t
node_foldkey_fixed_calc_bsize(const struct node_format_def *fmtdef,
			 const void *key, int part_count)
{
	u32 tuple_bsize = 0;

	say_warn("tree_format: %p", fmtdef);
	say_warn("part_count: %u", part_count);
	say_warn("first_field: %u", fmtdef->first_field);
	say_warn("first_offset: %u", fmtdef->first_offset);
	say_warn("max_field_no: %u", fmtdef->key_def->max_fieldno);

	const u8 *key_data = key;

	for (int field = 0;
	     field < fmtdef->key_def->max_fieldno && part_count > 0; ++field) {
		int part = fmtdef->key_def->cmp_order[field];
		say_warn("foldkey1: field=%u part_count=%d %u", field, part,
			 tuple_bsize);

		if (part == -1) {
			/* calc size of an empty field  */
			tuple_bsize += varint32_sizeof(0);
			continue;
		}

		u32 field_size = load_varint32((void *) &key_data);
		key_data += field_size;

		tuple_bsize += varint32_sizeof(field_size);
		tuple_bsize += field_size;
		--part_count;

	}
	assert (part_count == 0);

	return tuple_bsize;
}

static inline size_t
node_foldkey_generic(const struct node_format_def *fmtdef,
		     void **p_node, size_t *p_node_size,
		     const void *key, int part_count)
{
	assert(p_node != NULL);
	assert(part_count == 0 || (part_count > 0 && key != NULL));
	assert(fmtdef->first_offset == 0);

	size_t bsize = node_foldkey_fixed_calc_bsize(fmtdef, key, part_count);
	size_t node_size = fmtdef->node_size + sizeof(struct tuple) + bsize;
	node_realloc_ensure(p_node, p_node_size, node_size);

	struct tuple *tuple = (struct tuple *)
			((char *) *p_node + fmtdef->node_size);

	tuple->field_count = 0;
	tuple->bsize = bsize;

	u8 *tuple_data = tuple->data;
	const u8 *key_data = key;

	for (int field = 0;
	     field < fmtdef->key_def->max_fieldno && part_count > 0; ++field) {
		tuple->field_count++;

		int part = fmtdef->key_def->cmp_order[field];
		//u32 tuple_bsize = (tuple_data - tuple->data);
		say_warn("foldkey: field=%u part_count=%d %u", field, part,
			 (u32) (tuple_data - tuple->data));

		if (part == -1) {
			/* add stub field with zero size */
			tuple_data = save_varint32(tuple_data, 0);
			continue;
		}

		u32 field_size = load_varint32((void *) &key_data);
		tuple_data = save_varint32(tuple_data, field_size);
		memcpy(tuple_data, key_data, field_size);
		say_warn("field: %d size=%u", field, field_size);

		key_data = key_data + field_size;
		tuple_data += field_size;

		--part_count;
	}

	assert (part_count == 0);
	assert ((tuple->data + tuple->bsize) == tuple_data);

	fmtdef->foldfun(fmtdef, *p_node, tuple);
	return node_size;
}

static inline size_t
node_foldkey_fixed(const struct node_format_def *fmtdef,
		     void **p_node, size_t *p_node_size,
		     const void *key, int part_count)
{
	assert(p_node != NULL);

	if (part_count <= 0) {
		size_t node_size = fmtdef->node_size + sizeof(struct tuple);
		node_realloc_ensure(p_node, p_node_size, node_size);
		struct fixed_node *node = *p_node;
		struct tuple *tuple = (struct tuple *)
				((char *) *p_node + fmtdef->node_size);
		node->tuple = tuple;
		tuple->bsize = 0;
		tuple->field_count = 0;
		return node_size;
	}

	assert(part_count > 0 && key != NULL);

	u32 tuple_bsize = fmtdef->first_offset;
	const u8 *key_data = key;

	int p = part_count;
	for (u32 field = fmtdef->first_field;
	     field < fmtdef->key_def->max_fieldno && p > 0; ++field) {
		int part = fmtdef->key_def->cmp_order[field];

		if (part == -1) {
			/* calc size of an empty field  */
			tuple_bsize += varint32_sizeof(0);
			continue;
		}

		u32 field_size = load_varint32((void *) &key_data);
		key_data += field_size;

		tuple_bsize += varint32_sizeof(field_size);
		tuple_bsize += field_size;
		--p;
	}

	assert(p == 0);

	size_t node_size = fmtdef->node_size + sizeof(struct tuple) + tuple_bsize;
	node_realloc_ensure(p_node, p_node_size, node_size);

	struct tuple *tuple = (struct tuple *)
			((char *) *p_node + fmtdef->node_size);

	tuple->field_count = fmtdef->first_field;
	tuple->bsize = tuple_bsize;

	u8 *tuple_data = tuple->data + fmtdef->first_offset;
	key_data = key;
	p = part_count;

	for (u32 field = fmtdef->first_field;
	     field < fmtdef->key_def->max_fieldno && p > 0; ++field) {
		tuple->field_count++;

		int part = fmtdef->key_def->cmp_order[field];
		if (part == -1) {
			/* add stub field with zero size */
			tuple_data = save_varint32(tuple_data, 0);
			continue;
		}

		u32 field_size = load_varint32((void *) &key_data);
		tuple_data = save_varint32(tuple_data, field_size);
		memcpy(tuple_data, key_data, field_size);

		key_data = key_data + field_size;
		tuple_data += field_size;

		--p;
	}

	assert (p == 0);
	assert ((tuple->data + tuple->bsize) == tuple_data);


	node_fold_fixed(fmtdef, *p_node, tuple);

	say_warn("foldkey_fixed: %p %p tuple = %p (%u)", *p_node, tuple, node_unfold_fixed(fmtdef, *p_node), tuple->field_count);

	return node_size;
}

/* node_foldkey_generic }}} ***************************************************/

int
node_format_init(struct node_format_def *fmtdef, struct space *space,
	  struct key_def *key_def)
{
	fmtdef->key_def = key_def;
	fmtdef->format = find_node_format(space, fmtdef->key_def);
	fmtdef->is_linear = key_is_linear(fmtdef->key_def);
	fmtdef->first_field = 0;
	fmtdef->first_offset = 0;

	/* TODO: generate random hash salt */
	fmtdef->hash_salt = 0;

	switch (fmtdef->format) {
	case NODE_FORMAT_SPARSE:
		say_info("Sparse");
		fmtdef->node_size = sizeof(struct sparse_node) +
				SIZEOF_SPARSE_PARTS(fmtdef->key_def);
		fmtdef->foldfun = node_fold_sparse;
		fmtdef->unfoldfun = node_unfold_sparse;
		fmtdef->comparefun = node_compare_sparse;
		fmtdef->hashfun = node_hash_sparse;
		fmtdef->foldkeyfun = node_foldkey_generic;
		return 0;
	case NODE_FORMAT_NUM32:
		say_warn("Num32");
		fmtdef->node_size = sizeof(struct num32_node);
		fmtdef->foldfun = node_fold_num32;
		fmtdef->unfoldfun = node_unfold_num32;
		fmtdef->comparefun = node_compare_num32;
		fmtdef->hashfun = node_hash_num32;
		fmtdef->foldkeyfun = node_foldkey_num32;
		return 0;
	case NODE_FORMAT_DENSE:
		say_warn("Dense");
		fmtdef->node_size = sizeof(struct dense_node);
		fmtdef->foldfun = node_fold_dense;
		fmtdef->unfoldfun = node_unfold_dense;
		if (fmtdef->key_def->part_count == 1) {
			fmtdef->comparefun = node_compare_dense_single;
			fmtdef->hashfun = node_hash_dense_single;
			fmtdef->foldkeyfun = node_foldkey_generic;
		} else if (fmtdef->is_linear){
			fmtdef->comparefun = node_compare_dense_linear;
			fmtdef->hashfun = node_hash_dense_linear;
			fmtdef->foldkeyfun = node_foldkey_generic;
		} else {
			fmtdef->comparefun = node_compare_dense;
			fmtdef->hashfun = node_hash_dense;
			fmtdef->foldkeyfun = node_foldkey_generic;
		}

		fmtdef->first_field = find_first_field(fmtdef->key_def);
		return 0;
	case NODE_FORMAT_FIXED:
		say_warn("Fixed");
		fmtdef->node_size = sizeof(struct fixed_node);
		fmtdef->foldfun = node_fold_fixed;
		fmtdef->unfoldfun = node_unfold_fixed;
		if (fmtdef->key_def->part_count == 1) {
			fmtdef->comparefun = node_compare_fixed_single;
			fmtdef->hashfun = node_hash_fixed_single;
			fmtdef->foldkeyfun = node_foldkey_fixed;
			//fmtdef->foldkeyfun = node_foldkey_fixed_single;
		} else if (fmtdef->is_linear){
			fmtdef->comparefun = node_compare_fixed_linear;
			fmtdef->hashfun = node_hash_fixed_linear;
			fmtdef->foldkeyfun = node_foldkey_fixed;
		} else {
			fmtdef->comparefun = node_compare_fixed;
			fmtdef->hashfun = node_hash_fixed;
			fmtdef->foldkeyfun = node_foldkey_fixed;
		}
		fmtdef->first_field = find_first_field(fmtdef->key_def);
		fmtdef->first_offset = find_fixed_offset(space, fmtdef->first_field, 0);
		return 0;
	default:
		assert(false);
	}

	return -1;
}

void
node_format_deinit(struct node_format_def *fmtdef)
{
	(void) fmtdef;
	/* nothing */
}

/* }}} */

