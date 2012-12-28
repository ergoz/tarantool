#ifndef TARANTOOL_BOX_INDEX_NODE_H_INCLUDED
#define TARANTOOL_BOX_INDEX_NODE_H_INCLUDED

#include "index.h"

struct space;
struct tuple;

enum node_format {
	NODE_FORMAT_SPARSE,
	NODE_FORMAT_DENSE,
	NODE_FORMAT_NUM32,
	NODE_FORMAT_FIXED
};

struct node_format_def;

typedef u32
(*node_hash_t)(const struct node_format_def *fmtdef, const void *node);
typedef int
(*node_compare_t)(const struct node_format_def *fmtdef,
		  const void *node_a, const void *node_b);

typedef struct tuple *
(*node_unfold_t)(const struct node_format_def *fmtdef, const void *node);

typedef void
(*node_fold_t)(const struct node_format_def *fmtdef, void *node,
	       struct tuple *tuple);

typedef size_t
(*node_foldkey_t)(const struct node_format_def *fmtdef,
		  void **p_node, size_t *p_node_size,
		  const void *key, int part_count);

struct node_format_def {
	enum node_format format;
	struct key_def *key_def;
	size_t node_size;

	bool is_linear;
	u32 first_field;
	u32 first_offset;

	node_hash_t hashfun;
	node_compare_t comparefun;
	node_fold_t foldfun;
	node_unfold_t unfoldfun;
	node_foldkey_t foldkeyfun;

	u32 hash_salt;
};

int
node_format_init(struct node_format_def *fmtdef, struct space *space,
	  struct key_def *key_def);

void
node_format_deinit(struct node_format_def *fmtdef);

static inline size_t
node_size(const struct node_format_def *fmtdef)
{
	return fmtdef->node_size;
}

static inline struct tuple *
node_unfold(const struct node_format_def *fmtdef, const void *node)
{
	return fmtdef->unfoldfun(fmtdef, node);
}

static inline void
node_fold(const struct node_format_def *fmtdef, void *node,
	  struct tuple *tuple)
{
	return fmtdef->foldfun(fmtdef, node, tuple);
}

static inline size_t
node_foldkey(const struct node_format_def *fmtdef,
	      void **p_node, size_t *p_node_size,
	      const void *key, int part_count)
{
	return fmtdef->foldkeyfun(fmtdef, p_node, p_node_size, key, part_count);
}

static inline int
node_compare(const struct node_format_def *fmtdef,
	     const void *node_a, const void *node_b)
{
	return fmtdef->comparefun(fmtdef, node_a, node_b);
}

static inline int
node_compare_addr(const struct node_format_def *fmtdef,
		  const void *node_a, const void *node_b)
{
	struct tuple *tuple_a = node_unfold(fmtdef, node_a);
	struct tuple *tuple_b = node_unfold(fmtdef, node_b);
	if (!tuple_a)
		return 0;
	if (!tuple_b)
		return 0;
	return tuple_a < tuple_b ? -1 : (tuple_a > tuple_b);
}

static inline u32
node_hash(const struct node_format_def *fmtdef, const void *node)
{
	return fmtdef->hashfun(fmtdef, node);
}

#endif /* TARANTOOL_BOX_INDEX_NODE_H_INCLUDED */
