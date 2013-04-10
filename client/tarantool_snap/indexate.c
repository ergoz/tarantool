
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <connector/c/include/tarantool/tnt.h>
#include <connector/c/include/tarantool/tnt_xlog.h>
#include <connector/c/include/tarantool/tnt_snapshot.h>
#include <connector/c/include/tarantool/tnt_dir.h>

#include <cfg/prscfg.h>
#include <cfg/tarantool_box_cfg.h>

#include "key.h"
#include "hash.h"
#include "options.h"
#include "space.h"
#include "sha1.h"
#include "ref.h"
#include "ts.h"
#include "indexate.h"

extern struct ts tss;

inline uint32_t
search_hash(const struct ts_key *k, struct ts_space *s)
{
	register uint32_t *v = (uint32_t*)k->key;
	switch (s->key_div) {
	case 5:
		return v[0] ^ v[1] ^ v[2] ^ v[3] ^ v[4];
	case 4:
		return v[0] ^ v[1] ^ v[2] ^ v[3];
	case 3:
		return v[0] ^ v[1] ^ v[2];
	case 2:
		return v[0] ^ v[1];
	case 1:
		return v[0];
	}
	return 0;
}

inline int
search_equal(const struct ts_key *a,
	     const struct ts_key *b, struct ts_space *s)
{
	return memcmp(a->key, b->key, s->key_size) == 0;
}

static int
snapshot_process_row(struct ts_spaces *s, int fileid, int offset,
                     struct tnt_iter_storage *is,
                     struct tnt_stream_snapshot *ss)
{
	struct ts_space *space =
		ts_space_match(s, ss->log.current.row_snap.space);
	struct ts_key *k = ts_space_keyalloc(space, &is->t, fileid, offset);
	if (k == NULL) {
		printf("failed to create key\n");
		return -1;
	}

	const struct ts_key *node = k;

	/* make sure that there is no collisions possible */
	assert(mh_pk_get(space->index, &node, space) == mh_end(space->index));

	mh_int_t pos = mh_pk_put(space->index, &node, NULL, space);
	if (pos == mh_end(space->index)) {
		free(k);
		return -1;
	}
	return 0;
}

static int
snapshot_process(void)
{
	char path[1024];
	snprintf(path, sizeof(path), "%s/%020llu.snap", tss.opts.cfg.snap_dir,
	         (unsigned long long) tss.last_snap_lsn);

	int fileid = ts_reftable_add(&tss.rt, path, 1);
	if (fileid == -1)
		return -1;

	struct tnt_stream st;
	tnt_snapshot(&st);
	if (tnt_snapshot_open(&st, path) == -1) {
		printf("failed to open snapshot file\n");
		tnt_stream_free(&st);
		return -1;
	}
	struct tnt_iter i;
	tnt_iter_storage(&i, &st);
	int rc = 0;
	int count = 0;
	while (tnt_next(&i)) {
		struct tnt_iter_storage *is = TNT_ISTORAGE(&i);
		struct tnt_stream_snapshot *ss =
			TNT_SSNAPSHOT_CAST(TNT_IREQUEST_STREAM(&i));
		rc = snapshot_process_row(&tss.s, fileid, ss->log.current_offset, is, ss);
		if (rc == -1)
			goto done;
		if (count % 10000 == 0) {
			printf("(snapshot) %020llu.snap %.3fM processed\r",
			       (unsigned long long) tss.last_snap_lsn,
			       (float)count / 1000000);
			fflush(stdout);
		}
		count++;
	}

	printf("\n");
	if (i.status == TNT_ITER_FAIL) {
		printf("snapshot parsing failed: %s\n", tnt_snapshot_strerror(&st));
		rc = -1;
	}
done:
	tnt_iter_free(&i);
	tnt_stream_free(&st);
	return rc;
}

static inline int
snapdir_process(void)
{
	/* open snapshot directory */
	struct tnt_dir snap_dir;
	tnt_dir_init(&snap_dir, TNT_DIR_SNAPSHOT);

	int rc = tnt_dir_scan(&snap_dir, tss.opts.cfg.snap_dir);
	if (rc == -1) {
		printf("failed to open snapshot directory\n");
		goto error;
	}

	/* find newest snapshot lsn */
	rc = tnt_dir_match_gt(&snap_dir, &tss.last_snap_lsn);
	if (rc == -1) {
		printf("failed to match greatest snapshot lsn\n");
		goto error;
	}
	printf("last snapshot lsn: %"PRIu64"\n", tss.last_snap_lsn);

	/* process snapshot */
	rc = snapshot_process();
	if (rc == -1)
		goto error;
	return 0;
error:
	tnt_dir_free(&snap_dir);
	return -1;
}

static int
xlog_process_row(struct ts_spaces *s, int fileid, int offset, struct tnt_request *r)
{
	/* validate operation */
	uint32_t ns = 0;
	struct tnt_tuple *t = NULL;

	switch (r->h.type) {
	case TNT_OP_INSERT:
		ns = r->r.insert.h.ns;
		t = &r->r.insert.t;
		break;
	case TNT_OP_DELETE:
		ns = r->r.del.h.ns;
		t = &r->r.del.t;
		return 0;
	case TNT_OP_UPDATE:
		assert(0);
		break;
	default:
		assert(0);
		break;
	}

	/* match space */
	struct ts_space *space = ts_space_match(s, ns);
	if (space == NULL) {
		printf("space %d is not defined\n", ns);
		return -1;
	}

	/* create key */
	struct ts_key *k = ts_space_keyalloc(space, t, fileid, offset);
	if (k == NULL) {
		printf("failed to create key\n");
		return -1;
	}

	/* place to the index */
	const struct ts_key *node = k;
	mh_int_t pos;

	switch (r->h.type) {
	case TNT_OP_INSERT:
		pos = mh_pk_put(space->index, &node, NULL, space);
		if (pos == mh_end(space->index)) {
			free(k);
			return -1;
		}
		break;
	case TNT_OP_DELETE:
		assert(mh_pk_get(space->index, &node, space) != mh_end(space->index));
		mh_pk_remove(space->index, &node, space);
		free(k);
		break;
	case TNT_OP_UPDATE:
	default:
		break;
	}

	return 0;
}

static int
xlog_process(struct ts_spaces *s, char *wal_dir, uint64_t file_lsn,
             uint64_t start, uint64_t *last)
{
	char path[1024];
	snprintf(path, sizeof(path), "%s/%020llu.xlog", wal_dir,
		 (unsigned long long) file_lsn);

	int fileid = ts_reftable_add(&tss.rt, path, 0);
	if (fileid == -1)
		return -1;

	struct tnt_stream st;
	tnt_xlog(&st);
	if (tnt_xlog_open(&st, path) == -1) {
		printf("failed to open xlog file\n");
		tnt_stream_free(&st);
		return -1;
	}

	struct tnt_iter i;
	tnt_iter_request(&i, &st);
	int count = 0;
	int rc = 0;
	while (tnt_next(&i)) {
		struct tnt_request *r = TNT_IREQUEST_PTR(&i);
		struct tnt_stream_xlog *xs =
			TNT_SXLOG_CAST(TNT_IREQUEST_STREAM(&i));
		if (xs->log.current.hdr.lsn > *last)
			*last = xs->log.current.hdr.lsn;
		if (xs->log.current.hdr.lsn <= start)
			continue;
		rc = xlog_process_row(s, fileid, xs->log.current_offset, r);
		if (rc == -1)
			goto done;
		if (count % 10000 == 0) {
			printf("(xlog) %020llu.xlog %.3fM processed\r",
			       (unsigned long long) file_lsn,
			       (float)count / 1000000);
			fflush(stdout);
		}
		count++;
	}

	printf("\n");
	if (i.status == TNT_ITER_FAIL) {
		printf("xlog parsing failed: %s\n", tnt_xlog_strerror(&st));
		rc = -1;
	}
done:
	tnt_iter_free(&i);
	tnt_stream_free(&st);
	return rc;
}

static int
waldir_processof(struct ts_spaces *s, struct tnt_dir *wal_dir, int i)
{
	int rc;
	if (i < wal_dir->count) {
		rc = xlog_process(s, wal_dir->path, wal_dir->files[i].lsn,
		                  tss.last_snap_lsn, &tss.last_xlog_lsn);
		if (rc == -1)
			return -1;
	}
	for (i++; i < wal_dir->count; i++) {
		rc = xlog_process(s, wal_dir->path, wal_dir->files[i].lsn,
		                  0, &tss.last_xlog_lsn);
		if (rc == -1)
			return -1;
	}
	return 0;
}

static int
waldir_process(void)
{
	/* get latest existing lsn after snapshot */
	struct tnt_dir wal_dir;
	tnt_dir_init(&wal_dir, TNT_DIR_XLOG);

	int rc = tnt_dir_scan(&wal_dir, tss.opts.cfg.wal_dir);
	if (rc == -1) {
		printf("failed to open wal directory\n");
		tnt_dir_free(&wal_dir);
		return -1;
	}

	/* match xlog file containling latest snapshot lsn record */
	if (tss.last_snap_lsn == 1) {
		rc = waldir_processof(&tss.s, &wal_dir, 0);
		if (rc == -1) {
			tnt_dir_free(&wal_dir);
			return -1;
		}
		goto done;
	}
	uint64_t xlog_inc = 0;
	rc = tnt_dir_match_inc(&wal_dir, tss.last_snap_lsn, &xlog_inc);
	if (rc == -1) {
		printf("failed to match xlog with snapshot lsn\n");
		tnt_dir_free(&wal_dir);
		return -1;
	}

	/* index all xlog records from xlog file (a:last_snap_lsn) to 
	 * latest existing xlog lsn */
	int i = 0;
	while (i < wal_dir.count && wal_dir.files[i].lsn != xlog_inc)
		i++;

	rc = waldir_processof(&tss.s, &wal_dir, i);
	if (rc == -1) {
		tnt_dir_free(&wal_dir);
		return -1;
	}
done:
	tnt_dir_free(&wal_dir);
	return 0;
}

int
ts_indexate(void)
{
	int rc = snapdir_process();
	if (rc == -1)
		return -1;
	rc = waldir_process();
	if (rc == -1)
		return -1;
	return 0;
}
