
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

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <connector/c/include/tarantool/tnt.h>

#include <cfg/prscfg.h>
#include <cfg/tarantool_box_cfg.h>

#define MH_SOURCE 1
#include "key.h"
#include "hash.h"
#include "options.h"
#include "config.h"
#include "space.h"
#include "indexate.h"

int main(int argc, char *argv[])
{
	struct ts_options opts;
	struct ts_spaces s;

	ts_options_init(&opts);
	memset(&s, 0, sizeof(s));

	/* parse arguments */
	switch (ts_options_process(&opts, argc, argv)) {
	case TS_MODE_USAGE:
		ts_options_free(&opts);
		return ts_options_usage();
	case TS_MODE_VERSION:
		ts_options_free(&opts);
		return 0;
	case TS_MODE_CREATE:
		break;
	}

	/* load configuration file */
	int rc = ts_config_load(&opts);
	if (rc == -1)
		goto done;

	/* create spaces */
	rc = ts_space_init(&s);
	if (rc == -1)
		goto done;
	rc = ts_space_fill(&s, &opts);
	if (rc == -1)
		goto done;

	printf("work_dir: %s\n", opts.cfg.work_dir);
	printf("snap_dir: %s\n", opts.cfg.snap_dir);
	printf("wal_dir:  %s\n", opts.cfg.wal_dir);
	printf("spaces:   %d\n", mh_size(s.t));

	/* indexate snapshot and xlog data */
	rc = ts_indexate(&opts, &s);
	if (rc == -1)
		goto done;

	printf("complete.\n");
	
done:
	ts_options_free(&opts);
	ts_space_free(&s);
	return (rc == -1 ? 1 : 0);
}
