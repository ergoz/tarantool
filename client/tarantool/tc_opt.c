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
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <third_party/gopt/gopt.h>

#include "client/tarantool/tc_opt.h"

#define TC_DEFAULT_HOST "localhost"
#define TC_DEFAULT_PORT 33013
#define TC_DEFAULT_PORT_ADMIN 33015

/* supported cli options */
static const void *tc_options_def = gopt_start(
	gopt_option('h', GOPT_ARG, gopt_shorts('h'),
		    gopt_longs("host"), " <host>", "server address"),
	gopt_option('p', GOPT_ARG, gopt_shorts('p'),
		    gopt_longs("port"), " <port>", "server port"),
	gopt_option('a', GOPT_ARG, gopt_shorts('a'),
		    gopt_longs("admin-port"), " <port>", "server admin port"),
	gopt_option('C', GOPT_ARG, gopt_shorts('C'),
		    gopt_longs("cat"), " <file>", "print xlog or snapshot file content"),
	gopt_option('I', 0, gopt_shorts('C'),
		    gopt_longs("catin"), NULL, "print xlog content from stdin"),
	gopt_option('P', GOPT_ARG, gopt_shorts('P'),
		    gopt_longs("play"), " <file>", "replay xlog file to the specified server"),
	gopt_option('S', GOPT_ARG, gopt_shorts('S'),
		    gopt_longs("space"), " <space>", "xlog file space number"),
	gopt_option('F', GOPT_ARG, gopt_shorts('F'),
		    gopt_longs("from"), " <lsn>", "start xlog file from the specified lsn"),
	gopt_option('T', GOPT_ARG, gopt_shorts('T'),
		    gopt_longs("to"), " <lsn>", "stop on specified xlog lsn"),
	gopt_option('M', GOPT_ARG, gopt_shorts('M'),
		    gopt_longs("format"), " <name>", "cat output format (tarantool, raw)"),
	gopt_option('H', 0, gopt_shorts('H'),
		    gopt_longs("header"), NULL, "add file headers for the raw output"),
	gopt_option('R', GOPT_ARG, gopt_shorts('R'),
		    gopt_longs("rpl"), " <lsn>", "act as replica for the specified server"),
	gopt_option('?', 0, gopt_shorts(0), gopt_longs("help"),
		    NULL, "display this help and exit"),
	gopt_option('v', 0, gopt_shorts('v'), gopt_longs("version"),
		    NULL, "display version information and exit")
);

void tc_opt_usage(void)
{
	printf("usage: tarantool [options] [query]\n\n");
	printf("tarantool client.\n");
	gopt_help(tc_options_def);
	exit(0);
}

void tc_opt_version(void)
{
	printf("tarantool client, version %s.%s\n",
	       TC_VERSION_MAJOR,
	       TC_VERSION_MINOR);
	exit(0);
}

enum tc_opt_mode tc_opt_init(struct tc_opt *opt, int argc, char **argv)
{
	/* usage */
	void *tc_options = gopt_sort(&argc, (const char**)argv, tc_options_def);
	if (gopt(tc_options, '?')) {
		opt->mode = TC_OPT_USAGE;
		goto done;
	}

	/* version */
	if (gopt(tc_options, 'v')) {
		opt->mode = TC_OPT_VERSION;
		goto done;
	}

	/* server host */
	gopt_arg(tc_options, 'h', &opt->host);
	if (opt->host == NULL)
		opt->host = TC_DEFAULT_HOST;

	/* server port */
	const char *arg = NULL;
	opt->port = TC_DEFAULT_PORT;
	if (gopt_arg(tc_options, 'p', &arg))
		opt->port = atoi(arg);

	/* server admin port */
	opt->port_admin = TC_DEFAULT_PORT_ADMIN;
	if (gopt_arg(tc_options, 'a', &arg))
		opt->port_admin = atoi(arg);

	/* space */
	opt->space = 0;
	opt->space_set = 0;
	if (gopt_arg(tc_options, 'S', &arg)) {
		opt->space = atoi(arg);
		opt->space_set = 1;
	}

	/* from lsn */
	opt->lsn_from = 0;
	if (gopt_arg(tc_options, 'F', &arg)) {
		opt->lsn_from = strtoll(arg, NULL, 10);
		opt->lsn_from_set = 1;
	}

	/* to lsn */
	opt->lsn_to = 0;
	if (gopt_arg(tc_options, 'T', &arg)) {
		opt->lsn_to = strtoll(arg, NULL, 10);
		opt->lsn_to_set = 1;
	}

	/* output format */
	opt->raw = 0;
	opt->format = NULL;
	if (gopt_arg(tc_options, 'M', &arg))
		opt->format = arg;

	opt->raw_with_headers = 0;
	if (gopt(tc_options, 'H'))
		opt->raw_with_headers = 1;

	/* replica mode */
	if (gopt_arg(tc_options, 'R', &arg)) {
		opt->mode = TC_OPT_RPL;
		opt->lsn = strtoll(arg, NULL, 10);
		goto done;
	}

	/* wal-cat mode */
	if (gopt_arg(tc_options, 'C', &opt->file)) {
		opt->mode = TC_OPT_WAL_CAT;
		goto done;
	}

	/* wal-cat mode from stdin */
	if (gopt(tc_options, 'I')) {
		opt->mode = TC_OPT_WAL_CAT;
		opt->file = NULL;
		goto done;
	}

	/* wal-play mode */
	if (gopt_arg(tc_options, 'P', &opt->file)) {
		opt->mode = TC_OPT_WAL_PLAY;
		goto done;
	}

	/* default */
	if (argc >= 2) {
		opt->cmdv = argv + 1;
		opt->cmdc = argc - 1;
		opt->mode = TC_OPT_CMD;
	} else {
		opt->mode = TC_OPT_INTERACTIVE;
	}
done:
	gopt_free(tc_options);
	return opt->mode;
}
