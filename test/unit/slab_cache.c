#include "lib/small/slab_cache.h"
#include <stdio.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>


enum { NRUNS = 100, ITERATIONS = 10000, MAX_ALLOC = 105000 };
static struct slab *runs[NRUNS];

int main()
{
	srand(time(0));

	struct slab_cache cache;
	slab_cache_create(&cache);

	int i = 0;

	while (i < ITERATIONS) {
		int run = random() % NRUNS;
		int size = random() % MAX_ALLOC;
		if (runs[run]) {
			slab_put(&cache, runs[run]);
		}
		runs[run] = slab_get(&cache, size);
		assert(runs[run]);
		i++;
	}

	slab_cache_destroy(&cache);
}
