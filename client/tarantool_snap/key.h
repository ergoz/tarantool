#ifndef TS_KEY_H_INCLUDED
#define TS_KEY_H_INCLUDED

struct ts_key {
	uint32_t file;
	uint32_t offset;
	unsigned char key[];
} __attribute__((packed));

#endif
