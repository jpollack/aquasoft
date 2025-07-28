#include <stdio.h>
#include "ripemd160.hpp"
#include <chrono>
#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

void to_hex (void *dst, const void* src, size_t sz)
{
    const char lut[] = "0123456789ABCDEF";
    const uint8_t *sp = (const uint8_t *)src;
    char *dp = (char *)dst;

    const uint8_t *ep = sp + sz;
    while (sp != ep) {
	*dp++ = lut[(*sp & 0xF0)>>4];
	*dp++ = lut[(*sp++ & 0x0F)];
    }
}

int main (int argc, char **argv)
{

  
}
