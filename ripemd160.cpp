#include "ripemd160.hpp"
#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <bit>

static inline void ripemd160_process(uint32_t* state, const unsigned char data[64]);

Hasher& Hasher::reset() noexcept
{
  memset(buffer, 0, 64);
  total[0] = 0;
  total[1] = 0;

  state[0] = 0x67452301;
  state[1] = 0xEFCDAB89;
  state[2] = 0x98BADCFE;
  state[3] = 0x10325476;
  state[4] = 0xC3D2E1F0;
  return *this;
}

Hasher& Hasher::update(const void* buf, uint32_t nbytes) noexcept
{
  const unsigned char* input = (const unsigned char*)buf;

  size_t left = total[0] & 0x3F;
  uint32_t fill = (uint32_t)(64 - left);

  total[0] += nbytes;

  if (total[0] < (uint32_t) nbytes) {
    total[1]++;
  }

  if (left && nbytes >= fill) {
    memcpy((void*)(buffer + left), input, fill);

    ripemd160_process(state, buffer);

    input += fill;
    nbytes  -= fill;
    left = 0;
  }

  while (nbytes >= 64) {
    ripemd160_process(state, input);
    input += 64;
    nbytes  -= 64;
  }

  if (nbytes > 0) {
    memcpy((void*)(buffer + left), input, nbytes);
  }
  return *this;
}

void Hasher::digest_to(void* buf) const noexcept
{
  Hasher tmp = *this;
  tmp.finalize();
  memcpy (buf, &tmp.state[0], 20);
}

void Hasher::finalize() noexcept
{
  static const unsigned char PADDING[64] = { 0x80 };
  uint32_t high = (total[0] >> 29) | (total[1] <<  3);
  uint32_t low  = (total[0] <<  3);
  uint32_t last = total[0] & 0x3F;
  uint32_t padn = (last < 56) ? (56 - last) : (120 - last);
  update(PADDING, padn);
  update(&low, 4);
  update(&high, 4);
}

static inline void
ripemd160_process(uint32_t* state, const unsigned char data[64])
{
  static_assert(std::endian::native == std::endian::little);
  struct {
		uint32_t A, B, C, D, E, Ap, Bp, Cp, Dp, Ep, X[16];
	} local;

	memcpy(local.X, data, 64);
	local.A = local.Ap = state[0];
	local.B = local.Bp = state[1];
	local.C = local.Cp = state[2];
	local.D = local.Dp = state[3];
	local.E = local.Ep = state[4];

#define F1(x, y, z)   ((x) ^ (y) ^ (z))
#define F2(x, y, z)   (((x) & (y)) | (~(x) & (z)))
#define F3(x, y, z)   (((x) | ~(y)) ^ (z))
#define F4(x, y, z)   (((x) & (z)) | ((y) & ~(z)))
#define F5(x, y, z)   ((x) ^ ((y) | ~(z)))

#define S(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

#define P(a, b, c, d, e, r, s, f, k) \
	do { \
		(a) += f((b), (c), (d)) + local.X[r] + (k); \
		(a) = S((a), (s)) + (e); \
		(c) = S((c), 10); \
	} while (0)

#define P2(a, b, c, d, e, r, s, rp, sp) \
	do { \
		P((a), (b), (c), (d), (e), (r), (s), F, K); \
		P(a ## p, b ## p, c ## p, d ## p, e ## p, \
		   (rp), (sp), Fp, Kp); \
	} while (0)

#define F   F1
#define K   0x00000000
#define Fp  F5
#define Kp  0x50A28BE6
	P2(local.A, local.B, local.C, local.D, local.E,  0, 11,  5,  8);
	P2(local.E, local.A, local.B, local.C, local.D,  1, 14, 14,  9);
	P2(local.D, local.E, local.A, local.B, local.C,  2, 15,  7,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  3, 12,  0, 11);
	P2(local.B, local.C, local.D, local.E, local.A,  4,  5,  9, 13);
	P2(local.A, local.B, local.C, local.D, local.E,  5,  8,  2, 15);
	P2(local.E, local.A, local.B, local.C, local.D,  6,  7, 11, 15);
	P2(local.D, local.E, local.A, local.B, local.C,  7,  9,  4,  5);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 11, 13,  7);
	P2(local.B, local.C, local.D, local.E, local.A,  9, 13,  6,  7);
	P2(local.A, local.B, local.C, local.D, local.E, 10, 14, 15,  8);
	P2(local.E, local.A, local.B, local.C, local.D, 11, 15,  8, 11);
	P2(local.D, local.E, local.A, local.B, local.C, 12,  6,  1, 14);
	P2(local.C, local.D, local.E, local.A, local.B, 13,  7, 10, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  9,  3, 12);
	P2(local.A, local.B, local.C, local.D, local.E, 15,  8, 12,  6);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F2
#define K   0x5A827999
#define Fp  F4
#define Kp  0x5C4DD124
	P2(local.E, local.A, local.B, local.C, local.D,  7,  7,  6,  9);
	P2(local.D, local.E, local.A, local.B, local.C,  4,  6, 11, 13);
	P2(local.C, local.D, local.E, local.A, local.B, 13,  8,  3, 15);
	P2(local.B, local.C, local.D, local.E, local.A,  1, 13,  7,  7);
	P2(local.A, local.B, local.C, local.D, local.E, 10, 11,  0, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  6,  9, 13,  8);
	P2(local.D, local.E, local.A, local.B, local.C, 15,  7,  5,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  3, 15, 10, 11);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  7, 14,  7);
	P2(local.A, local.B, local.C, local.D, local.E,  0, 12, 15,  7);
	P2(local.E, local.A, local.B, local.C, local.D,  9, 15,  8, 12);
	P2(local.D, local.E, local.A, local.B, local.C,  5,  9, 12,  7);
	P2(local.C, local.D, local.E, local.A, local.B,  2, 11,  4,  6);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  7,  9, 15);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 13,  1, 13);
	P2(local.E, local.A, local.B, local.C, local.D,  8, 12,  2, 11);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F3
#define K   0x6ED9EBA1
#define Fp  F3
#define Kp  0x6D703EF3
	P2(local.D, local.E, local.A, local.B, local.C,  3, 11, 15,  9);
	P2(local.C, local.D, local.E, local.A, local.B, 10, 13,  5,  7);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  6,  1, 15);
	P2(local.A, local.B, local.C, local.D, local.E,  4,  7,  3, 11);
	P2(local.E, local.A, local.B, local.C, local.D,  9, 14,  7,  8);
	P2(local.D, local.E, local.A, local.B, local.C, 15,  9, 14,  6);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 13,  6,  6);
	P2(local.B, local.C, local.D, local.E, local.A,  1, 15,  9, 14);
	P2(local.A, local.B, local.C, local.D, local.E,  2, 14, 11, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  7,  8,  8, 13);
	P2(local.D, local.E, local.A, local.B, local.C,  0, 13, 12,  5);
	P2(local.C, local.D, local.E, local.A, local.B,  6,  6,  2, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 13,  5, 10, 13);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 12,  0, 13);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  7,  4,  7);
	P2(local.D, local.E, local.A, local.B, local.C, 12,  5, 13,  5);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F4
#define K   0x8F1BBCDC
#define Fp  F2
#define Kp  0x7A6D76E9
	P2(local.C, local.D, local.E, local.A, local.B,  1, 11,  8, 15);
	P2(local.B, local.C, local.D, local.E, local.A,  9, 12,  6,  5);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 14,  4,  8);
	P2(local.E, local.A, local.B, local.C, local.D, 10, 15,  1, 11);
	P2(local.D, local.E, local.A, local.B, local.C,  0, 14,  3, 14);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 15, 11, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  9, 15,  6);
	P2(local.A, local.B, local.C, local.D, local.E,  4,  8,  0, 14);
	P2(local.E, local.A, local.B, local.C, local.D, 13,  9,  5,  6);
	P2(local.D, local.E, local.A, local.B, local.C,  3, 14, 12,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  7,  5,  2, 12);
	P2(local.B, local.C, local.D, local.E, local.A, 15,  6, 13,  9);
	P2(local.A, local.B, local.C, local.D, local.E, 14,  8,  9, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  6,  7,  5);
	P2(local.D, local.E, local.A, local.B, local.C,  6,  5, 10, 15);
	P2(local.C, local.D, local.E, local.A, local.B,  2, 12, 14,  8);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F5
#define K   0xA953FD4E
#define Fp  F1
#define Kp  0x00000000
	P2(local.B, local.C, local.D, local.E, local.A,  4,  9, 12,  8);
	P2(local.A, local.B, local.C, local.D, local.E,  0, 15, 15,  5);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  5, 10, 12);
	P2(local.D, local.E, local.A, local.B, local.C,  9, 11,  4,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  7,  6,  1, 12);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  8,  5,  5);
	P2(local.A, local.B, local.C, local.D, local.E,  2, 13,  8, 14);
	P2(local.E, local.A, local.B, local.C, local.D, 10, 12,  7,  6);
	P2(local.D, local.E, local.A, local.B, local.C, 14,  5,  6,  8);
	P2(local.C, local.D, local.E, local.A, local.B,  1, 12,  2, 13);
	P2(local.B, local.C, local.D, local.E, local.A,  3, 13, 13,  6);
	P2(local.A, local.B, local.C, local.D, local.E,  8, 14, 14,  5);
	P2(local.E, local.A, local.B, local.C, local.D, 11, 11,  0, 15);
	P2(local.D, local.E, local.A, local.B, local.C,  6,  8,  3, 13);
	P2(local.C, local.D, local.E, local.A, local.B, 15,  5,  9, 11);
	P2(local.B, local.C, local.D, local.E, local.A, 13,  6, 11, 11);
#undef F
#undef K
#undef Fp
#undef Kp

	local.C       = state[1] + local.C + local.Dp;
	state[1] = state[2] + local.D + local.Ep;
	state[2] = state[3] + local.E + local.Ap;
	state[3] = state[4] + local.A + local.Bp;
	state[4] = state[0] + local.B + local.Cp;
	state[0] = local.C;
}
