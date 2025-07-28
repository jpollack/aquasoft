#pragma once
#include <stddef.h>
#include <stdint.h>

class Hasher {
public:
  Hasher() noexcept { reset(); }
  Hasher& reset() noexcept;
  Hasher& update(const void* buf, uint32_t nbytes) noexcept;
  void digest_to(void* buf) const noexcept;
private:
  void finalize() noexcept;
  uint32_t total[2];
  uint32_t state[5];
  alignas(uint32_t) unsigned char buffer[64];
};
