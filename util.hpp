#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <nlohmann/json.hpp>
#include "as_proto.hpp"

inline void __dul (const char *m, const char *f, int l) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", f, l, m); abort (); }
#define dieunless(EX) ((EX) || (__dul (#EX, __FILE__, __LINE__),false))

std::vector<uint8_t> addr_resolve (const std::string& hostport);
size_t add_integer_key_digest (void *dst, const std::string& sn, uint64_t ki);
size_t add_string_key_digest (void *dst, const std::string& sn, const std::string& si);
void hash_combine(std::size_t& seed, std::size_t value);

uint64_t usec_now (void);
uint32_t secs_since_cfepoch (void);
std::string get_labeled (const std::string& str, const std::string& l);
void to_hex (void *dst, const void* src, size_t sz);
void from_hex (void *dst, const void* src, size_t sz);
nlohmann::json to_json (const as_msg *msg);
