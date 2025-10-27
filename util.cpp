#include "util.hpp"
#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <cstring>
#include "ripemd160.hpp"
#include "as_proto.hpp"
#include <time.h>
#include <chrono>
#include <nlohmann/json.hpp>
#include <arpa/inet.h>

using json = nlohmann::json;
using namespace std;

std::string get_labeled (const std::string& str, const std::string& l)
{
    size_t fp = str.find (l + "=");
    if (fp == std::string::npos)	return "";
    fp += l.length () + 1;
    size_t lp = str.find (':', fp);
    if (lp == std::string::npos)	lp = str.length ();

    return str.substr (fp, (lp - fp));
}

std::vector<uint8_t> addr_resolve (const std::string& hostport)
{
    // Resolve hostname:port string to something we can pass to connect(2).
    size_t cpos = hostport.find (':');
    dieunless (cpos != std::string::npos);
    std::string f1=hostport.substr (0, cpos), f2=hostport.substr (cpos + 1);

    addrinfo *addri;
    addrinfo hints{};
    hints.ai_family = AF_INET; // I want ipv4  AF_UNSPEC for ipv6
    hints.ai_flags = AI_NUMERICHOST | AI_ADDRCONFIG;
    if (std::any_of (f1.begin (), f1.end (),
		     [](const char c) { return (c < '.') || (c > '9'); }))
	hints.ai_flags &= ~AI_NUMERICHOST;
    dieunless (getaddrinfo (f1.c_str (), f2.c_str (), &hints, &addri) == 0);

    // just take the first in a linked list of addrinfo.  lazy.
    std::vector<uint8_t> ret (addri->ai_addrlen);
    memcpy (ret.data (), addri->ai_addr, ret.size ());
    freeaddrinfo (addri);
    return ret;
}

int tcp_connect (const std::string& hostport)
{
    // Create TCP connection with appropriate socket options
    int fd, one = 1;
    auto ab = addr_resolve(hostport);
    dieunless((fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless(::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
    dieunless(::connect(fd, (sockaddr *)ab.data(), ab.size()) == 0);
    return fd;
}

size_t add_integer_key_digest (void *dst, const std::string& sn, uint64_t ki)
{
  Hasher hasher;
  hasher.update(sn.c_str (), sn.length ());
  {
    char buf[9];
    buf[0] = (char) as_particle::type::t_integer; // key type
    *(uint64_t *)&buf[1] = htobe64 (ki);
    hasher.update(buf, 9);
  }
  hasher.digest_to(dst);
  return 20;
}
size_t add_string_key_digest (void *dst, const std::string& sn, const std::string& si)
{
    char t = (char) as_particle::type::t_string; // key type
    Hasher hasher;
    hasher
      .update(sn.c_str (), sn.length ())
      .update(&t, 1)
      .update(si.c_str(), si.length())
      .digest_to(dst);
    return 20;
}

void hash_combine(std::size_t& seed, std::size_t value)
{
    seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

uint32_t secs_since_cfepoch (void)
{
    const uint64_t cfepoch = 1262304000;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec - cfepoch;
}

uint64_t usec_now (void) {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

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

constexpr uint8_t hex2dec (char h) {
  if ((h >= '0') && (h <= '9')) {
    return h - '0';
  } else if ((h >= 'A') && (h <= 'F')) {
    return 10 + (h - 'A');
  } else if ((h >= 'a') && (h <= 'f')) {
    return 10 + (h - 'a');
  } else {
    return 0;
  }
}

void from_hex (void *dst, const void* src, size_t sz)
{
    const char *sp = (const char *)src;
    uint8_t *dp = (uint8_t *)dst;

    const uint8_t *ep = dp + sz;
    while (dp != ep) {
      *dp++ = (hex2dec(*sp++) << 4) + hex2dec(*sp++);
    }
}

// nlohmann::json to_json (const as_msg *msg)
// {


// }

nlohmann::json to_json (const as_msg *msg) {
    nlohmann::json ret = nlohmann::json::object ();

    ret["flags"] = msg->flags;
    if (msg->result_code)		ret["result_code"] = msg->result_code;
    if (msg->be_generation)		ret["generation"] = be32toh (msg->be_generation);
    if (msg->be_record_ttl)		ret["record_ttl"] = be32toh (msg->be_record_ttl);
    if (msg->be_transaction_ttl)	ret["transaction_ttl"] = be32toh (msg->be_transaction_ttl);

    if (msg->be_fields) {
	ret["fields"] = nlohmann::json::object ();
	as_field *f = (as_field *)msg->data;
	for (auto ii = msg->n_fields (); ii--; f = f->next ()) {
	    const std::string fname = to_string (f->t);
	    std::string val;
	    val.resize (2 * f->data_sz ());
	    to_hex (&(val[0]), f->data, f->data_sz ());
	    ret["fields"][fname] = val;
	}
    }

    if (msg->be_ops) {
	ret["ops"] = nlohmann::json::array ();
	as_op *o = msg->ops_begin ();
	for (auto ii = msg->n_ops (); ii--; o = o->next ()) {
	    nlohmann::json jop;
	    jop["type"] = to_string (o->op_type);
	    if (o->name_sz) {
		jop["name"] = std::string ((const char *)&(o->name[0]), o->name_sz);
	    }
	    if (o->data_sz ()) {
		std::string val;
		val.resize (2 * o->data_sz ());
		to_hex (&(val[0]), o->data (), o->data_sz ());
		jop["data"] = val;
	    }
	    ret["ops"].push_back (jop);
	}
    }

    return ret;
}

// Helper function to manually pack msgpack for expressions
// Bin names are plain msgpack strings, string values use msgpack ext type
static void pack_expr_element(std::vector<uint8_t>& out, const json& j, bool is_bin_name = false)
{
    switch (j.type()) {
        case json::value_t::null:
            out.push_back(0xC0); // msgpack nil
            break;

        case json::value_t::boolean:
            out.push_back(j.get<bool>() ? 0xC3 : 0xC2); // msgpack true/false
            break;

        case json::value_t::number_integer: {
            int64_t val = j.get<int64_t>();
            if (val >= 0 && val <= 127) {
                // positive fixint
                out.push_back(static_cast<uint8_t>(val));
            } else if (val < 0 && val >= -32) {
                // negative fixint
                out.push_back(static_cast<uint8_t>(val));
            } else if (val >= 0 && val <= 0xFF) {
                out.push_back(0xCC); // uint8
                out.push_back(static_cast<uint8_t>(val));
            } else if (val >= 0 && val <= 0xFFFF) {
                out.push_back(0xCD); // uint16
                uint16_t v = htons(static_cast<uint16_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 2);
            } else if (val >= 0 && val <= 0xFFFFFFFF) {
                out.push_back(0xCE); // uint32
                uint32_t v = htonl(static_cast<uint32_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 4);
            } else if (val >= 0) {
                out.push_back(0xCF); // uint64
                uint64_t v = htobe64(static_cast<uint64_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 8);
            } else if (val >= -128) {
                out.push_back(0xD0); // int8
                out.push_back(static_cast<uint8_t>(val));
            } else if (val >= -32768) {
                out.push_back(0xD1); // int16
                int16_t v = htons(static_cast<int16_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 2);
            } else if (val >= -2147483648LL) {
                out.push_back(0xD2); // int32
                int32_t v = htonl(static_cast<int32_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 4);
            } else {
                out.push_back(0xD3); // int64
                int64_t v = htobe64(val);
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 8);
            }
            break;
        }

        case json::value_t::number_unsigned: {
            uint64_t val = j.get<uint64_t>();
            if (val <= 127) {
                out.push_back(static_cast<uint8_t>(val));
            } else if (val <= 0xFF) {
                out.push_back(0xCC); // uint8
                out.push_back(static_cast<uint8_t>(val));
            } else if (val <= 0xFFFF) {
                out.push_back(0xCD); // uint16
                uint16_t v = htons(static_cast<uint16_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 2);
            } else if (val <= 0xFFFFFFFF) {
                out.push_back(0xCE); // uint32
                uint32_t v = htonl(static_cast<uint32_t>(val));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 4);
            } else {
                out.push_back(0xCF); // uint64
                uint64_t v = htobe64(val);
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&v), reinterpret_cast<uint8_t*>(&v) + 8);
            }
            break;
        }

        case json::value_t::string: {
            const std::string& str = j.get_ref<const std::string&>();
            size_t len = str.size();

            if (is_bin_name) {
                // Bin names: plain msgpack string
                if (len <= 31) {
                    out.push_back(0xA0 | static_cast<uint8_t>(len));
                } else if (len <= 0xFF) {
                    out.push_back(0xD9);
                    out.push_back(static_cast<uint8_t>(len));
                } else if (len <= 0xFFFF) {
                    out.push_back(0xDA);
                    uint16_t l = htons(static_cast<uint16_t>(len));
                    out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 2);
                } else {
                    out.push_back(0xDB);
                    uint32_t l = htonl(static_cast<uint32_t>(len));
                    out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 4);
                }
                out.insert(out.end(), str.begin(), str.end());
            } else {
                // String values: msgpack bin with Aerospike type byte prefix
                size_t data_len = len + 1; // +1 for type byte
                if (data_len <= 0xFF) {
                    // bin 8
                    out.push_back(0xC4);
                    out.push_back(static_cast<uint8_t>(data_len));
                } else if (data_len <= 0xFFFF) {
                    // bin 16
                    out.push_back(0xC5);
                    uint16_t l = htons(static_cast<uint16_t>(data_len));
                    out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 2);
                } else {
                    // bin 32
                    out.push_back(0xC6);
                    uint32_t l = htonl(static_cast<uint32_t>(data_len));
                    out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 4);
                }
                out.push_back(0x03); // AS_BYTES_STRING type
                out.insert(out.end(), str.begin(), str.end());
            }
            break;
        }

        case json::value_t::array: {
            size_t len = j.size();
            if (len <= 15) {
                // fixarray
                out.push_back(0x90 | static_cast<uint8_t>(len));
            } else if (len <= 0xFFFF) {
                // array 16
                out.push_back(0xDC);
                uint16_t l = htons(static_cast<uint16_t>(len));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 2);
            } else {
                // array 32
                out.push_back(0xDD);
                uint32_t l = htonl(static_cast<uint32_t>(len));
                out.insert(out.end(), reinterpret_cast<uint8_t*>(&l), reinterpret_cast<uint8_t*>(&l) + 4);
            }

            // Detect bin() call: [81, type, name] or bin_type(): [82, name]
            // In these cases, the last string element is a bin name (no type byte)
            bool is_bin_call = (len == 3 && j[0].is_number_integer() && j[0].get<int>() == 81) ||  // bin()
                               (len == 2 && j[0].is_number_integer() && j[0].get<int>() == 82);    // bin_type()

            // Recursively pack elements
            int index = 0;
            for (const auto& elem : j) {
                bool elem_is_bin_name = is_bin_call && (index == len - 1); // Last element in bin() or bin_type()
                pack_expr_element(out, elem, elem_is_bin_name);
                index++;
            }
            break;
        }

        case json::value_t::number_float: {
            // float 64 (double)
            out.push_back(0xCB);
            double val = j.get<double>();
            uint64_t bits;
            memcpy(&bits, &val, sizeof(double));
            bits = htobe64(bits);
            out.insert(out.end(), reinterpret_cast<uint8_t*>(&bits), reinterpret_cast<uint8_t*>(&bits) + 8);
            break;
        }

        default:
            throw std::runtime_error("Unsupported json type for expression encoding");
    }
}

std::vector<uint8_t> to_expr_msgpack(const json& expr)
{
    std::vector<uint8_t> result;
    pack_expr_element(result, expr);
    return result;
}

std::vector<uint8_t> to_expr_msgpack_wrapped(const json& expr, as_exp::flags flags)
{
    // Encode the expression with special msgpack rules
    auto expr_bytes = to_expr_msgpack(expr);

    // Build wrapper array: [expr_msgpack, flags]
    std::vector<uint8_t> result;
    result.push_back(0x92); // fixarray with 2 elements

    // Element 1: raw expression msgpack bytes
    result.insert(result.end(), expr_bytes.begin(), expr_bytes.end());

    // Element 2: flags as fixint
    result.push_back((uint8_t)flags);

    return result;
}