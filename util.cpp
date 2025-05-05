#include "util.hpp"
#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <cstring>
#include "ripemd160.hpp"
#include "as_proto.hpp"
#include <time.h>
#include <chrono>

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

// nlohmann::json to_json (const as_msg *msg)
// {


// }

nlohmann::json to_json (const as_msg *msg) {
    nlohmann::json ret;

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
