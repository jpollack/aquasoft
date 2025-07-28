#include "as_proto.hpp"
#include "util.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/tcp.h>
#include <nlohmann/json.hpp>
#include <numeric>
#include <queue>
#include <random>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;

using namespace std;
unordered_map<string,string> p;

atomic<bool> g_running;
auto g_rng = std::default_random_engine {};
void sigint_handler (int signum) { g_running.store(false); }
atomic<uint32_t> g_idx;
vector<uint32_t> g_buf;

int tcp_connect (const std::string& str)
{
  int fd;
  int one = 1;
  auto ab = addr_resolve (str);
  dieunless ((fd = ::socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
  dieunless (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
  dieunless (::connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);
  dieunless (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0);
  return fd;
}

as_msg *visit (as_msg *msg, int ri, int flags)
{
  msg->clear ();
  msg->flags = flags;
  msg->be_transaction_ttl = htobe32 (1000);
  dieunless (msg->add (as_field::type::t_namespace, p["NS"]));
  dieunless (msg->add (as_field::type::t_set, p["SN"]));
  add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, 20)->data, p["SN"], ri);
  return msg;
}
as_msg *set_bin (as_msg *msg, uint16_t bidx, int64_t val)
{
  char buf[16] = {0};
  dieunless (sizeof(buf) > snprintf (buf, sizeof(buf), "b%05d", bidx));
  as_op *op = msg->add (as_op::type::t_write, buf, 8);
  op->data_type = as_particle::type::t_integer;
  *(uint64_t *)op->data () = htobe64 (val);
  return msg;
}

as_msg *get_bin (as_msg *msg, uint16_t bidx)
{
  char buf[16] = {0};
  dieunless (sizeof(buf) > snprintf (buf, sizeof(buf), "b%05d", bidx));
  as_op *op = msg->add (as_op::type::t_read, buf, 0);
  return msg;
}

int64_t bin_value (as_msg *msg)
{
  return be64toh (*(int64_t *)msg->ops_begin ()->data ());
}

void record_init (as_msg *msg, int ri, size_t numBins, size_t padSize)
{
  visit (msg, ri, (!numBins && !padSize) ? AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE : AS_MSG_FLAG_WRITE);
  if (numBins > 0) {
    vector<size_t> v (numBins);
    std::iota (v.begin (), v.end (), 1);
    std::shuffle (v.begin (), v.end (), g_rng);
    for (const auto& e : v) set_bin (msg, e, e);
  }

  if (padSize > 0) {
    std::string padstring (padSize, 'x');
    dieunless (msg->add (as_op::type::t_write, "padding", padstring.length (), padstring.c_str (), as_particle::type::t_string));
  }
}

void record_size (as_msg *msg, int ri)
{
  visit (msg, ri, AS_MSG_FLAG_READ);
  auto pload = json::to_msgpack ({ { 74 }, 0 });
  dieunless (msg->add (as_op::type::t_exp_read, "size", pload.size (), pload.data ()));
}

void workload_entry (int rate, bool doWrite)
{
  int fd = tcp_connect (p["ASDB"]);
  auto nbins = stoi (p["NBINS"]);
  auto id_lb = stoi (p["KEYLB"]);
  auto id_ub = stoi (p["KEYUB"]);

  thread_local static std::random_device rd;
  thread_local static std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(id_lb, id_ub); // define the range
  std::uniform_int_distribution<> distb(1, nbins); // define the range
  std::uniform_int_distribution<> distv(0, std::numeric_limits<int32_t>::max ());

  std::uniform_real_distribution<double> distd (0, 1);

  uint64_t tnow = usec_now ();
  uint64_t tnext;
  int64_t ri;
  int64_t val;
  size_t bidx;
  char buf[2048];
  as_msg *res = (as_msg *)(buf + 64);
  as_msg *req = (as_msg *)(buf + 1024);
  uint64_t duration;

  double idi = rate ? 1000000.0f / (double)rate : 0.0;

  while (g_running.load ()) {
    tnext = tnow + -log (1.0f - distd (gen)) * idi;
    if (rate == 0)
      tnext = tnow;
    uint16_t bidx = stoi (p["BIDX"]) < 0 ? distb (gen) : stoi (p["BIDX"]);
    visit (req, distr (gen), doWrite ? AS_MSG_FLAG_WRITE : AS_MSG_FLAG_READ);
    if (doWrite) {
      set_bin (req, bidx, distv (gen));
    } else {
      get_bin (req, bidx);
    }

    while (g_running.load () && ((tnow = usec_now ()) < tnext)) {
      uint64_t td = tnext - tnow;
      usleep ((td>10) ? (td-10) : 1);
    }
    if (!g_running.load ()) {
      break;
    }

    auto idx = g_idx.fetch_add (2);
    auto ii = (idx / 2) + ((idx & 1) * (g_buf.size () / 2));
    dieunless ((1024-64) > call (fd, &res, req, g_buf.data () + ii));
    dieunless (res->result_code == 0);
  }

  close (fd);

}

void print_entry (int rate)
{
  uint64_t tlast = 0;
  uint64_t tnow = usec_now ();
  json jo = { { "now", tnow } };

  while (g_running.load()) {
    while (g_running.load() && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
      uint64_t td = (tlast + (1000000 / rate)) - tnow;
      usleep ((td>50) ? (td-50) : 10);
    }
    if (!g_running.load())	    break;
    tlast = tnow;
    jo["now"] = tnow;
    jo["data"] = json::array ();
    auto nidx = !g_idx & 1; // ping pong
    auto lidx = g_idx.exchange (nidx);
    auto idxb = (lidx & 1) * (g_buf.size () / 2);
    jo["data"].get_ptr<json::array_t*>()->reserve (lidx / 2);
    for (auto ii = 0; ii < (lidx / 2); ii++) {
      jo["data"][ii] = g_buf[idxb + ii];
      g_buf[idxb + ii] = 0;
    }

    printf ("%s\n", jo.dump ().c_str ());
    fflush (stdout);
  }

}

void update_entry (bool doWrite)
{
  int nth = stoi (p["THREADS"]);
  vector<thread> vth;

  g_idx = 0;
  g_buf.resize (1024*1024);

  if (stoi (p["DURATION"]) > 0)
    vth.emplace_back ([&](){ sleep (stoi (p["DURATION"])); g_running.store(false); });

  for (int ii=0; ii < nth; ii++)
    vth.emplace_back (workload_entry, stoi (p["RATE"]), doWrite);

  vth.emplace_back (print_entry, 1);

  while (g_running.load()) {
    usleep (1000);
  }

  for (auto& th : vth) {
    th.join ();
  }

}

void init_entry (void)
{
  uint64_t tb0 = usec_now ();
  printf ("%lu\n", tb0);
  int fd = tcp_connect (p["ASDB"]);
  auto recsize = stoi (p["RECSIZE"]);
  auto nbins = stoi (p["NBINS"]);
  auto id_lb = stoi (p["KEYLB"]);
  auto id_ub = stoi (p["KEYUB"]);
  char *buf = (char *)malloc (2 * 1024 * 1024);
  as_msg *res = (as_msg *)(buf + 64);
  as_msg *req = (as_msg *)(buf + 1024);
  uint32_t dur = 0;
  json jo = { { "type", "insert" }, { "id", 0 }, { "bins", nbins } };

  if (stoi (p["TRUNCATE"])) {
    auto sret = call_info (fd, "truncate:namespace=" + p["NS"] + ";set=" + p["SN"] + ";\n", &dur);
    // sret == 'truncate:namespace=ns0;set=demo;\tok'
    json jt0 = { { "type", "truncate" }, { "dur", dur } };
    printf ("%s\n", jt0.dump ().c_str ());
  }

  record_init (req, 0, nbins, 1);
  dieunless ((1024-64) > call (fd, &res, req, &dur));
  dieunless (res->result_code == 0);
  record_size (req, 0);
  dieunless ((1024-64) > call (fd, &res, req));
  dieunless (res->result_code == 0);
  auto rsize = bin_value (res);
  auto psize = (recsize - rsize) + 1;
  // without padding, record is rsize
  jo["bytes"] = rsize;
  jo["dur"] = dur;
  printf ("%s\n", jo.dump ().c_str ());
  jo["bytes"] = recsize;

  if (psize <= 1) psize = 0;
  for (auto id = id_lb; id <= id_ub; id++) {
    record_init (req, id, nbins, psize);
    jo["id"] = id;
    jo["bins"] = nbins;
    dieunless ((1024-64) > call (fd, &res, req, &dur));
    dieunless (res->result_code == 0);
    jo["dur"] = dur;
    printf ("%s\n", jo.dump ().c_str ());
  }
  free (buf);
}

const char g_ep_str[] = "WORKLOAD_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

int main (int argc, char **argv, char **envp)
{
  // srand ((unsigned int)clock ());
  // Defaults for required variables.
  p = {
    { "ASDB",		"localhost:3000" },
    { "BIDX",		"-1" },
    { "DURATION",		"0" },
    { "KEYLB",		"1" },
    { "KEYUB",		"10" },
    { "MODE",		"read"},
    { "NBINS",		"20000" },
    { "NS",			"ns0" },
    { "RATE",		"100" },
    { "RECSIZE",		"500000" },
    { "SN",			"demo" },
    { "THREADS",		"1" },
    { "TRUNCATE",		"1" },
  };
  // Override from environment
  for (auto ep = *envp; ep; ep = *(++envp))
    if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
      auto vs = strchr (ep, '=');
      auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
      if (ks.length ()) p[ks] = string (vs + 1);
    }

  for (auto ii=1; ii<argc; ii++) {
    auto ap = argv[ii];
    auto vs = strchr (ap, '=');
    dieunless (vs);
    auto ks = string (ap).substr (0, vs - ap);
    transform (ks.begin (), ks.end (), ks.begin (), ::toupper);
    if (ks.length ()) p[ks] = string (vs + 1);
  }

  signal (SIGINT, sigint_handler);
  g_running.store(true);

  string cmd (p["MODE"]);

  if (!cmd.compare ("init"))				init_entry ();
  else if (!cmd.compare ("update"))			update_entry (true);
  else if (!cmd.compare ("read"))			update_entry (false);

  return 0;
}
