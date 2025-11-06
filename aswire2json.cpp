#include <algorithm>
#include <endian.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <numeric>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include <nlohmann/json.hpp>
#include "as_proto.hpp"
#include "util.hpp"

using json = nlohmann::json;
using namespace std;

int main (int argc, char **argv, char **envp)
{
  string hline;
  while (getline (cin, hline) && !hline.empty ()) {
	if (hline.size() % 2) {
	  fprintf (stderr, "Invalid length: %d\n", hline.size());
	  continue;
	}
	auto sz = hline.size() / 2;
	vector<uint8_t> buf(sz, 0);
	from_hex(buf.data(), hline.c_str(), sz);
	printf ("%s\n", to_json((as_msg*)(buf.data() + 8)).dump().c_str());
  }

  return 0;
}
