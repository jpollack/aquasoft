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
#include "as_proto.hpp"
#include "util.hpp"

using namespace std;

const char g_ep_str[] = "JP_INFO_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

// envp is POSIX but not C++
int main (int argc, char **argv, char **envp)
{
    // Defaults for required variables.
    unordered_map<string,string> p = {
	{ "ASDB",		"localhost:3000" }
    };
    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp))
	if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
	    auto vs = strchr (ep, '=');
	    auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
	    if (ks.length ()) p[ks] = string (vs + 1);
	}

    auto ab = addr_resolve (p["ASDB"]);

    // Make a client fd and connect
    int cfd;
    dieunless ((cfd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (cfd, (sockaddr *)ab.data (), ab.size ()) == 0);
    string line;
    char *res = nullptr;
    while (getline (cin, line) && !line.empty ()) {
	string resp;
	auto sz = call_info (cfd, resp, line + "\n");
	printf ("%s\n", resp.c_str ());
    }

    // set-config:context=xdr;dc=dc0;node-adress-port=127.0.0.1:3333;

    close (cfd);

    return 0;
}
