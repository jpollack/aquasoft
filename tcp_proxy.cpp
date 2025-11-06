// TCP proxy that captures wire protocol bytes between client and Aerospike server
// Usage: ./tcp_proxy <listen_port> <target_host:port>
// Example: ./tcp_proxy 7000 localhost:3000

#include <iostream>
#include <iomanip>
#include <vector>
#include <thread>
#include <string>
#include <cstring>
#include <unistd.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "util.hpp"

using namespace std;

void hex_dump(const string& label, const uint8_t* data, size_t len) {
    // Skip info protocol messages (type 0x01), only show database protocol (type 0x03)
    if (len >= 2 && data[0] == 0x02 && data[1] == 0x01) {
        return; // Skip info messages
    }

    cout << "\n========== " << label << " (" << len << " bytes) ==========\n";
    for (size_t i = 0; i < len; i++) {
        cout << hex << setw(2) << setfill('0') << (int)data[i] << " ";
        if ((i + 1) % 16 == 0) cout << "\n";
    }
    if (len % 16 != 0) cout << "\n";
    cout << dec;
	cout << to_json((as_msg*)(data + 8)).dump() << "\n";
    cout.flush();

}

void proxy_connection(int client_fd, const string& target) {
    // Connect to target server
    int server_fd, one = 1;
    auto ab = addr_resolve(target);

    server_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (server_fd < 0) {
        cerr << "Failed to create socket to server\n";
        close(client_fd);
        return;
    }

    ::setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    if (::connect(server_fd, (sockaddr *)ab.data(), ab.size()) != 0) {
        cerr << "Failed to connect to " << target << "\n";
        close(server_fd);
        close(client_fd);
        return;
    }

    cout << "Proxying connection to " << target << "\n";

    // Set non-blocking mode for select()
    fd_set readfds;
    uint8_t buffer[8192];
    int max_fd = max(client_fd, server_fd);

    while (true) {
        FD_ZERO(&readfds);
        FD_SET(client_fd, &readfds);
        FD_SET(server_fd, &readfds);

        struct timeval timeout = {30, 0}; // 30 second timeout
        int activity = select(max_fd + 1, &readfds, nullptr, nullptr, &timeout);

        if (activity < 0) {
            cerr << "Select error\n";
            break;
        }

        if (activity == 0) {
            // Timeout - just continue
            continue;
        }

        // Data from client to server
        if (FD_ISSET(client_fd, &readfds)) {
            ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);
            if (n <= 0) {
                cout << "Client disconnected\n";
                break;
            }

            hex_dump("CLIENT -> SERVER", buffer, n);

            ssize_t sent = send(server_fd, buffer, n, 0);
            if (sent != n) {
                cerr << "Failed to forward to server\n";
                break;
            }
        }

        // Data from server to client
        if (FD_ISSET(server_fd, &readfds)) {
            ssize_t n = recv(server_fd, buffer, sizeof(buffer), 0);
            if (n <= 0) {
                cout << "Server disconnected\n";
                break;
            }

            hex_dump("SERVER -> CLIENT", buffer, n);

            ssize_t sent = send(client_fd, buffer, n, 0);
            if (sent != n) {
                cerr << "Failed to forward to client\n";
                break;
            }
        }
    }

    close(server_fd);
    close(client_fd);
    cout << "Connection closed\n";
}

int main(int argc, char** argv) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <listen_port> <target_host:port>\n";
        cerr << "Example: " << argv[0] << " 7000 localhost:3000\n";
        return 1;
    }

    int listen_port = atoi(argv[1]);
    string target = argv[2];

    // Create listening socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        cerr << "Failed to create listening socket\n";
        return 1;
    }

    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(listen_port);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "Failed to bind to port " << listen_port << "\n";
        return 1;
    }

    if (listen(listen_fd, 5) < 0) {
        cerr << "Failed to listen\n";
        return 1;
    }

    cout << "TCP Proxy listening on port " << listen_port << "\n";
    cout << "Forwarding to " << target << "\n";
    cout << "Point your Aerospike client at localhost:" << listen_port << "\n\n";

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            cerr << "Failed to accept connection\n";
            continue;
        }

        cout << "\n=================================\n";
        cout << "New connection accepted\n";
        cout << "=================================\n";

        // Handle each connection in a separate thread
        thread proxy_thread(proxy_connection, client_fd, target);
        proxy_thread.detach();
    }

    close(listen_fd);
    return 0;
}
