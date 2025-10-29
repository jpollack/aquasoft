#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>
#include <cstring>
#include <unordered_map>
#include <unistd.h>

using namespace std;
using json = nlohmann::json;

unordered_map<string,string> p;

// Helper to strip Aerospike CDT extension markers from t_read responses
// When reading bins with t_read (not t_cdt_read), Aerospike includes msgpack
// extension markers (C7 XX YY) for list/map ordering flags. The deserializer
// doesn't handle these, so we need to strip them manually.
//
// Format for CDT bins from t_read:
// - Simple lists/maps: 9X/8X C7 00 [type] [elements...]
// - Nested with creation flags: 9X/8X C7 00 [type] [flag] [elements...]
//
// Returns cleaned msgpack data in a vector (decrements count, removes extension bytes)
vector<uint8_t> strip_cdt_extension(const uint8_t* data, size_t sz) {
    if (sz < 4) return vector<uint8_t>(data, data + sz);

    uint8_t first = data[0];

    // Check if it's fixarray (0x90-0x9f) or fixmap (0x80-0x8f)
    bool is_fixarray = (first >= 0x90 && first <= 0x9f);
    bool is_fixmap = (first >= 0x80 && first <= 0x8f);

    if (!is_fixarray && !is_fixmap) {
        return vector<uint8_t>(data, data + sz);
    }

    // Check for ext8 marker (C7 LEN TYPE) at position 1
    if (data[1] == 0xC7) {
        uint8_t ext_len = data[2];  // Extension data length
        uint8_t ext_type = data[3]; // Extension type

        // Total bytes to skip: 1(C7) + 1(len) + 1(type) + ext_len(data)
        size_t skip_bytes = 3 + ext_len;

        // Special case: for nested structures with creation flags,
        // there's an extra metadata byte after the 0-length extension
        // Check if next byte looks like a creation flag (0x40, 0x80, 0xC0)
        if (ext_len == 0 && sz > skip_bytes + 1) {
            uint8_t next_byte = data[1 + skip_bytes];
            // Check if it's a creation flag value
            if (next_byte == 0x40 || next_byte == 0x80 || next_byte == 0xC0) {
                skip_bytes++; // Skip the creation flag byte too
            }
        }

        if (sz < skip_bytes + 1) {
            // Not enough data, return as-is
            return vector<uint8_t>(data, data + sz);
        }

        // Decrement the count in the header (lower 4 bits)
        uint8_t new_header = first - 1;

        // Build new buffer: new_header + data after extension
        vector<uint8_t> result;
        result.reserve(sz - skip_bytes);
        result.push_back(new_header);
        result.insert(result.end(), data + 1 + skip_bytes, data + sz);
        return result;
    }

    return vector<uint8_t>(data, data + sz);
}

// Build request message with key
as_msg *visit(as_msg *msg, int ri, int flags)
{
    msg->clear();
    msg->flags = flags;
    msg->be_transaction_ttl = htobe32(1000);
    dieunless(msg->add(as_field::type::t_namespace, p["NS"]));
    dieunless(msg->add(as_field::type::t_set, p["SN"]));
    add_integer_key_digest(msg->add(as_field::type::t_digest_ripe, 20)->data, p["SN"], ri);
    return msg;
}

int main(int argc, char** argv, char** envp)
{
    // Setup defaults
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", ""}
    };

    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp)) {
        const char* prefix = "JP_INFO_";
        if (!strncmp(prefix, ep, 8)) {
            auto vs = strchr(ep, '=');
            auto ks = string(ep).substr(8, (vs - ep) - 8);
            if (ks.length()) p[ks] = string(vs + 1);
        }
    }

    cout << "Connecting to " << p["ASDB"] << " (ns=" << p["NS"] << ", set=" << p["SN"] << ")" << endl;

    // Connect to server
    int fd = tcp_connect(p["ASDB"]);

    // Stack-allocated buffers
    char buf[4096];
    as_msg *req = (as_msg *)(buf + 2048);
    as_msg *res = nullptr;

    int test_key = 9000;

    cout << "\n=== TEST 1: Direct Ordered List Creation ===" << endl;

    // Delete existing record
    visit(req, test_key, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
    res = nullptr;

    // Create ordered list bin and append values in non-sorted order
    visit(req, test_key, AS_MSG_FLAG_WRITE);

    // Set list type to ORDERED (flag = 1)
    auto set_ordered = cdt::list::set_type(as_cdt::list_order::ordered);
    dieunless(req->add(as_op::type::t_cdt_modify, "mylist", set_ordered));

    call(fd, (void**)&res, req);
    cout << "Created ordered list, result code: " << (int)res->result_code << endl;
    free(res);
    res = nullptr;

    // Append values: 50, 10, 30, 20, 40
    int values[] = {50, 10, 30, 20, 40};
    for (int val : values) {
        visit(req, test_key, AS_MSG_FLAG_WRITE);

        auto append_operation = cdt::list::append(val);
        dieunless(req->add(as_op::type::t_cdt_modify, "mylist", append_operation));

        call(fd, (void**)&res, req);
        auto* result_op = res->ops_begin();
        int64_t list_size = be64toh(*(int64_t*)result_op->data());
        cout << "Appended " << val << ", list size: " << list_size << endl;
        free(res);
        res = nullptr;
    }

    // Read entire list
    visit(req, test_key, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_read, "mylist", 0));

    call(fd, (void**)&res, req);
    auto* op = res->ops_begin();

    // Strip CDT extension markers for t_read operations
    auto cleaned = strip_cdt_extension(op->data(), op->data_sz());
    auto result = json::from_msgpack(cleaned.begin(), cleaned.end());

    cout << "\nPhysical list order (get whole bin): " << result.dump() << endl;
    free(res);
    res = nullptr;

    // Get by rank to see logical sorted order
    visit(req, test_key, AS_MSG_FLAG_READ);

    auto get_by_rank_range = cdt::list::get_by_rank_range(0, 5);
    dieunless(req->add(as_op::type::t_cdt_read, "mylist", get_by_rank_range));

    call(fd, (void**)&res, req);
    op = res->ops_begin();
    result = json::from_msgpack(op->data(), op->data() + op->data_sz());

    cout << "Logical sorted order (get_by_rank_range): " << result.dump() << endl;
    free(res);
    res = nullptr;

    cout << "\n=== TEST 2: Ordered List via Context Creation ===" << endl;

    // Delete test record
    test_key = 9001;
    visit(req, test_key, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
    res = nullptr;

    // Create nested structure: map["data"] -> ordered list
    // Use context creation flag 0xc0 (list_ordered) with map_key context
    auto ctx_ordered = json::array({
        static_cast<int>(as_cdt::ctx_type::map_key) | static_cast<int>(as_cdt::ctx_create::list_ordered),
        "data"
    });

    // Append values via context: 50, 10, 30, 20, 40
    for (int val : values) {
        visit(req, test_key, AS_MSG_FLAG_WRITE);

        auto ctx_append = cdt::subcontext_eval(ctx_ordered, cdt::list::append(val));
        dieunless(req->add(as_op::type::t_cdt_modify, "mapbin", ctx_append));

        call(fd, (void**)&res, req);
        auto* ctx_op = res->ops_begin();
        int64_t ctx_size = be64toh(*(int64_t*)ctx_op->data());
        cout << "Context append " << val << ", list size: " << ctx_size << endl;
        free(res);
        res = nullptr;
    }

    // Read entire map bin
    visit(req, test_key, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_read, "mapbin", 0));

    call(fd, (void**)&res, req);
    op = res->ops_begin();

    // Strip CDT extension markers for t_read operations
    cleaned = strip_cdt_extension(op->data(), op->data_sz());
    result = json::from_msgpack(cleaned.begin(), cleaned.end());

    cout << "\nFull map bin: " << result.dump() << endl;
    free(res);
    res = nullptr;

    // Read nested list via context
    visit(req, test_key, AS_MSG_FLAG_READ);

    auto ctx_read = cdt::subcontext_eval(
        json::array({as_cdt::ctx_type::map_key, "data"}),
        cdt::list::get_range(0, 5)
    );
    dieunless(req->add(as_op::type::t_cdt_read, "mapbin", ctx_read));

    call(fd, (void**)&res, req);
    op = res->ops_begin();
    result = json::from_msgpack(op->data(), op->data() + op->data_sz());

    cout << "Physical nested list order: " << result.dump() << endl;
    free(res);
    res = nullptr;

    // Get by rank via context
    visit(req, test_key, AS_MSG_FLAG_READ);

    auto ctx_rank = cdt::subcontext_eval(
        json::array({as_cdt::ctx_type::map_key, "data"}),
        cdt::list::get_by_rank_range(0, 5)
    );
    dieunless(req->add(as_op::type::t_cdt_read, "mapbin", ctx_rank));

    call(fd, (void**)&res, req);
    op = res->ops_begin();
    result = json::from_msgpack(op->data(), op->data() + op->data_sz());

    cout << "Logical sorted order (via context): " << result.dump() << endl;
    free(res);
    res = nullptr;

    cout << "\n=== SUMMARY ===" << endl;
    cout << "Ordered lists maintain INSERTION order physically" << endl;
    cout << "But provide efficient RANK-based access via internal sorted index" << endl;

    close(fd);
    return 0;
}
