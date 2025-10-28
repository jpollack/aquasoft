// Test program to probe maximum CDT nesting depth
// Based on working nested examples from cdt_test.cpp

#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <cstring>
#include <unistd.h>

using json = nlohmann::json;
using namespace std;
using ct = as_cdt::ctx_type;

unordered_map<string,string> p;

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

// Delete test record
void reset_test_record(int fd, int record_id) {
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
}

// Test a specific nesting depth
// Returns true if successful, false if failed
bool test_nesting_depth(int fd, int depth, int record_id) {
    char buf[1024*1024];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    reset_test_record(fd, record_id);

    // Build nested structure: map -> list -> map -> list -> ...
    // Pattern: map["level0"] -> list[0] -> map["level1"] -> list[0] -> ... -> value

    // First, create the nested structure by setting a deeply nested value
    // Build context array for writing to depth
    json write_context = json::array();

    for (int i = 0; i < depth; i++) {
        if (i % 2 == 0) {
            // Even level: map key context with auto-create flag
            write_context.push_back(static_cast<int>(ct::map_key) | static_cast<int>(as_cdt::ctx_create::map_k_ordered));
            write_context.push_back("level" + to_string(i));
        } else {
            // Odd level: list index context with auto-create flag
            write_context.push_back(static_cast<int>(ct::list_index) | static_cast<int>(as_cdt::ctx_create::list_unordered));
            write_context.push_back(0);
        }
    }

    // Now add the final operation - either set a value in a map or append to a list
    json final_operation;
    if (depth % 2 == 0) {
        // Last level is a map, put a key-value pair
        final_operation = cdt::map::put("final", 42);
    } else {
        // Last level is a list, append a value
        final_operation = cdt::list::append(42);
    }

    // Build the full operation with subcontext
    auto operation = cdt::subcontext_eval(write_context, final_operation);

    // Execute write
    visit(req, record_id, AS_MSG_FLAG_WRITE);
    dieunless(req->add(as_op::type::t_cdt_modify, "nested", operation));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    if (res == nullptr) {
        cout << "Depth " << setw(3) << depth << " | FAILED | Server connection lost" << endl;
        return false;
    }

    bool write_success = (res->result_code == 0);
    uint8_t write_error = res->result_code;
    uint32_t write_dur = dur;

    free(res);
    res = nullptr;

    if (!write_success) {
        cout << "Depth " << setw(3) << depth
             << " | FAILED | Write error code " << (int)write_error
             << " | " << write_dur << " us" << endl;
        return false;
    }

    // Now try to read the value back
    // Build read context (same as write but without create flags)
    json read_context = json::array();

    for (int i = 0; i < depth; i++) {
        if (i % 2 == 0) {
            read_context.push_back(static_cast<int>(ct::map_key));
            read_context.push_back("level" + to_string(i));
        } else {
            read_context.push_back(static_cast<int>(ct::list_index));
            read_context.push_back(0);
        }
    }

    // Read operation
    json read_operation;
    if (depth % 2 == 0) {
        // Last level is a map
        read_operation = cdt::map::get_by_key("final");
    } else {
        // Last level is a list
        read_operation = cdt::list::get(0);
    }

    auto read_op = cdt::subcontext_eval(read_context, read_operation);

    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_cdt_read, "nested", read_op));

    dur = 0;
    call(fd, (void**)&res, req, &dur);

    if (res == nullptr) {
        cout << "Depth " << setw(3) << depth << " | FAILED | Server connection lost on read" << endl;
        return false;
    }

    bool read_success = (res->result_code == 0);
    uint8_t read_error = res->result_code;
    uint32_t read_dur = dur;

    // Verify the value
    bool value_correct = false;
    if (read_success) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0 && op->data_type == as_particle::type::t_integer) {
            int64_t actual = be64toh(*(int64_t*)op->data());
            value_correct = (actual == 42);
        }
    }

    free(res);

    if (read_success && value_correct) {
        cout << "Depth " << setw(3) << depth
             << " | SUCCESS | Write: " << write_dur << " us, Read: " << read_dur << " us" << endl;
        return true;
    } else if (!read_success) {
        cout << "Depth " << setw(3) << depth
             << " | FAILED | Read error code " << (int)read_error
             << " | Write: " << write_dur << " us" << endl;
        return false;
    } else {
        cout << "Depth " << setw(3) << depth
             << " | FAILED | Value mismatch"
             << " | Write: " << write_dur << " us, Read: " << read_dur << " us" << endl;
        return false;
    }
}

int main(int argc, char **argv, char **envp)
{
    // Setup defaults
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "nest_depth_test"}
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

    cout << "========================================================" << endl;
    cout << "CDT NESTING DEPTH PROBE TEST" << endl;
    cout << "========================================================" << endl;
    cout << "Connecting to " << p["ASDB"] << " (ns=" << p["NS"] << ", set=" << p["SN"] << ")" << endl;
    cout << "\nPattern: map -> list -> map -> list -> ... -> value" << endl;
    cout << "Testing incremental nesting depths to find maximum...\n" << endl;

    int fd = tcp_connect(p["ASDB"]);

    const int BASE_RECORD_ID = 8000;
    const int MAX_DEPTH_TO_TEST = 1000;  // Upper limit for testing

    int max_successful_depth = 0;
    int consecutive_failures = 0;

    // Cleanup - delete test records
    cout << "\nCleaning up test records..." << endl;
    for (int depth = 1; depth <= max_successful_depth && depth <= MAX_DEPTH_TO_TEST; depth++) {
        reset_test_record(fd, BASE_RECORD_ID + depth);
    }

    for (int depth = 1; depth <= MAX_DEPTH_TO_TEST; depth++) {
        bool success = test_nesting_depth(fd, depth, BASE_RECORD_ID + depth);

        if (success) {
            max_successful_depth = depth;
            consecutive_failures = 0;
        } else {
            consecutive_failures++;

            // If we get 3 consecutive failures, stop testing
            if (consecutive_failures >= 3) {
                cout << "\n*** Stopping after " << consecutive_failures << " consecutive failures ***" << endl;
                break;
            }
        }
    }

    cout << "\n========================================================" << endl;
    cout << "RESULTS" << endl;
    cout << "========================================================" << endl;
    cout << "Maximum successful nesting depth: " << max_successful_depth << endl;

    if (max_successful_depth >= MAX_DEPTH_TO_TEST) {
        cout << "\nNote: Reached test limit (" << MAX_DEPTH_TO_TEST << "). Actual limit may be higher." << endl;
    }


    close(fd);

    return (max_successful_depth > 0) ? 0 : 1;
}
