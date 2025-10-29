// SELECT Depth Probe - Test SELECT with increasingly deep navigation contexts
// Tests: select([map_key, "a", list_index, 0, ..., exp, filter], mode)

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

void reset_test_record(int fd, int record_id) {
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    if (res) free(res);
}

// Test SELECT at a specific navigation depth
// Returns true if successful, false if failed
bool test_select_depth(int fd, int depth, int record_id) {
    char buf[1024*1024];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    reset_test_record(fd, record_id);

    // Create nested structure: map -> list -> map -> list -> ... -> target list
    // Pattern: {"level0": [{"level1": [... {"target": [5, 15, 8, 20, 3, 25]} ...]}]}

    // Build the nested structure from inside out
    json target_list = {5, 15, 8, 20, 3, 25};
    json structure = target_list;

    // Wrap it in alternating map/list layers
    for (int i = depth - 1; i >= 0; i--) {
        if (i % 2 == 0) {
            // Even level: wrap in map with key "levelN"
            structure = json::object({{"level" + to_string(i), structure}});
        } else {
            // Odd level: wrap in list
            structure = json::array({structure});
        }
    }

    // The bin key to write to
    string bin_key = (depth == 0) ? "numbers" : "nested";

    // DEBUG: Print structure for first few depths
    if (depth <= 3) {
        cout << "DEBUG depth=" << depth << " structure: " << structure.dump() << endl;
    }

    // Write the structure directly as a bin value (not using map::put)
    visit(req, record_id, AS_MSG_FLAG_WRITE);
    if (depth == 0) {
        // For depth 0, write list directly
        dieunless(req->add(as_op::type::t_cdt_modify, bin_key, cdt::list::append_items(structure)));
    } else {
        // For depth > 0, write the nested map structure
        dieunless(req->add(as_op::type::t_cdt_modify, bin_key, cdt::map::put("level0", structure)));
    }

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    if (!res || res->result_code != 0) {
        cout << "Depth " << setw(3) << depth
             << " | FAILED | Write failed: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
        return false;
    }
    uint32_t write_dur = dur;
    free(res);
    res = nullptr;

    // Build SELECT context with navigation to the target list
    // Context format: [map_key, "level0", list_index, 0, map_key, "level1", ..., exp, filter]
    json select_context = json::array();

    // Add navigation steps
    if (depth == 0) {
        // Direct access: no navigation, just filter
        // Don't add any navigation context
    } else {
        // Navigate through nested structure
        select_context.push_back((int)ct::map_key);
        select_context.push_back("level0");

        for (int i = 1; i < depth; i++) {
            if (i % 2 == 1) {
                // After map, go into list at index 0
                select_context.push_back((int)ct::list_index);
                select_context.push_back(0);
            } else {
                // After list, go into map with next level key
                select_context.push_back((int)ct::map_key);
                select_context.push_back("level" + to_string(i));
            }
        }

        // Final navigation to target
        if (depth % 2 == 1) {
            // Last level is odd, so we're in a list - go to index 0
            select_context.push_back((int)ct::list_index);
            select_context.push_back(0);
        }
        // At this point we should be at the target list
    }

    // Add expression context for filtering (elements > 10)
    auto filter_expr = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 10);
    select_context.push_back((int)ct::exp);
    select_context.push_back(filter_expr);

    // DEBUG: Print context for first few depths
    if (depth <= 3) {
        cout << "DEBUG depth=" << depth << " context: " << select_context.dump() << endl;
    }

    // Create SELECT operation
    auto select_op = cdt::select(select_context, cdt::select_mode::tree);

    // Execute SELECT
    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_cdt_read, bin_key, select_op));

    dur = 0;
    call(fd, (void**)&res, req, &dur);

    if (!res) {
        cout << "Depth " << setw(3) << depth << " | FAILED | Server connection lost" << endl;
        return false;
    }

    bool read_success = (res->result_code == 0);
    uint8_t read_error = res->result_code;
    uint32_t read_dur = dur;

    // Verify the result
    bool value_correct = false;
    if (read_success) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0) {
            try {
                auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
                // Expected: [15, 20, 25] (elements > 10 from [5, 15, 8, 20, 3, 25])
                json expected = json::array({15, 20, 25});
                value_correct = (result == expected);

                if (!value_correct) {
                    cout << "Depth " << setw(3) << depth
                         << " | FAILED | Result mismatch: " << result.dump()
                         << " (expected [15,20,25])" << endl;
                }
            } catch (const exception& e) {
                cout << "Depth " << setw(3) << depth
                     << " | FAILED | Parse error: " << e.what() << endl;
            }
        }
    }

    free(res);

    if (read_success && value_correct) {
        cout << "Depth " << setw(3) << depth
             << " | SUCCESS | Write: " << write_dur << " us, Read: " << read_dur << " us" << endl;
        return true;
    } else if (!read_success) {
        cout << "Depth " << setw(3) << depth
             << " | FAILED | SELECT error code " << (int)read_error
             << " | Write: " << write_dur << " us" << endl;
        return false;
    } else {
        return false;
    }
}

int main(int argc, char **argv, char **envp)
{
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "select_depth_test"}
    };

    for (auto ep = *envp; ep; ep = *(++envp)) {
        const char* prefix = "JP_INFO_";
        if (!strncmp(prefix, ep, 8)) {
            auto vs = strchr(ep, '=');
            auto ks = string(ep).substr(8, (vs - ep) - 8);
            if (ks.length()) p[ks] = string(vs + 1);
        }
    }

    cout << "========================================================" << endl;
    cout << "CDT SELECT DEPTH PROBE TEST" << endl;
    cout << "========================================================" << endl;
    cout << "Connecting to " << p["ASDB"] << " (ns=" << p["NS"] << ", set=" << p["SN"] << ")" << endl;
    cout << "\nTesting SELECT with embedded navigation contexts" << endl;
    cout << "Pattern: select([map_key, \"level0\", list_index, 0, ..., exp, filter], tree)" << endl;
    cout << "Testing incremental navigation depths...\n" << endl;

    int fd = tcp_connect(p["ASDB"]);

    const int BASE_RECORD_ID = 7000;
    const int MAX_DEPTH_TO_TEST = 50;

    int max_successful_depth = 0;
    int consecutive_failures = 0;

    for (int depth = 0; depth <= MAX_DEPTH_TO_TEST; depth++) {
        bool success = test_select_depth(fd, depth, BASE_RECORD_ID + depth);

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
    cout << "Maximum successful SELECT navigation depth: " << max_successful_depth << endl;

    if (max_successful_depth >= MAX_DEPTH_TO_TEST) {
        cout << "\nNote: Reached test limit (" << MAX_DEPTH_TO_TEST << "). Actual limit may be higher." << endl;
    }

    if (max_successful_depth == 0) {
        cout << "\nWARNING: Only depth 0 (direct access) works!" << endl;
        cout << "This confirms SELECT cannot navigate nested structures." << endl;
    } else if (max_successful_depth < 5) {
        cout << "\nLIMITED: SELECT can navigate " << max_successful_depth << " level(s) deep" << endl;
    } else {
        cout << "\nSUCCESS: SELECT supports navigation up to " << max_successful_depth << " levels!" << endl;
    }

    // Cleanup
    cout << "\nCleaning up test records..." << endl;
    for (int depth = 0; depth <= max_successful_depth && depth <= MAX_DEPTH_TO_TEST; depth++) {
        reset_test_record(fd, BASE_RECORD_ID + depth);
    }

    close(fd);
    return (max_successful_depth >= 0) ? 0 : 1;
}
