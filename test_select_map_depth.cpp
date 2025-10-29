// SELECT Map Depth Probe - Test SELECT navigation through nested maps only (no lists)
// Tests: select([map_key, "a", map_key, "b", ..., exp, filter], mode)

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

// Test SELECT navigation through map-only paths
bool test_select_map_depth(int fd, int depth, int record_id) {
    char buf[1024*1024];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    reset_test_record(fd, record_id);

    // Create nested map structure: {"level0": {"level1": {"level2": ... {"target": [5, 15, 8, 20, 3, 25]}}}}
    json target_list = {5, 15, 8, 20, 3, 25};
    json structure = target_list;

    // Wrap in nested maps
    for (int i = depth - 1; i >= 0; i--) {
        structure = json::object({{"level" + to_string(i), structure}});
    }

    string bin_key = (depth == 0) ? "numbers" : "mapbin";

    // Write the structure - convert JSON to msgpack and write directly
    visit(req, record_id, AS_MSG_FLAG_WRITE);
    if (depth == 0) {
        dieunless(req->add(as_op::type::t_cdt_modify, bin_key, cdt::list::append_items(structure)));
    } else {
        // Write the map structure directly
        auto msgpack = json::to_msgpack(structure);
        auto* op = req->add(as_op::type::t_write, bin_key, msgpack.size());
        dieunless(op);
        op->data_type = as_particle::type::t_map;
        memcpy(op->data(), msgpack.data(), msgpack.size());
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

    // Build SELECT context - navigate through maps only
    json select_context = json::array();

    if (depth > 0) {
        // Navigate: map_key "level0" -> map_key "level1" -> ...
        for (int i = 0; i < depth; i++) {
            select_context.push_back((int)ct::map_key);
            select_context.push_back("level" + to_string(i));
        }
    }

    // Add expression filter
    auto filter_expr = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 10);
    select_context.push_back((int)ct::exp);
    select_context.push_back(filter_expr);

    // Create and execute SELECT
    auto select_op = cdt::select(select_context, cdt::select_mode::tree);

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

    bool value_correct = false;
    if (read_success) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0) {
            try {
                auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
                json expected_filtered = json::array({15, 20, 25});

                // For depth 0, expect exact match
                // For depth > 0, the filtered list should appear somewhere in the tree structure
                if (depth == 0) {
                    value_correct = (result == expected_filtered);
                } else {
                    // Check if the filtered list appears in the result string
                    // This is a simple check - we look for the pattern [15,20,25] anywhere
                    string result_str = result.dump();
                    value_correct = (result_str.find("[15,20,25]") != string::npos);
                }

                if (!value_correct && depth <= 5) {
                    cout << "Depth " << setw(3) << depth
                         << " | Result: " << result.dump() << endl;
                }
            } catch (const exception& e) {
                cout << "Depth " << setw(3) << depth << " | Parse error: " << e.what() << endl;
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
             << " | FAILED | SELECT error code " << (int)read_error << endl;
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
        {"SN", "select_map_depth"}
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
    cout << "CDT SELECT MAP-ONLY DEPTH PROBE" << endl;
    cout << "========================================================" << endl;
    cout << "Connecting to " << p["ASDB"] << " (ns=" << p["NS"] << ", set=" << p["SN"] << ")" << endl;
    cout << "\nTesting SELECT navigation through nested maps (no lists)" << endl;
    cout << "Pattern: select([map_key, \"level0\", map_key, \"level1\", ..., exp, filter], tree)\n" << endl;

    int fd = tcp_connect(p["ASDB"]);

    const int BASE_RECORD_ID = 6000;
    const int MAX_DEPTH_TO_TEST = 50;

    int max_successful_depth = -1;
    int consecutive_failures = 0;

    for (int depth = 0; depth <= MAX_DEPTH_TO_TEST; depth++) {
        bool success = test_select_map_depth(fd, depth, BASE_RECORD_ID + depth);

        if (success) {
            max_successful_depth = depth;
            consecutive_failures = 0;
        } else {
            consecutive_failures++;

            if (consecutive_failures >= 3) {
                cout << "\n*** Stopping after " << consecutive_failures << " consecutive failures ***" << endl;
                break;
            }
        }
    }

    cout << "\n========================================================" << endl;
    cout << "RESULTS" << endl;
    cout << "========================================================" << endl;
    cout << "Maximum successful map navigation depth: " << max_successful_depth << endl;

    if (max_successful_depth < 0) {
        cout << "\nCRITICAL: Even depth 0 failed!" << endl;
    } else if (max_successful_depth == 0) {
        cout << "\nLIMITATION: SELECT only works on direct (non-nested) lists" << endl;
    } else if (max_successful_depth >= MAX_DEPTH_TO_TEST) {
        cout << "\nEXCELLENT: SELECT supports map navigation up to " << max_successful_depth << "+ levels!" << endl;
    } else {
        cout << "\nSELECT supports map navigation up to " << max_successful_depth << " levels deep" << endl;
    }

    // Cleanup
    cout << "\nCleaning up test records..." << endl;
    for (int depth = 0; depth <= max_successful_depth && depth <= MAX_DEPTH_TO_TEST; depth++) {
        reset_test_record(fd, BASE_RECORD_ID + depth);
    }

    close(fd);
    return (max_successful_depth >= 0) ? 0 : 1;
}
