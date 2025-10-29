// Simple test to verify CDT SELECT works on nested structures
// This is a baseline before building the depth probe
//
// KNOWN LIMITATION: Tests 2-4 fail due to server bug
// - SELECT (opcode 0xFE) is incorrectly classified as a map-only operation
// - When used within subcontext_eval on lists, server returns error 12
// - Warning: "subcontext type 7 != expected type 8 (map)"
// - Server code: aerospike-server/as/src/base/cdt.c:3042-3054

#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>
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

int main(int argc, char **argv, char **envp)
{
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "select_nested_test"}
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
    cout << "CDT SELECT NESTED STRUCTURE TEST" << endl;
    cout << "========================================================" << endl;
    cout << "Connecting to " << p["ASDB"] << " (ns=" << p["NS"] << ", set=" << p["SN"] << ")\n" << endl;

    int fd = tcp_connect(p["ASDB"]);
    const int test_rec = 9999;
    json test_list = {5, 15, 8, 20, 3, 25};
    auto expr_gt_10 = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 10);

    // Test 1: SELECT on top-level list (baseline)
    cout << "=== Test 1: SELECT on top-level list (baseline) ===" << endl;
    reset_test_record(fd, test_rec);

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        visit(req, test_rec, AS_MSG_FLAG_WRITE);
        dieunless(req->add(as_op::type::t_cdt_modify, "numbers", cdt::list::append_items(test_list)));
        call(fd, (void**)&res, req);
        cout << "Created list, result code: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
    }

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        auto select_op = cdt::select(
            json::array({as_cdt::ctx_type::exp, expr_gt_10}),
            cdt::select_mode::tree
        );

        visit(req, test_rec, AS_MSG_FLAG_READ);
        dieunless(req->add(as_op::type::t_cdt_read, "numbers", select_op));
        call(fd, (void**)&res, req);

        if (res && res->result_code == 0) {
            auto op = res->ops_begin();
            auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "SELECT result: " << result.dump() << endl;
            cout << "Expected: [15,20,25]" << endl;
            cout << "Status: " << (result == json::array({15, 20, 25}) ? "PASS" : "FAIL") << "\n" << endl;
        } else {
            cout << "ERROR: result code " << (res ? (int)res->result_code : -1) << "\n" << endl;
        }
        if (res) free(res);
    }

    // Test 2: SELECT on nested list (1 level deep: map["data"] -> list)
    // NOTE: This test reveals a server limitation - SELECT (opcode 0xFE/254) is
    // incorrectly classified as a map-only operation when used within subcontext_eval.
    // The server's IS_CDT_LIST_OP macro treats SELECT as a map op (254 >= 64), causing
    // type mismatch errors when applying SELECT to lists within nested contexts.
    // Server location: cdt.c:3042-3054
    // Expected result: FAIL with error code 12 (AS_ERR_INCOMPATIBLE_TYPE)
    cout << "=== Test 2: SELECT on 1-level nested list ===" << endl;
    reset_test_record(fd, test_rec);

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        visit(req, test_rec, AS_MSG_FLAG_WRITE);
        dieunless(req->add(as_op::type::t_cdt_modify, "nested", cdt::map::put("data", test_list)));
        call(fd, (void**)&res, req);
        cout << "Created nested map, result code: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
    }

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        auto nested_select = cdt::subcontext_eval(
            json::array({ct::map_key, "data"}),
            cdt::select(
                json::array({as_cdt::ctx_type::exp, expr_gt_10}),
                cdt::select_mode::tree
            )
        );

        visit(req, test_rec, AS_MSG_FLAG_READ);
        dieunless(req->add(as_op::type::t_cdt_read, "nested", nested_select));
        call(fd, (void**)&res, req);

        if (res && res->result_code == 0) {
            auto op = res->ops_begin();
            auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "SELECT result: " << result.dump() << endl;
            cout << "Expected: [15,20,25]" << endl;
            cout << "Status: " << (result == json::array({15, 20, 25}) ? "PASS" : "FAIL") << "\n" << endl;
        } else {
            cout << "ERROR: result code " << (res ? (int)res->result_code : -1) << "\n" << endl;
        }
        if (res) free(res);
    }

    // Test 3: SELECT on 2-level nested list (map["level0"][0] -> list)
    // NOTE: Same server limitation as Test 2 - SELECT classified as map-only operation.
    // Expected result: FAIL with error code 12 (AS_ERR_INCOMPATIBLE_TYPE)
    cout << "=== Test 3: SELECT on 2-level nested list ===" << endl;
    reset_test_record(fd, test_rec);

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        visit(req, test_rec, AS_MSG_FLAG_WRITE);
        dieunless(req->add(as_op::type::t_cdt_modify, "nested2",
            cdt::map::put("level0", json::array({test_list}))));
        call(fd, (void**)&res, req);
        cout << "Created 2-level nested structure, result code: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
    }

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        auto nested_select_2 = cdt::subcontext_eval(
            json::array({ct::map_key, "level0", ct::list_index, 0}),
            cdt::select(
                json::array({as_cdt::ctx_type::exp, expr_gt_10}),
                cdt::select_mode::tree
            )
        );

        visit(req, test_rec, AS_MSG_FLAG_READ);
        dieunless(req->add(as_op::type::t_cdt_read, "nested2", nested_select_2));
        call(fd, (void**)&res, req);

        if (res && res->result_code == 0) {
            auto op = res->ops_begin();
            auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "SELECT result: " << result.dump() << endl;
            cout << "Expected: [15,20,25]" << endl;
            cout << "Status: " << (result == json::array({15, 20, 25}) ? "PASS" : "FAIL") << "\n" << endl;
        } else {
            cout << "ERROR: result code " << (res ? (int)res->result_code : -1) << "\n" << endl;
        }
        if (res) free(res);
    }

    // Test 4: SELECT on 3-level nested list (map["a"][0]["b"] -> list)
    // NOTE: Same server limitation as Tests 2-3 - SELECT classified as map-only operation.
    // Expected result: FAIL with error code 12 (AS_ERR_INCOMPATIBLE_TYPE)
    cout << "=== Test 4: SELECT on 3-level nested list ===" << endl;
    reset_test_record(fd, test_rec);

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        visit(req, test_rec, AS_MSG_FLAG_WRITE);
        dieunless(req->add(as_op::type::t_cdt_modify, "nested3",
            cdt::map::put("a", json::array({json::object({{"b", test_list}})}))));
        call(fd, (void**)&res, req);
        cout << "Created 3-level nested structure, result code: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
    }

    {
        char buf[8192];
        as_msg *req = (as_msg *)(buf + 4096);
        as_msg *res = nullptr;

        auto nested_select_3 = cdt::subcontext_eval(
            json::array({ct::map_key, "a", ct::list_index, 0, ct::map_key, "b"}),
            cdt::select(
                json::array({as_cdt::ctx_type::exp, expr_gt_10}),
                cdt::select_mode::tree
            )
        );

        visit(req, test_rec, AS_MSG_FLAG_READ);
        dieunless(req->add(as_op::type::t_cdt_read, "nested3", nested_select_3));
        call(fd, (void**)&res, req);

        if (res && res->result_code == 0) {
            auto op = res->ops_begin();
            auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "SELECT result: " << result.dump() << endl;
            cout << "Expected: [15,20,25]" << endl;
            cout << "Status: " << (result == json::array({15, 20, 25}) ? "PASS" : "FAIL") << "\n" << endl;
        } else {
            cout << "ERROR: result code " << (res ? (int)res->result_code : -1) << "\n" << endl;
        }
        if (res) free(res);
    }

    cout << "========================================================" << endl;
    cout << "SUMMARY" << endl;
    cout << "========================================================" << endl;
    cout << "Test 1 (top-level): Should PASS - SELECT works on top-level lists" << endl;
    cout << "Tests 2-4 (nested): KNOWN SERVER LIMITATION - SELECT on nested lists fails" << endl;
    cout << "  - Server incorrectly classifies SELECT (0xFE) as map-only operation" << endl;
    cout << "  - IS_CDT_LIST_OP(254) = false, so SELECT requires MAP type" << endl;
    cout << "  - Fails with error code 12: AS_ERR_INCOMPATIBLE_TYPE" << endl;
    cout << "  - Server warnings: 'subcontext type 7 != expected type 8 (map)'" << endl;
    cout << "  - Type 7=LIST, Type 8=MAP (msgpack type constants)" << endl;
    cout << "\nServer bug location: aerospike-server/as/src/base/cdt.c:3042-3054" << endl;
    cout << "Fix needed: Add special handling for SELECT in cdt_process_state_context_eval()\n" << endl;

    reset_test_record(fd, test_rec);
    close(fd);
    return 0;
}
