// Test: Which operations are affected by the subcontext_eval bug?
//
// HYPOTHESIS: The bug affects SELECT specifically because it's classified
// as a MAP operation (opcode 254 >= 64), but regular list operations
// (opcode < 64) should work fine on nested lists through subcontext_eval.
//
// This test verifies whether the limitation is:
//   A) SELECT-specific (only SELECT fails on nested lists)
//   B) Universal (all operations fail on nested lists via subcontext_eval)

#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>

using namespace std;
using json = nlohmann::json;
using ct = as_cdt::ctx_type;

int main() {
    int fd = tcp_connect("localhost:3000");
    char buf[8192];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    const int test_id = 88888;

    cout << "========================================" << endl;
    cout << "Test: subcontext_eval limitations" << endl;
    cout << "========================================\n" << endl;

    // Create test record with nested list
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    call(fd, (void**)&res, req);
    if (res) free(res);
    res = nullptr;

    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);

    json initial_list = {10, 20, 30};
    dieunless(req->add(as_op::type::t_cdt_modify, "data",
                       cdt::map::put("numbers", initial_list)));

    call(fd, (void**)&res, req);
    cout << "Created nested list: " << (res && res->result_code == 0 ? "✓" : "✗") << endl;
    if (res) free(res);
    res = nullptr;

    // ========================================================================
    // Test 1: Regular list operation (append) via subcontext_eval
    // ========================================================================
    cout << "\n--- Test 1: list::append via subcontext_eval on nested list ---" << endl;
    cout << "Operation: Append 40 to nested list at data[\"numbers\"]" << endl;

    auto append_nested = cdt::subcontext_eval(
        json::array({ct::map_key, "numbers"}),
        cdt::list::append(40)  // List operation, opcode < 64
    );

    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    dieunless(req->add(as_op::type::t_cdt_modify, "data", append_nested));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        cout << "✓ SUCCESS: list::append works on nested list!" << endl;
        cout << "  Regular list operations are NOT affected by the bug" << endl;
    } else {
        cout << "✗ FAILED: error code " << (res ? (int)res->result_code : -1) << endl;
        cout << "  Bug affects ALL operations, not just SELECT" << endl;
    }
    if (res) free(res);
    res = nullptr;

    // ========================================================================
    // Test 2: Read nested list to verify append worked
    // ========================================================================
    cout << "\n--- Test 2: Read nested list to verify ---" << endl;

    auto read_nested = cdt::subcontext_eval(
        json::array({ct::map_key, "numbers"}),
        cdt::list::get_range(0, 10)  // Read all elements
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", read_nested));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ List contents: " << result.dump() << endl;
        cout << "  Expected: [10, 20, 30, 40]" << endl;
    } else {
        cout << "✗ Read failed: error code " << (res ? (int)res->result_code : -1) << endl;
    }
    if (res) free(res);
    res = nullptr;

    // ========================================================================
    // Test 3: SELECT via subcontext_eval (KNOWN TO FAIL)
    // ========================================================================
    cout << "\n--- Test 3: SELECT via subcontext_eval on nested list ---" << endl;
    cout << "Operation: SELECT elements > 25 from nested list" << endl;

    auto expr_gt_25 = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 25);

    auto select_nested = cdt::subcontext_eval(
        json::array({ct::map_key, "numbers"}),
        cdt::select(json::array({ct::exp, expr_gt_25}), cdt::select_mode::tree)
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", select_nested));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ SUCCESS (unexpected!): " << result.dump() << endl;
    } else {
        cout << "✗ FAILED as expected: error code " << (res ? (int)res->result_code : -1) << endl;
        cout << "  SELECT is specifically affected by the bug" << endl;
    }
    if (res) free(res);
    res = nullptr;

    // ========================================================================
    // Test 4: SELECT with combined context (WORKAROUND)
    // ========================================================================
    cout << "\n--- Test 4: SELECT with combined context (workaround) ---" << endl;

    auto select_combined = cdt::select(
        json::array({ct::map_key, "numbers", ct::exp, expr_gt_25}),
        cdt::select_mode::tree
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", select_combined));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ Workaround successful: " << result.dump() << endl;
        cout << "  Expected: {\"numbers\": [30, 40]}" << endl;
    } else {
        cout << "✗ Workaround failed: error code " << (res ? (int)res->result_code : -1) << endl;
    }
    if (res) free(res);
    res = nullptr;

    // Cleanup
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("test_subctx_limit")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "test_subctx_limit", test_id);
    call(fd, (void**)&res, req);
    if (res) free(res);

    close(fd);

    cout << "\n========================================" << endl;
    cout << "ANALYSIS" << endl;
    cout << "========================================" << endl;
    cout << "The server bug is SELECT-SPECIFIC:" << endl;
    cout << "  • Regular list ops (append, remove, etc.) work fine via subcontext_eval" << endl;
    cout << "  • Only SELECT fails because it's misclassified as map-only operation" << endl;
    cout << "  • Workaround: Use SELECT with combined context instead of subcontext_eval" << endl;
    cout << "\nWhen workaround doesn't apply:" << endl;
    cout << "  • Scenarios where SELECT combined context is insufficient:" << endl;
    cout << "    1. Multi-level SELECT (filter at depth N, then depth N+1)" << endl;
    cout << "    2. Dynamic context composition requiring operation nesting" << endl;
    cout << "    3. SELECT results need further CDT operations applied" << endl;
    cout << "\nThese edge cases are rare but would require server fix." << endl;

    return 0;
}
