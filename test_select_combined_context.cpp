// Test: Can SELECT work with combined navigation + expression context?
//
// HYPOTHESIS: Instead of wrapping SELECT in subcontext_eval, we can use
// SELECT directly with a context that combines:
//   [map_key, "transactions", exp, expression]
//
// This would mean:
//   1. Navigate to bin["data"]["transactions"]
//   2. Apply expression filter to the list
//
// If this works, it avoids the subcontext_eval codepath that has the bug!

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

    const int test_id = 99999;

    cout << "========================================" << endl;
    cout << "Test: SELECT with combined context" << endl;
    cout << "========================================\n" << endl;

    // Create test record: {"data": {"transactions": [100, 250, 50, 500, 75, 300]}}
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", test_id);
    call(fd, (void**)&res, req);
    if (res) free(res);
	res = nullptr;

    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", test_id);

    json transactions = {100, 250, 50, 500, 75, 300};
    dieunless(req->add(as_op::type::t_cdt_modify, "data",
                       cdt::map::put("transactions", transactions)));

    call(fd, (void**)&res, req);
    cout << "Created record with nested transactions: "
         << (res && res->result_code == 0 ? "✓" : "✗") << endl;
    if (res) free(res);
	res = nullptr;

    // Build expression: VALUE > 200
    auto expr_over_200 = expr::gt(
        expr::var_builtin_int(as_cdt::builtin_var::value),
        200
    );

    // ========================================================================
    // APPROACH 1: subcontext_eval (KNOWN TO FAIL)
    // ========================================================================
    cout << "\n--- Approach 1: subcontext_eval + SELECT (known to fail) ---" << endl;

    auto nested_select_subctx = cdt::subcontext_eval(
        json::array({ct::map_key, "transactions"}),
        cdt::select(
            json::array({ct::exp, expr_over_200}),
            cdt::select_mode::tree
        )
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", test_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", nested_select_subctx));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ SUCCESS (unexpected!): " << result.dump() << endl;
    } else {
        cout << "✗ FAILED as expected: error code " << (res ? (int)res->result_code : -1) << endl;
    }
    if (res) free(res);
	res = nullptr;

    // ========================================================================
    // APPROACH 2: SELECT with combined context (TEST THIS!)
    // ========================================================================
    cout << "\n--- Approach 2: SELECT with combined context [map_key, \"transactions\", exp, expr] ---" << endl;

    // Combine navigation and expression in single context array
    auto combined_context = json::array({
        ct::map_key, "transactions",    // Navigate to data["transactions"]
        ct::exp, expr_over_200           // Apply expression filter
    });

    auto select_combined = cdt::select(combined_context, cdt::select_mode::leaf_list);

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", test_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", select_combined));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ SUCCESS: " << result.dump() << endl;
        cout << "  Expected: [250, 500, 300]" << endl;
        cout << "\n🎉 WORKAROUND FOUND! Combined context avoids the subcontext_eval bug!" << endl;
    } else {
        int error_code = res ? (int)res->result_code : -1;
        cout << "✗ FAILED: error code " << error_code << endl;
        if (error_code == 12) {
            cout << "  Same bug - combined context doesn't help" << endl;
        }
    }
    if (res) free(res);
	res = nullptr;

    // Cleanup
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", test_id);
    call(fd, (void**)&res, req);
    if (res) free(res);
	res = nullptr;

    close(fd);

    cout << "\n========================================" << endl;
    cout << "SUMMARY" << endl;
    cout << "========================================" << endl;
    cout << "Approach 1 (subcontext_eval): Uses opcode 0xFF wrapping 0xFE" << endl;
    cout << "              - Triggers type checking bug" << endl;
    cout << "              - Expects MAP, finds LIST → error 12" << endl;
    cout << "\nApproach 2 (combined context): Uses opcode 0xFE directly" << endl;
    cout << "              - Context: [map_key, \"transactions\", exp, expr]" << endl;
    cout << "              - May avoid subcontext_eval codepath" << endl;
    cout << "              - Results shown above" << endl;

    return 0;
}
