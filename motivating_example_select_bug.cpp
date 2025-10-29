// Motivating Example: CDT SELECT on Nested Lists Bug
//
// SCENARIO: You have a user record with transaction history stored as:
//   {
//     "user_id": 12345,
//     "transactions": [100, 250, 50, 500, 75, 300]  // transaction amounts
//   }
//
// GOAL: Find all transactions over $200 using expression-based filtering.
//
// WHY THIS MATTERS: In real applications, data is naturally nested in maps.
// You can't always store everything at the top level. Expression-based
// filtering (SELECT) should work on nested lists just like it works on
// top-level lists.
//
// CURRENT STATUS: This simple, natural operation FAILS due to server bug.

#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>

using namespace std;
using json = nlohmann::json;
using ct = as_cdt::ctx_type;

int main() {
    int fd = tcp_connect("localhost:3000");
    char buf[1024*1024];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    const int user_id = 12345;

    // ========================================================================
    // STEP 1: Create a realistic user record with nested transaction data
    // ========================================================================

    cout << "Creating user record with nested transactions..." << endl;

    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    call(fd, (void**)&res, req);
    if (res) free(res);
	res = nullptr;

    // Create record: {"user_id": 12345, "transactions": [100, 250, 50, 500, 75, 300]}
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);

    // Write user_id
    dieunless(req->add(as_op::type::t_write, "user_id", user_id));

    // Write transactions as nested list inside the "data" map
    // In real apps, you'd have: {"user_id": 12345, "email": "...", "transactions": [...]}
    json transactions = {100, 250, 50, 500, 75, 300};
    dieunless(req->add(as_op::type::t_cdt_modify, "data",
                       cdt::map::put("transactions", transactions)));

    call(fd, (void**)&res, req);
    if (res && res->result_code == 0) {
        cout << "✓ Created user record with transactions: [100, 250, 50, 500, 75, 300]" << endl;
    } else {
        cout << "✗ Failed to create record: " << (res ? (int)res->result_code : -1) << endl;
        if (res) free(res);
		res = nullptr;
        close(fd);
        return 1;
    }
    if (res) free(res);
	res = nullptr;

    // ========================================================================
    // STEP 2: Try to find all transactions over $200 using SELECT
    // ========================================================================

    cout << "\nAttempting to find transactions > 200..." << endl;

    // Build expression: VALUE > 200
    // This is checking each transaction amount against 200
    auto expr_over_200 = expr::gt(
        expr::var_builtin_int(as_cdt::builtin_var::value),  // The transaction amount
        200                                                   // The threshold
    );

    // Navigate to the nested list and apply SELECT
    // Path: bin["data"]["transactions"] -> apply SELECT with expression
    auto find_large_transactions = cdt::subcontext_eval(
        json::array({ct::map_key, "transactions"}),  // Navigate to data["transactions"]
        cdt::select(
            json::array({ct::exp, expr_over_200}),   // Filter: value > 200
            cdt::select_mode::tree                    // Return matching values
        )
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    dieunless(req->add(as_op::type::t_cdt_read, "data", find_large_transactions));

    call(fd, (void**)&res, req);

    cout << "\n========================================" << endl;
    cout << "RESULT" << endl;
    cout << "========================================" << endl;

    if (res && res->result_code == 0) {
        // SUCCESS: This would work if the bug were fixed
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ SUCCESS: Found large transactions: " << result.dump() << endl;
        cout << "  Expected: [250, 500, 300]" << endl;
    } else {
        // FAILURE: This is what actually happens due to the bug
        int error_code = res ? (int)res->result_code : -1;
        cout << "✗ FAILED with error code: " << error_code << endl;

        if (error_code == 12) {
            cout << "\nThis is the SELECT-on-nested-list bug!" << endl;
            cout << "Server incorrectly classifies SELECT as map-only operation." << endl;
            cout << "\nServer log shows:" << endl;
            cout << "  WARNING: subcontext type 7 != expected type 8 (map)" << endl;
            cout << "  Type 7 = LIST (what we have)" << endl;
            cout << "  Type 8 = MAP (what server expects)" << endl;
            cout << "\nWORKAROUND: You would need to either:" << endl;
            cout << "  1. Store transactions at top level (loses data organization)" << endl;
            cout << "  2. Read entire list and filter client-side (inefficient)" << endl;
            cout << "  3. Use positional operations (can't express 'value > 200')" << endl;
            cout << "\nNone of these are acceptable for production applications." << endl;
        }
    }
    if (res) free(res);
	res = nullptr;

    // ========================================================================
    // STEP 3: Show that top-level SELECT works (for comparison)
    // ========================================================================

    cout << "\n========================================" << endl;
    cout << "COMPARISON: Top-level SELECT works" << endl;
    cout << "========================================" << endl;

    // Delete old record
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    call(fd, (void**)&res, req);
    if (res) free(res);
	res = nullptr;

    // Create record with transactions at TOP LEVEL
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    dieunless(req->add(as_op::type::t_cdt_modify, "transactions",
                       cdt::list::append_items(transactions)));
    call(fd, (void**)&res, req);
    if (res) free(res);
	res = nullptr;

    // Apply SELECT directly (no subcontext)
    auto top_level_select = cdt::select(
        json::array({ct::exp, expr_over_200}),
        cdt::select_mode::tree
    );

    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    dieunless(req->add(as_field::type::t_namespace, string("test")));
    dieunless(req->add(as_field::type::t_set, string("users")));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    dieunless(req->add(as_op::type::t_cdt_read, "transactions", top_level_select));

    call(fd, (void**)&res, req);

    if (res && res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << "✓ Top-level SELECT works: " << result.dump() << endl;
        cout << "  But this forces flat data structure!" << endl;
    }
    if (res) free(res);
	res = nullptr;

    // // Cleanup
    // req->clear();
    // req->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE;
    // req->be_transaction_ttl = htobe32(1000);
    // dieunless(req->add(as_field::type::t_namespace, string("test")));
    // dieunless(req->add(as_field::type::t_set, string("users")));
    // add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, "users", user_id);
    // call(fd, (void**)&res, req);
    // if (res) free(res);
	// res = nullptr;

    close(fd);

    cout << "\n========================================" << endl;
    cout << "CONCLUSION" << endl;
    cout << "========================================" << endl;
    cout << "The inability to use SELECT on nested lists forces developers to:" << endl;
    cout << "  • Flatten their data model (loses organization)" << endl;
    cout << "  • Read + filter client-side (wastes bandwidth)" << endl;
    cout << "  • Accept incomplete functionality" << endl;
    cout << "\nThis is a fundamental limitation that affects real-world applications." << endl;

    return 0;
}
