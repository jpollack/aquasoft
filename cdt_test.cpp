// CDT Comprehensive Test Suite - Complete coverage of all CDT operations
#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <cstring>
#include <unistd.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>

using json = nlohmann::json;
using namespace std;
using ct = as_cdt::ctx_type;
using rt = as_cdt::return_type;

unordered_map<string,string> p;

// Test tracking
int tests_passed = 0;
int tests_failed = 0;

void report_pass(const char* test_name) {
    tests_passed++;
    cout << " | PASS";
}

void report_fail(const char* test_name, const string& details) {
    tests_failed++;
    cout << " | FAIL: " << details;
}

// Validation result structure
struct validation_result {
    bool passed;
    string message;
};

// Validate integer result
template<typename T>
validation_result validate_result(as_op* op, const T& expected);

template<>
validation_result validate_result<int64_t>(as_op* op, const int64_t& expected) {
    if (op->data_sz() > 0 && op->data_type == as_particle::type::t_integer) {
        int64_t actual = be64toh(*(int64_t*)op->data());
        if (actual == expected) {
            return {true, "OK: " + to_string(actual)};
        } else {
            return {false, "expected " + to_string(expected) + ", got " + to_string(actual)};
        }
    }
    return {false, "unexpected result type"};
}

// Validate JSON result (lists, maps, mixed types)
template<>
validation_result validate_result<json>(as_op* op, const json& expected) {
    if (op->data_sz() > 0) {
        try {
            json actual;
            if (op->data_type == as_particle::type::t_integer) {
                actual = be64toh(*(int64_t*)op->data());
            } else if (op->data_type == as_particle::type::t_string) {
                actual = string((char*)op->data(), op->data_sz());
            } else if (op->data_type == as_particle::type::t_list || op->data_type == as_particle::type::t_map) {
                actual = json::from_msgpack(op->data(), op->data() + op->data_sz());
            } else {
                return {false, "unexpected data type " + to_string((int)op->data_type)};
            }

            if (actual == expected) {
                return {true, "OK: " + actual.dump()};
            } else {
                return {false, "expected " + expected.dump() + ", got " + actual.dump()};
            }
        } catch (const exception& e) {
            return {false, string("parse error: ") + e.what()};
        }
    }
    return {false, "no data returned"};
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

// Delete test record
void reset_test_record(int fd, int record_id) {
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
}

// Generic CDT test with validation
template<typename T>
void test_cdt_operation(int fd, const char* name, const string& bin_name,
                        as_op::type op_type, const json& cdt_op, int record_id, const T& expected)
{
    char buf[4096];
    as_msg *req = (as_msg *)(buf + 2048);
    as_msg *res = nullptr;

    visit(req, record_id, (op_type == as_op::type::t_cdt_modify) ? AS_MSG_FLAG_WRITE : AS_MSG_FLAG_READ);
    dieunless(req->add(op_type, bin_name, cdt_op));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(55) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = validate_result(op, expected);
        cout << result.message;
        if (result.passed) {
            report_pass(name);
        } else {
            report_fail(name, result.message);
        }
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed with code " + to_string((int)res->result_code));
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

// Test CDT operation without validation (just check success)
void test_cdt_success(int fd, const char* name, const string& bin_name,
                      as_op::type op_type, const json& cdt_op, int record_id)
{
    char buf[4096];
    as_msg *req = (as_msg *)(buf + 2048);
    as_msg *res = nullptr;

    visit(req, record_id, (op_type == as_op::type::t_cdt_modify) ? AS_MSG_FLAG_WRITE : AS_MSG_FLAG_READ);
    dieunless(req->add(op_type, bin_name, cdt_op));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(55) << name << " | ";

    if (res->result_code == 0) {
        cout << "OK";
        report_pass(name);
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

int main(int argc, char **argv, char **envp)
{
    srand(time(nullptr));

    // Setup defaults
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "cdt_test"}
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
    int fd = tcp_connect(p["ASDB"]);

    cout << "\n" << string(120, '=') << endl;
    cout << "CDT COMPREHENSIVE TEST SUITE - Complete Operation Coverage" << endl;
    cout << string(120, '=') << endl;

    // ========================================================================
    // PART 1: LIST OPERATIONS - Complete Coverage (27 operations)
    // ========================================================================

    cout << "\n" << string(120, '=') << endl;
    cout << "PART 1: LIST OPERATIONS" << endl;
    cout << string(120, '=') << endl;

    int list_rec = 100;

    // --- List Modify Operations ---
    cout << "\n--- List Modify Operations: Basic Append/Insert ---" << endl;
    reset_test_record(fd, list_rec);

    // Build list: []
    json expected_list = json::array();

    // Append: [] -> [10]
    expected_list.push_back(10);
    test_cdt_operation(fd, "list::append(10)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(10), list_rec, (int64_t)expected_list.size());

    // Append: [10] -> [10, 20]
    expected_list.push_back(20);
    test_cdt_operation(fd, "list::append(20)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(20), list_rec, (int64_t)expected_list.size());

    // Append: [10, 20] -> [10, 20, 30]
    expected_list.push_back(30);
    test_cdt_operation(fd, "list::append(30)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(30), list_rec, (int64_t)expected_list.size());

    // Insert at index: [10, 20, 30] -> [10, 15, 20, 30]
    expected_list.insert(expected_list.begin() + 1, 15);
    test_cdt_operation(fd, "list::insert(1, 15)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::insert(1, 15), list_rec, (int64_t)expected_list.size());

    // Append items: [10, 15, 20, 30] -> [10, 15, 20, 30, 40, 50]
    json items = json::array({40, 50});
    for (auto& item : items) {
        expected_list.push_back(item);
    }
    test_cdt_operation(fd, "list::append_items([40, 50])", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append_items(items), list_rec, (int64_t)expected_list.size());

    cout << "\n--- List Modify Operations: Set, Increment, Trim ---" << endl;

    // Set element: [10, 15, 20, 30, 40, 50] -> [10, 15, 25, 30, 40, 50]
    expected_list[2] = 25;
    test_cdt_success(fd, "list::set(2, 25)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::set(2, 25), list_rec);

    // Increment: [10, 15, 25, 30, 40, 50] -> [10, 15, 25, 30, 45, 50]
    int inc_result = expected_list[4].get<int>() + 5;
    expected_list[4] = inc_result;
    test_cdt_operation(fd, "list::increment(4, 5)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::increment(4, 5), list_rec, (int64_t)inc_result);

    // Trim (keep index 1-4): [10, 15, 25, 30, 45, 50] -> [15, 25, 30, 45]
    json trimmed = json::array({expected_list[1], expected_list[2], expected_list[3], expected_list[4]});
    expected_list = trimmed;
    test_cdt_operation(fd, "list::trim(1, 4)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::trim(1, 4), list_rec, (int64_t)expected_list.size());

    cout << "\n--- List Modify Operations: Pop, Remove, Sort, Clear ---" << endl;

    // Pop element: [15, 25, 30, 45] -> [15, 25, 45]
    int popped = expected_list[2].get<int>();
    expected_list.erase(expected_list.begin() + 2);
    test_cdt_operation(fd, "list::pop(2)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::pop(2), list_rec, (int64_t)popped);

    // Pop range: [15, 25, 45] -> [15]
    json popped_range = json::array({expected_list[1], expected_list[2]});
    expected_list.erase(expected_list.begin() + 1, expected_list.end());
    test_cdt_operation(fd, "list::pop_range(1, 2)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::pop_range(1, 2), list_rec, popped_range);

    // Rebuild for sort test
    reset_test_record(fd, list_rec);
    json unsorted = json::array({50, 10, 30, 20, 40});
    test_cdt_success(fd, "list::append_items([50,10,30,20,40])", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append_items(unsorted), list_rec);

    // Sort
    test_cdt_success(fd, "list::sort()", "mylist", as_op::type::t_cdt_modify,
        cdt::list::sort(), list_rec);

    // Verify sorted order
    json sorted_expected = json::array({10, 20, 30, 40, 50});
    test_cdt_operation(fd, "list::get_range(0, 5) [after sort]", "mylist", as_op::type::t_cdt_read,
        cdt::list::get_range(0, 5), list_rec, sorted_expected);

    // Clear
    test_cdt_success(fd, "list::clear()", "mylist", as_op::type::t_cdt_modify,
        cdt::list::clear(), list_rec);

    test_cdt_operation(fd, "list::size() [after clear]", "mylist", as_op::type::t_cdt_read,
        cdt::list::size(), list_rec, (int64_t)0);

    // --- List Read Operations ---
    cout << "\n--- List Read Operations: Size, Get, Get Range ---" << endl;
    reset_test_record(fd, list_rec);
    json read_list = json::array({100, 200, 300, 400, 500});
    test_cdt_success(fd, "Setup: list::append_items([100,200,300,400,500])", "readlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(read_list), list_rec);

    test_cdt_operation(fd, "list::size()", "readlist", as_op::type::t_cdt_read,
        cdt::list::size(), list_rec, (int64_t)5);

    test_cdt_operation(fd, "list::get(0)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get(0), list_rec, (int64_t)100);

    test_cdt_operation(fd, "list::get(2)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get(2), list_rec, (int64_t)300);

    test_cdt_operation(fd, "list::get_range(1, 3)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_range(1, 3), list_rec, json::array({200, 300, 400}));

    cout << "\n--- List Get By Index/Value/Rank Operations ---" << endl;

    test_cdt_operation(fd, "list::get_by_index(3, VALUE)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_index(3, rt::value), list_rec, (int64_t)400);

    test_cdt_operation(fd, "list::get_by_index(3, INDEX)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_index(3, rt::index), list_rec, (int64_t)3);

    test_cdt_operation(fd, "list::get_by_index(3, RANK)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_index(3, rt::rank), list_rec, (int64_t)3);

    test_cdt_operation(fd, "list::get_by_value(300, VALUE)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_value(300, rt::value), list_rec, json::array({300}));

    test_cdt_operation(fd, "list::get_by_value(300, INDEX)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_value(300, rt::index), list_rec, json::array({2}));

    test_cdt_operation(fd, "list::get_by_value(300, COUNT)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_value(300, rt::count), list_rec, (int64_t)1);

    test_cdt_operation(fd, "list::get_by_rank(2, VALUE)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_rank(2, rt::value), list_rec, (int64_t)300);

    test_cdt_operation(fd, "list::get_by_rank(2, INDEX)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_rank(2, rt::index), list_rec, (int64_t)2);

    test_cdt_operation(fd, "list::get_by_index_range(1, 3, VALUE)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_index_range(1, 3, rt::value), list_rec, json::array({200, 300, 400}));

    test_cdt_operation(fd, "list::get_by_rank_range(0, 3, VALUE)", "readlist", as_op::type::t_cdt_read,
        cdt::list::get_by_rank_range(0, 3, rt::value), list_rec, json::array({100, 200, 300}));

    // List with duplicates for value-based operations
    reset_test_record(fd, list_rec);
    json dup_list = json::array({5, 10, 5, 20, 5, 30});
    test_cdt_success(fd, "Setup: list::append_items([5,10,5,20,5,30])", "duplist", as_op::type::t_cdt_modify,
        cdt::list::append_items(dup_list), list_rec);

    test_cdt_operation(fd, "list::get_all_by_value(5, VALUE)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_all_by_value(5, rt::value), list_rec, json::array({5, 5, 5}));

    test_cdt_operation(fd, "list::get_all_by_value(5, INDEX)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_all_by_value(5, rt::index), list_rec, json::array({0, 2, 4}));

    test_cdt_operation(fd, "list::get_all_by_value(5, COUNT)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_all_by_value(5, rt::count), list_rec, (int64_t)3);

    test_cdt_operation(fd, "list::get_all_by_value_list([10,20], VALUE)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_all_by_value_list(json::array({10, 20}), rt::value), list_rec, json::array({10, 20}));

    test_cdt_operation(fd, "list::get_by_value_interval(10, 25, VALUE)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_by_value_interval(10, 25, rt::value), list_rec, json::array({10, 20}));

    test_cdt_operation(fd, "list::get_by_value_rel_rank_range(5, 1, 2, VALUE)", "duplist", as_op::type::t_cdt_read,
        cdt::list::get_by_value_rel_rank_range(5, 1, 2, rt::value), list_rec, json::array({10, 20}));

    cout << "\n--- List Remove By Index/Value/Rank Operations ---" << endl;
    reset_test_record(fd, list_rec);
    json remove_list = json::array({10, 20, 30, 40, 50});
    test_cdt_success(fd, "Setup: list::append_items([10,20,30,40,50])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(remove_list), list_rec);

    test_cdt_operation(fd, "list::remove_by_index(2, VALUE)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_index(2, rt::value), list_rec, (int64_t)30);

    test_cdt_operation(fd, "list::size() [after remove_by_index]", "remlist", as_op::type::t_cdt_read,
        cdt::list::size(), list_rec, (int64_t)4);

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([5,10,5,20,5])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({5, 10, 5, 20, 5})), list_rec);

    test_cdt_operation(fd, "list::remove_by_value(5, COUNT)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_value(5, rt::count), list_rec, (int64_t)1);

    test_cdt_operation(fd, "list::remove_all_by_value(5, COUNT)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_all_by_value(5, rt::count), list_rec, (int64_t)2);

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([10,20,30,40,50])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 20, 30, 40, 50})), list_rec);

    test_cdt_operation(fd, "list::remove_by_index_range(1, 3, VALUE)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_index_range(1, 3, rt::value), list_rec, json::array({20, 30, 40}));

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([10,20,30,40,50])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 20, 30, 40, 50})), list_rec);

    test_cdt_operation(fd, "list::remove_by_rank(0, VALUE)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_rank(0, rt::value), list_rec, (int64_t)10);

    test_cdt_operation(fd, "list::remove_by_rank_range(0, 2, VALUE)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_rank_range(0, 2, rt::value), list_rec, json::array({20, 30}));

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([10,20,30,40,50])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 20, 30, 40, 50})), list_rec);

    test_cdt_operation(fd, "list::remove_all_by_value_list([20,40], COUNT)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_all_by_value_list(json::array({20, 40}), rt::count), list_rec, (int64_t)2);

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([10,20,30,40,50])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 20, 30, 40, 50})), list_rec);

    test_cdt_operation(fd, "list::remove_by_value_interval(20, 45, COUNT)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_value_interval(20, 45, rt::count), list_rec, (int64_t)3);

    reset_test_record(fd, list_rec);
    test_cdt_success(fd, "Setup: list::append_items([10,10,20,30,40])", "remlist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 10, 20, 30, 40})), list_rec);

    test_cdt_operation(fd, "list::remove_by_value_rel_rank_range(10, 1, 2, COUNT)", "remlist", as_op::type::t_cdt_modify,
        cdt::list::remove_by_value_rel_rank_range(10, 1, 2, rt::count), list_rec, (int64_t)2);

    // ========================================================================
    // PART 2: MAP OPERATIONS - Complete Coverage (31 operations)
    // ========================================================================

    cout << "\n" << string(120, '=') << endl;
    cout << "PART 2: MAP OPERATIONS" << endl;
    cout << string(120, '=') << endl;

    int map_rec = 200;

    // --- Map Modify Operations ---
    cout << "\n--- Map Modify Operations: Put, Add, Replace ---" << endl;
    reset_test_record(fd, map_rec);

    json expected_map = json::object();

    // Put: {} -> {"a": 1}
    expected_map["a"] = 1;
    test_cdt_operation(fd, "map::put(\"a\", 1)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put("a", 1), map_rec, (int64_t)expected_map.size());

    // Put: {"a": 1} -> {"a": 1, "b": 2}
    expected_map["b"] = 2;
    test_cdt_operation(fd, "map::put(\"b\", 2)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put("b", 2), map_rec, (int64_t)expected_map.size());

    // Put: {"a": 1, "b": 2} -> {"a": 1, "b": 2, "c": 3}
    expected_map["c"] = 3;
    test_cdt_operation(fd, "map::put(\"c\", 3)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put("c", 3), map_rec, (int64_t)expected_map.size());

    // Put items
    json put_items = json::object({{"d", 4}, {"e", 5}});
    expected_map["d"] = 4;
    expected_map["e"] = 5;
    test_cdt_operation(fd, "map::put_items({\"d\":4,\"e\":5})", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put_items(put_items), map_rec, (int64_t)expected_map.size());

    // Add (only if doesn't exist) - should succeed
    expected_map["f"] = 6;
    test_cdt_operation(fd, "map::add(\"f\", 6) [new key]", "mymap", as_op::type::t_cdt_modify,
        cdt::map::add("f", 6), map_rec, (int64_t)expected_map.size());

    // Replace (only if exists) - should succeed
    expected_map["a"] = 10;
    test_cdt_operation(fd, "map::replace(\"a\", 10) [existing key]", "mymap", as_op::type::t_cdt_modify,
        cdt::map::replace("a", 10), map_rec, (int64_t)expected_map.size());

    cout << "\n--- Map Modify Operations: Increment, Decrement, Clear ---" << endl;

    // Increment
    test_cdt_operation(fd, "map::increment(\"b\", 5)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::increment("b", 5), map_rec, (int64_t)7);

    // Decrement
    test_cdt_operation(fd, "map::decrement(\"c\", 1)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::decrement("c", 1), map_rec, (int64_t)2);

    // Clear
    test_cdt_success(fd, "map::clear()", "mymap", as_op::type::t_cdt_modify,
        cdt::map::clear(), map_rec);

    test_cdt_operation(fd, "map::size() [after clear]", "mymap", as_op::type::t_cdt_read,
        cdt::map::size(), map_rec, (int64_t)0);

    // --- Map Read Operations ---
    cout << "\n--- Map Read Operations: Size, Get By Key ---" << endl;
    reset_test_record(fd, map_rec);
    json read_map = json::object({{"name", "Alice"}, {"age", 30}, {"score", 100}});
    test_cdt_success(fd, "Setup: map::put_items({\"name\":\"Alice\",\"age\":30,\"score\":100})", "readmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(read_map), map_rec);

    test_cdt_operation(fd, "map::size()", "readmap", as_op::type::t_cdt_read,
        cdt::map::size(), map_rec, (int64_t)3);

    test_cdt_operation(fd, "map::get_by_key(\"name\", VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("name", rt::value), map_rec, json("Alice"));

    test_cdt_operation(fd, "map::get_by_key(\"age\", VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("age", rt::value), map_rec, (int64_t)30);

    test_cdt_operation(fd, "map::get_by_key(\"score\", KEY)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("score", rt::key), map_rec, json("score"));

    test_cdt_operation(fd, "map::get_by_key_list([\"name\",\"age\"], VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key_list(json::array({"name", "age"}), rt::value), map_rec, json::array({"Alice", 30}));

    cout << "\n--- Map Get By Index/Value/Rank Operations ---" << endl;

    test_cdt_success(fd, "map::get_by_index(0, VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_index(0, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_index(1, KEY)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_index(1, rt::key), map_rec);

    test_cdt_success(fd, "map::get_by_value(30, KEY)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_value(30, rt::key), map_rec);

    test_cdt_success(fd, "map::get_all_by_value(30, VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_all_by_value(30, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_rank(0, VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_rank(0, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_index_range(0, 2, VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_index_range(0, 2, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_rank_range(0, 2, VALUE)", "readmap", as_op::type::t_cdt_read,
        cdt::map::get_by_rank_range(0, 2, rt::value), map_rec);

    // K_ORDERED map for interval tests
    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "map::set_type(K_ORDERED)", "ordmap", as_op::type::t_cdt_modify,
        cdt::map::set_type(1), map_rec);

    json ord_map = json::object({{"a", 10}, {"b", 20}, {"c", 30}, {"d", 40}, {"e", 50}});
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30,\"d\":40,\"e\":50})", "ordmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(ord_map), map_rec);

    test_cdt_operation(fd, "map::get_by_key_interval(\"b\", \"d\", VALUE)", "ordmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key_interval("b", "d", rt::value), map_rec, json::array({20, 30}));

    test_cdt_success(fd, "map::get_by_value_interval(20, 45, VALUE)", "ordmap", as_op::type::t_cdt_read,
        cdt::map::get_by_value_interval(20, 45, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_key_rel_index_range(\"c\", -1, 3, VALUE)", "ordmap", as_op::type::t_cdt_read,
        cdt::map::get_by_key_rel_index_range("c", -1, 3, rt::value), map_rec);

    test_cdt_success(fd, "map::get_by_value_rel_rank_range(30, -1, 3, VALUE)", "ordmap", as_op::type::t_cdt_read,
        cdt::map::get_by_value_rel_rank_range(30, -1, 3, rt::value), map_rec);

    cout << "\n--- Map Remove Operations ---" << endl;
    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_key(\"b\", VALUE)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_key("b", rt::value), map_rec, (int64_t)20);

    test_cdt_operation(fd, "map::size() [after remove_by_key]", "remmap", as_op::type::t_cdt_read,
        cdt::map::size(), map_rec, (int64_t)2);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30,\"d\":40})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}, {"d", 40}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_key_list([\"a\",\"c\"], COUNT)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_key_list(json::array({"a", "c"}), rt::count), map_rec, (int64_t)2);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_index(0, VALUE)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_index(0, rt::value), map_rec, (int64_t)10);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30,\"d\":40})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}, {"d", 40}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_index_range(1, 2, COUNT)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_index_range(1, 2, rt::count), map_rec, (int64_t)2);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":10,\"c\":20})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 10}, {"c", 20}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_value(10, COUNT)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_value(10, rt::count), map_rec, (int64_t)1);

    test_cdt_operation(fd, "map::remove_all_by_value(10, COUNT)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_all_by_value(10, rt::count), map_rec, (int64_t)1);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_rank(0, VALUE)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_rank(0, rt::value), map_rec, (int64_t)10);

    reset_test_record(fd, map_rec);
    test_cdt_success(fd, "Setup: map::put_items({\"a\":10,\"b\":20,\"c\":30,\"d\":40})", "remmap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object({{"a", 10}, {"b", 20}, {"c", 30}, {"d", 40}})), map_rec);

    test_cdt_operation(fd, "map::remove_by_rank_range(1, 2, COUNT)", "remmap", as_op::type::t_cdt_modify,
        cdt::map::remove_by_rank_range(1, 2, rt::count), map_rec, (int64_t)2);

    // ========================================================================
    // PART 3: NESTED OPERATIONS
    // ========================================================================

    cout << "\n" << string(120, '=') << endl;
    cout << "PART 3: NESTED OPERATIONS" << endl;
    cout << string(120, '=') << endl;

    int nest_rec = 300;

    cout << "\n--- Nested: 2-Level Deep (map[key][index]) ---" << endl;
    reset_test_record(fd, nest_rec);

    json nested_data = json::object({
        {"users", json::array({
            json::object({{"name", "Alice"}, {"age", 30}}),
            json::object({{"name", "Bob"}, {"age", 25}})
        })}
    });

    test_cdt_success(fd, "Setup: Create nested map", "nested", as_op::type::t_cdt_modify,
        cdt::map::put("users", nested_data["users"]), nest_rec);

    test_cdt_operation(fd, "nested: users[0][\"name\"]", "nested", as_op::type::t_cdt_read,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "users", (int)ct::list_index, 0}),
            cdt::map::get_by_key("name")
        ), nest_rec, json("Alice"));

    test_cdt_operation(fd, "nested: users[1][\"age\"]", "nested", as_op::type::t_cdt_read,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "users", (int)ct::list_index, 1}),
            cdt::map::get_by_key("age")
        ), nest_rec, (int64_t)25);

    cout << "\n--- Nested: 3-Level Deep (map[key][index][key]) ---" << endl;
    reset_test_record(fd, nest_rec);

    json deep_data = json::object({
        {"data", json::array({
            json::object({{"metrics", json::array({10, 20, 30})}}),
            json::object({{"metrics", json::array({40, 50, 60})}})
        })}
    });

    test_cdt_success(fd, "Setup: Create 3-level nested map", "deep", as_op::type::t_cdt_modify,
        cdt::map::put("data", deep_data["data"]), nest_rec);

    test_cdt_operation(fd, "nested: data[0][\"metrics\"][2]", "deep", as_op::type::t_cdt_read,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "data", (int)ct::list_index, 0, (int)ct::map_key, "metrics"}),
            cdt::list::get(2)
        ), nest_rec, (int64_t)30);

    test_cdt_operation(fd, "nested: data[1][\"metrics\"][1]", "deep", as_op::type::t_cdt_read,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "data", (int)ct::list_index, 1, (int)ct::map_key, "metrics"}),
            cdt::list::get(1)
        ), nest_rec, (int64_t)50);

    cout << "\n--- Nested: Modify Operations ---" << endl;

    test_cdt_success(fd, "nested: Set data[0][\"metrics\"][1] = 99", "deep", as_op::type::t_cdt_modify,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "data", (int)ct::list_index, 0, (int)ct::map_key, "metrics"}),
            cdt::list::set(1, 99)
        ), nest_rec);

    test_cdt_operation(fd, "nested: Verify data[0][\"metrics\"][1] == 99", "deep", as_op::type::t_cdt_read,
        cdt::subcontext_eval(
            json::array({(int)ct::map_key, "data", (int)ct::list_index, 0, (int)ct::map_key, "metrics"}),
            cdt::list::get(1)
        ), nest_rec, (int64_t)99);

    // ========================================================================
    // PART 4: EDGE CASES
    // ========================================================================

    cout << "\n" << string(120, '=') << endl;
    cout << "PART 4: EDGE CASES & BOUNDARY CONDITIONS" << endl;
    cout << string(120, '=') << endl;

    int edge_rec = 400;

    cout << "\n--- Edge Case: Negative Indices ---" << endl;
    reset_test_record(fd, edge_rec);
    test_cdt_success(fd, "Setup: list [10, 20, 30, 40, 50]", "neglist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array({10, 20, 30, 40, 50})), edge_rec);

    test_cdt_operation(fd, "list::get(-1) [last element]", "neglist", as_op::type::t_cdt_read,
        cdt::list::get(-1), edge_rec, (int64_t)50);

    test_cdt_operation(fd, "list::get(-2) [second-to-last]", "neglist", as_op::type::t_cdt_read,
        cdt::list::get(-2), edge_rec, (int64_t)40);

    cout << "\n--- Edge Case: Empty Containers ---" << endl;
    reset_test_record(fd, edge_rec);
    test_cdt_success(fd, "Create empty list", "emptylist", as_op::type::t_cdt_modify,
        cdt::list::append_items(json::array()), edge_rec);

    test_cdt_operation(fd, "list::size() [empty]", "emptylist", as_op::type::t_cdt_read,
        cdt::list::size(), edge_rec, (int64_t)0);

    reset_test_record(fd, edge_rec);
    test_cdt_success(fd, "Create empty map", "emptymap", as_op::type::t_cdt_modify,
        cdt::map::put_items(json::object()), edge_rec);

    test_cdt_operation(fd, "map::size() [empty]", "emptymap", as_op::type::t_cdt_read,
        cdt::map::size(), edge_rec, (int64_t)0);

    cout << "\n--- Edge Case: Large Collections ---" << endl;
    reset_test_record(fd, edge_rec);
    json large_list = json::array();
    for (int i = 0; i < 100; i++) {
        large_list.push_back(i);
    }
    test_cdt_success(fd, "Create list with 100 elements", "largelist", as_op::type::t_cdt_modify,
        cdt::list::append_items(large_list), edge_rec);

    test_cdt_operation(fd, "list::size() [100 elements]", "largelist", as_op::type::t_cdt_read,
        cdt::list::size(), edge_rec, (int64_t)100);

    test_cdt_operation(fd, "list::get(99) [last of 100]", "largelist", as_op::type::t_cdt_read,
        cdt::list::get(99), edge_rec, (int64_t)99);

    // ========================================================================
    // CLEANUP & SUMMARY
    // ========================================================================

    cout << "\n--- Cleanup ---" << endl;
    reset_test_record(fd, list_rec);
    reset_test_record(fd, map_rec);
    reset_test_record(fd, nest_rec);
    reset_test_record(fd, edge_rec);
    cout << "Test records deleted" << endl;

    close(fd);

    cout << "\n" << string(120, '=') << endl;
    cout << "TEST SUMMARY" << endl;
    cout << string(120, '=') << endl;
    cout << "Passed: " << tests_passed << endl;
    cout << "Failed: " << tests_failed << endl;
    cout << "Total:  " << (tests_passed + tests_failed) << endl;

    if (tests_failed == 0) {
        cout << "\nAll tests PASSED!" << endl;
        return 0;
    } else {
        cout << "\n" << tests_failed << " test(s) FAILED" << endl;
        return 1;
    }
}
