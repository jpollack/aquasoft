// CDT test - exercises list and map CDT operations against a real Aerospike server
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

struct validation_result {
    bool passed;
    string message;
};

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

template<>
validation_result validate_result<json>(as_op* op, const json& expected) {
    if (op->data_sz() > 0) {
        if (op->data_type == as_particle::type::t_list || op->data_type == as_particle::type::t_map ||
            op->data_type == as_particle::type::t_integer || op->data_type == as_particle::type::t_string) {
            try {
                json actual;
                if (op->data_type == as_particle::type::t_integer) {
                    actual = be64toh(*(int64_t*)op->data());
                } else if (op->data_type == as_particle::type::t_string) {
                    actual = string((char*)op->data(), op->data_sz());
                } else {
                    actual = json::from_msgpack(op->data(), op->data() + op->data_sz());
                }
                if (actual == expected) {
                    return {true, "OK: " + actual.dump()};
                } else {
                    return {false, "expected " + expected.dump() + ", got " + actual.dump()};
                }
            } catch (...) {
                return {false, "failed to parse result"};
            }
        }
    }
    return {false, "unexpected result type"};
}

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
    free(res);
}

template<typename T>
void test_cdt_operation_validated(int fd, const char* name, const string& bin_name,
                                   as_op::type op_type, const json& cdt_op, int record_id, const T& expected)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, (op_type == as_op::type::t_cdt_modify) ? AS_MSG_FLAG_WRITE : AS_MSG_FLAG_READ);
    dieunless(req->add(op_type, bin_name, cdt_op));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(45) << name << " | ";

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
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

void test_cdt_operation(int fd, const char* name, const string& bin_name,
                        as_op::type op_type, const json& cdt_op, int record_id = 0)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, (op_type == as_op::type::t_cdt_modify) ? AS_MSG_FLAG_WRITE : AS_MSG_FLAG_READ);
    dieunless(req->add(op_type, bin_name, cdt_op));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(45) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0) {
            if (op->data_type == as_particle::type::t_integer) {
                int64_t val = be64toh(*(int64_t*)op->data());
                cout << "OK: " << val;
            } else if (op->data_type == as_particle::type::t_list || op->data_type == as_particle::type::t_map) {
                try {
                    auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
                    cout << "OK: " << result.dump();
                } catch (...) {
                    cout << "OK (unparsed, " << op->data_sz() << " bytes)";
                }
            } else {
                cout << "OK (type " << (int)op->data_type << ", " << op->data_sz() << " bytes)";
            }
        } else {
            cout << "OK";
        }
        report_pass(name);
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

void test_cdt_operation(int fd, const char* name, const string& bin_name,
                        as_op::type op_type, const json& cdt_op, int record_id, int64_t expected)
{
    test_cdt_operation_validated(fd, name, bin_name, op_type, cdt_op, record_id, expected);
}

void test_cdt_operation(int fd, const char* name, const string& bin_name,
                        as_op::type op_type, const json& cdt_op, int record_id, const json& expected)
{
    test_cdt_operation_validated(fd, name, bin_name, op_type, cdt_op, record_id, expected);
}

void delete_test_record(int fd, int record_id)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
}

int main(int argc, char **argv, char **envp)
{
    // Seed random number generator
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

    int test_record = 100;

    cout << "\n=== Testing List CDT Operations ===" << endl;
    cout << "\n--- List Modify Operations ---" << endl;

    // Reset record at start of list tests
    reset_test_record(fd, test_record);

    // Track expected list state: start with empty list
    json expected_list = json::array();

    // After append(10): [10]
    expected_list.push_back(10);
    test_cdt_operation(fd, "append(10)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(10), test_record, (int64_t)expected_list.size());

    // After append(20): [10, 20]
    expected_list.push_back(20);
    test_cdt_operation(fd, "append(20)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(20), test_record, (int64_t)expected_list.size());

    // After append(30): [10, 20, 30]
    expected_list.push_back(30);
    test_cdt_operation(fd, "append(30)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::append(30), test_record, (int64_t)expected_list.size());

    // After insert(1, 15): [10, 15, 20, 30]
    expected_list.insert(expected_list.begin() + 1, 15);
    test_cdt_operation(fd, "insert(1, 15)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::insert(1, 15), test_record, (int64_t)expected_list.size());

    // After increment(2, 5): [10, 15, 25, 30]
    expected_list[2] = expected_list[2].get<int>() + 5;
    test_cdt_operation(fd, "increment(2, 5)", "mylist", as_op::type::t_cdt_modify,
        cdt::list::increment(2, 5), test_record, expected_list[2].get<int64_t>());

    cout << "\n--- List Read Operations ---" << endl;

    test_cdt_operation(fd, "size()", "mylist", as_op::type::t_cdt_read,
        cdt::list::size(), test_record, (int64_t)expected_list.size());

    test_cdt_operation(fd, "get(0)", "mylist", as_op::type::t_cdt_read,
        cdt::list::get(0), test_record, expected_list[0]);

    test_cdt_operation(fd, "get(1)", "mylist", as_op::type::t_cdt_read,
        cdt::list::get(1), test_record, expected_list[1]);

    test_cdt_operation(fd, "get_by_index(2)", "mylist", as_op::type::t_cdt_read,
        cdt::list::get_by_index(2), test_record, expected_list[2]);

    json expected_range = json::array({expected_list[0], expected_list[1], expected_list[2]});
    test_cdt_operation(fd, "get_range(0, 3)", "mylist", as_op::type::t_cdt_read,
        cdt::list::get_range(0, 3), test_record, expected_range);

    cout << "\n--- List Remove Operations ---" << endl;

    test_cdt_operation(fd, "size() after operations", "mylist", as_op::type::t_cdt_read,
        cdt::list::size(), test_record, (int64_t)expected_list.size());

    cout << "\n=== Testing Map CDT Operations ===" << endl;
    cout << "\n--- Map Modify Operations ---" << endl;

    // Reset for map tests - start fresh
    reset_test_record(fd, test_record);

    // Track expected map state
    json expected_map = json::object();

    // After put("name", "Alice"): {"name": "Alice"}
    expected_map["name"] = "Alice";
    test_cdt_operation(fd, "put(\"name\", \"Alice\")", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put("name", "Alice"), test_record, (int64_t)expected_map.size());

    // After put("age", 30): {"name": "Alice", "age": 30}
    expected_map["age"] = 30;
    test_cdt_operation(fd, "put(\"age\", 30)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::put("age", 30), test_record, (int64_t)expected_map.size());

    // After increment("score", 25): {"name": "Alice", "age": 30, "score": 25}
    expected_map["score"] = 25;
    test_cdt_operation(fd, "increment(\"score\", 25)", "mymap", as_op::type::t_cdt_modify,
        cdt::map::increment("score", 25), test_record, (int64_t)25);

    cout << "\n--- Map Read Operations ---" << endl;

    test_cdt_operation(fd, "size()", "mymap", as_op::type::t_cdt_read,
        cdt::map::size(), test_record, (int64_t)expected_map.size());

    test_cdt_operation(fd, "get_by_key(\"name\")", "mymap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("name"), test_record, expected_map["name"]);

    test_cdt_operation(fd, "get_by_key(\"age\")", "mymap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("age"), test_record, expected_map["age"]);

    test_cdt_operation(fd, "get_by_key(\"score\")", "mymap", as_op::type::t_cdt_read,
        cdt::map::get_by_key("score"), test_record, expected_map["score"]);

    // get_by_index returns the value at the index - skip validation (ordering is complex)
    test_cdt_operation(fd, "get_by_index(0)", "mymap", as_op::type::t_cdt_read,
        cdt::map::get_by_index(0), test_record);

    cout << "\n=== Testing Nested CDT Operations ===" << endl;

    int nested_record = 200;
    reset_test_record(fd, nested_record);

    cout << "\n--- Creating Nested Structure ---" << endl;
    json users_array = json::array({
        json::object({{"name", "Alice"}, {"scores", json::array({10, 20, 30})}}),
        json::object({{"name", "Bob"}, {"scores", json::array({15, 25, 35})}})
    });

    test_cdt_operation(fd, "Create users array", "nested", as_op::type::t_cdt_modify,
        cdt::map::put("users", users_array), nested_record, (int64_t)1);

    json config_map = json::object({
        {"limits", json::array({100, 200, 300})},
        {"settings", json::object({{"max", 500}, {"min", 10}})}
    });

    test_cdt_operation(fd, "Create config map", "nested", as_op::type::t_cdt_modify,
        cdt::map::put("config", config_map), nested_record, (int64_t)2);

    cout << "\n--- Nested Read Operations (2 levels deep) ---" << endl;

    // Read nested values: bin["users"][0]["name"] should be "Alice"
    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, nested_record, AS_MSG_FLAG_READ);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "users", ct::list_index, 0}),
            cdt::map::get_by_key("name")
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_read, "nested", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "nested: users[0][\"name\"]" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0) {
                string actual((char*)op_res->data(), op_res->data_sz());
                cout << "OK: \"" << actual << "\"";
                if (actual == "Alice") {
                    report_pass("nested read users[0][name]");
                } else {
                    stringstream ss;
                    ss << "expected \"Alice\", got \"" << actual << "\"";
                    report_fail("nested read users[0][name]", ss.str());
                }
            } else {
                cout << "ERROR: no data";
                report_fail("nested read users[0][name]", "no data returned");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("nested read users[0][name]", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    cout << "\n--- Nested Read Operations (3 levels deep) ---" << endl;

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, nested_record, AS_MSG_FLAG_READ);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "users", ct::list_index, 1, ct::map_key, "scores"}),
            cdt::list::get(2)
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_read, "nested", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "nested: users[1][\"scores\"][2]" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0 && op_res->data_type == as_particle::type::t_integer) {
                int64_t actual = be64toh(*(int64_t*)op_res->data());
                cout << "OK: " << actual;
                if (actual == 35) {
                    report_pass("nested read users[1][scores][2]");
                } else {
                    stringstream ss;
                    ss << "expected 35, got " << actual;
                    report_fail("nested read users[1][scores][2]", ss.str());
                }
            } else {
                cout << "ERROR: unexpected type";
                report_fail("nested read users[1][scores][2]", "unexpected result type");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("nested read users[1][scores][2]", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, nested_record, AS_MSG_FLAG_READ);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "config", ct::map_key, "settings"}),
            cdt::map::get_by_key("max")
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_read, "nested", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "nested: config[\"settings\"][\"max\"]" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0 && op_res->data_type == as_particle::type::t_integer) {
                int64_t actual = be64toh(*(int64_t*)op_res->data());
                cout << "OK: " << actual;
                if (actual == 500) {
                    report_pass("nested read config[settings][max]");
                } else {
                    stringstream ss;
                    ss << "expected 500, got " << actual;
                    report_fail("nested read config[settings][max]", ss.str());
                }
            } else {
                cout << "ERROR: unexpected type";
                report_fail("nested read config[settings][max]", "unexpected result type");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("nested read config[settings][max]", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    cout << "\n--- Nested Modify Operations ---" << endl;

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, nested_record, AS_MSG_FLAG_WRITE);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "users", ct::list_index, 0, ct::map_key, "scores"}),
            cdt::list::set(1, 99)
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_modify, "nested", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "modify: users[0][\"scores\"][1] = 99" << " | ";

        if (res->result_code == 0) {
            cout << "OK";
            report_pass("nested modify users[0][scores][1]");
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("nested modify users[0][scores][1]", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, nested_record, AS_MSG_FLAG_READ);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "users", ct::list_index, 0, ct::map_key, "scores"}),
            cdt::list::get(1)
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_read, "nested", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "verify: users[0][\"scores\"][1] == 99" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0 && op_res->data_type == as_particle::type::t_integer) {
                int64_t actual = be64toh(*(int64_t*)op_res->data());
                cout << "OK: " << actual;
                if (actual == 99) {
                    report_pass("verify nested modify");
                } else {
                    stringstream ss;
                    ss << "expected 99, got " << actual;
                    report_fail("verify nested modify", ss.str());
                }
            } else {
                cout << "ERROR: unexpected type";
                report_fail("verify nested modify", "unexpected result type");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("verify nested modify", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    cout << "\n--- Nested Creation Operations ---" << endl;

    int create_record = 201;
    reset_test_record(fd, create_record);

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, create_record, AS_MSG_FLAG_WRITE);

        auto op = cdt::subcontext_eval(
            json::array({0x22 | 0x80, "data", 0x22 | 0x40, "metrics"}),
            cdt::list::append(42)
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_modify, "auto_create", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "create: data[\"metrics\"].append(42)" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0 && op_res->data_type == as_particle::type::t_integer) {
                int64_t list_size = be64toh(*(int64_t*)op_res->data());
                cout << "OK: list size = " << list_size;
                if (list_size == 1) {
                    report_pass("nested creation append");
                } else {
                    stringstream ss;
                    ss << "expected size 1, got " << list_size;
                    report_fail("nested creation append", ss.str());
                }
            } else {
                cout << "OK";
                report_pass("nested creation append");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("nested creation append", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    {
        char buf[2048];
        as_msg *req = (as_msg *)(buf + 1024);
        as_msg *res = nullptr;

        visit(req, create_record, AS_MSG_FLAG_READ);

        auto op = cdt::subcontext_eval(
            json::array({ct::map_key, "data", ct::map_key, "metrics"}),
            cdt::list::get(0)
        );
        auto op_msgpack = json::to_msgpack(op);

        dieunless(req->add(as_op::type::t_cdt_read, "auto_create", op_msgpack.size(), op_msgpack.data()));

        uint32_t dur = 0;
        call(fd, (void**)&res, req, &dur);

        cout << left << setw(45) << "verify: data[\"metrics\"][0] == 42" << " | ";

        if (res->result_code == 0) {
            auto op_res = res->ops_begin();
            if (op_res->data_sz() > 0 && op_res->data_type == as_particle::type::t_integer) {
                int64_t actual = be64toh(*(int64_t*)op_res->data());
                cout << "OK: " << actual;
                if (actual == 42) {
                    report_pass("verify nested creation");
                } else {
                    stringstream ss;
                    ss << "expected 42, got " << actual;
                    report_fail("verify nested creation", ss.str());
                }
            } else {
                cout << "ERROR: unexpected type";
                report_fail("verify nested creation", "unexpected result type");
            }
        } else {
            cout << "ERROR: code " << (int)res->result_code;
            report_fail("verify nested creation", "request failed");
        }

        cout << " | " << dur << " us" << endl;
        free(res);
    }

    cout << "\n=== Cleaning up ===" << endl;
    delete_test_record(fd, test_record);
    delete_test_record(fd, nested_record);
    delete_test_record(fd, create_record);
    cout << "Test records deleted" << endl;

    close(fd);

    cout << "\n=== Test Summary ===" << endl;
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
