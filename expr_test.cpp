// Expression test - unified testing for read and write expressions
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

as_msg *visit(as_msg *msg, int ri, int flags)
{
    msg->clear();
    msg->flags = flags;
    msg->be_transaction_ttl = htobe32(1000);
    dieunless(msg->add(as_field::type::t_namespace, p["NS"]));
    dieunless(msg->add(as_field::type::t_set, p["SN"]));

    auto key_field = msg->add(as_field::type::t_key, 9);
    key_field->data[0] = (uint8_t)as_particle::type::t_integer;
    *(uint64_t *)(key_field->data + 1) = htobe64(ri);

    add_integer_key_digest(msg->add(as_field::type::t_digest_ripe, 20)->data, p["SN"], ri);
    return msg;
}

struct TestRecord {
    int age;
    int score;
    string status;
    int value;
    int counter;
};

void create_test_record(int fd, int record_id, int age, int score, const string& status)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE);

    auto op = req->add(as_op::type::t_write, "age", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(age);

    op = req->add(as_op::type::t_write, "score", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(score);

    req->add(as_op::type::t_write, "status", status);

    call(fd, (void**)&res, req);
    dieunless(res->result_code == 0);
    free(res);
}

void create_test_record_vc(int fd, int record_id, int value, int counter)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_WRITE);

    auto op = req->add(as_op::type::t_write, "value", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(value);

    op = req->add(as_op::type::t_write, "counter", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(counter);

    call(fd, (void**)&res, req);
    dieunless(res->result_code == 0);
    free(res);
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

TestRecord reset_test_record(int fd, int record_id) {
    delete_test_record(fd, record_id);

    TestRecord rec;
    rec.age = 18 + (rand() % 48);
    rec.score = 20 + (rand() % 81);
    rec.status = (rand() % 2) ? "active" : "inactive";

    create_test_record(fd, record_id, rec.age, rec.score, rec.status);
    return rec;
}

TestRecord reset_test_record_vc(int fd, int record_id) {
    delete_test_record(fd, record_id);

    TestRecord rec;
    rec.value = 10 + (rand() % 91);
    rec.counter = rand() % 51;

    create_test_record_vc(fd, record_id, rec.value, rec.counter);
    return rec;
}

// Test expression - no validation
void test_expression(int fd, const char* name, const json& expr, int record_id = 0)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_exp_read, "result", expr));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(40) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();

        if (op->data_sz() > 0) {
            if (op->data_type == as_particle::type::t_integer) {
                int64_t val = be64toh(*(int64_t*)op->data());
                cout << "OK: " << val;
            } else if (op->data_type == as_particle::type::t_boolean) {
                bool val = *(uint8_t*)op->data();
                cout << "OK: " << (val ? "true" : "false");
            } else if (op->data_type == as_particle::type::t_string) {
                string val((char*)op->data(), op->data_sz());
                cout << "OK: \"" << val << "\"";
            } else {
                cout << "OK (type " << (int)op->data_type << ", " << op->data_sz() << " bytes)";
            }
        } else {
            cout << "OK (no data)";
        }
    } else {
        cout << "ERROR: code " << (int)res->result_code;
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

// Test expression - validate integer
void test_expression(int fd, const char* name, const json& expr, int record_id, int64_t expected)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_exp_read, "result", expr));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(40) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0 && op->data_type == as_particle::type::t_integer) {
            int64_t actual = be64toh(*(int64_t*)op->data());
            cout << "OK: " << actual;
            if (actual == expected) {
                report_pass(name);
            } else {
                stringstream ss;
                ss << "expected " << expected << ", got " << actual;
                report_fail(name, ss.str());
            }
        } else {
            cout << "ERROR: unexpected type";
            report_fail(name, "unexpected result type");
        }
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

// Test expression - validate boolean
void test_expression(int fd, const char* name, const json& expr, int record_id, bool expected)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_exp_read, "result", expr));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(40) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0 && op->data_type == as_particle::type::t_boolean) {
            bool actual = *(uint8_t*)op->data();
            cout << "OK: " << (actual ? "true" : "false");
            if (actual == expected) {
                report_pass(name);
            } else {
                stringstream ss;
                ss << "expected " << (expected ? "true" : "false") << ", got " << (actual ? "true" : "false");
                report_fail(name, ss.str());
            }
        } else {
            cout << "ERROR: unexpected type";
            report_fail(name, "unexpected result type");
        }
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

// Test expression - validate string
void test_expression(int fd, const char* name, const json& expr, int record_id, const string& expected)
{
    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    visit(req, record_id, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_exp_read, "result", expr));

    uint32_t dur = 0;
    call(fd, (void**)&res, req, &dur);

    cout << left << setw(40) << name << " | ";

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        if (op->data_sz() > 0 && op->data_type == as_particle::type::t_string) {
            string actual((char*)op->data(), op->data_sz());
            cout << "OK: \"" << actual << "\"";
            if (actual == expected) {
                report_pass(name);
            } else {
                stringstream ss;
                ss << "expected \"" << expected << "\", got \"" << actual << "\"";
                report_fail(name, ss.str());
            }
        } else {
            cout << "ERROR: unexpected type";
            report_fail(name, "unexpected result type");
        }
    } else {
        cout << "ERROR: code " << (int)res->result_code;
        report_fail(name, "request failed");
    }

    cout << " | " << dur << " us" << endl;
    free(res);
}

// Helper to read a bin value
int read_bin_value(int fd, as_msg* req, as_msg** res, int record_id, const string& bin_name) {
    visit(req, record_id, AS_MSG_FLAG_READ);
    req->add(as_op::type::t_read, bin_name, 0);
    call(fd, (void**)res, req);

    if ((*res)->result_code == 0 && (*res)->n_ops() > 0) {
        auto resp_op = (*res)->ops_begin();
        if (resp_op->data_type == as_particle::type::t_integer && resp_op->data_sz() == 8) {
            return be64toh(*(int64_t*)resp_op->data());
        }
    }
    return -1;
}

// Helper to test expression write operations
bool test_expr_write(int fd, as_msg* req, as_msg** res,
                     const string& test_name, int record_id,
                     const json& filter_expr,
                     const string& write_bin, int write_value,
                     bool expect_success = true) {

    visit(req, record_id, AS_MSG_FLAG_WRITE);

    if (!filter_expr.is_null()) {
        req->add(as_field::type::t_predexp, filter_expr);
    }

    auto op = req->add(as_op::type::t_write, write_bin, 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(write_value);

    uint32_t dur = 0;
    call(fd, (void**)res, req, &dur);

    bool success = ((*res)->result_code == 0);

    cout << left << setw(50) << test_name << " | ";

    if (success == expect_success) {
        cout << "OK";
        report_pass(test_name.c_str());
    } else {
        cout << "UNEXPECTED: code " << (int)(*res)->result_code;
        stringstream ss;
        ss << "expected " << (expect_success ? "success" : "failure")
           << ", got code " << (int)(*res)->result_code;
        report_fail(test_name.c_str(), ss.str());
    }

    cout << " | " << dur << " us" << endl;
    return success == expect_success;
}

// Helper to test expression modify operations with optional validation
bool test_expr_modify(int fd, as_msg* req, as_msg** res,
                      const string& test_name, int record_id,
                      const string& result_bin, const json& modify_expr,
                      int expected_value = -1, bool validate = false) {

    visit(req, record_id, AS_MSG_FLAG_WRITE);
    req->add(as_op::type::t_exp_modify, result_bin, modify_expr);

    uint32_t dur = 0;
    call(fd, (void**)res, req, &dur);

    cout << left << setw(50) << test_name << " | ";

    if ((*res)->result_code != 0) {
        cout << "ERROR: code " << (int)(*res)->result_code;
        report_fail(test_name.c_str(), "request failed");
        cout << " | " << dur << " us" << endl;
        return false;
    }

    if (validate) {
        free(*res);
        *res = nullptr;
        int actual_value = read_bin_value(fd, req, res, record_id, result_bin);

        cout << "OK: " << actual_value;

        if (actual_value == expected_value) {
            report_pass(test_name.c_str());
        } else {
            stringstream ss;
            ss << "expected " << expected_value << ", got " << actual_value;
            report_fail(test_name.c_str(), ss.str());
        }
    } else {
        cout << "OK";
        report_pass(test_name.c_str());
    }

    cout << " | " << dur << " us" << endl;
    return true;
}

int main(int argc, char **argv, char **envp)
{
    srand(time(nullptr));

    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "expr_test"}
    };

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

    // ========================================================================
    // EXPRESSION READ TESTS
    // ========================================================================

    cout << "\n=== Testing Metadata Expressions ===" << endl;
    test_expression(fd, "record_size()", expr::record_size(), 1);
    test_expression(fd, "ttl()", expr::ttl(), 1);
    test_expression(fd, "void_time()", expr::void_time(), 1);
    test_expression(fd, "last_update()", expr::last_update(), 1);
    test_expression(fd, "since_update()", expr::since_update(), 1);
    test_expression(fd, "set_name()", expr::set_name(), 1);
    test_expression(fd, "key_exists()", expr::key_exists(), 1);
    test_expression(fd, "is_tombstone()", expr::is_tombstone(), 1);
    test_expression(fd, "digest_mod(3)", expr::digest_mod(3), 1);

    cout << "\n=== Testing Bin Access ===" << endl;
    auto rec1 = reset_test_record(fd, 1);
    test_expression(fd, "bin(\"age\")", expr::bin("age", as_exp::result_type::t_int), 1, (int64_t)rec1.age);
    test_expression(fd, "bin(\"score\")", expr::bin("score", as_exp::result_type::t_int), 1, (int64_t)rec1.score);
    test_expression(fd, "bin(\"status\")", expr::bin("status", as_exp::result_type::t_str), 1, rec1.status);
    test_expression(fd, "bin_type(\"age\")", expr::bin_type("age"), 1, (int64_t)1);
    test_expression(fd, "bin_type(\"status\")", expr::bin_type("status"), 1, (int64_t)3);
    test_expression(fd, "rec_key(int)", expr::rec_key(as_exp::result_type::t_int), 1, (int64_t)1);

    cout << "\n=== Testing Comparison Expressions ===" << endl;
    rec1 = reset_test_record(fd, 1);
    test_expression(fd, "bin(\"age\") > 21", expr::gt(expr::bin("age", as_exp::result_type::t_int), 21), 1, rec1.age > 21);
    test_expression(fd, "bin(\"age\") >= 25", expr::ge(expr::bin("age", as_exp::result_type::t_int), 25), 1, rec1.age >= 25);
    test_expression(fd, "bin(\"age\") < 30", expr::lt(expr::bin("age", as_exp::result_type::t_int), 30), 1, rec1.age < 30);
    test_expression(fd, "bin(\"score\") == 100", expr::eq(expr::bin("score", as_exp::result_type::t_int), 100), 1, rec1.score == 100);
    test_expression(fd, "bin(\"status\") == \"active\"", expr::eq(expr::bin("status", as_exp::result_type::t_str), "active"), 1, rec1.status == "active");

    cout << "\n=== Testing Logical Expressions ===" << endl;
    rec1 = reset_test_record(fd, 1);
    test_expression(fd, "age > 21 AND score >= 100",
        expr::and_(
            expr::gt(expr::bin("age", as_exp::result_type::t_int), 21),
            expr::ge(expr::bin("score", as_exp::result_type::t_int), 100)
        ), 1, (rec1.age > 21) && (rec1.score >= 100));

    test_expression(fd, "age < 18 OR score > 50",
        expr::or_(
            expr::lt(expr::bin("age", as_exp::result_type::t_int), 18),
            expr::gt(expr::bin("score", as_exp::result_type::t_int), 50)
        ), 1, (rec1.age < 18) || (rec1.score > 50));

    test_expression(fd, "NOT(age < 21)",
        expr::not_(expr::lt(expr::bin("age", as_exp::result_type::t_int), 21)), 1, !(rec1.age < 21));

    cout << "\n=== Testing Arithmetic Expressions ===" << endl;
    rec1 = reset_test_record(fd, 1);
    test_expression(fd, "age + score",
        expr::add(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int)), 1, (int64_t)(rec1.age + rec1.score));

    test_expression(fd, "score - age",
        expr::sub(expr::bin("score", as_exp::result_type::t_int), expr::bin("age", as_exp::result_type::t_int)), 1, (int64_t)(rec1.score - rec1.age));

    test_expression(fd, "age * 2",
        expr::mul(expr::bin("age", as_exp::result_type::t_int), 2), 1, (int64_t)(rec1.age * 2));

    test_expression(fd, "score / 10",
        expr::div(expr::bin("score", as_exp::result_type::t_int), 10), 1, (int64_t)(rec1.score / 10));

    test_expression(fd, "pow(to_float(age), 2.0)",
        expr::pow(expr::to_float(expr::bin("age", as_exp::result_type::t_int)), 2.0), 1);

    test_expression(fd, "mod(score, 10)",
        expr::mod(expr::bin("score", as_exp::result_type::t_int), 10), 1, (int64_t)(rec1.score % 10));

    test_expression(fd, "abs(age - score)",
        expr::abs(expr::sub(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int))), 1, (int64_t)abs(rec1.age - rec1.score));

    test_expression(fd, "floor(to_float(score) / 3.0)",
        expr::floor(expr::div(expr::to_float(expr::bin("score", as_exp::result_type::t_int)), 3.0)), 1);

    test_expression(fd, "ceil(to_float(score) / 3.0)",
        expr::ceil(expr::div(expr::to_float(expr::bin("score", as_exp::result_type::t_int)), 3.0)), 1);

    cout << "\n=== Testing Bitwise Operations ===" << endl;
    rec1 = reset_test_record(fd, 1);
    test_expression(fd, "int_and(score, 15)",
        expr::int_and(expr::bin("score", as_exp::result_type::t_int), 15), 1, (int64_t)(rec1.score & 15));

    test_expression(fd, "int_or(age, 128)",
        expr::int_or(expr::bin("age", as_exp::result_type::t_int), 128), 1, (int64_t)(rec1.age | 128));

    test_expression(fd, "int_xor(score, 255)",
        expr::int_xor(expr::bin("score", as_exp::result_type::t_int), 255), 1, (int64_t)(rec1.score ^ 255));

    test_expression(fd, "int_not(age)",
        expr::int_not(expr::bin("age", as_exp::result_type::t_int)), 1, (int64_t)(~rec1.age));

    test_expression(fd, "int_lshift(age, 2)",
        expr::int_lshift(expr::bin("age", as_exp::result_type::t_int), 2), 1, (int64_t)(rec1.age << 2));

    test_expression(fd, "int_rshift(score, 1)",
        expr::int_rshift(expr::bin("score", as_exp::result_type::t_int), 1), 1, (int64_t)(rec1.score >> 1));

    test_expression(fd, "int_count(score)",
        expr::int_count(expr::bin("score", as_exp::result_type::t_int)), 1, (int64_t)__builtin_popcountll(rec1.score));

    test_expression(fd, "int_lscan(score, true)",
        expr::int_lscan(expr::bin("score", as_exp::result_type::t_int), true), 1, (int64_t)__builtin_clzll(rec1.score));

    test_expression(fd, "int_rscan(score, true)",
        expr::int_rscan(expr::bin("score", as_exp::result_type::t_int), true), 1, (int64_t)(63 - __builtin_ctzll(rec1.score)));

    cout << "\n=== Testing Type Conversion ===" << endl;
    test_expression(fd, "to_int(to_float(score))",
        expr::to_int(expr::to_float(expr::bin("score", as_exp::result_type::t_int))), 1, (int64_t)rec1.score);

    test_expression(fd, "to_float(age)",
        expr::to_float(expr::bin("age", as_exp::result_type::t_int)), 1);

    cout << "\n=== Testing Complex Nested Expressions ===" << endl;
    rec1 = reset_test_record(fd, 1);
    test_expression(fd, "(age + score) > 120",
        expr::gt(
            expr::add(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int)),
            120
        ), 1, (rec1.age + rec1.score) > 120);

    test_expression(fd, "min(age, score)",
        expr::min(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int)), 1, (int64_t)min(rec1.age, rec1.score));

    test_expression(fd, "max(age, score)",
        expr::max(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int)), 1, (int64_t)max(rec1.age, rec1.score));

    cout << "\n=== Testing Conditional Expression ===" << endl;
    test_expression(fd, "if(age >= 25) then 1 else 0",
        expr::cond(
            expr::ge(expr::bin("age", as_exp::result_type::t_int), 25),
            1,
            0
        ), 1, (int64_t)(rec1.age >= 25 ? 1 : 0));

    cout << "\n=== Testing Expressions on Different Records ===" << endl;
    rec1 = reset_test_record(fd, 1);
    auto rec2 = reset_test_record(fd, 2);
    auto rec3 = reset_test_record(fd, 3);

    test_expression(fd, "Record 1: age",
        expr::bin("age", as_exp::result_type::t_int), 1, (int64_t)rec1.age);

    test_expression(fd, "Record 2: age",
        expr::bin("age", as_exp::result_type::t_int), 2, (int64_t)rec2.age);

    test_expression(fd, "Record 3: age",
        expr::bin("age", as_exp::result_type::t_int), 3, (int64_t)rec3.age);

    test_expression(fd, "Record 1: status",
        expr::bin("status", as_exp::result_type::t_str), 1, rec1.status);

    test_expression(fd, "Record 2: status",
        expr::bin("status", as_exp::result_type::t_str), 2, rec2.status);

    test_expression(fd, "Record 3: score < 60",
        expr::lt(expr::bin("score", as_exp::result_type::t_int), 60), 3, rec3.score < 60);

    test_expression(fd, "Record 2: (age < 21 AND score > 70)",
        expr::and_(
            expr::lt(expr::bin("age", as_exp::result_type::t_int), 21),
            expr::gt(expr::bin("score", as_exp::result_type::t_int), 70)
        ), 2, (rec2.age < 21) && (rec2.score > 70));

    cout << "\n=== Testing Edge Cases ===" << endl;
    test_expression(fd, "Non-existent bin returns nil",
        expr::bin("nonexistent", as_exp::result_type::t_int), 1);

    test_expression(fd, "Division by small number",
        expr::div(expr::bin("age", as_exp::result_type::t_int), 1), 1, (int64_t)(rec1.age / 1));

    test_expression(fd, "Multiply by zero",
        expr::mul(expr::bin("age", as_exp::result_type::t_int), 0), 1, (int64_t)0);

    test_expression(fd, "Complex nested: ((age*2) + (score/10)) > 50",
        expr::gt(
            expr::add(
                expr::mul(expr::bin("age", as_exp::result_type::t_int), 2),
                expr::div(expr::bin("score", as_exp::result_type::t_int), 10)
            ),
            50
        ), 1, ((rec1.age * 2) + (rec1.score / 10)) > 50);

    // ========================================================================
    // EXPRESSION WRITE TESTS
    // ========================================================================

    char buf[8192];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    cout << "\n=== Testing Conditional Writes (Expression Filters) ===" << endl;

    auto vc1 = reset_test_record_vc(fd, 10);
    bool expect_success_1 = (vc1.value > 25);
    test_expr_write(fd, req, &res, "Write if value > 25", 10,
                    expr::gt(expr::bin("value", as_exp::result_type::t_int), 25),
                    "counter", 100, expect_success_1);
    free(res); res = nullptr;

    auto vc2 = reset_test_record_vc(fd, 11);
    bool expect_success_2 = (vc2.value > 25);
    test_expr_write(fd, req, &res, "Write if value > 25 (rec 2)", 11,
                    expr::gt(expr::bin("value", as_exp::result_type::t_int), 25),
                    "counter", 100, expect_success_2);
    free(res); res = nullptr;

    auto vc3 = reset_test_record_vc(fd, 12);
    bool expect_success_3 = (vc3.value == 50);
    test_expr_write(fd, req, &res, "Write if value == 50", 12,
                    expr::eq(expr::bin("value", as_exp::result_type::t_int), 50),
                    "counter", 200, expect_success_3);
    free(res); res = nullptr;

    vc3 = reset_test_record_vc(fd, 12);
    bool expect_success_4 = (vc3.value >= 15 && vc3.value <= 35);
    test_expr_write(fd, req, &res, "Write if value in [15, 35]", 12,
                    expr::and_(expr::ge(expr::bin("value", as_exp::result_type::t_int), 15),
                         expr::le(expr::bin("value", as_exp::result_type::t_int), 35)),
                    "counter", 300, expect_success_4);
    free(res); res = nullptr;

    auto vc5 = reset_test_record_vc(fd, 14);
    bool expect_success_5 = (vc5.value == 10 || vc5.value == 50);
    test_expr_write(fd, req, &res, "Write if value == 10 OR value == 50", 14,
                    expr::or_(expr::eq(expr::bin("value", as_exp::result_type::t_int), 10),
                        expr::eq(expr::bin("value", as_exp::result_type::t_int), 50)),
                    "counter", 500, expect_success_5);
    free(res); res = nullptr;

    auto vc4 = reset_test_record_vc(fd, 13);
    bool expect_success_6 = !(vc4.value < 20);
    test_expr_write(fd, req, &res, "Write if NOT(value < 20)", 13,
                    expr::not_(expr::lt(expr::bin("value", as_exp::result_type::t_int), 20)),
                    "counter", 400, expect_success_6);
    free(res); res = nullptr;

    cout << "\n=== Testing Expression Modify Operations ===" << endl;

    vc1 = reset_test_record_vc(fd, 10);
    int expected_val = vc1.value * 2;
    test_expr_modify(fd, req, &res, "Modify: value * 2", 10,
                     "computed", expr::mul(expr::bin("value", as_exp::result_type::t_int), 2),
                     expected_val, true);
    free(res); res = nullptr;

    vc2 = reset_test_record_vc(fd, 11);
    expected_val = vc2.value + vc2.counter;
    test_expr_modify(fd, req, &res, "Modify: value + counter", 11,
                     "sum", expr::add(expr::bin("value", as_exp::result_type::t_int),
                               expr::bin("counter", as_exp::result_type::t_int)),
                     expected_val, true);
    free(res); res = nullptr;

    vc3 = reset_test_record_vc(fd, 12);
    expected_val = (vc3.value + vc3.counter) * 2;
    test_expr_modify(fd, req, &res, "Modify: (value + counter) * 2", 12,
                     "complex", expr::mul(expr::add(expr::bin("value", as_exp::result_type::t_int),
                                       expr::bin("counter", as_exp::result_type::t_int)), 2),
                     expected_val, true);
    free(res); res = nullptr;

    vc4 = reset_test_record_vc(fd, 13);
    expected_val = (vc4.value > 30) ? 1000 : 100;
    test_expr_modify(fd, req, &res, "Modify: if value > 30 then 1000 else 100", 13,
                     "conditional", expr::cond(expr::gt(expr::bin("value", as_exp::result_type::t_int), 30),
                                        1000, 100),
                     expected_val, true);
    free(res); res = nullptr;

    vc5 = reset_test_record_vc(fd, 14);
    expected_val = std::min(vc5.value, vc5.counter);
    test_expr_modify(fd, req, &res, "Modify: min(value, counter)", 14,
                     "minimum", expr::min(expr::bin("value", as_exp::result_type::t_int),
                                   expr::bin("counter", as_exp::result_type::t_int)),
                     expected_val, true);
    free(res); res = nullptr;

    expected_val = std::max(vc5.value, vc5.counter);
    test_expr_modify(fd, req, &res, "Modify: max(value, counter)", 14,
                     "maximum", expr::max(expr::bin("value", as_exp::result_type::t_int),
                                   expr::bin("counter", as_exp::result_type::t_int)),
                     expected_val, true);
    free(res); res = nullptr;

    cout << "\n=== Testing Complex Conditional Write Scenarios ===" << endl;

    vc1 = reset_test_record_vc(fd, 10);
    bool expect_success_7 = (vc1.counter != 0);
    test_expr_write(fd, req, &res, "Write if counter != 0 (record 1)", 10,
                    expr::ne(expr::bin("counter", as_exp::result_type::t_int), 0),
                    "modified", 1, expect_success_7);
    free(res); res = nullptr;

    vc2 = reset_test_record_vc(fd, 11);
    bool expect_success_8 = (vc2.counter != 0);
    test_expr_write(fd, req, &res, "Write if counter != 0 (record 2)", 11,
                    expr::ne(expr::bin("counter", as_exp::result_type::t_int), 0),
                    "modified", 1, expect_success_8);
    free(res); res = nullptr;

    vc3 = reset_test_record_vc(fd, 12);
    bool expect_success_9 = ((vc3.value % 10) == 0);
    test_expr_write(fd, req, &res, "Write if value % 10 == 0", 12,
                    expr::eq(expr::mod(expr::bin("value", as_exp::result_type::t_int), 10), 0),
                    "divisible", 1, expect_success_9);
    free(res); res = nullptr;

    vc4 = reset_test_record_vc(fd, 13);
    bool expect_success_10 = ((vc4.value & 16) != 0);
    test_expr_write(fd, req, &res, "Write if value & 16 != 0", 13,
                    expr::ne(expr::int_and(expr::bin("value", as_exp::result_type::t_int), 16), 0),
                    "has_bit", 1, expect_success_10);
    free(res); res = nullptr;

    cout << "\n=== Testing Expression Modify with Type Conversions ===" << endl;

    vc1 = reset_test_record_vc(fd, 10);
    test_expr_modify(fd, req, &res, "Modify: sqrt-like (value^0.5)", 10,
                     "float_result", expr::pow(expr::to_float(expr::bin("value", as_exp::result_type::t_int)), 0.5));
    free(res); res = nullptr;

    vc2 = reset_test_record_vc(fd, 11);
    expected_val = (int)floor((double)vc2.value / 3.0);
    test_expr_modify(fd, req, &res, "Modify: floor(value / 3)", 11,
                     "floored", expr::to_int(expr::floor(expr::div(expr::to_float(expr::bin("value", as_exp::result_type::t_int)), 3.0))),
                     expected_val, true);
    free(res); res = nullptr;

    vc5 = reset_test_record_vc(fd, 14);
    expected_val = abs(vc5.value - 100);
    test_expr_modify(fd, req, &res, "Modify: abs(value - 100)", 14,
                     "distance", expr::abs(expr::sub(expr::bin("value", as_exp::result_type::t_int), 100)),
                     expected_val, true);
    free(res); res = nullptr;

    cout << "\n=== Testing Metadata-Based Conditional Writes ===" << endl;

    vc1 = reset_test_record_vc(fd, 10);
    test_expression(fd, "record_size()", expr::record_size(), 10);
    test_expr_write(fd, req, &res, "Write if record_size > 100", 10,
                    expr::gt(expr::record_size(), 100),
                    "size_check", 1, true);
    free(res); res = nullptr;

    vc2 = reset_test_record_vc(fd, 11);
    test_expr_write(fd, req, &res, "Write if ttl < 0 (never expire)", 11,
                    expr::lt(expr::ttl(), 0),
                    "ttl_check", 1, true);
    free(res); res = nullptr;

    // ========================================================================
    // STRING AND REGEX EXPRESSION TESTS
    // ========================================================================

    cout << "\n=== Testing String/Regex Expressions ===" << endl;

    // Create string test records
    create_test_record(fd, 20, 25, 75, "active");
    create_test_record(fd, 21, 30, 80, "inactive");
    create_test_record(fd, 22, 35, 90, "pending");

    test_expression(fd, "status == \"active\"",
        expr::eq(expr::bin("status", as_exp::result_type::t_str), "active"), 20, true);

    test_expression(fd, "status != \"inactive\"",
        expr::ne(expr::bin("status", as_exp::result_type::t_str), "inactive"), 20, true);

    // Regex test - match "act" pattern
    test_expression(fd, "regex(status, \".*act.*\")",
        expr::regex(expr::bin("status", as_exp::result_type::t_str), ".*act.*"), 20);

    test_expression(fd, "regex(status, \"^in.*\")",
        expr::regex(expr::bin("status", as_exp::result_type::t_str), "^in.*"), 21);

    // ========================================================================
    // NIL/NULL HANDLING TESTS
    // ========================================================================

    cout << "\n=== Testing Nil/Null Handling ===" << endl;

    // Test on record with missing bins
    delete_test_record(fd, 25);
    create_test_record(fd, 25, 40, 60, "test");

    // Access non-existent bin (should return nil)
    test_expression(fd, "bin(\"nonexistent\") [nil]",
        expr::bin("nonexistent", as_exp::result_type::t_int), 25);

    // Comparison with non-existent bin
    test_expression(fd, "bin(\"nonexistent\") == 0",
        expr::eq(expr::bin("nonexistent", as_exp::result_type::t_int), 0), 25);

    // ========================================================================
    // ADDITIONAL EDGE CASES
    // ========================================================================

    cout << "\n=== Testing Additional Edge Cases ===" << endl;

    rec1 = reset_test_record(fd, 1);

    // Zero operations
    test_expression(fd, "age * 0 == 0",
        expr::eq(expr::mul(expr::bin("age", as_exp::result_type::t_int), 0), 0), 1, true);

    test_expression(fd, "age + 0 == age",
        expr::eq(
            expr::add(expr::bin("age", as_exp::result_type::t_int), 0),
            expr::bin("age", as_exp::result_type::t_int)
        ), 1, true);

    // Identity operations
    test_expression(fd, "age / 1 == age",
        expr::eq(
            expr::div(expr::bin("age", as_exp::result_type::t_int), 1),
            expr::bin("age", as_exp::result_type::t_int)
        ), 1, true);

    // Negative number handling
    test_expression(fd, "abs(-100)",
        expr::abs(-100), 1, (int64_t)100);

    test_expression(fd, "age + (-10)",
        expr::add(expr::bin("age", as_exp::result_type::t_int), -10), 1, (int64_t)(rec1.age - 10));

    // Large number operations
    test_expression(fd, "1000000 + 2000000",
        expr::add(1000000, 2000000), 1, (int64_t)3000000);

    // ========================================================================
    // COMPLEX NESTED EXPRESSION TESTS
    // ========================================================================

    cout << "\n=== Testing Complex Nested Expressions ===" << endl;

    rec1 = reset_test_record(fd, 1);

    // 4-level nesting: ((age + score) * 2) / 10
    test_expression(fd, "((age + score) * 2) / 10",
        expr::div(
            expr::mul(
                expr::add(
                    expr::bin("age", as_exp::result_type::t_int),
                    expr::bin("score", as_exp::result_type::t_int)
                ),
                2
            ),
            10
        ), 1, (int64_t)(((rec1.age + rec1.score) * 2) / 10));

    // Complex logical: (age > 21 AND score > 50) OR (age < 21 AND score > 80)
    test_expression(fd, "(age > 21 AND score > 50) OR (age < 21 AND score > 80)",
        expr::or_(
            expr::and_(
                expr::gt(expr::bin("age", as_exp::result_type::t_int), 21),
                expr::gt(expr::bin("score", as_exp::result_type::t_int), 50)
            ),
            expr::and_(
                expr::lt(expr::bin("age", as_exp::result_type::t_int), 21),
                expr::gt(expr::bin("score", as_exp::result_type::t_int), 80)
            )
        ), 1, ((rec1.age > 21 && rec1.score > 50) || (rec1.age < 21 && rec1.score > 80)));

    // Nested conditional: if (age > 30) then (score * 2) else (score + 10)
    int cond_result = (rec1.age > 30) ? (rec1.score * 2) : (rec1.score + 10);
    test_expression(fd, "if (age > 30) then (score * 2) else (score + 10)",
        expr::cond(
            expr::gt(expr::bin("age", as_exp::result_type::t_int), 30),
            expr::mul(expr::bin("score", as_exp::result_type::t_int), 2),
            expr::add(expr::bin("score", as_exp::result_type::t_int), 10)
        ), 1, (int64_t)cond_result);

    // Bitwise and arithmetic combination: (age | 15) * 2
    test_expression(fd, "(age | 15) * 2",
        expr::mul(
            expr::int_or(expr::bin("age", as_exp::result_type::t_int), 15),
            2
        ), 1, (int64_t)((rec1.age | 15) * 2));

    // Min/Max with arithmetic: max(age, score) + min(age, score)
    test_expression(fd, "max(age, score) + min(age, score)",
        expr::add(
            expr::max(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int)),
            expr::min(expr::bin("age", as_exp::result_type::t_int), expr::bin("score", as_exp::result_type::t_int))
        ), 1, (int64_t)(max(rec1.age, rec1.score) + min(rec1.age, rec1.score)));

    // ========================================================================
    // ALL LOGICAL OPERATOR COMBINATIONS
    // ========================================================================

    cout << "\n=== Testing All Logical Operator Combinations ===" << endl;

    rec1 = reset_test_record(fd, 1);

    // Three-clause AND
    test_expression(fd, "age > 18 AND score > 20 AND status == \"active\"",
        expr::and_(
            expr::and_(
                expr::gt(expr::bin("age", as_exp::result_type::t_int), 18),
                expr::gt(expr::bin("score", as_exp::result_type::t_int), 20)
            ),
            expr::eq(expr::bin("status", as_exp::result_type::t_str), "active")
        ), 1, (rec1.age > 18) && (rec1.score > 20) && (rec1.status == "active"));

    // Three-clause OR
    test_expression(fd, "age < 18 OR score > 90 OR status == \"pending\"",
        expr::or_(
            expr::or_(
                expr::lt(expr::bin("age", as_exp::result_type::t_int), 18),
                expr::gt(expr::bin("score", as_exp::result_type::t_int), 90)
            ),
            expr::eq(expr::bin("status", as_exp::result_type::t_str), "pending")
        ), 1, (rec1.age < 18) || (rec1.score > 90) || (rec1.status == "pending"));

    // NOT with complex expression
    test_expression(fd, "NOT(age > 21 AND score < 50)",
        expr::not_(
            expr::and_(
                expr::gt(expr::bin("age", as_exp::result_type::t_int), 21),
                expr::lt(expr::bin("score", as_exp::result_type::t_int), 50)
            )
        ), 1, !((rec1.age > 21) && (rec1.score < 50)));

    // Exclusive OR (XOR)
    test_expression(fd, "exclusive(age > 30, score > 50)",
        expr::exclusive(
            expr::gt(expr::bin("age", as_exp::result_type::t_int), 30),
            expr::gt(expr::bin("score", as_exp::result_type::t_int), 50)
        ), 1, ((rec1.age > 30) != (rec1.score > 50)));

    // ========================================================================
    // CLEANUP
    // ========================================================================

    cout << "\n--- Final Cleanup ---" << endl;
    delete_test_record(fd, 20);
    delete_test_record(fd, 21);
    delete_test_record(fd, 22);
    delete_test_record(fd, 25);

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
