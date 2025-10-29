#include <iostream>
#include <iomanip>
#include <map>
#include <string>
#include <cstring>
#include <unistd.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <nlohmann/json.hpp>
#include "as_proto.hpp"
#include "util.hpp"

using namespace std;
using json = nlohmann::json;

map<string, string> p;

as_msg* visit(as_msg* msg, int ri, uint32_t flags) {
    msg->clear();
    msg->flags = flags;
    msg->add(as_field::type::t_namespace, p["NS"]);
    msg->add(as_field::type::t_set, p["SN"]);
    add_integer_key_digest(msg->add(as_field::type::t_digest_ripe, 20)->data, p["SN"], ri);
    return msg;
}

int main(int argc, char **argv, char **envp) {
    p = {
        {"ASDB", "localhost:3000"},
        {"NS", "test"},
        {"SN", "expr_test"}
    };

    cout << "Connecting to " << p["ASDB"] << endl;
    int fd = tcp_connect(p["ASDB"]);

    char buf[2048];
    as_msg *req = (as_msg *)(buf + 1024);
    as_msg *res = nullptr;

    // Create test record
    cout << "\n=== Creating test record ===" << endl;
    visit(req, 1, AS_MSG_FLAG_WRITE);
    auto op = req->add(as_op::type::t_write, "age", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(25);

    op = req->add(as_op::type::t_write, "score", 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data() = htobe64(100);

    req->add(as_op::type::t_write, std::string("status"), std::string("active"));

    call(fd, (void**)&res, req);
    cout << "Write result code: " << (int)res->result_code << endl;
    free(res);
    res = nullptr;

    // Read back the bins
    cout << "\n=== Reading bins back ===" << endl;
    visit(req, 1, AS_MSG_FLAG_READ);
    req->add(as_op::type::t_read, "age", 0);
    req->add(as_op::type::t_read, "score", 0);
    req->add(as_op::type::t_read, "status", 0);

    call(fd, (void**)&res, req);
    cout << "Read result code: " << (int)res->result_code << endl;
    cout << "Number of operations in response: " << res->n_ops() << endl;

    auto resp_op = res->ops_begin();
    for (int i = 0; i < res->n_ops(); i++) {
        string bin_name((char*)resp_op->name, resp_op->name_sz);
        cout << "  Bin '" << bin_name << "': type=" << (int)resp_op->data_type
             << ", size=" << resp_op->data_sz() << endl;

        if (resp_op->data_type == as_particle::type::t_integer && resp_op->data_sz() == 8) {
            int64_t val = be64toh(*(int64_t*)resp_op->data());
            cout << "    Value: " << val << endl;
        } else if (resp_op->data_type == as_particle::type::t_string) {
            string val((char*)resp_op->data() + 1, resp_op->data_sz() - 1);
            cout << "    Value: \"" << val << "\"" << endl;
        }

        resp_op = resp_op->next();
    }
    free(res);
    res = nullptr;

    // Try expression read: bin("age")
    cout << "\n=== Testing expression read: bin(\"age\") ===" << endl;
    visit(req, 1, AS_MSG_FLAG_READ);
    cout << "After visit" << endl;

    json age_expr = expr::bin("age", as_exp::result_type::t_int);
    cout << "Expression JSON: " << age_expr.dump() << endl;

    // Try simpler msgpack first
    cout << "Testing simple msgpack..." << endl;
    auto simple = json::to_msgpack(json::array({1, 2, 3}));
    cout << "Simple msgpack works: " << simple.size() << " bytes" << endl;

    cout << "Testing expression msgpack..." << endl;
    auto expr_mp = json::to_msgpack(age_expr);
    cout << "Expression msgpack works: " << expr_mp.size() << " bytes" << endl;

    cout << "Testing wrapper..." << endl;
    json wrapper_obj = {age_expr, 0};
    cout << "Wrapper created, dumping: " << wrapper_obj.dump() << endl;

    auto pload = json::to_msgpack(wrapper_obj);
    cout << "Wrapped msgpack: " << pload.size() << " bytes" << endl;

    cout << "Adding operation..." << endl;
    req->add(as_op::type::t_exp_read, "result", pload.size(), pload.data());
    cout << "Operation added" << endl;

    cout << "Calling server..." << endl;
    call(fd, (void**)&res, req);
    cout << "Expression read result code: " << (int)res->result_code << endl;

    if (res->result_code == 0) {
        cout << "Number of operations in response: " << res->n_ops() << endl;
        auto exp_op = res->ops_begin();
        string bin_name((char*)exp_op->name, exp_op->name_sz);
        cout << "  Result bin '" << bin_name << "': type=" << (int)exp_op->data_type
             << ", size=" << exp_op->data_sz() << endl;

        if (exp_op->data_type == as_particle::type::t_integer && exp_op->data_sz() == 8) {
            int64_t val = be64toh(*(int64_t*)exp_op->data());
            cout << "    Value: " << val << endl;
        }
    }
    free(res);
    res = nullptr;

    // Delete record
    cout << "\n=== Cleaning up ===" << endl;
    visit(req, 1, AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE);
    call(fd, (void**)&res, req);
    free(res);
    res = nullptr;

    return 0;
}
