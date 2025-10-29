// Quick diagnostic test for SELECT operations
#include "as_proto.hpp"
#include "util.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace std;

int main() {
    int fd = tcp_connect("localhost:3000");
    char buf[4096];
    as_msg *req = (as_msg *)(buf + 2048);
    as_msg *res = nullptr;

    // Step 1: Write a simple list
    req->clear();
    req->flags = AS_MSG_FLAG_WRITE;
    req->be_transaction_ttl = htobe32(1000);
    req->add(as_field::type::t_namespace, string("test"));
    req->add(as_field::type::t_set, string("select_test"));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, string("select_test"), 9999);

    json test_list = {5, 15, 8, 20};
    req->add(as_op::type::t_write, string("numbers"), test_list);

    call(fd, (void**)&res, req);
    cout << "Write result: " << (int)res->result_code << endl;
    free(res);

    // Step 2: Read it back
    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    req->add(as_field::type::t_namespace, string("test"));
    req->add(as_field::type::t_set, string("select_test"));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, string("select_test"), 9999);
    req->add(as_op::type::t_read, string("numbers"), 0);

    call(fd, (void**)&res, req);
    cout << "Read result code: " << (int)res->result_code << endl;

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        cout << "Read data type: " << (int)op->data_type << " size: " << op->data_sz() << endl;
        if (op->data_type == as_particle::type::t_list) {
            auto data = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "Read data: " << data.dump() << endl;
        }
    }
    free(res);

    // Step 3: Try SELECT
    req->clear();
    req->flags = AS_MSG_FLAG_READ;
    req->be_transaction_ttl = htobe32(1000);
    req->add(as_field::type::t_namespace, string("test"));
    req->add(as_field::type::t_set, string("select_test"));
    add_integer_key_digest(req->add(as_field::type::t_digest_ripe, 20)->data, string("select_test"), 9999);

    auto expr = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 10);
    auto op_json = cdt::select(
        json::array({as_cdt::ctx_type::exp, expr}),
        cdt::select_mode::tree
    );
    req->add(as_op::type::t_cdt_read, string("numbers"), op_json);

    call(fd, (void**)&res, req);
    cout << "SELECT result code: " << (int)res->result_code << endl;

    if (res->result_code == 0) {
        auto op = res->ops_begin();
        cout << "SELECT data type: " << (int)op->data_type << " size: " << op->data_sz() << endl;
        if (op->data_type == as_particle::type::t_list) {
            auto data = json::from_msgpack(op->data(), op->data() + op->data_sz());
            cout << "SELECT result: " << data.dump() << endl;
        }
    }
    free(res);

    close(fd);
    return 0;
}
