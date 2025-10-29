// Test different approaches for CDT SELECT on nested structures
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
        {"SN", "select_ctx_test"}
    };

    for (auto ep = *envp; ep; ep = *(++envp)) {
        const char* prefix = "JP_INFO_";
        if (!strncmp(prefix, ep, 8)) {
            auto vs = strchr(ep, '=');
            auto ks = string(ep).substr(8, (vs - ep) - 8);
            if (ks.length()) p[ks] = string(vs + 1);
        }
    }

    cout << "Testing alternative approaches for nested SELECT\n" << endl;
    int fd = tcp_connect(p["ASDB"]);
    char buf[8192];
    as_msg *req = (as_msg *)(buf + 4096);
    as_msg *res = nullptr;

    const int test_rec = 9998;
    json test_list = {5, 15, 8, 20, 3, 25};
    auto expr_gt_10 = expr::gt(expr::var_builtin_int(as_cdt::builtin_var::value), 10);

    // Approach 1: Put navigation context INSIDE the SELECT context array
    cout << "=== Approach 1: Navigation context inside SELECT ===" << endl;
    reset_test_record(fd, test_rec);

    visit(req, test_rec, AS_MSG_FLAG_WRITE);
    dieunless(req->add(as_op::type::t_cdt_modify, "nested", cdt::map::put("data", test_list)));
    call(fd, (void**)&res, req);
    if (res) free(res);
    res = nullptr;

    // Try: select([map_key, "data", exp, expr], mode)
    auto select_with_nav = cdt::select(
        json::array({ct::map_key, "data", as_cdt::ctx_type::exp, expr_gt_10}),
        cdt::select_mode::tree
    );

    visit(req, test_rec, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_cdt_read, "nested", select_with_nav));
    call(fd, (void**)&res, req);

    cout << "Result code: " << (int)res->result_code;
    if (res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << " | Result: " << result.dump() << endl;
    } else {
        cout << " (FAILED)" << endl;
    }
    if (res) free(res);
    res = nullptr;

    // Approach 2: Use regular CDT operation on nested path, not SELECT
    cout << "\n=== Approach 2: Regular get_range on nested list (control) ===" << endl;
    reset_test_record(fd, test_rec);

    visit(req, test_rec, AS_MSG_FLAG_WRITE);
    dieunless(req->add(as_op::type::t_cdt_modify, "nested", cdt::map::put("data", test_list)));
    call(fd, (void**)&res, req);
    if (res) free(res);
    res = nullptr;

    auto regular_nested = cdt::subcontext_eval(
        json::array({ct::map_key, "data"}),
        cdt::list::get_range(0, 6)
    );

    visit(req, test_rec, AS_MSG_FLAG_READ);
    dieunless(req->add(as_op::type::t_cdt_read, "nested", regular_nested));
    call(fd, (void**)&res, req);

    cout << "Result code: " << (int)res->result_code;
    if (res->result_code == 0) {
        auto op = res->ops_begin();
        auto result = json::from_msgpack(op->data(), op->data() + op->data_sz());
        cout << " | Result: " << result.dump() << " (confirms nesting works)" << endl;
    } else {
        cout << " (FAILED)" << endl;
    }
    if (res) free(res);
    res = nullptr;

    reset_test_record(fd, test_rec);
    close(fd);
    return 0;
}
