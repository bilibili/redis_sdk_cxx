#include <butil/rand_util.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "redis_sdk/include/client_impl.h"
#include "test/mock_node.h"
#include "test/test_common.h"

namespace rsdk {
namespace client {

std::string to_string(const brpc::RedisResponse& resp);
DECLARE_bool(log_request_verbose);
void log_reply(const brpc::RedisReply& reply);
void log_response(const brpc::RedisResponse& resp);
Status check_brpc_redis_reply_bug();
uint32_t key_to_slot_id(const char* key, int keylen);

namespace test {

class ClientImplTest : public ::testing::Test {
public:
    ClientImplTest() {
    }
    static void SetUpTestCase() {
        int ret = system("rm -rf ./log/");
        ret = system("mkdir -p ./log/");
        (void)ret;
    }
    void SetUp() {
        FLAGS_log_request_verbose = true;
    }
    void TearDown() {
    }
    std::string build_cluster_name() {
        ++_cluster_id;
        return "cluster" + std::to_string(_cluster_id);
    }
    ClientManagerPtr new_client_mgr_impl() {
        auto cli = std::make_shared<ClientManagerImpl>();
        return cli;
    }
    ClientPtr setup_simple(int server_cnt, int slave_per_server, MockEnv* env);
    void check_mget_resp(int cnt, bool str, const brpc::RedisResponse& resp);
    void check_mget_resp(int cnt, const RecordList& records);
    int _cluster_id = 0;
};

void clear_keys(std::vector<MockRedisServerPtr>* servers, const std::vector<std::string>& keys) {
    for (auto& key : keys) {
        for (auto& server : *servers) {
            server->_service->_kv.erase(key);
        }
    }
}

ClientPtr ClientImplTest::setup_simple(int server_cnt, int slave_per_server, MockEnv* env) {
    env->_cname = build_cluster_name();
    setup_cluster(env->_cname, server_cnt, slave_per_server, &env->_servers, &env->_seeds);
    env->_mgr = new_client_mgr_impl();
    if (env->_mgr == nullptr) {
        return nullptr;
    }
    Status status = add_cluster(env->_mgr, env->_cname, env->_seeds);
    if (!status.ok()) {
        return nullptr;
    }
    env->_opt._cname = env->_cname;
    ClientPtr client = env->_mgr->new_client(env->_opt);
    return client;
}

TEST_F(ClientImplTest, RedisReply) {
    Status ret = check_brpc_redis_reply_bug();
    ASSERT_TRUE(ret.ok()) << ret;
}

// TEST_F(ClientImplTest, RedisResponse) {
//     std::string msg = "+";
//     msg.resize(300, 'x');
//     msg.append("\r\n");

//     std::unique_ptr<brpc::RedisResponse> resp1(new brpc::RedisResponse());
//     std::unique_ptr<brpc::RedisResponse> resp2(new brpc::RedisResponse());

//     resp1.get()->Swap(resp2.get());

//     char* ptr = (char*)resp2.get();
//     resp2 = nullptr;
//     // make sure memory is definitely freed
//     memset(ptr, 0xff, sizeof(brpc::RedisResponse));

//     butil::IOBuf buf;
//     buf.append(msg);
//     auto err = resp1->ConsumePartialIOBuf(buf, 1);
//     ASSERT_EQ(0, err);
// }

TEST_F(ClientImplTest, Sanity) {
    int server_cnt = 6;
    int slave_per_server = 2;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;
    brpc::RedisRequest req;
    req.AddCommand("set a 1");
    client->exec(req, &resp);
    ASSERT_TRUE(resp.reply(0).is_string());
    ASSERT_STREQ("OK", resp.reply(0).c_str());

    brpc::RedisRequest req2;
    brpc::RedisResponse resp2;
    req2.AddCommand("get a");
    client->exec(req2, &resp2);
    ASSERT_TRUE(resp2.reply(0).is_string());
    ASSERT_STREQ("1", resp2.reply(0).c_str());
}

TEST_F(ClientImplTest, SanityOnMsetMget) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);
    RecordList records;
    Record rec;
    rec._key = "x";
    rec._data = "XXXXX";
    records.push_back(rec);
    client->mset(&records, nullptr);
    ASSERT_EQ(records[0]._errno, 0) << records[0]._err_msg;

    records.clear();
    Record rec1;
    rec1._key = "x";
    records.push_back(rec1);
    client->mget(&records, nullptr);
    ASSERT_EQ(records[0]._errno, 0) << records[0]._err_msg;
    ASSERT_STREQ("XXXXX", records[0]._data.c_str());
}

TEST_F(ClientImplTest, SanityOnMsetMgetV2) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);
    RecordList records;
    Record rec;
    rec._key = "x";
    std::string value = "XXXXXXX";
    rec._data = value;
    records.push_back(rec);
    std::vector<brpc::RedisResponse> resps;
    client->mset(records, &resps, nullptr);
    ASSERT_EQ(resps.size(), 1);
    ASSERT_EQ(resps[0].reply(0).type(), brpc::REDIS_REPLY_STATUS);
    ASSERT_STREQ(resps[0].reply(0).c_str(), "OK");

    records.clear();
    Record rec1;
    rec1._key = "x";
    records.push_back(rec1);
    client->mget(records, &resps, nullptr);
    ASSERT_EQ(resps.size(), 1);
    ASSERT_TRUE(resps[0].reply(0).is_string());
    auto str = resps[0].reply(0).data();
    ASSERT_EQ(str.size(), value.size());
    ASSERT_STREQ(value.c_str(), str.data());
}

TEST_F(ClientImplTest, StripAskHead) {
    butil::IOBuf buf1;
    std::string msg = "+";
    msg.resize(100, 'x');
    msg.append("\r\n");
    buf1.append(msg);

    msg = "$-1\r\n";
    buf1.append(msg);

    msg = "$-1\r\n";
    buf1.append(msg);

    msg.clear();
    msg.append("+");
    msg.resize(201, 'y');
    msg.append("\r\n");
    buf1.append(msg);

    msg.clear();
    msg.append("+");
    msg.resize(301, 'z');
    msg.append("\r\n");
    buf1.append(msg);

    msg.clear();
    msg.append(":");
    msg.append(std::to_string(123456789));
    msg.append("\r\n");
    buf1.append(msg);

    msg.clear();
    msg.append("-");
    msg.append("ASK");
    msg.append("\r\n");
    buf1.append(msg);

    brpc::RedisResponse resp1;
    auto err = resp1.ConsumePartialIOBuf(buf1, 7);
    ASSERT_EQ(0, err);

    brpc::RedisRequest req;
    req.AddCommand("cmd1");
    std::string key = "key";
    IOCtx ctx(nullptr, "unknown", "sub", std::move(key), std::move(req), &resp1, nullptr);
    ctx._has_ask_head = true;
    ctx.may_skip_ask_head();
    ASSERT_EQ(6, resp1.reply_size());
    ASSERT_TRUE(resp1.reply(0).is_nil());
    ASSERT_TRUE(resp1.reply(1).is_nil());
    ASSERT_TRUE(resp1.reply(2).is_string());
    ASSERT_EQ(200, resp1.reply(2).size());
    std::string expect(200, 'y');
    ASSERT_STREQ(expect.c_str(), resp1.reply(2).c_str());

    ASSERT_TRUE(resp1.reply(3).is_string());
    ASSERT_EQ(300, resp1.reply(3).size());
    std::string expect2(300, 'z');
    ASSERT_STREQ(expect2.c_str(), resp1.reply(3).c_str());

    ASSERT_TRUE(resp1.reply(4).is_integer());
    ASSERT_EQ(123456789, resp1.reply(4).integer());

    ASSERT_TRUE(resp1.reply(5).is_error());
    ASSERT_STREQ("ASK", resp1.reply(5).error_message());
    log_response(resp1);
}

void ClientImplTest::check_mget_resp(int cnt, bool str_mode, const brpc::RedisResponse& resp) {
    ASSERT_EQ(cnt, resp.reply_size());
    for (int i = 0; i < cnt; ++i) {
        ASSERT_TRUE(resp.reply(i).is_string());
        if (str_mode) {
            std::string str = resp.reply(i).c_str();
            std::string exp = std::to_string(i);
            ASSERT_EQ(str, exp);
        } else {
            ASSERT_EQ(sizeof(int), resp.reply(i).size());
            int value = 0;
            memcpy(&value, resp.reply(i).c_str(), sizeof(value));
            ASSERT_EQ(i, value);
        }
    }
}

void ClientImplTest::check_mget_resp(int cnt, const RecordList& records) {
    ASSERT_EQ(cnt, records.size());
    for (int i = 0; i < cnt; ++i) {
        const Record& rec = records[i];
        ASSERT_EQ(rec._errno, 0) << rec._err_msg;
        ASSERT_EQ(sizeof(int), rec._data.size());
        int value = 0;
        memcpy(&value, rec._data.c_str(), sizeof(value));
        ASSERT_EQ(i, value);
    }
}

TEST_F(ClientImplTest, BatchCmd) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);

    brpc::RedisRequest req1;
    build_mset(100, false, &req1);
    {
        brpc::RedisResponse resp1;
        client->exec(req1, &resp1);
        ASSERT_EQ(100, resp1.reply_size());
        for (int i = 0; i < 100; ++i) {
            ASSERT_TRUE(resp1.reply(i).is_string());
            ASSERT_STREQ("OK", resp1.reply(i).c_str());
        }
    }
    {
        brpc::RedisRequest req2;
        brpc::RedisResponse resp2;
        build_mget(100, &req2);
        client->exec(req2, &resp2);
        ASSERT_EQ(100, resp2.reply_size());
        ASSERT_NO_FATAL_FAILURE(check_mget_resp(100, false, resp2));
    }
    {
        // delete some key, let it missed
        std::vector<std::string> to_del;
        for (int i = 0; i < 100; ++i) {
            if (i % 3 == 0) {
                to_del.push_back(std::to_string(i));
            }
        }
        clear_keys(&env._servers, to_del);
        brpc::RedisRequest req3;
        brpc::RedisResponse resp3;
        build_mget(100, &req3);
        client->exec(req3, &resp3);
        ASSERT_EQ(100, resp3.reply_size());
        for (int i = 0; i < 100; ++i) {
            if (i % 3 != 0) {
                ASSERT_TRUE(resp3.reply(i).is_string());
                ASSERT_EQ(sizeof(int), resp3.reply(i).size());
                int data = 0;
                memcpy(&data, resp3.reply(i).c_str(), sizeof(data));
                ASSERT_EQ(data, i);

            } else {
                ASSERT_TRUE(resp3.reply(i).is_nil()) << rsdk::client::to_string(resp3);
            }
        }
    }
    int key = 100;
    int value = 200;
    {
        brpc::RedisResponse resp4;
        brpc::RedisRequest req4;
        std::string fmt = "mset %b  %b";
        req4.AddCommand(fmt.c_str(), (char*)&key, sizeof(key), (char*)&value, sizeof(value));
        client->exec(req4, &resp4);
        ASSERT_EQ(1, resp4.reply_size());
        ASSERT_TRUE(resp4.reply(0).is_string());
        ASSERT_STREQ("OK", resp4.reply(0).c_str());
    }
    {
        brpc::RedisRequest req5;
        brpc::RedisResponse resp5;
        req5.AddCommand("mget %b", (char*)&key, sizeof(key));
        client->exec(req5, &resp5);
        ASSERT_EQ(1, resp5.reply_size());
        ASSERT_TRUE(resp5.reply(0).is_string());
        ASSERT_EQ(sizeof(int), resp5.reply(0).size());
        int v = 0;
        memcpy(&v, resp5.reply(0).c_str(), sizeof(v));
        ASSERT_EQ(v, value);
    }
}

TEST_F(ClientImplTest, BatchCmdOnMsetMget) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);

    ASSERT_TRUE(client != nullptr);
    RecordList records1;
    int cnt = 500;
    build_mset(cnt, &records1);
    {
        client->mset(&records1);
        for (int i = 0; i < cnt; ++i) {
            ASSERT_EQ(0, records1[i]._errno);
        }
    }
    {
        RecordList records2;
        build_mget(cnt, &records2);
        client->mget(&records2);
        ASSERT_EQ(cnt, records2.size());
        ASSERT_NO_FATAL_FAILURE(check_mget_resp(cnt, records2));
    }
    {
        // delete some key, let it missed
        std::vector<std::string> to_del;
        for (int i = 0; i < cnt; ++i) {
            if (i % 3 == 0) {
                to_del.push_back(std::to_string(i));
            }
        }
        clear_keys(&env._servers, to_del);
        RecordList records3;
        build_mget(cnt, &records3);
        client->mget(&records3);
        ASSERT_EQ(cnt, records3.size());
        for (int i = 0; i < cnt; ++i) {
            Record& rec = records3[i];
            if (i % 3 != 0) {
                ASSERT_EQ(sizeof(int), rec._data.size());
                int data = 0;
                memcpy(&data, rec._data.c_str(), sizeof(data));
                ASSERT_EQ(data, i);

            } else {
                ASSERT_TRUE(rec._data.empty()) << rec._data << " " << rec._err_msg;
            }
        }
    }
}

TEST_F(ClientImplTest, MsetMgetPartialSucceed) {
    VLOG(3) << "start to run mget mset succeed test";

    int cnt = 1000;
    MockEnv env;
    ClientPtr client = setup_simple(6, 0, &env);
    ASSERT_TRUE(client != nullptr);

    RecordList records1;
    build_mset(cnt, &records1);
    client->mset(&records1);
    for (size_t i = 0; i < env._servers.size(); ++i) {
        if (i % 3 == 0) {
            env._servers[i]->stop();
        }
    }
    RecordList records2;
    build_mget(cnt, &records2);
    client->mget(&records2);
    int failed_cnt = 0;
    int succ_cnt = 0;
    for (int i = 0; i < cnt; ++i) {
        Record& rec = records2[i];
        if (rec._errno == 0) {
            ++succ_cnt;
            int value = 0;
            memcpy(&value, rec._data.c_str(), sizeof(value));
            ASSERT_EQ(i, value);
        } else {
            std::string msg = rec._err_msg;
            ASSERT_TRUE(msg.find("TIMEOUT") != std::string::npos) << msg;
            if (msg.find("TIMEOUT") != std::string::npos) {
                ++failed_cnt;
            } else {
                ASSERT_TRUE(false) << "no timeout key word find in msg";
            }
        }
    }
    ASSERT_TRUE(failed_cnt > 0) << failed_cnt;
    ASSERT_TRUE(succ_cnt > 0) << succ_cnt;
    ASSERT_EQ(cnt, (failed_cnt + succ_cnt)) << "failed:" << failed_cnt << " succ:" << succ_cnt;

    RecordList records3;
    build_mset(cnt, &records3);

    client->mset(&records3);
    failed_cnt = 0;
    succ_cnt = 0;
    for (int i = 0; i < cnt; ++i) {
        Record& rec = records3[i];
        if (rec._errno == 0) {
            ++succ_cnt;
        } else {
            ASSERT_TRUE(rec._err_msg.find("TIMEOUT") != std::string::npos) << rec._err_msg;
            ++failed_cnt;
        }
    }
    ASSERT_TRUE(failed_cnt > 0) << failed_cnt;
    ASSERT_TRUE(succ_cnt > 0) << succ_cnt;

    ASSERT_EQ(cnt, (failed_cnt + succ_cnt)) << " failed:" << failed_cnt << " succ:" << succ_cnt;
}

TEST_F(ClientImplTest, MsetMgetV2PartialSucceed) {
    VLOG(3) << "start to run mget mset succeed test";

    int cnt = 1000;
    MockEnv env;
    ClientPtr client = setup_simple(6, 0, &env);
    ASSERT_TRUE(client != nullptr);

    RecordList records1;
    build_mset(cnt, &records1);
    std::vector<brpc::RedisResponse> resps;
    SyncClosure sync(1);
    client->mset(records1, &resps, &sync);
    for (size_t i = 0; i < env._servers.size(); ++i) {
        if (i % 3 == 0) {
            env._servers[i]->stop();
        }
    }
    sync.wait();
    RecordList records2;
    build_mget(cnt, &records2);
    client->mget(records2, &resps);
    int failed_cnt = 0;
    int succ_cnt = 0;
    for (int i = 0; i < cnt; ++i) {
        const brpc::RedisReply& reply = resps[i].reply(0);
        if (reply.is_string()) {
            ++succ_cnt;
            int value = 0;
            memcpy(&value, reply.c_str(), sizeof(value));
            ASSERT_EQ(i, value);
        } else {
            ASSERT_TRUE(reply.is_error());
            std::string msg = reply.error_message();
            ASSERT_TRUE(msg.find("TIMEOUT") != std::string::npos) << msg;
            if (msg.find("TIMEOUT") != std::string::npos) {
                ++failed_cnt;
            } else {
                ASSERT_TRUE(false) << "no timeout key word find in msg";
            }
        }
    }
    ASSERT_TRUE(failed_cnt > 0) << failed_cnt;
    ASSERT_TRUE(succ_cnt > 0) << succ_cnt;
    ASSERT_EQ(cnt, (failed_cnt + succ_cnt)) << "failed:" << failed_cnt << " succ:" << succ_cnt;

    RecordList records3;
    build_mset(cnt, &records3);

    client->mset(records3, &resps);
    failed_cnt = 0;
    succ_cnt = 0;
    for (int i = 0; i < cnt; ++i) {
        const brpc::RedisReply& reply = resps[i].reply(0);
        if (reply.type() == brpc::REDIS_REPLY_STATUS) {
            ++succ_cnt;
        } else {
            std::string msg = reply.error_message();
            ASSERT_TRUE(msg.find("TIMEOUT") != std::string::npos) << msg;
            ++failed_cnt;
        }
    }
    ASSERT_TRUE(failed_cnt > 0) << failed_cnt;
    ASSERT_TRUE(succ_cnt > 0) << succ_cnt;

    ASSERT_EQ(cnt, (failed_cnt + succ_cnt)) << " failed:" << failed_cnt << " succ:" << succ_cnt;
}

TEST_F(ClientImplTest, BatchPartialSucceed) {
    VLOG(3) << "start to run partial succeed test";

    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);

    brpc::RedisRequest req;
    brpc::RedisResponse resp;
    build_mset(100, false, &req);
    for (size_t i = 0; i < env._servers.size(); ++i) {
        if (i % 3 == 0) {
            env._servers[i]->stop();
        }
    }
    client->exec(req, &resp);
    ASSERT_EQ(100, resp.reply_size());
    int failed_cnt = 0;
    int succ_cnt = 0;
    for (int i = 0; i < 100; ++i) {
        if (resp.reply(i).is_string()) {
            ASSERT_STREQ("OK", resp.reply(i).c_str());
            ++succ_cnt;
        } else if (resp.reply(i).is_error()) {
            std::string msg = resp.reply(i).error_message();
            ASSERT_TRUE(msg.find("TIMEOUT") != std::string::npos) << msg;
            if (msg.find("TIMEOUT") != std::string::npos) {
                ++failed_cnt;
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_EQ(100, (failed_cnt + succ_cnt)) << "failed:" << failed_cnt << " succ:" << succ_cnt;

    brpc::RedisRequest req2;
    build_mget(100, &req2);
    brpc::RedisResponse resp2;
    client->exec(req2, &resp2);
    ASSERT_EQ(100, resp2.reply_size());
    failed_cnt = 0;
    succ_cnt = 0;
    for (int i = 0; i < 100; ++i) {
        if (resp2.reply(i).is_string()) {
            ASSERT_EQ(sizeof(int), resp2.reply(i).size());
            int value = 0;
            memcpy(&value, resp2.reply(i).c_str(), sizeof(value));
            ASSERT_EQ(i, value);
            ++succ_cnt;
        } else if (resp2.reply(i).is_error()) {
            ASSERT_TRUE(std::string(resp2.reply(i).error_message()).find("TIMEOUT")
                        != std::string::npos)
                << resp2.reply(i).error_message();
            ++failed_cnt;
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_EQ(100, (failed_cnt + succ_cnt)) << "get failed:" << failed_cnt << " succ:" << succ_cnt;
}

TEST_F(ClientImplTest, InvalidParam) {
    VLOG(3) << "run invalid param case";

    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;
    brpc::RedisRequest req;
    req.AddCommand("mset");
    client->exec(req, &resp);
    ASSERT_EQ(1, resp.reply_size());
    ASSERT_TRUE(std::string(resp.reply(0).error_message()).find("no cmd in req")
                != std::string::npos)
        << resp.reply(0).error_message();
    LOG(INFO) << "client resp:" << resp.reply(0).error_message();

    brpc::RedisRequest req2;
    brpc::RedisResponse resp2;
    req2.AddCommand("mset 1 2 3");
    client->exec(req2, &resp2);
    ASSERT_EQ(1, resp2.reply_size());
    ASSERT_TRUE(std::string(resp2.reply(0).error_message()).find("invalid args count")
                != std::string::npos)
        << resp2.reply(0).error_message();
    LOG(INFO) << "client resp2:" << resp2.reply(0).error_message();

    brpc::RedisResponse resp3;
    brpc::RedisRequest req3;
    req3.AddCommand("msets 1 2");
    client->exec(req3, &resp3);
    ASSERT_EQ(1, resp3.reply_size());
    ASSERT_TRUE(std::string(resp3.reply(0).error_message()).find("ERR unknown command")
                != std::string::npos)
        << resp3.reply(0).error_message();
    LOG(INFO) << "client resp3:" << resp3.reply(0).error_message();

    brpc::RedisRequest req4;
    brpc::RedisResponse resp4;
    req4.AddCommand("rename 1 2");
    client->exec(req4, &resp4);
    ASSERT_EQ(1, resp4.reply_size());
    ASSERT_TRUE(std::string(resp4.reply(0).error_message()).find("unsupport cmd")
                != std::string::npos)
        << resp4.reply(0).error_message();
    LOG(INFO) << "client resp4:" << resp4.reply(0).error_message();

    brpc::RedisRequest req5;
    brpc::RedisResponse resp5;
    client->exec(req5, &resp5);
    ASSERT_EQ(1, resp5.reply_size());
    ASSERT_TRUE(std::string(resp5.reply(0).error_message()).find("more than one cmd, cnt:0")
                != std::string::npos)
        << resp5.reply(0).error_message();
    LOG(INFO) << "client resp5:" << resp5.reply(0).error_message();

    brpc::RedisRequest req6;
    brpc::RedisResponse resp6;
    req6.AddCommand("set x 1");
    req6.AddCommand("set y 1");
    client->exec(req6, &resp6);
    ASSERT_EQ(1, resp6.reply_size());
    ASSERT_TRUE(std::string(resp6.reply(0).error_message()).find("more than one cmd, cnt:2")
                != std::string::npos)
        << resp6.reply(0).error_message();
    LOG(INFO) << "client resp6:" << resp6.reply(0).error_message();
}

TEST_F(ClientImplTest, ParserSanity) {
    MockEnv env;
    ClientPtr base = setup_simple(4, 1, &env);
    ASSERT_TRUE(base != nullptr);

    ClientImpl* client = dynamic_cast<ClientImpl*>(base.get());
    ASSERT_TRUE(client != nullptr);
    {
        std::vector<butil::StringPiece> args;
        std::string main_cmd;
        std::string err_msg;

        brpc::RedisCommandParser parser;
        butil::Arena arena;

        int cmd_cnt = 0;
        int arg_cnt = -1;
        std::string cmd = "set x 1";
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        brpc::RedisResponse resp;
        bool ret = client->check_and_parse(req, &parser, &arena, &main_cmd, &args, &err_msg,
                                           &cmd_cnt, &arg_cnt);
        ASSERT_TRUE(ret) << err_msg;
        ASSERT_EQ(main_cmd, std::string("set"));
        ASSERT_EQ(cmd_cnt, 1);
        ASSERT_EQ(arg_cnt, 2);
    }
    {
        std::vector<butil::StringPiece> args;
        std::string main_cmd;
        std::string err_msg;

        brpc::RedisCommandParser parser;
        butil::Arena arena;

        int cmd_cnt = 0;
        int arg_cnt = -1;
        std::string cmd = "get x";
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        brpc::RedisResponse resp;
        bool ret = client->check_and_parse(req, &parser, &arena, &main_cmd, &args, &err_msg,
                                           &cmd_cnt, &arg_cnt);
        ASSERT_TRUE(ret) << err_msg;
        ASSERT_EQ(main_cmd, std::string("get"));
        ASSERT_EQ(cmd_cnt, 1);
        ASSERT_EQ(arg_cnt, 1);
    }

    {
        std::vector<butil::StringPiece> args;
        std::string main_cmd;
        std::string err_msg;

        brpc::RedisCommandParser parser;
        butil::Arena arena;

        int cmd_cnt = 0;
        int arg_cnt = -1;
        std::string cmd = "mget x y z";
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        brpc::RedisResponse resp;
        bool ret = client->check_and_parse(req, &parser, &arena, &main_cmd, &args, &err_msg,
                                           &cmd_cnt, &arg_cnt);
        ASSERT_TRUE(ret) << err_msg;
        ASSERT_EQ(main_cmd, std::string("mget"));
        ASSERT_EQ(cmd_cnt, 3);
        ASSERT_EQ(arg_cnt, 1);
    }

    {
        std::vector<butil::StringPiece> args;
        std::string main_cmd;
        std::string err_msg;

        brpc::RedisCommandParser parser;
        butil::Arena arena;

        int cmd_cnt = 0;
        int arg_cnt = -1;
        std::string cmd = "mset a 1 b 2 c 3 d 4";
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        brpc::RedisResponse resp;
        bool ret = client->check_and_parse(req, &parser, &arena, &main_cmd, &args, &err_msg,
                                           &cmd_cnt, &arg_cnt);
        ASSERT_TRUE(ret) << err_msg;
        ASSERT_EQ(main_cmd, std::string("mset"));
        ASSERT_EQ(cmd_cnt, 4);
        ASSERT_EQ(arg_cnt, 2);
    }
}

TEST_F(ClientImplTest, HashTag) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);

    ASSERT_TRUE(client != nullptr);
    {
        brpc::RedisResponse resp;
        std::string put_cmd = "mset {1} 1_1 {2} 2_2";
        brpc::RedisRequest req;
        req.AddCommand(put_cmd);
        client->exec(req, &resp);
        ASSERT_EQ(2, resp.reply_size());
        ASSERT_STREQ("OK", resp.reply(0).c_str());
        ASSERT_STREQ("OK", resp.reply(1).c_str());

        std::string get_cmd = "mget {1} {2}";
        brpc::RedisRequest req2;
        brpc::RedisResponse resp2;
        req2.AddCommand(get_cmd);
        client->exec(req2, &resp2);
        ASSERT_EQ(2, resp2.reply_size());
        ASSERT_STREQ("1_1", resp2.reply(0).c_str());
        ASSERT_STREQ("2_2", resp2.reply(1).c_str());

        get_cmd = "mget {1} 2";
        brpc::RedisRequest req3;
        brpc::RedisResponse resp3;
        req3.AddCommand(get_cmd);
        client->exec(req3, &resp3);
        ASSERT_EQ(2, resp3.reply_size());
        ASSERT_STREQ("1_1", resp3.reply(0).c_str());
        ASSERT_TRUE(resp3.reply(1).is_nil());
    }
    {
        brpc::RedisRequest req;
        brpc::RedisResponse resp;
        std::string put_cmd = "mset {{100}} 100_1 {{200}} 200_2";
        req.AddCommand(put_cmd);
        client->exec(req, &resp);
        ASSERT_EQ(2, resp.reply_size());
        ASSERT_STREQ("OK", resp.reply(0).c_str());
        ASSERT_STREQ("OK", resp.reply(1).c_str());

        std::string get_cmd = "mget {{100}} {{200}}";
        brpc::RedisResponse resp2;
        brpc::RedisRequest req2;
        req2.AddCommand(get_cmd);
        client->exec(req2, &resp2);
        ASSERT_EQ(2, resp2.reply_size());
        ASSERT_STREQ("100_1", resp2.reply(0).c_str());
        ASSERT_STREQ("200_2", resp2.reply(1).c_str());
    }
    {
        brpc::RedisRequest req;
        brpc::RedisResponse resp;
        std::string put_cmd = "mset {{101 101_1 {}202 202_2";
        req.AddCommand(put_cmd);
        client->exec(req, &resp);
        ASSERT_EQ(2, resp.reply_size());
        ASSERT_STREQ("OK", resp.reply(0).c_str());
        ASSERT_STREQ("OK", resp.reply(1).c_str());

        std::string get_cmd = "mget {{101 {}202";
        brpc::RedisRequest req2;
        brpc::RedisResponse resp2;
        req2.AddCommand(get_cmd);
        client->exec(req2, &resp2);
        ASSERT_EQ(2, resp2.reply_size());
        ASSERT_STREQ("101_1", resp2.reply(0).c_str());
        ASSERT_STREQ("202_2", resp2.reply(1).c_str());
    }
}

TEST_F(ClientImplTest, HostDownWhenInit) {
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    seeds.push_back("127.0.0.2:1111");
    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.is_sys_error()) << status;
    ASSERT_TRUE(status.to_string().find("failed to get cluster nodes when init cluster")
                != std::string::npos)
        << status;
}

TEST_F(ClientImplTest, ClusterNodesInvalid1) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    for (auto& server : servers) {
        server->_service->_cluster_nodes = "";
    }

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.is_sys_error()) << status;
    ASSERT_TRUE(status.to_string().find("cluster nodes response is empty") != std::string::npos)
        << status;
}

TEST_F(ClientImplTest, ClusterNodesInvalid2) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    for (auto& server : servers) {
        server->_service->_cluster_nodes = "0-16382";
    }

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.is_sys_error()) << status;
    ASSERT_TRUE(status.to_string().find("cluster nodes every response line") != std::string::npos)
        << status;
}

TEST_F(ClientImplTest, ClusterNodesSomeSlotMissed) {
    int server_cnt = 1;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    for (auto& server : servers) {
        std::vector<std::string> entries;
        butil::SplitString(server->_service->_cluster_nodes, ' ', &entries);
        std::string resp = "";
        for (int i = 0; i < 7; ++i) {
            resp.append(entries[i] + " ");
        }
        resp.append("0-16382");
        server->_service->_cluster_nodes = resp;
    }

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.is_sys_error()) << status;
    ASSERT_TRUE(status.to_string().find("some slot not filled") != std::string::npos) << status;
}

TEST_F(ClientImplTest, HandleMoved) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;
    std::string key = "keya";
    uint32_t slot_id = key_to_slot_id(key.c_str(), key.length());
    update_slot_pointer(cname, manager, slot_id);

    brpc::RedisRequest req;
    req.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_string()) << rsdk::client::to_string(resp);
    ASSERT_STREQ("OK", resp.reply(0).c_str());
    brpc::RedisResponse resp2;
    brpc::RedisRequest req2;
    req2.AddCommand(fmt::format("get {}", key));
    client->exec(req2, &resp2);
    ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp);
    ASSERT_STREQ(key.c_str(), resp2.reply(0).c_str());
}

TEST_F(ClientImplTest, HandleMovedToFail) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    opt.max_redirect = 3;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    set_crud_status(servers, opt.max_redirect + 1, "MOVED");
    brpc::RedisResponse resp;
    std::string key = "keya";

    brpc::RedisRequest req;
    req.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_error()) << rsdk::client::to_string(resp);
    std::string err_msg = resp.reply(0).error_message();
    VLOG(3) << "got error msg:" << err_msg;
    ASSERT_TRUE(err_msg.find("retry cnt:3 exceed max:3") != std::string::npos) << err_msg;
}

TEST_F(ClientImplTest, HandleRetryToSucc) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    set_crud_status(servers, opt.max_redirect, "MOVED");
    brpc::RedisResponse resp;
    std::string key = "succeed_in_end";

    brpc::RedisRequest req;
    req.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_string()) << rsdk::client::to_string(resp);
    ASSERT_STREQ("OK", resp.reply(0).c_str());
    brpc::RedisResponse resp2;
    client->exec(fmt::format("get {}", key), &resp2);
    ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp);
    ASSERT_STREQ(key.c_str(), resp2.reply(0).c_str());

    resp.Clear();
    brpc::RedisRequest req2;
    build_mset(100, false, &req2);
    client->exec(req2, &resp);
    ASSERT_EQ(resp.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        ASSERT_STREQ(resp.reply(i).c_str(), "OK");
    }

    resp.Clear();
    brpc::RedisRequest req3;
    build_mget(100, &req3);
    client->exec(req3, &resp);
    ASSERT_EQ(resp.reply_size(), 100);
    ASSERT_NO_FATAL_FAILURE(check_mget_resp(100, false, resp));
}

TEST_F(ClientImplTest, HandleAskToSucc) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    set_crud_status(servers, 1, "ASK");
    std::string key = "key_for_ask";
    brpc::RedisResponse resp0;
    brpc::RedisRequest req0;
    req0.AddCommand(fmt::format("get {}", key));
    client->exec(req0, &resp0);
    ASSERT_TRUE(resp0.reply(0).is_nil()) << rsdk::client::to_string(resp0);

    set_crud_status(servers, 1, "ASK");
    brpc::RedisResponse resp1;
    brpc::RedisRequest req1;
    req1.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req1, &resp1);
    ASSERT_EQ(resp1.reply_size(), 1);
    ASSERT_TRUE(resp1.reply(0).is_string()) << rsdk::client::to_string(resp1);
    ASSERT_STREQ("OK", resp1.reply(0).c_str());
    set_crud_status(servers, 1, "ASK");
    brpc::RedisResponse resp2;
    brpc::RedisRequest req2;
    req2.AddCommand(fmt::format("get {}", key));
    client->exec(req2, &resp2);
    ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp2);
    ASSERT_STREQ(key.c_str(), resp2.reply(0).c_str());
}

TEST_F(ClientImplTest, HandleAskOnBatch) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    set_crud_status(servers, 1, "ASK");

    brpc::RedisResponse resp0;
    brpc::RedisRequest req0;
    int key_cnt = 10;
    build_mget(key_cnt, &req0);
    client->exec(req0, &resp0);
    ASSERT_EQ(key_cnt, resp0.reply_size());
    for (int i = 0; i < key_cnt; ++i) {
        ASSERT_TRUE(resp0.reply(i).is_nil()) << rsdk::client::to_string(resp0);
    }

    set_crud_status(servers, 1, "ASK");
    brpc::RedisResponse resp1;
    brpc::RedisRequest req1;
    build_mset(key_cnt, false, &req1);
    client->exec(req1, &resp1);
    ASSERT_EQ(resp1.reply_size(), key_cnt);
    for (int i = 0; i < key_cnt; ++i) {
        ASSERT_TRUE(resp1.reply(i).is_string()) << rsdk::client::to_string(resp1);
        ASSERT_STREQ("OK", resp1.reply(i).c_str());
    }
    set_crud_status(servers, 1, "ASK");
    brpc::RedisResponse resp2;
    brpc::RedisRequest req2;
    build_mget(key_cnt, &req2);
    client->exec(req2, &resp2);
    ASSERT_NO_FATAL_FAILURE(check_mget_resp(key_cnt, false, resp2));
}

TEST_F(ClientImplTest, HandleAskOnSet) {
    MockEnv env;
    ClientPtr client = setup_simple(3, 0, &env);
    ASSERT_TRUE(client != nullptr);

    std::string key = "x";
    std::string get_cmd = "smembers " + key;
    int mcnt = 10;
    {
        set_crud_status(env._servers, 1, "ASK");
        brpc::RedisResponse resp0;
        brpc::RedisRequest req0;
        req0.AddCommand(get_cmd);

        client->exec(req0, &resp0);
        ASSERT_EQ(1, resp0.reply_size());
        ASSERT_TRUE(resp0.reply(0).is_array()) << rsdk::client::to_string(resp0);
        ASSERT_EQ(resp0.reply(0).size(), 0);
    }
    {
        set_crud_status(env._servers, 1, "ASK");
        brpc::RedisResponse resp1;
        brpc::RedisRequest req1;
        std::string set_cmd = "sadd " + key;
        for (int i = 0; i < mcnt; ++i) {
            set_cmd += fmt::format(" {} ", i);
        }
        req1.AddCommand(set_cmd);
        client->exec(req1, &resp1);
        ASSERT_EQ(resp1.reply_size(), 1);
        ASSERT_TRUE(resp1.reply(0).is_integer()) << rsdk::client::to_string(resp1);
        ASSERT_EQ(mcnt, resp1.reply(0).integer());
    }
    {
        set_crud_status(env._servers, 1, "ASK");
        brpc::RedisResponse resp2;
        brpc::RedisRequest req2;
        req2.AddCommand(get_cmd);
        client->exec(req2, &resp2);
        ASSERT_EQ(resp2.reply_size(), 1);
        ASSERT_TRUE(resp2.reply(0).is_array()) << rsdk::client::to_string(resp2);
        ASSERT_EQ(resp2.reply(0).size(), mcnt);
        for (int i = 0; i < mcnt; ++i) {
            ASSERT_TRUE(resp2.reply(0)[i].is_string()) << rsdk::client::to_string(resp2);
        }
    }
    {
        set_crud_status(env._servers, 2, "ASK");
        brpc::RedisResponse resp2;
        brpc::RedisRequest req2;
        req2.AddCommand(get_cmd);
        client->exec(req2, &resp2);
        ASSERT_EQ(resp2.reply_size(), 1);
        ASSERT_TRUE(resp2.reply(0).is_error()) << rsdk::client::to_string(resp2);
        std::string err = resp2.reply(0).error_message();
        ASSERT_STREQ(err.c_str(), "ASK");
    }
}

TEST_F(ClientImplTest, FirstTimeoutThenSucc) {
    MockEnv env;
    ClientPtr client = setup_simple(3, 0, &env);

    ASSERT_TRUE(client != nullptr);
    env._opt.timeout_ms = 10;
    client = setup_simple(3, 0, &env);
    ASSERT_TRUE(client != nullptr);
    set_sleep_time(env._servers, 30, 50);
    brpc::RedisResponse resp;
    std::string key = "key_for_sleep";
    brpc::RedisRequest req;
    req.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_error()) << rsdk::client::to_string(resp);
    std::string err_msg = resp.reply(0).error_message();
    ASSERT_TRUE(err_msg.find("TIMEOUT") != std::string::npos) << err_msg;

    env._opt.timeout_ms = 200;
    client = env._mgr->new_client(env._opt);
    brpc::RedisResponse resp2;
    brpc::RedisRequest req2;
    req2.AddCommand(fmt::format("set {} {}", key, key));
    client->exec(req2, &resp2);
    ASSERT_EQ(resp2.reply_size(), 1);
    ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp2);
    ASSERT_STREQ("OK", resp2.reply(0).c_str());

    brpc::RedisResponse resp3;
    brpc::RedisRequest req3;
    req3.AddCommand(fmt::format("get {}", key));
    client->exec(req3, &resp3);
    ASSERT_TRUE(resp3.reply(0).is_string()) << rsdk::client::to_string(resp3);
    ASSERT_STREQ(key.c_str(), resp3.reply(0).c_str());
}

TEST_F(ClientImplTest, InvalidRespOnExist) {
    int server_cnt = 3;
    int slave_per_server = 0;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt.timeout_ms = 10;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    for (auto& server : servers) {
        server->_service->_mock_exist_ret = "imok";
    }

    {
        brpc::RedisResponse resp;
        brpc::RedisRequest req;
        req.AddCommand("exists X");
        client->exec(req, &resp);
        ASSERT_EQ(resp.reply_size(), 1);
        ASSERT_TRUE(resp.reply(0).is_error()) << rsdk::client::to_string(resp);
        std::string err_msg = resp.reply(0).error_message();
        ASSERT_TRUE(err_msg.find("bug, invalid response from") != std::string::npos) << err_msg;
    }
    {
        brpc::RedisResponse resp;
        std::string cmd = "exists ";
        for (int i = 0; i < 1000; ++i) {
            cmd += fmt::format("{} ", i);
        }
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        client->exec(req, &resp);
        ASSERT_EQ(resp.reply_size(), 1);
        ASSERT_TRUE(resp.reply(0).is_error()) << rsdk::client::to_string(resp);
        std::string err_msg1 = resp.reply(0).error_message();
        ASSERT_TRUE(err_msg1.find("bug, invalid response from") != std::string::npos) << err_msg1;
    }
    int index = 0;
    for (auto& server : servers) {
        server->_service->_mock_exist_ret = "";
        if (index == 0 || index == 1) {
            server->_service->_sleep_cnt = 100;
            server->_service->_sleep_ms = 100;
        }
        ++index;
    }
    {
        brpc::RedisResponse resp;
        std::string cmd = "exists ";
        for (int i = 0; i < 1000; ++i) {
            cmd += fmt::format("{} ", i);
        }
        brpc::RedisRequest req;
        req.AddCommand(cmd);
        client->exec(req, &resp);
        ASSERT_EQ(resp.reply_size(), 1);
        ASSERT_TRUE(resp.reply(0).is_error()) << rsdk::client::to_string(resp);
        std::string err_msg1 = resp.reply(0).error_message();
        ASSERT_TRUE(err_msg1.find("[TIMEOUT, failed to access redis") == 0) << err_msg1;
    }
}

TEST_F(ClientImplTest, PartialSucceedWhenMergeResp) {
    VLOG(3) << "start to run merge on part success";

    MockEnv env;
    ClientPtr client = setup_simple(3, 0, &env);
    ASSERT_TRUE(client != nullptr);
    env._opt._merge_batch_as_array = true;
    client = env._mgr->new_client(env._opt);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;
    brpc::RedisRequest req;
    build_mget(100, &req);
    for (size_t i = 0; i < env._servers.size(); ++i) {
        if (i % 3 == 0) {
            env._servers[i]->stop();
        }
    }
    client->exec(req, &resp);
    ASSERT_EQ(1, resp.reply_size());
    ASSERT_EQ(100, resp.reply(0).size());
    ASSERT_TRUE(resp.reply(0).is_array()) << rsdk::client::to_string(resp);
    int failed_cnt = 0;
    int succ_cnt = 0;
    for (int i = 0; i < 100; ++i) {
        if (resp.reply(0)[i].is_nil()) {
            ++succ_cnt;
        } else if (resp.reply(0)[i].is_error()) {
            std::string msg = resp.reply(0)[i].error_message();
            ASSERT_TRUE(msg.find("TIMEOUT") != std::string::npos) << msg;
            if (msg.find("TIMEOUT") != std::string::npos) {
                ++failed_cnt;
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_TRUE(failed_cnt > 0);
    ASSERT_TRUE(succ_cnt > 0);
    ASSERT_EQ(100, (failed_cnt + succ_cnt)) << "failed:" << failed_cnt << " succ:" << succ_cnt;
}

static const std::string& S_TIMEOUT_KEY_PREFIX = "timeout_key_";
static const std::string& S_MOVED_KEY_PREFIX = "moved_key_";

static void handle_mset_success(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 100);
    for (int i = 0; i < resp->reply_size(); ++i) {
        ASSERT_STREQ(resp->reply(i).c_str(), "OK");
    }
    cv->notify_one();
}

static void handle_mget_success(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 100);
    for (int i = 0; i < resp->reply_size(); ++i) {
        ASSERT_TRUE(resp->reply(i).is_string());
        EXPECT_STREQ(std::to_string(i).c_str(), resp->reply(i).c_str());
    }
    cv->notify_one();
}

static void handle_exists_success(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 1);
    ASSERT_TRUE(resp->reply(0).is_integer());
    ASSERT_EQ(100, resp->reply(0).integer());
    cv->notify_one();
}

TEST_F(ClientImplTest, BatchCmdSanityFull) {
    MockEnv env;
    ClientPtr client = setup_simple(6, 2, &env);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;

    // mset success sync
    brpc::RedisRequest req;
    build_mset(100, true, &req);
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        ASSERT_STREQ(resp.reply(i).c_str(), "OK");
    }

    // mget success sync
    resp.Clear();
    brpc::RedisRequest req2;
    build_mget(100, &req2);
    client->exec(req2, &resp);
    ASSERT_EQ(resp.reply_size(), 100);
    ASSERT_NO_FATAL_FAILURE(check_mget_resp(100, true, resp));

    // exists succss sync
    resp.Clear();
    std::string exists_cmd = fmt::format("exists {}", 0);
    for (int i = 1; i < 100; ++i) {
        exists_cmd += fmt::format(" {}", i);
    }
    brpc::RedisRequest req3;
    req3.AddCommand(exists_cmd);
    client->exec(req3, &resp);
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_integer());
    ASSERT_EQ(100, resp.reply(0).integer());

    std::condition_variable cv;
    std::mutex mtx;
    // mset success async
    {
        resp.Clear();
        brpc::RedisRequest req;
        build_mset(100, true, &req);
        client->exec(req, &resp, brpc::NewCallback(handle_mset_success, &resp, &cv));
        std::unique_lock<std::mutex> lck(mtx);
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // mget success async
    {
        resp.Clear();
        brpc::RedisRequest req;
        build_mget(100, &req);
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req, &resp, brpc::NewCallback(handle_mget_success, &resp, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // exists success async
    {
        resp.Clear();
        brpc::RedisRequest req;
        req.AddCommand(exists_cmd);
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req, &resp, brpc::NewCallback(handle_exists_success, &resp, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }
}

static void handle_mset_partial_error(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(resp->reply(i).is_error());
            ASSERT_STREQ("timeout", resp->reply(i).error_message());
        } else {
            ASSERT_STREQ(resp->reply(i).c_str(), "OK");
        }
    }
    cv->notify_one();
}

static void handle_mget_partial_error(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(resp->reply(i).is_error());
            ASSERT_STREQ("timeout", resp->reply(i).error_message());
        } else {
            ASSERT_TRUE(resp->reply(i).is_string());
            ASSERT_STREQ(std::to_string(i).c_str(), resp->reply(i).c_str());
        }
    }
    cv->notify_one();
}

static void handle_exists_partial_error(brpc::RedisResponse* resp, std::condition_variable* cv) {
    ASSERT_EQ(resp->reply_size(), 1);
    ASSERT_TRUE(resp->reply(0).is_error());
    std::string error_message = resp->reply(0).error_message();
    ASSERT_TRUE(error_message.find("timeout") != std::string::npos);
    cv->notify_one();
}

TEST_F(ClientImplTest, batch_cmd_partial_error) {
    int server_cnt = 6;
    int slave_per_server = 2;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp1;

    // mset partial timeout sync
    std::string mset_partial_error_cmd = "mset";
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            std::string timeout_key = fmt::format("{}{}", S_TIMEOUT_KEY_PREFIX, i);
            mset_partial_error_cmd += fmt::format(" {} {}", timeout_key, i);
        } else {
            mset_partial_error_cmd += fmt::format(" {} {}", i, i);
        }
    }
    brpc::RedisRequest req;
    req.AddCommand(mset_partial_error_cmd);
    client->exec(req, &resp1);
    ASSERT_EQ(resp1.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(resp1.reply(i).is_error());
            ASSERT_STREQ("timeout", resp1.reply(i).error_message());
        } else {
            ASSERT_STREQ(resp1.reply(i).c_str(), "OK");
        }
    }

    // mget partial timeout sync
    brpc::RedisResponse resp2;
    std::string mget_partial_error_cmd = "mget";
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            std::string timeout_key = fmt::format("{}{}", S_TIMEOUT_KEY_PREFIX, i);
            mget_partial_error_cmd += fmt::format(" {}", timeout_key);
        } else {
            mget_partial_error_cmd += fmt::format(" {}", i);
        }
    }
    brpc::RedisRequest req2;
    req2.AddCommand(mget_partial_error_cmd);
    client->exec(req2, &resp2);
    ASSERT_EQ(resp2.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(resp2.reply(i).is_error());
            ASSERT_STREQ("timeout", resp2.reply(i).error_message());
        } else {
            ASSERT_TRUE(resp2.reply(i).is_string());
            ASSERT_STREQ(std::to_string(i).c_str(), resp2.reply(i).c_str());
        }
    }

    // exists partial timeout sync
    brpc::RedisResponse resp3;
    brpc::RedisRequest req3;
    std::string exists_partial_error_cmd = "exists";
    for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            std::string timeout_key = fmt::format("{}{}", S_TIMEOUT_KEY_PREFIX, i);
            exists_partial_error_cmd += fmt::format(" {}", timeout_key);
        } else {
            exists_partial_error_cmd += fmt::format(" {}", i);
        }
    }
    req3.AddCommand(exists_partial_error_cmd);
    client->exec(exists_partial_error_cmd, &resp3);
    ASSERT_EQ(resp3.reply_size(), 1);
    ASSERT_TRUE(resp3.reply(0).is_error());
    std::string error_msg = resp3.reply(0).error_message();
    ASSERT_TRUE(error_msg.find("timeout") != std::string::npos);

    std::condition_variable cv;
    std::mutex mtx;
    // mset partial timeout async
    brpc::RedisResponse resp4;
    brpc::RedisRequest req4;
    req4.AddCommand(mset_partial_error_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req4, &resp4, brpc::NewCallback(handle_mset_partial_error, &resp4, &cv));

        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // mget partial timeout async
    brpc::RedisResponse resp5;
    brpc::RedisRequest req5;
    req5.AddCommand(mget_partial_error_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req5, &resp5, brpc::NewCallback(handle_mget_partial_error, &resp5, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // exists partial timeout async
    brpc::RedisResponse resp6;
    brpc::RedisRequest req6;
    req6.AddCommand(exists_partial_error_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req6, &resp6, brpc::NewCallback(handle_exists_partial_error, &resp6, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(ClientImplTest, batch_cmd_partial_moved) {
    int server_cnt = 6;
    int slave_per_server = 2;
    std::string cname = build_cluster_name();
    std::vector<std::string> seeds;
    std::vector<MockRedisServerPtr> servers;
    setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

    ClientManagerPtr manager = new_client_mgr_impl();
    ASSERT_TRUE(manager != nullptr);
    Status status = add_cluster(manager, cname, seeds);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    brpc::RedisResponse resp;

    // mset partial moved sync
    std::string mset_partial_moved_cmd = "mset";
    for (int i = 0; i < 100; ++i) {
        if (i == 5 || i == 8) {
            std::string moved_key = fmt::format("{}{}", S_MOVED_KEY_PREFIX, i);
            mset_partial_moved_cmd += fmt::format(" {} {}", moved_key, i);
        } else {
            mset_partial_moved_cmd += fmt::format(" {} {}", i, i);
        }
    }
    brpc::RedisRequest req;
    req.AddCommand(mset_partial_moved_cmd);
    client->exec(req, &resp);
    ASSERT_EQ(resp.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        ASSERT_STREQ(resp.reply(i).c_str(), "OK");
    }

    // mget partial moved sync
    brpc::RedisResponse resp2;
    brpc::RedisRequest req2;
    std::string mget_partial_moved_cmd = "mget";
    for (int i = 0; i < 100; ++i) {
        if (i == 5 || i == 8) {
            std::string moved_key = fmt::format("{}{}", S_MOVED_KEY_PREFIX, i);
            mget_partial_moved_cmd += fmt::format(" {}", moved_key);
        } else {
            mget_partial_moved_cmd += fmt::format(" {}", i);
        }
    }
    req2.AddCommand(mget_partial_moved_cmd);
    client->exec(req2, &resp2);
    ASSERT_EQ(resp2.reply_size(), 100);
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(resp2.reply(i).is_string());
        ASSERT_STREQ(std::to_string(i).c_str(), resp2.reply(i).c_str());
    }

    // exists partial moved sync
    brpc::RedisResponse resp3;
    brpc::RedisRequest req3;
    std::string exists_partial_moved_cmd = "exists";
    for (int i = 0; i < 100; ++i) {
        if (i == 5 || i == 8) {
            std::string moved_key = fmt::format("{}{}", S_MOVED_KEY_PREFIX, i);
            exists_partial_moved_cmd += fmt::format(" {}", moved_key);
        } else {
            exists_partial_moved_cmd += fmt::format(" {}", i);
        }
    }
    req3.AddCommand(exists_partial_moved_cmd);
    client->exec(req3, &resp3);
    ASSERT_EQ(resp3.reply_size(), 1);
    ASSERT_TRUE(resp3.reply(0).is_integer());
    ASSERT_EQ(100, resp3.reply(0).integer());

    std::condition_variable cv;
    std::mutex mtx;
    // mset partial moved async
    brpc::RedisResponse resp4;
    brpc::RedisRequest req4;
    req4.AddCommand(mset_partial_moved_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req4, &resp4, brpc::NewCallback(handle_mset_success, &resp4, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // mget partial moved async
    brpc::RedisResponse resp5;
    brpc::RedisRequest req5;
    req5.AddCommand(mget_partial_moved_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req5, &resp5, brpc::NewCallback(handle_mget_success, &resp5, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }

    // exists partial moved async
    brpc::RedisResponse resp6;
    brpc::RedisRequest req6;
    req6.AddCommand(exists_partial_moved_cmd);
    {
        std::unique_lock<std::mutex> lck(mtx);
        client->exec(req6, &resp6, brpc::NewCallback(handle_exists_success, &resp6, &cv));
        while (cv.wait_for(lck, std::chrono::seconds(30)) == std::cv_status::timeout) {
            LOG(ERROR) << "too long to wait for response";
            ASSERT_TRUE(false);
        }
    }
}

// TEST_F(ClientImplTest, RoundRobinRead) {
//     int server_cnt = 8;
//     int slave_per_server = 3;  // two groups, 4 nodes per group
//     std::string cname = build_cluster_name();
//     std::vector<std::string> seeds;
//     std::vector<MockRedisServerPtr> servers;
//     setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

//     ClientManagerPtr manager = new_client_mgr_impl();
//     ASSERT_TRUE(manager != nullptr);
//     Status status = add_cluster(manager, cname, seeds);
//     ASSERT_TRUE(status.ok()) << status;
//     AccessOptions opt;
//     opt.timeout_ms = 10;
//     opt._cname = cname;
//     opt.read_policy = ReadPolicy::ROUNDROBIN;
//     ClientPtr client = manager->new_client(opt);
//     ASSERT_TRUE(client != nullptr);
//     for (auto & server : servers) {
//         server->_service->_enable_follow_read = true;
//     }
//     for (int round = 0; round < 10; ++round) {
//         for (int i = 0; i < 100; ++i) {
//             std::string key = fmt::format("key_{}", i);
//             brpc::RedisResponse resp2;
//             client->exec(fmt::format("set {} {}", key, key), &resp2);
//             ASSERT_EQ(resp2.reply_size(), 1);
//             ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp2);
//             ASSERT_STREQ("OK", resp2.reply(0).c_str());

//             brpc::RedisResponse resp3;
//             client->exec(fmt::format("get {}", key), &resp3);
//             ASSERT_TRUE(resp3.reply(0).is_string() || resp3.reply(0).is_nil()) <<
//             rsdk::client::to_string(resp3);
//         }
//     }
//     int hit_cnt = 0;
//     for (auto & server : servers) {
//         if (server->_service->_get_cnt > 0) {
//             ++hit_cnt;
//         }
//     }
//     ASSERT_TRUE(hit_cnt > 2);
// }

// TEST_F(ClientImplTest, LargeCluster) {
//     int server_cnt = 960;
//     int slave_per_server = 0;
//     std::string cname = build_cluster_name();
//     std::vector<std::string> seeds;
//     std::vector<MockRedisServerPtr> servers;
//     setup_cluster(cname, server_cnt, slave_per_server, &servers, &seeds);

//     ClientManagerPtr manager = new_client_mgr_impl();
//     ASSERT_TRUE(manager != nullptr);
//     Status status = add_cluster(manager, cname, seeds);
//     ASSERT_TRUE(status.ok()) << status;
//     AccessOptions opt;
//     opt._cname = cname;
//     ClientPtr client = manager->new_client(opt);
//     ASSERT_TRUE(client != nullptr);
//     rand_put_get(client, 1000);
// }

} // namespace test
} // namespace client
} // namespace rsdk

DECLARE_string(flagfile);
DECLARE_int32(logbufsecs);
DECLARE_int32(v);
DECLARE_string(log_dir); // defined in glog

int main(int argc, char* argv[]) {
    FLAGS_v = 3;
    FLAGS_logbufsecs = 0;
    FLAGS_log_dir = "log";
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);

    ::mkdir(FLAGS_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::FATAL);

    int ret = RUN_ALL_TESTS();
    return ret;
}
