#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include <brpc/redis_command.h>
#include <butil/rand_util.h>
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
uint32_t key_to_slot_id(const char* key, int keylen);

DECLARE_bool(log_request_verbose);
DEFINE_int64(key_cnt, 10000, "key count for test");
DEFINE_int32(conn_per_node, 1, "connection count per node");
DEFINE_string(seed_node, "127.0.0.1:21010", "seed node of cluster");

namespace test {

class ReadEnvTest : public ::testing::Test {
public:
    ReadEnvTest() {
    }
    ~ReadEnvTest() {
    }
    static void SetUpTestCase() {
        int ret = system("rm -rf ./log/");
        ret = system("mkdir -p ./log/");
        (void)ret;
    }
    void SetUp() {
        // FLAGS_log_request_verbose = true;
    }
    void TearDown() {
    }
    std::string build_cluster_name() {
        ++_cluster_id;
        return "cluster" + std::to_string(_cluster_id);
    }
    int _cluster_id = 0;
};

TEST_F(ReadEnvTest, MgetMset) {
    std::vector<std::string> seeds;
    seeds.push_back(FLAGS_seed_node);

    std::string cname = "local_test";
    ClientManagerPtr manager = new_client_mgr();
    ASSERT_TRUE(manager != nullptr);
    ClusterOptions coptions;
    coptions._name = cname;
    coptions._seeds = seeds;
    coptions._server_options._conn_per_node = 1;
    coptions._server_options._conn_hc_interval_ms = 10;
    Status status = manager->add_cluster(coptions);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt.timeout_ms = 50;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);

    int batch_cnt = 200;
    int ikey = 0;
    std::vector<brpc::RedisResponse> resps;
    RecordList records;
    std::vector<std::string> expects;
    {
        for (int x = 0; x < batch_cnt; ++x) {
            Record record;
            record._key = fmt::format("rec_key_{}", ikey);
            record._data = fmt::format("rec_value_{}", ikey);
            expects.push_back(record._data);
            records.emplace_back(record);
            ++ikey;
        }
        client->mset(records, &resps);
        ASSERT_EQ(resps.size(), batch_cnt);
        for (int x = 0; x < batch_cnt; ++x) {
            const brpc::RedisReply& reply = resps[x].reply(0);
            ASSERT_TRUE(reply.is_string()) << reply.type();
            ASSERT_STREQ("OK", reply.c_str());
        }
    }
    //
    {
        client->mget(records, &resps);
        ASSERT_EQ(resps.size(), batch_cnt);
        for (int x = 0; x < batch_cnt; ++x) {
            const brpc::RedisReply& sub = resps[x].reply(0);
            ASSERT_TRUE(sub.is_string()) << rsdk::client::to_string(resps[x]);
            std::string value = sub.c_str();
            std::string expect = expects[x];
            ASSERT_EQ(expect, value);
        }
    }
}

TEST_F(ReadEnvTest, BatchAsArray) {
    std::vector<std::string> seeds;
    seeds.push_back(FLAGS_seed_node);

    std::string cname = "local_test";
    ClientManagerPtr manager = new_client_mgr();
    ASSERT_TRUE(manager != nullptr);
    ClusterOptions coptions;
    coptions._name = cname;
    coptions._seeds = seeds;
    coptions._server_options._conn_per_node = 1;
    coptions._server_options._conn_hc_interval_ms = 10;
    Status status = manager->add_cluster(coptions);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt.timeout_ms = 50;
    opt._cname = cname;
    opt._merge_batch_as_array = true;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);

    int batch_cnt = 10;
    int ikey = 0;
    for (int i = 0; i < 1; ++i) {
        brpc::RedisResponse resp;
        std::string cmd = "mset";
        for (int x = 0; x < batch_cnt; ++x) {
            cmd += fmt::format(" key_{} value_{}", ikey, ikey);
            ++ikey;
        }
        client->exec(cmd, &resp);
        ASSERT_EQ(resp.reply_size(), 1);
        ASSERT_EQ(resp.reply(0).size(), batch_cnt);
        ASSERT_TRUE(resp.reply(0).is_array()) << rsdk::client::to_string(resp);
        for (int x = 0; x < batch_cnt; ++x) {
            const brpc::RedisReply& sub = resp.reply(0)[x];
            ASSERT_TRUE(sub.is_string() || sub.is_error()) << sub.type();
            if (sub.is_string()) {
                ASSERT_STREQ("OK", sub.c_str());
            }
        }
    }
    ikey = 0;
    for (int i = 0; i < 1; ++i) {
        brpc::RedisResponse resp;
        std::string cmd = "mget";
        std::vector<std::string> expects;
        for (int x = 0; x < batch_cnt; ++x) {
            std::string tmp_cmd = fmt::format(" key_{}", ikey);
            cmd += tmp_cmd;
            std::string value = fmt::format("value_{}", ikey);
            ++ikey;
            expects.push_back(value);
        }
        client->exec(cmd, &resp);
        ASSERT_EQ(resp.reply_size(), 1) << rsdk::client::to_string(resp);
        ASSERT_EQ(resp.reply(0).size(), batch_cnt) << rsdk::client::to_string(resp);
        ASSERT_TRUE(resp.reply(0).is_array()) << rsdk::client::to_string(resp);
        for (int x = 0; x < batch_cnt; ++x) {
            const brpc::RedisReply& sub = resp.reply(0)[x];
            ASSERT_TRUE(sub.is_string() || sub.is_error()) << rsdk::client::to_string(resp);
            if (sub.is_string()) {
                std::string value = sub.c_str();
                std::string expect = expects[x];
                ASSERT_EQ(expect, value);
            }
        }
    }
    ikey = 0;
    for (int i = 0; i < 1; ++i) {
        brpc::RedisResponse resp;
        std::string cmd = "mget";
        std::vector<std::string> expects;
        for (int x = 0; x < batch_cnt; ++x) {
            std::string tmp_cmd = fmt::format(" not_key_{}", ikey);
            cmd += tmp_cmd;
            ++ikey;
        }
        client->exec(cmd, &resp);
        ASSERT_EQ(resp.reply_size(), 1) << rsdk::client::to_string(resp);
        ASSERT_EQ(resp.reply(0).size(), batch_cnt) << rsdk::client::to_string(resp);
        ASSERT_TRUE(resp.reply(0).is_array()) << rsdk::client::to_string(resp);
        for (int x = 0; x < batch_cnt; ++x) {
            const brpc::RedisReply& sub = resp.reply(0)[x];
            ASSERT_TRUE(sub.is_nil() || sub.is_error()) << rsdk::client::to_string(resp);
        }
    }
}

TEST_F(ReadEnvTest, ReadServerWithMove) {
    std::vector<std::string> seeds;
    seeds.push_back(FLAGS_seed_node);

    std::string cname = "local_test";
    ClientManagerPtr manager = new_client_mgr();
    ASSERT_TRUE(manager != nullptr);
    ClusterOptions coptions;
    coptions._name = cname;
    coptions._seeds = seeds;
    coptions._server_options._conn_per_node = 1;
    coptions._server_options._conn_hc_interval_ms = 10;
    Status status = manager->add_cluster(coptions);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt.timeout_ms = 10;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);

    for (int i = 0; i < 100; ++i) {
        std::string key = fmt::format("key_{}", i);
        uint32_t slot_id = key_to_slot_id(key.c_str(), key.length());
        update_slot_pointer(cname, manager, slot_id);
    }

    for (int i = 0; i < 10000; ++i) {
        brpc::RedisResponse resp2;
        std::string key = fmt::format("key_{}", i);
        client->exec(fmt::format("set {} {}", key, key), &resp2);
        ASSERT_EQ(resp2.reply_size(), 1);
        ASSERT_TRUE(resp2.reply(0).is_string() || resp2.reply(0).is_error())
            << rsdk::client::to_string(resp2);
        if (resp2.reply(0).is_string()) {
            ASSERT_STREQ("OK", resp2.reply(0).c_str());
        }
    }
    for (int i = 0; i < 100; ++i) {
        std::string key = fmt::format("key_{}", i);
        uint32_t slot_id = key_to_slot_id(key.c_str(), key.length());
        update_slot_pointer(cname, manager, slot_id);
    }
    RecordList records;
    for (int i = 0; i < 1000; ++i) {
        Record rec;
        rec._errno = -100;
        rec._key = fmt::format("key_{}", i);
        records.push_back(std::move(rec));
    }
    client->mget(&records);
    for (auto& rec : records) {
        ASSERT_EQ(0, rec._errno) << rec._err_msg;
        ASSERT_STREQ(rec._key.c_str(), rec._data.c_str());
    }
}

void work_thread(int id, ClientPtr client, int64_t start_key, int cnt) {
    TimeCost cost;
    int64_t ikey = start_key;
    for (int i = 0; i < cnt; ++i) {
        brpc::RedisResponse resp2;
        std::string key = fmt::format("key_{}", ikey);
        client->exec(fmt::format("set {} {}", key, key), &resp2);
        ASSERT_EQ(resp2.reply_size(), 1);
        ASSERT_TRUE(resp2.reply(0).is_string() || resp2.reply(0).is_error())
            << rsdk::client::to_string(resp2);
        if (resp2.reply(0).is_string()) {
            EXPECT_STREQ("OK", resp2.reply(0).c_str());
        }
        ++ikey;
    }

    LOG(INFO) << "thread:" << id << " use " << cost.cost_us() << "us to put " << cnt << " keys";
}

TEST_F(ReadEnvTest, MultiThread) {
    std::vector<std::string> seeds;
    seeds.push_back(FLAGS_seed_node);

    std::string cname = "multi_thread";
    ClientManagerPtr manager = new_client_mgr();
    ASSERT_TRUE(manager != nullptr);
    ClusterOptions coptions;
    coptions._name = cname;
    coptions._seeds = seeds;
    coptions._server_options._conn_per_node = FLAGS_conn_per_node;
    coptions._server_options._conn_hc_interval_ms = 10;
    Status status = manager->add_cluster(coptions);
    ASSERT_TRUE(status.ok()) << status;
    AccessOptions opt;
    opt.timeout_ms = 100;
    opt._cname = cname;
    ClientPtr client = manager->new_client(opt);
    ASSERT_TRUE(client != nullptr);
    int thread_cnt = 10;
    std::vector<std::thread> threads;

    for (int i = 0; i < thread_cnt; ++i) {
        std::thread th(work_thread, i, client, 0, FLAGS_key_cnt);
        threads.push_back(std::move(th));
    }

    for (auto& th : threads) {
        th.join();
    }
}

// TEST_F(ReadEnvTest, RoundRobinRead) {
//     std::vector<std::string> seeds;
//     seeds.push_back("127.0.0.1:21010");
//     seeds.push_back("127.0.0.1:21011");
//     seeds.push_back("127.0.0.1:21020");
//     seeds.push_back("127.0.0.1:21021");
//     seeds.push_back("127.0.0.1:21030");
//     seeds.push_back("127.0.0.1:21031");

//     std::string cname = "local_test";
//     ClientManagerPtr manager = new_client_mgr();
//     ASSERT_TRUE(manager != nullptr);
//     ClusterOptions coptions;
//     coptions._name = cname;
//     coptions._seeds = seeds;
//     coptions._server_options._conn_per_node = 1;
//     coptions._server_options._conn_hc_interval_ms = 10;
//     Status status = manager->add_cluster(coptions);
//     ASSERT_TRUE(status.ok()) << status;
//     AccessOptions opt;
//     opt.timeout_ms = 10;
//     opt._cname = cname;
//     opt.read_policy = ReadPolicy::ROUNDROBIN;
//     ClientPtr client = manager->new_client(opt);
//     ASSERT_TRUE(client != nullptr);

//     for (int round = 0; round < 10; ++round) {
//         for (int i = 0; i < 20000; ++i) {
//             brpc::RedisResponse resp2;
//             std::string key = fmt::format("key_{}", i);
//             if (round == 0) {
//                 client->exec(fmt::format("set {} {}", key, key), &resp2);
//                 ASSERT_EQ(resp2.reply_size(), 1);
//                 ASSERT_TRUE(resp2.reply(0).is_string()) << rsdk::client::to_string(resp2);
//                 ASSERT_STREQ("OK", resp2.reply(0).c_str());
//             }

//             brpc::RedisResponse resp3;
//             client->exec(fmt::format("get {}", key), &resp3);
//             ASSERT_TRUE(resp3.reply(0).is_string()) << rsdk::client::to_string(resp3);
//             ASSERT_STREQ(key.c_str(), resp3.reply(0).c_str());
//         }
//     }
//     // while (true) {
//     //     usleep(100000);
//     // }
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
    rsdk::client::FLAGS_log_request_verbose = true;
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);

    ::mkdir(FLAGS_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::FATAL);

    int ret = RUN_ALL_TESTS();
    return ret;
}
