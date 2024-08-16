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

class RedirectTest : public ::testing::Test {
public:
    RedirectTest() {
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
    void build_mset(RecordList* recs, int cnt, int moved_percent);
    void check_mset_resp(int cnt, const RecordList& records);
    int _cluster_id = 0;
};

void RedirectTest::build_mset(RecordList* recs, int cnt, int moved_percent) {
    for (int i = 0; i < cnt; ++i) {
        std::string key = fmt::format("{}", i);
        int ratio = butil::fast_rand() % 100;
        if (ratio < moved_percent) {
            key = "moved_key_" + key;
        }
        Record rec;
        rec._key = key;
        rec._data = key;
        rec._errno = -100;
        recs->push_back(rec);
    }
}

void RedirectTest::check_mset_resp(int cnt, const RecordList& records) {
    for (int i = 0; i < cnt; ++i) {
        const Record& rec = records[i];
        ASSERT_EQ(rec._errno, 0) << rec._err_msg;
    }
}

TEST_F(RedirectTest, HandleMovedInMgetMset) {
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

    RecordList recs;
    int cnt = 500;
    build_mset(&recs, cnt, 0);
    Record moved;
    moved._key = key;
    moved._data = key;
    recs.push_back(moved);
    client->mset(&recs);
    ASSERT_NO_FATAL_FAILURE(check_mset_resp(cnt, recs));
    ASSERT_EQ(0, recs[cnt]._errno) << recs[cnt]._err_msg;

    update_slot_pointer(cname, manager, slot_id);
    std::vector<brpc::RedisResponse> resps;
    client->mget(recs, &resps);
    ASSERT_EQ(recs.size(), resps.size());
    for (size_t i = 0; i < resps.size(); ++i) {
        brpc::RedisResponse resp = resps[i];
        ASSERT_TRUE(resp.reply(0).is_string());
        std::string data = resp.reply(0).c_str();
        ASSERT_STREQ(data.c_str(), recs[i]._key.c_str());
    }
}

TEST_F(RedirectTest, PartialMovedToFailed) {
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
    { servers[0]->_service->_always_moved = true; }
    RecordList recs;
    int cnt = 500;
    build_mset(&recs, cnt, 50);

    std::vector<brpc::RedisResponse> resps;
    client->mset(recs, &resps);
    ASSERT_EQ(resps.size(), recs.size());

    int failed_cnt = 0;
    int succ_cnt = 0;
    for (auto& resp : resps) {
        ASSERT_EQ(resp.reply_size(), 1);
        if (resp.reply(0).is_error()) {
            std::string err_msg = resp.reply(0).error_message();
            VLOG(3) << "got error msg:" << err_msg;
            ASSERT_TRUE(err_msg.find("MOVED ") != std::string::npos) << err_msg;
            ASSERT_TRUE(err_msg.find("exceed max:") != std::string::npos) << err_msg;
            ++failed_cnt;
        } else {
            ++succ_cnt;
        }
    }
    ASSERT_TRUE(failed_cnt > 0);
    ASSERT_TRUE(succ_cnt > 0);
    ASSERT_EQ(cnt, failed_cnt + succ_cnt) << "failed:" << failed_cnt << "succ:" << succ_cnt;
}

TEST_F(RedirectTest, PartialMovedRetryToSucc) {
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
    {
        servers[0]->_service->_mock_status_cnt = opt.max_redirect;
        servers[0]->_service->_crud_status = "MOVED";
    }
    RecordList recs;
    int cnt = 500;
    build_mset(&recs, cnt, 50);

    client->mset(&recs);
    for (auto& rec : recs) {
        ASSERT_EQ(0, rec._errno) << rec._err_msg;
    }
    clear_value(&recs);
    client->mget(&recs);

    {
        servers[0]->_service->_mock_status_cnt = opt.max_redirect;
        servers[0]->_service->_crud_status = "MOVED";
    }
    for (auto& rec : recs) {
        ASSERT_EQ(0, rec._errno) << rec._err_msg;
        ASSERT_STREQ(rec._key.c_str(), rec._data.c_str());
    }
}

TEST_F(RedirectTest, MeetAskOnPipeline) {
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
    {
        servers[0]->_service->_mock_status_cnt = 10;
        servers[0]->_service->_crud_status = "ASK";
    }
    RecordList recs;
    int cnt = 500;
    build_mset(&recs, cnt, 0);

    client->mset(&recs);

    for (auto& rec : recs) {
        ASSERT_EQ(0, rec._errno) << rec._err_msg;
    }
    clear_value(&recs);
    client->mget(&recs);

    {
        servers[0]->_service->_mock_status_cnt = 20;
        servers[0]->_service->_crud_status = "ASK";
    }
    for (auto& rec : recs) {
        ASSERT_EQ(0, rec._errno) << rec._err_msg;
        ASSERT_STREQ(rec._key.c_str(), rec._data.c_str());
    }
}

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
