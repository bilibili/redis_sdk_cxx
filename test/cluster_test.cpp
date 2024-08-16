

#include "cluster.h"
#include <brpc/redis.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include "ctx.h"
#include "redis_sdk/api_status.h"
#include "redis_sdk/client.h"
#include "redis_sdk/include/api_util.h"
#include "redis_sdk/include/redis_server.h"
#include "test/mock_node.h"

namespace rsdk {
namespace client {

void log_response(const brpc::RedisResponse& resp);
void* period_update_dist(void* args);
std::string to_string(const brpc::RedisResponse& resp);
std::string redis_status_to_str(const RedisStatus& s);
RedisStatus parse_redis_error(const std::string& str);
int64_t rand_logid();
void log_reply(const brpc::RedisReply& reply);
uint32_t key_to_slot_id(const char* key, int keylen);

namespace test {

class ClusterTest : public ::testing::Test {
public:
    ClusterTest() {
    }
    static void SetUpTestCase() {
        int ret = system("rm -rf ./log/");
        ret = system("mkdir -p ./log/");
        (void)ret;
    }
    void SetUp() {
    }
    void TearDown() {
    }
};

TEST_F(ClusterTest, ParseNodeTest) {
    ClusterOptions options;
    options._name = "mock_cluster";
    Cluster cluster{options};

    // parse normal cluster nodes data
    std::string normal_cluster_nodes_data =
        "00000000d724c55d3c2b4f1bb180cff306145bf3 172.23.47.15:8958@18958 slave "
        "34f9cb515b9790e645090deda73c5be0439cf73a 0 1690389971000 41 connected";
    ClusterNodeState state;
    Status status = cluster.parse_node(normal_cluster_nodes_data, &state);
    ASSERT_EQ(status.code(), Status::Code::OK);

    // 0@0 ip port
    std::string cluster_nodes_date_contain_0 = "00000000d724c55d3c2b4f1bb180cff306145bf3 :0@0 "
                                               "slave "
                                               "34f9cb515b9790e645090deda73c5be0439cf73a 0 "
                                               "1690389971000 41 connected";
    status = cluster.parse_node(cluster_nodes_date_contain_0, &state);
    ASSERT_EQ(status.code(), Status::Code::E_AGAIN);

    // invaild epoch
    std::string invalid_cluster_nodes_epoch =
        "00000000d724c55d3c2b4f1bb180cff306145bf3 172.23.47.15:8958@18958 slave "
        "34f9cb515b9790e645090deda73c5be0439cf73a 0 1690389971000 922337203685477581109 connected";
    status = cluster.parse_node(invalid_cluster_nodes_epoch, &state);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("invalid epoch") != std::string::npos);

    // invalid size
    std::string invalid_cluster_nodes_size_0 = "172.23.47.15:8958@18958 slave";
    status = cluster.parse_node(invalid_cluster_nodes_size_0, &state);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("at least 8") != std::string::npos);

    // size greater 8
    std::string normal_cluster_nodes_size_1 = "f54ea16ac67b14c448f8f55e43e793e4af559d85 "
                                              ":6379@16379 myself,master - 0 0 1 connected 0-13 "
                                              "14-18";
    status = cluster.parse_node(normal_cluster_nodes_size_1, &state);
    ASSERT_EQ(status.code(), Status::Code::OK);
}

TEST_F(ClusterTest, ParseClusterNodesTest) {
    ClusterOptions options;
    options._name = "mock_cluster";
    Cluster cluster{options};
    brpc::RedisResponse resp;
    butil::StringPiece mock_cluster_nodes_resp =
        "31a146f6fe2f0386e0e3899e82a0272bf3e537b8 127.0.0.1:7004@17004 slave "
        "0a45d695e922c2ce5ca7bfd9505097b989533eb9 0 1691063472000 5 "
        "connected\n66391d2bcfa1d1ba857f34a26636880969a3fdd7 127.0.0.1:7005@17005 slave "
        "98a595f26ad440ff9f7fa09e86fd6227053d0abe 0 1691063473450 6 "
        "connected\n1c87b535c3c03ddeb6a2a3d2caf1de8fe8cb0e24 127.0.0.1:7002@17002 master - 0 "
        "1691063471447 3 connected 10923-16383\n7bcb98e26e2b1dbe9a79f8fcf8244448d79a6c27 "
        "127.0.0.1:7003@17003 slave 1c87b535c3c03ddeb6a2a3d2caf1de8fe8cb0e24 0 1691063473000 4 "
        "connected\n0a45d695e922c2ce5ca7bfd9505097b989533eb9 127.0.0.1:7000@17000 myself,master - "
        "0 1691063471000 1 connected 0-5460\n98a595f26ad440ff9f7fa09e86fd6227053d0abe "
        "127.0.0.1:7001@17001 master - 0 1691063474452 2 connected 5461-10922\n";
    resp._first_reply.SetString(mock_cluster_nodes_resp);
    resp._nreply = 1;
    ASSERT_EQ(resp.reply_size(), 1);
    ASSERT_TRUE(resp.reply(0).is_string());
    SlotMgr slot_mgr;
    SlotMgrPtr slot_mgr_ptr = std::make_shared<SlotMgr>();
    // normal
    Status status = cluster.parse_cluster_nodes(resp, slot_mgr_ptr);
    ASSERT_EQ(status.code(), Status::Code::OK);

    // cluster nodes state parse error
    butil::StringPiece invalid_cluster_nodes_str =
        "00000000d724c55d3c2b4f1bb180cff306145bf3 :0@0 slave "
        "34f9cb515b9790e645090deda73c5be0439cf73a 0 1690389971000 41 "
        "connected\n66391d2bcfa1d1ba857f34a26636880969a3fdd7\n";
    brpc::RedisResponse invalid_cluster_nodes_resp;
    invalid_cluster_nodes_resp._first_reply.SetString(invalid_cluster_nodes_str);
    invalid_cluster_nodes_resp._nreply = 1;
    status = cluster.parse_cluster_nodes(invalid_cluster_nodes_resp, slot_mgr_ptr);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("at least 8") != std::string::npos);

    // empty state
    butil::StringPiece empty_cluster_nodes_str = "00000000d724c55d3c2b4f1bb180cff306145bf3 :0@0 "
                                                 "slave 34f9cb515b9790e645090deda73c5be0439cf73a 0 "
                                                 "1690389971000 41 connected\n";
    brpc::RedisResponse empty_cluster_nodes_resp;
    empty_cluster_nodes_resp._first_reply.SetString(empty_cluster_nodes_str);
    empty_cluster_nodes_resp._nreply = 1;
    status = cluster.parse_cluster_nodes(empty_cluster_nodes_resp, slot_mgr_ptr);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("response is empty") != std::string::npos);

    // invalid response size
    brpc::RedisResponse error_resp;
    status = cluster.parse_cluster_nodes(error_resp, nullptr);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("response expected") != std::string::npos);

    // error response
    set_error_to_resp(&error_resp, "error msg");
    status = cluster.parse_cluster_nodes(error_resp, nullptr);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("expected to be a string") != std::string::npos);
}

TEST_F(ClusterTest, RefreshDistTest) {
    ClusterOptions options;
    options._name = "mock_cluster";
    Cluster cluster(options);
    // skip refresh cluster
    cluster._last_refresh_time_ms = get_current_time_ms() + 100;
    Status status = cluster.refresh_dist(false);
    ASSERT_EQ(status.code(), Status::Code::OK);

    // refresh dist actively
    cluster._last_refresh_time_ms = 0;
    ASSERT_TRUE(cluster.may_update_dist(RedisStatus::MOVED, "msg"));

    // refresh_cluster_dist_by_cluster_nodes error
    status = cluster.refresh_cluster_dist_by_cluster_nodes("address_not_found", nullptr);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("failed to init") != std::string::npos);
}

TEST_F(ClusterTest, UpdateClusterDistTest) {
    ClusterOptions options;
    options._name = "mock_cluster";
    Cluster* cluster = new Cluster(options);
    SlotMgrPtr slot_mgr_ptr = std::make_shared<SlotMgr>();
    ASSERT_EQ(bthread_start_background(&(cluster->_bg_update_dist_thread), nullptr,
                                       period_update_dist, cluster),
              0);
    cluster->mutable_refresh_cond()->notify_all();
    cluster->may_triggle_update_dist();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    cluster->_has_bg_update_dist_thread = true;
    delete cluster;
}

TEST_F(ClusterTest, ClusterManagerTest) {
    ClusterManager cluster_mgr;
    ClusterOptions options;
    options._name = "mock_cluster_0";

    // add cluster
    auto status = cluster_mgr.add_cluster(options);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    cluster_mgr._clusters.Modify(ClusterManager::add_cluster_impl,
                                 std::make_shared<Cluster>(options));
    status = cluster_mgr.add_cluster(options);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("exist") != std::string::npos);

    // get cluster
    auto cluster_ptr = cluster_mgr.get("cluster_not_found");
    ASSERT_EQ(cluster_ptr, nullptr);
    cluster_ptr = cluster_mgr.get("mock_cluster_0");
    ASSERT_TRUE(cluster_ptr != nullptr);

    // remove cluster
    bool ret = cluster_mgr.remove("mock_cluster_0");
    ASSERT_TRUE(ret);
    ret = cluster_mgr.remove("cluster_not_found");
    ASSERT_TRUE(ret);
}

TEST_F(ClusterTest, GetNodeTest) {
    ClusterOptions options;
    options._name = "mock_cluster";
    Cluster cluster{options};
    auto server = cluster.add_and_get_node("invalid_address");
    ASSERT_EQ(server, nullptr);

    // get node
    auto mock_server = std::make_shared<RedisServer>("mock_address");
    cluster._ip2nodes["mock_ip"] = mock_server;
    server = cluster.get_node("mock_ip");
    ASSERT_EQ(server, mock_server);
    server = cluster.get_node("ip_not_found");
    ASSERT_EQ(server, nullptr);

    // node state
    ClusterNodeState node_state;
    node_state._id = "008";
    ASSERT_TRUE(node_state.to_str().find("008") != std::string::npos);
}

TEST_F(ClusterTest, RedisSeverUtilTest) {
    // str_to_role
    ASSERT_EQ(ServerRole::LEADER, str_to_role("master"));
    ASSERT_EQ(ServerRole::FOLLOWER, str_to_role("slave"));
    ASSERT_EQ(ServerRole::FAIL, str_to_role("fail"));
    ASSERT_EQ(ServerRole::NO_ADDR, str_to_role("noaddr"));
    ASSERT_EQ(ServerRole::UNKNOWN, str_to_role("unknown"));

    // role_to_str
    ASSERT_EQ("master", role_to_str(ServerRole::LEADER));
    ASSERT_EQ("slave", role_to_str(ServerRole::FOLLOWER));
    ASSERT_EQ("fail", role_to_str(ServerRole::FAIL));
    ASSERT_EQ("noaddr", role_to_str(ServerRole::NO_ADDR));
    ASSERT_EQ("unknown", role_to_str(ServerRole::UNKNOWN));

    // parse_role_state
    RedisServer server("mock_addr");
    ASSERT_EQ(ServerRole::LEADER, server.parse_role_state("master"));
    ASSERT_EQ(ServerRole::FOLLOWER, server.parse_role_state("slave"));
    ASSERT_EQ(ServerRole::FAIL, server.parse_role_state("fail"));
    ASSERT_EQ(ServerRole::NO_ADDR, server.parse_role_state("noaddr"));

    // link state
    ASSERT_EQ(LinkState::CONNECTED, str_to_link_state("connected"));
    ASSERT_EQ(LinkState::DIS_CONNECTED, str_to_link_state("disconnected"));
    ASSERT_EQ(LinkState::UNKNOWN, str_to_link_state("unknown"));

    ASSERT_EQ("connected", link_state_to_str(LinkState::CONNECTED));
    ASSERT_EQ("disconnected", link_state_to_str(LinkState::DIS_CONNECTED));
    ASSERT_EQ("unknown", link_state_to_str(LinkState::UNKNOWN));

    // redis_status_to_str
    ASSERT_EQ(S_STATUS_OK, redis_status_to_str(RedisStatus::OK));
    ASSERT_EQ(S_STATUS_MOVED, redis_status_to_str(RedisStatus::MOVED));
    ASSERT_EQ(S_STATUS_ASK, redis_status_to_str(RedisStatus::ASK));
    ASSERT_EQ(S_STATUS_TRYAGAIN, redis_status_to_str(RedisStatus::TRY_AGAIN));
    ASSERT_EQ(S_STATUS_CROSSSLOT, redis_status_to_str(RedisStatus::CROSS_SLOT));
    ASSERT_EQ(S_STATUS_CLUSTERDOWN, redis_status_to_str(RedisStatus::CLUSTER_DOWN));

    // parse_redis_error
    ASSERT_EQ(RedisStatus::OK, parse_redis_error(S_STATUS_OK));
    ASSERT_EQ(RedisStatus::MOVED, parse_redis_error(S_STATUS_MOVED));
    ASSERT_EQ(RedisStatus::ASK, parse_redis_error(S_STATUS_ASK));
    ASSERT_EQ(RedisStatus::TRY_AGAIN, parse_redis_error(S_STATUS_TRYAGAIN));
    ASSERT_EQ(RedisStatus::CROSS_SLOT, parse_redis_error(S_STATUS_CROSSSLOT));
    ASSERT_EQ(RedisStatus::CLUSTER_DOWN, parse_redis_error(S_STATUS_CLUSTERDOWN));

    // rand_logid
    rand_logid();
}

TEST_F(ClusterTest, ClusterUtilTest) {
    // redis respone to string
    brpc::RedisResponse err_resp;
    butil::StringPiece err_msg = "error";
    err_resp._nreply = 1;
    err_resp._first_reply.SetError(err_msg);
    ASSERT_EQ("error: cnt:1 first:error", to_string(err_resp));

    brpc::RedisResponse int_resp;
    int_resp._nreply = 1;
    int_resp._first_reply.SetInteger(111);
    ASSERT_EQ("integer cnt:1 first:111", to_string(int_resp));

    brpc::RedisResponse str_resp;
    str_resp._nreply = 1;
    str_resp._first_reply.SetString("test str");
    ASSERT_EQ("str: cnt:1 first:test str", to_string(str_resp));

    brpc::RedisResponse array_resp;
    array_resp._nreply = 1;
    array_resp._first_reply.SetArray(3);
    array_resp._first_reply[0].SetInteger(1);
    array_resp._first_reply[1].SetInteger(11);
    array_resp._first_reply[2].SetInteger(111);
    ASSERT_EQ("cnt:1 first type:array", to_string(array_resp));

    // key_to_slot_id
    ASSERT_EQ(11111, key_to_slot_id("{test_tag}test_key", 18));
    ASSERT_EQ(7159, key_to_slot_id("{test_tagtest_key", 17));

    // time
    uint64_t current_time_us = get_current_time_us();
    ASSERT_GT(current_time_us, 0);
    uint64_t current_time_s = get_current_time_s();
    ASSERT_GT(current_time_s, 0);

    // log reply
    brpc::RedisResponse resp;
    log_response(resp);
    brpc::RedisResponse error_resp;
    set_error_to_resp(&error_resp, "error_msg");
    log_response(error_resp);
    resp._first_reply.SetInteger(1);
    log_reply(resp._first_reply);
    resp._first_reply.SetArray(2);
    resp._first_reply[0].SetInteger(0);
    resp._first_reply[1].SetInteger(1);
    log_reply(resp._first_reply);
    resp._first_reply.SetString("test_string");
    log_reply(resp._first_reply);
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
