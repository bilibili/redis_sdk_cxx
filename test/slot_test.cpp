
#include "redis_sdk/include/slot.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "redis_sdk/client.h"

namespace rsdk {
namespace client {
uint32_t key_to_slot_id(const char* key, int keylen);
namespace test {

class SlotTest : public ::testing::Test {
public:
    SlotTest() {
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

TEST_F(SlotTest, UpdateServersTest) {
    std::string cname = "test_cluster";
    Slot slot = {cname, 3, 0};

    // update servers early return
    std::vector<RedisServerPtr> servers;
    RedisServerPtr leader = std::make_shared<RedisServer>("mock_server_0");
    servers.push_back(std::make_shared<RedisServer>("mock_server_1"));
    slot.update_servers(nullptr, servers, 1);
    ASSERT_EQ(slot._config_epoch, 0);
    slot.update_servers(leader, servers, -1);
    ASSERT_EQ(slot._config_epoch, 0);

    // update servers success
    slot.update_servers(leader, servers, 5);
    ASSERT_EQ(slot._config_epoch, 5);
}

TEST_F(SlotTest, UpdateSlotTest) {
    SlotMgr slot_mgr;
    std::string slot_range = "0-16383";
    std::vector<RedisServerPtr> servers;
    RedisServerPtr leader = std::make_shared<RedisServer>("mock_server_0");
    servers.push_back(std::make_shared<RedisServer>("mock_server_1"));

    // update slot no leader
    Status status = slot_mgr.update_slot(slot_range, nullptr, servers, 0);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("bug, invalid slot") != std::string::npos);

    // invalid slot range format
    std::string invalid_slot_0 = "1-0-16383";
    status = slot_mgr.update_slot(invalid_slot_0, leader, servers, 0);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("invalid slot range") != std::string::npos);

    // invalid slot range
    std::string invaid_slot_1 = "99999-12345678901234567890";
    status = slot_mgr.update_slot(invaid_slot_1, leader, servers, 0);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("invalid slot range") != std::string::npos);

    // invalid single slot
    std::string invalid_single_slot = "123abc";
    status = slot_mgr.update_slot(invalid_single_slot, leader, servers, 0);
    ASSERT_EQ(status.code(), Status::Code::SYS_ERROR);
    ASSERT_TRUE(status.msg().find("invalid slot") != std::string::npos);

    // valid slot
    std::string valid_slot = "11";
    status = slot_mgr.update_slot(valid_slot, leader, servers, 0);
    ASSERT_EQ(status.code(), Status::Code::OK);

    auto slot = std::make_shared<Slot>("", 0, 0);
    ASSERT_TRUE(slot_mgr.add_slot(slot));
    ASSERT_FALSE(slot_mgr.add_slot(slot));
}

TEST_F(SlotTest, GetServerTest) {
    std::string cname = "test_cluster";
    Slot slot = {cname, 3, 0};
    std::vector<RedisServerPtr> servers;
    RedisServerPtr leader = std::make_shared<RedisServer>("mock_server_0");
    servers.push_back(std::make_shared<RedisServer>("mock_server_1"));
    slot.update_servers(leader, servers, 5);

    ASSERT_EQ(slot.get_leader(), leader);
    ASSERT_EQ(slot.get_server_for_read(ReadPolicy::LEADER_ONLY), leader);

    slot._servers.clear();
    ASSERT_EQ(slot.get_server_for_read(ReadPolicy::LEADER_ONLY), leader);
}

TEST_F(SlotTest, SlotUtilTest) {
    SlotMgr slot_mgr;
    std::string slot_range = "0-16383";
    std::vector<RedisServerPtr> servers;
    RedisServerPtr leader = std::make_shared<RedisServer>("mock_server_0");
    servers.push_back(std::make_shared<RedisServer>("mock_server_1"));
    Status status = slot_mgr.update_slot(slot_range, leader, servers, 5);
    ASSERT_EQ(status.code(), Status::Code::OK);
    ASSERT_EQ(slot_mgr._slots.size(), 16384);
    uint32_t slot_id = key_to_slot_id("test_key", 8);
    ASSERT_EQ(slot_mgr._slots[slot_id], slot_mgr.get_slot_by_key("test_key"));

    Slot slot = {"cname", 3, 0};
    slot.update_servers(leader, servers, 5);
    std::string describe = slot.describe();
    ASSERT_EQ(describe, "id:3 sn:0 version:0 config_epoch:5 addr:mock_server_0");
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