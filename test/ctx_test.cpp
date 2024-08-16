
#include "redis_sdk/include/ctx.h"
#include <brpc/redis.h>
#include <butil/strings/string_split.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include "redis_sdk/api_status.h"
#include "redis_sdk/client.h"

namespace rsdk {
namespace client {
namespace test {

class CtxTest : public ::testing::Test {
public:
    CtxTest() {
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

TEST_F(CtxTest, ShouldRetryTest) {
    ClusterOptions opitons;
    opitons._name = "mock_cluster";
    auto cluster = std::make_shared<Cluster>(opitons);
    std::string normal_cmd = "get key";
    brpc::RedisRequest req;
    req.AddCommand(normal_cmd);
    brpc::RedisResponse resp;
    std::string key = "key";
    IOCtx ctx(cluster, "get", "get", std::move(key), std::move(req), &resp, nullptr);

    // time exhausted
    ctx._start_time_us = get_current_time_us() - 10000 * 1000L;
    RedisStatus move_status = RedisStatus::MOVED;
    ASSERT_FALSE(ctx.should_retry(move_status));

    // retry
    ctx._start_time_us = get_current_time_us();
    ASSERT_TRUE(ctx.should_retry(move_status));

    // has ask head
    ctx.add_ask_head();
    // again
    ctx.add_ask_head();
    ctx._start_time_us = get_current_time_us() + 10000 * 1000L;
    ASSERT_FALSE(ctx.should_retry(move_status));
}

TEST_F(CtxTest, HandleResponseTest) {
    ClusterOptions opitons;
    opitons._name = "mock_cluster";
    auto cluster = std::make_shared<Cluster>(opitons);
    std::string normal_cmd = "get key";
    std::string key = "key";
    brpc::RedisRequest req;
    req.AddCommand(normal_cmd);
    brpc::RedisResponse resp;
    resp._nreply = 1;
    resp._first_reply.SetError("MOVED MOVED MOVED");
    IOCtx* ctx = new IOCtx(cluster, "get", "get", std::move(key), std::move(req), &resp, nullptr);

    ctx->_start_time_us = get_current_time_us();
    ctx->_retry_cnt += 2;
    auto mock_server = std::make_shared<RedisServer>("mock_address");
    ctx->handle_resp(mock_server);
    const brpc::RedisReply& reply0 = resp.reply(0);
    ASSERT_TRUE(reply0.is_error());
    std::string msg = reply0.error_message();
    ASSERT_TRUE(msg.find("exceed max:2") != std::string::npos);
}

TEST_F(CtxTest, SkipAskHeadTest) {
    ClusterOptions opitons;
    opitons._name = "mock_cluster";
    auto cluster = std::make_shared<Cluster>(opitons);
    std::string normal_cmd = "get key";
    std::string key = "key";
    brpc::RedisRequest req;
    req.AddCommand(normal_cmd);

    brpc::RedisResponse resp;
    resp._nreply = 1;
    IOCtx ctx(cluster, "get", "get", std::move(key), std::move(req), &resp, nullptr);
    ctx.add_ask_head();
    ctx.may_skip_ask_head();
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
