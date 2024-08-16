#ifndef RSDK_TEST_MOCK_NODE_H
#define RSDK_TEST_MOCK_NODE_H

#include <memory>
#include <vector>

#include <brpc/controller.h> // brpc::Controller
#include <brpc/redis.h>
#include <brpc/server.h>
#include <butil/crc32c.h>
#include <butil/strings/string_split.h>
#include <gflags/gflags.h>
#include <unordered_map>

#include <fmt/format.h>

#include <butil/time.h>

namespace rsdk {
namespace client {
namespace test {

struct Value {
    Value() {
    }
    Value(const std::string& v) : _value(v) {
    }
    Value(const std::string& v, int s) : _value(v), _ttl_at(s) {
    }
    bool timeout() const;
    std::string _value;
    int _ttl_at = 0;
};

class RedisServiceImpl : public brpc::RedisService {
public:
    bool set(const std::string& key, const std::string& value, int ttl, brpc::RedisReply* output);
    bool exists(const std::string& key, brpc::RedisReply* output);
    bool get(const std::string& key, brpc::RedisReply* output);
    bool cluster_nodes(brpc::RedisReply* output);
    bool handle_asking(brpc::RedisReply* output);
    void reset_put_cnt() {
    }
    void reset_get_cnt() {
    }
    void may_sleep();
    bool handle_sadd(const std::string& key, const std::vector<std::string>& params,
                     brpc::RedisReply* output);
    bool handle_smember(const std::string& key, brpc::RedisReply* output);

    std::atomic<bool> _running = {true};
    butil::Mutex _mutex;
    std::string _addr;
    std::string _leader_addr;
    std::unordered_map<int, std::string> _slot2addr;
    std::map<std::string, Value> _kv;
    std::map<std::string, std::set<std::string>> _sets;
    int _sleep_ms = 0;
    std::atomic<int> _sleep_cnt = {0};
    std::string _desc;
    std::string _cluster_nodes;
    std::string _cluster_nodes_status = "OK";
    std::string _crud_status = "OK";
    std::string _asking_status = "OK";
    std::string _mock_exist_ret;
    int _mock_status_cnt = 0;
    int _get_cnt = 0;
    bool _enable_follow_read = false;
    int _moved_cnt = 0;
    bool _always_moved = false;
};

class GetCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit GetCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class ExistsCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit ExistsCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class SetCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit SetCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class ClusterCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit ClusterCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class AskingCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit AskingCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};
class PingCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit PingCommandHandler() {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;
};

class SAddCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit SAddCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class SMembersCommandHandler : public brpc::RedisCommandHandler {
public:
    explicit SMembersCommandHandler(RedisServiceImpl* rsimpl) : _rsimpl(rsimpl) {
    }

    brpc::RedisCommandHandlerResult Run(const std::vector<butil::StringPiece>& args,
                                        brpc::RedisReply* output, bool /*flush_batched*/) override;

private:
    RedisServiceImpl* _rsimpl = nullptr;
};

class MockRedisServer {
public:
    MockRedisServer(int index) : _port(0), _index(index) {
    }
    ~MockRedisServer() {
        stop();
    }
    int start();
    void stop();
    int port() {
        return _port;
    }
    std::string addr() {
        return std::string("127.0.0.1:") + std::to_string(_port);
    }
    void reset_put_cnt() {
        _service->reset_put_cnt();
    }
    void reset_get_cnt() {
        _service->reset_get_cnt();
    }
    std::string name() {
        return addr();
    }
    void set_leader_name(const std::string& leader_name) {
        _leader_name = leader_name;
        _service->_leader_addr = leader_name;
    }
    void set_slot_ids(const std::vector<int>& slot_ids);
    void set_slot_dist(const std::string& slot_dist) {
        _slot_dist = slot_dist;
    }
    void set_slot2addr(const std::unordered_map<int, std::string>& slot2addr) {
        _service->_slot2addr = slot2addr;
    }
    std::string build_cluster_nodes();

public:
    RedisServiceImpl* _service = nullptr;
    brpc::Server _server;
    int _port = 0;
    int _index = 0;
    bool _runing = true;
    std::string _leader_name;
    std::set<int> _slot_ids;
    std::string _slot_dist;
    std::string _state = "connected";
    int _config_epoch = 0;
};

typedef std::shared_ptr<MockRedisServer> MockRedisServerPtr;
typedef std::vector<MockRedisServerPtr> MockRedisServerPtrList;

struct LeaderAndSlaves {
    MockRedisServerPtr _leader;
    MockRedisServerPtrList _slaves;
};
struct ClusterDist {
    std::vector<LeaderAndSlaves> _leaders_and_slaves;
};

} // namespace test
} // namespace client
} // namespace rsdk

#endif // RSDK_TEST_MOCK_NODE_H
