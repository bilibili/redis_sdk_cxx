#ifndef RSDK_API_INCLUDE_REDIS_SERVER_H
#define RSDK_API_INCLUDE_REDIS_SERVER_H

#include <map>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
#include <butil/logging.h>
#include <bvar/bvar.h>
#include <fmt/format.h>

#include "redis_sdk/api_status.h"
#include "redis_sdk/client.h"
#include "redis_sdk/include/api_util.h"

namespace rsdk {
namespace client {

enum class ServerRole {
    FOLLOWER = 0,
    LEADER = 1,
    FAIL = 2,
    NO_ADDR = 3,
    UNKNOWN = 999,
};

ServerRole str_to_role(const std::string& str);
std::string role_to_str(ServerRole role);
enum class LinkState {
    CONNECTED = 0,
    DIS_CONNECTED = 1,
    UNKNOWN = 999,
};
LinkState str_to_link_state(const std::string& str);
std::string link_state_to_str(LinkState state);

struct IOTask {
    int64_t _id = 0;
};

class RedisServer : public std::enable_shared_from_this<RedisServer> {
public:
    RedisServer(const std::string& addr) : _addr(addr) {
    }
    RedisServer(const std::string& uuid, const std::string& addr, ServerRole role,
                const std::string& lid, int64_t cepoch, LinkState lstate) :
            _id(uuid),
            _addr(addr),
            _role(role),
            _leader_id(lid),
            _config_epoch(cepoch),
            _link_state(lstate) {
    }
    virtual ~RedisServer();
    Status init(const RedisServerOptions& options);
    virtual void stop();
    virtual void join();

    template <typename CtxType>
    void send_cmd(CtxType* req);

    Status send_inner_cmd(const std::string& cmd, brpc::RedisResponse* resp, int index = 0);
    const std::string& addr() const {
        return _addr;
    }
    bool is_leader() const {
        return _role == ServerRole::LEADER;
    }
    void set_server_role(ServerRole r) {
        _role = r;
    }
    std::string id() const {
        return _id;
    }
    void set_leader_id(const std::string& lid) {
        _leader_id = lid;
    }
    // void set_epoch(int64_t e) {
    //     _config_epoch = e;
    // }
    int64_t config_epoch() const {
        return _config_epoch;
    }
    void set_linke_state(LinkState s) {
        _link_state = s;
    }
    bool update(const std::string& id, ServerRole role, const std::string& lid, int64_t epoch,
                LinkState lstate) {
        std::lock_guard<bthread::Mutex> lock(_update_mutex);
        if (!_id.empty() && _id != id) {
            LOG(ERROR) << "bug, try to update redis server id from " << _id << " to " << id
                       << " at:" << _addr;
            return false;
        }
        // TODO: 相等时，是否需要更新
        if (epoch < _config_epoch) {
            return false;
        }
        if (_id.empty()) {
            _id = id;
        }
        _role = role;
        _leader_id = lid;
        _config_epoch = epoch;
        _link_state = lstate;
        return true;
    }
    void ping();
    std::string to_str() const;
    RedisServerOptions options() const {
        return _options;
    }
    void set_online(bool on) {
        _online = on;
    }

    static ServerRole parse_role_state(const std::string& role_str);
    static int execute_io_tasks(void* meta, bthread::TaskIterator<IOTask>& iter);

public:
    RedisServerOptions _options;
    std::vector<std::unique_ptr<brpc::Channel>> _channels;
    std::unique_ptr<brpc::Channel> _slow_channel;
    std::string _id;
    std::string _addr;
    bthread::Mutex _update_mutex;
    ServerRole _role = {ServerRole::FOLLOWER};
    std::string _leader_id;
    int64_t _config_epoch = 0;
    LinkState _link_state = {LinkState::DIS_CONNECTED};

    bool _has_bg_worker = false;
    bthread_t _bg_ping_thread;
    int _ping_index = 0;
    bool _online = true;
};
typedef std::shared_ptr<RedisServer> RedisServerPtr;

class Node {
public:
    RedisServerPtr _master;
    std::vector<RedisServerPtr> _slaves;
};

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_REDIS_SERVER_H
