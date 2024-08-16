#include <map>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <brpc/policy/hasher.h>
#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <butil/strings/string_split.h>
#include <butil/strings/string_util.h>
#include <fmt/format.h>

#include "redis_sdk/include/ctx.h"
#include "redis_sdk/include/redis_server.h"

namespace brpc {
DECLARE_bool(redis_verbose_crlf2space);
} // namespace brpc

namespace rsdk {
namespace client {

DEFINE_bool(log_request_verbose, false, "log request verbose");
DEFINE_bool(feature_enable_bg_ping, true, "enable bg ping for node");
DEFINE_int32(bg_cmd_timeout_ms, 3000, "timeout for bg cmd");

BRPC_VALIDATE_GFLAG(log_request_verbose, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(feature_enable_bg_ping, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(bg_cmd_timeout_ms, brpc::PassValidate);

static const int S_API_VERSION = 100;

int64_t rand_logid() {
    return (int64_t)butil::fast_rand();
}
std::string to_string(const brpc::RedisResponse& resp);
ServerRole str_to_role(const std::string& str) {
    if (str == "master") {
        return ServerRole::LEADER;
    }
    if (str == "slave") {
        return ServerRole::FOLLOWER;
    }
    if (str == "fail") {
        return ServerRole::FAIL;
    }
    if (str == "noaddr") {
        return ServerRole::NO_ADDR;
    }
    return ServerRole::UNKNOWN;
}
std::string role_to_str(ServerRole role) {
    switch (role) {
    case ServerRole::LEADER:
        return "master";
    case ServerRole::FOLLOWER:
        return "slave";
    case ServerRole::FAIL:
        return "fail";
    case ServerRole::NO_ADDR:
        return "noaddr";
    case ServerRole::UNKNOWN:
    default:
        return "unknown";
    }
    return "unknown";
}
LinkState str_to_link_state(const std::string& str) {
    if (str == "connected") {
        return LinkState::CONNECTED;
    }
    if (str == "disconnected") {
        return LinkState::DIS_CONNECTED;
    }
    return LinkState::UNKNOWN;
}
std::string link_state_to_str(LinkState state) {
    switch (state) {
    case LinkState::CONNECTED:
        return "connected";
    case LinkState::DIS_CONNECTED:
        return "disconnected";
    case LinkState::UNKNOWN:
        return "unknown";
    }
    return "unknown";
}

RedisServer::~RedisServer() {
    stop();
    join();
}

void RedisServer::stop() {
    if (_has_bg_worker) {
        bthread_stop(_bg_ping_thread);
    }
}
void RedisServer::join() {
    if (_has_bg_worker) {
        bthread_join(_bg_ping_thread, nullptr);
    }
}

void* exec_bg_ping(void* arg) {
    if (!FLAGS_feature_enable_bg_ping) {
        return nullptr;
    }
    RedisServer* server = static_cast<RedisServer*>(arg);
    RedisServerOptions options = server->options();
    int64_t interval_ms = options._conn_hc_interval_ms;
    interval_ms += butil::fast_rand() % interval_ms;
    while (!bthread_stopped(bthread_self())) {
        bthread_usleep(interval_ms * 1000);
        server->ping();
        if (!FLAGS_feature_enable_bg_ping) {
            break;
        }
    }
    LOG(INFO) << "redis server:[" << server->to_str() << "] bg ping thread exit now";
    return nullptr;
}

Status RedisServer::init(const RedisServerOptions& options) {
    _options = options;
    int normal = std::max(1, _options._conn_per_node);
    for (int i = 0; i < normal + 1; ++i) {
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_REDIS;
        options.connection_type = "single";
        options.timeout_ms = _options._conn_timeout_ms;
        options.max_retry = _options._conn_retry_cnt;
        // 这里特地设置不同的connection_group，避免"single"的时候，会真的只有一个tcp 链接
        // Pool方式，链接数量不可控， 会有超过的情况，故使用single
        options.connection_group = fmt::format("redis_server_{}", i);
        if (i == normal) {
            options.connection_group = fmt::format("redis_server_slow_{}", i);
        }
        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
        if (channel->Init(_addr.c_str(), &options) != 0) {
            std::string msg = fmt::format("failed to init channel to redis server:{} with "
                                          "timeout:{}",
                                          _addr, _options._conn_timeout_ms);
            LOG(WARNING) << msg;
            return Status::SysError(msg);
        }
        if (i == normal) {
            _slow_channel = std::move(channel);
            continue;
        } else {
            _channels.push_back(std::move(channel));
        }
    }
    if (bthread_start_background(&_bg_ping_thread, nullptr, exec_bg_ping, this) == 0) {
        _has_bg_worker = true;
    }
    return Status::OK();
}

int RedisServer::execute_io_tasks(void* meta, bthread::TaskIterator<IOTask>& iter) {
    return 0;
}

std::string RedisServer::to_str() const {
    std::string str = fmt::format("id:{} at:{} role:{} leader_id:{} epoch:{} link_state:{}", _id,
                                  _addr, role_to_str(_role), _leader_id, _config_epoch,
                                  link_state_to_str(_link_state));
    return str;
}

// master
// slave
// myself,master
// master,fail,noaddr
ServerRole RedisServer::parse_role_state(const std::string& role_str) {
    ServerRole state = ServerRole::UNKNOWN;
    auto pos = role_str.find("master");
    if (pos != std::string::npos) {
        state = ServerRole::LEADER;
    } else {
        pos = role_str.find("slave");
        if (pos != std::string::npos) {
            state = ServerRole::FOLLOWER;
        }
    }
    pos = role_str.find("fail");
    if (pos != std::string::npos) {
        state = ServerRole::FAIL;
    }
    pos = role_str.find("noaddr");
    if (pos != std::string::npos) {
        state = ServerRole::NO_ADDR;
    }
    return state;
}

Status RedisServer::send_inner_cmd(const std::string& cmd, brpc::RedisResponse* resp, int index) {
    brpc::RedisRequest request;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_bg_cmd_timeout_ms);
    request.AddCommand(cmd);
    if (index < 0 && _slow_channel) {
        _slow_channel->CallMethod(nullptr, &cntl, &request, resp, nullptr);
    } else {
        index = (index < 0) ? 0 : index;
        index = index % _channels.size();
        _channels[index]->CallMethod(nullptr, &cntl, &request, resp, nullptr);
    }
    if (cntl.Failed()) {
        std::string msg = fmt::format("failed to access redis server:{} with cmd:[{}], got err:{}",
                                      _addr, cmd, cntl.ErrorText());
        return Status::SysError(msg);
    }
    if (resp->reply_size() == 0) {
        std::string msg = fmt::format("cmd:[{}] to node:{} reply size is 0", cmd, _addr);
        return Status::SysError(msg);
    }
    const brpc::RedisReply& reply0 = resp->reply(0);
    if (reply0.is_error()) {
        std::string msg = fmt::format("cmd:[{}] to node:{} reply is error:{}", cmd, _addr,
                                      reply0.error_message());
        return Status::SysError(msg);
    }
    VLOG(3) << "succeed to send cmd[" << cmd << "] to " << _addr;
    //  << ", got resp:" << to_string(*resp);
    return Status::OK();
}

void RedisServer::ping() {
    if (!_online) {
        return;
    }
    brpc::RedisRequest request;
    brpc::RedisResponse response;
    brpc::Controller cntl;
    request.AddCommand("ping");
    int channel_cnt = _channels.size() + 1;
    ++_ping_index;
    _ping_index = _ping_index % channel_cnt;
    Status ret;
    if ((_ping_index + 1) == channel_cnt) {
        ret = send_inner_cmd("ping", &response, -1);
    } else {
        ret = send_inner_cmd("ping", &response, _ping_index);
    }
    if (!ret.ok()) {
        LOG(WARNING) << "failed to ping node:" << _addr << ", got err:" << ret;
        return;
    }
}

RedisStatus parse_redis_error(const std::string& str) {
    if (str == S_STATUS_OK) {
        return RedisStatus::OK;
    }
    if (str == S_STATUS_MOVED) {
        return RedisStatus::MOVED;
    }
    if (str == S_STATUS_ASK) {
        return RedisStatus::ASK;
    }
    if (str == S_STATUS_TRYAGAIN) {
        return RedisStatus::TRY_AGAIN;
    }
    if (str == S_STATUS_CROSSSLOT) {
        return RedisStatus::CROSS_SLOT;
    }
    if (str == S_STATUS_CLUSTERDOWN) {
        return RedisStatus::CLUSTER_DOWN;
    }
    return RedisStatus::UNKNOWN;
}

std::string redis_status_to_str(const RedisStatus& s) {
    switch (s) {
    case RedisStatus::OK:
        return S_STATUS_OK;
    case RedisStatus::MOVED:
        return S_STATUS_MOVED;
    case RedisStatus::ASK:
        return S_STATUS_ASK;
    case RedisStatus::TRY_AGAIN:
        return S_STATUS_TRYAGAIN;

    case RedisStatus::CROSS_SLOT:
        return S_STATUS_CROSSSLOT;
    case RedisStatus::CLUSTER_DOWN:
        return S_STATUS_CLUSTERDOWN;
    default:
        return "UNKNOWN";
    }
}

} // namespace client
} // namespace rsdk
