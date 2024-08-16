#ifndef RSDK_API_INCLUDE_CTX_H
#define RSDK_API_INCLUDE_CTX_H

#include <brpc/redis.h>
#include <butil/base64.h>
#include <butil/endpoint.h>

#include "redis_sdk/api_status.h"
#include "redis_sdk/include/api_util.h"
#include "redis_sdk/include/cluster.h"

namespace rsdk {
namespace client {

extern const std::string S_STATUS_OK;
extern const std::string S_STATUS_MOVED;
extern const std::string S_STATUS_ASK;
extern const std::string S_STATUS_TRYAGAIN;
extern const std::string S_STATUS_CROSSSLOT;
extern const std::string S_STATUS_CLUSTERDOWN;

enum class RedisStatus {
    OK = 0,
    MOVED = 1,
    ASK = 2,
    TRY_AGAIN = 3,
    CROSS_SLOT = 4,
    CLUSTER_DOWN = 5,
    UNKNOWN = 999,
};

// #define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
// #define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
// #define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
// #define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
// #define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
// #define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
// #define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */
// #define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

class Slot;
class Cluster;
struct AccessOptions;
class RedisServer;
class IOCtx {
public:
    IOCtx(std::shared_ptr<Cluster> cluster, const std::string& mcmd, const std::string& scmd,
          std::string&& key, brpc::RedisRequest&& req, brpc::RedisResponse* resp,
          ::google::protobuf::Closure* cb) :
            _cluster(cluster),
            _main_cmd(mcmd),
            _sub_cmd(scmd),
            _key(key),
            _req(std::move(req)),
            _resp(resp),
            _user_cb(cb) {
    }
    virtual ~IOCtx();
    Status init(const AccessOptions& options);
    bool need_to_update_dist(RedisStatus status);
    bool should_retry(RedisStatus status);
    void retry(RedisStatus status, std::shared_ptr<RedisServer> from,
               const std::vector<std::string>& err_list, const std::string& err_msg);
    void send_cmd();
    void handle_err(const std::string& msg);
    void handle_resp(std::shared_ptr<RedisServer> from);
    brpc::RedisResponse* mutable_resp() {
        return _resp;
    }
    brpc::RedisRequest* mutable_req() {
        return &_req;
    }
    const std::string& key() const {
        return _key;
    }
    void set_big_key(bool s) {
        _big_key = s;
    }
    bool big_key() const {
        return _big_key;
    }
    std::shared_ptr<RedisServer> get_server();
    void add_ask_head();
    void may_skip_ask_head();
    int64_t left_time_ms();
    void set_start_time(int64_t ts) {
        _start_time_us = ts;
    }
    void set_server_id(const std::string& id) {
        _server_id = id;
    }

private:
    void add_or_remove_big_key();
    void handle_read_resp();

private:
    std::shared_ptr<Cluster> _cluster;
    std::string _main_cmd;
    std::string _sub_cmd;
    std::string _key;
    uint32_t _slot_id = UINT32_MAX;
    int64_t _slot_config_epoch = -1;
    std::string _server_id;
    std::shared_ptr<Slot> _slot;
    brpc::RedisRequest _req;
    brpc::RedisResponse* _resp = nullptr;
    ::google::protobuf::Closure* _user_cb = nullptr;
    AccessOptions _options;
    int64_t _start_time_us = 0;
    int64_t _end_time_us = 0;
    int _retry_cnt = 0;
    std::string _err_msg;
    bool _has_ask_head = false;
    bool _read_op = false;
    // big releated
    bool _big_key = false;
    uint64_t _key_sign = 0;
    uint64_t _req_size_bytes = 0;
    uint64_t _resp_size_bytes = 0;
    bool _add_to_key_cache = false;
    bool _remove_from_key_cache = false;
    uint64_t _big_key_threshold = 0;
};

class PipeCtx;
class RpcCallback : public ::google::protobuf::Closure {
public:
    RpcCallback(IOCtx* ctx, std::shared_ptr<RedisServer> server);
    RpcCallback(PipeCtx* req, std::shared_ptr<RedisServer> server);
    ~RpcCallback() {
        _ctx = nullptr;
        _pipe_req = nullptr;
    }
    void Run() override;
    brpc::Controller* mutable_cntl() {
        return &_cntl;
    }

private:
    brpc::Controller _cntl;
    IOCtx* _ctx = nullptr;
    PipeCtx* _pipe_req = nullptr;
    std::shared_ptr<RedisServer> _server;
};

inline void sleep_in_bthread(int wait_time_ms, int cnt, int64_t deadline, int64_t now_ms) {
    static constexpr int64_t S_MAX_RETRY_INTERVAL_MS = 50;
    if (deadline < now_ms) {
        return;
    }
    if (0 == cnt) {
        cnt = 1;
    }
    int64_t interval = std::min(int64_t(wait_time_ms * cnt), deadline - now_ms);
    bthread_usleep(std::min(interval, S_MAX_RETRY_INTERVAL_MS) * 1000);
}

int64_t rand_logid();
std::string key_to_string(const std::string& key);
void set_error_to_resp(brpc::RedisResponse* resp, const std::string& msg);
void set_integer_to_resp(brpc::RedisResponse* resp, int64_t input);

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_CTX_H
