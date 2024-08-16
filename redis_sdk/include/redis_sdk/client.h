#ifndef REDIS_SDK_INCLUDE_REDISK_SDK_CLIENT_H
#define REDIS_SDK_INCLUDE_REDISK_SDK_CLIENT_H

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <butil/iobuf.h>
#include <google/protobuf/stubs/callback.h>

#include "redis_sdk/api_status.h"

namespace brpc {
class RedisRequest;
class RedisResponse;
} // namespace brpc

namespace prometheus {
class Registry;
} // namespace prometheus

using ClusterKeys = std::unordered_map<std::string, std::unordered_set<std::string>>;

namespace rsdk {
namespace client {

struct RedisServerOptions {
    int _conn_per_node = 1;
    int _conn_timeout_ms = 500;
    int _conn_hc_interval_ms = 3000;
    int _conn_retry_cnt = 1;
};

struct ClusterOptions {
    std::string _name;
    // interval to rerefresh cluster info, eg: `cluster nodes`
    int _bg_refresh_interval_s = 20;
    // big key threshold in bytes, if not set, use global FLAGS_big_key_threshold_bytes
    uint64_t _big_key_threshold_bytes = 0;
    std::vector<std::string> _seeds;
    RedisServerOptions _server_options;
};

typedef std::map<std::string, std::string> ClusterTokens;

enum class ReadPolicy {
    LEADER_ONLY = 0,
};

struct AccessOptions {
    // timeout for one rpc call.
    int64_t timeout_ms = 500;

    // request node max redirect cnt, <= 0 means no retry.
    // total access time will be (1 + max_redirect)
    int max_redirect = 2;

    ReadPolicy read_policy = ReadPolicy::LEADER_ONLY;

    std::string _cname;
    bool _merge_batch_as_array = false;
};

struct Record {
    std::string _key;  // input
    std::string _data; // input&output,
                       // in output case, if KEY NOT FOUND, _data.size() will be 0
    int _ttl_s = 0;    // input, in seconds, used as: 'set _key _value EX _ttl_s'
    int _errno = 0;    // output
    std::string _err_msg;
};
using RecordList = std::vector<Record>;

class Client {
public:
    Client() = default;
    virtual ~Client() = default;
    // mget&mset, less cpu cost than exec
    virtual void mget(RecordList* records, ::google::protobuf::Closure* done = nullptr) = 0;
    virtual void mget(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
                      ::google::protobuf::Closure* done = nullptr) = 0;
    virtual void mset(RecordList* records, ::google::protobuf::Closure* done = nullptr) = 0;
    virtual void mset(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
                      ::google::protobuf::Closure* done = nullptr) = 0;

    virtual void exec(const brpc::RedisRequest& req, brpc::RedisResponse* reply,
                      ::google::protobuf::Closure* done = nullptr) = 0;
    // binary data is not support in this interface
    virtual void exec(const std::string& cmd, brpc::RedisResponse* reply,
                      ::google::protobuf::Closure* done = nullptr) = 0;
};

using ClientPtr = std::shared_ptr<Client>;

struct ClientMgrOptions {
    prometheus::Registry* _registry = nullptr;
};

class ClientManager {
public:
    ClientManager() = default;
    virtual ~ClientManager() = default;
    virtual ClientPtr new_client(const AccessOptions& opt) = 0;
    virtual Status add_cluster(const ClusterOptions& options) = 0;
    virtual void stop() = 0;
    virtual void join() = 0;

    virtual bool has_cluster(const std::string& name) = 0;
    // 探测不再打印error日志
    virtual void set_cluster_state(const std::string& cluster_name, bool online_or_not) = 0;

    virtual void dump_big_key(ClusterKeys* cluster_big_keys) = 0;
};

using ClientManagerPtr = std::shared_ptr<ClientManager>;

// 每个进程创建一个
// 过多创建，会造成对后端redis分布信息的多余刷新, 加重后端压力
ClientManagerPtr new_client_mgr(const ClientMgrOptions& options = ClientMgrOptions());

} // namespace client
} // namespace rsdk

#endif // REDIS_SDK_INCLUDE_REDISK_SDK_CLIENT_H
