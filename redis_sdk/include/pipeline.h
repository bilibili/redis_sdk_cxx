#ifndef RSDK_API_INCLUDE_PIPELINE_H
#define RSDK_API_INCLUDE_PIPELINE_H

#include <brpc/redis.h>
#include <butil/base64.h>
#include <butil/endpoint.h>

#include "redis_sdk/api_status.h"
#include "redis_sdk/include/api_util.h"
#include "redis_sdk/include/cluster.h"
#include "redis_sdk/include/ctx.h"

namespace rsdk {
namespace client {

struct RetryRequest {
    const Record* _input_rec = nullptr;
    Record* _output_rec = nullptr;
    brpc::RedisResponse* _output_resp = nullptr;
    RedisStatus _rs;
    std::vector<std::string> _err_list;
    std::string _err_msg;
};

using RetryRequestList = std::vector<RetryRequest>;

class PipeCtx {
public:
    PipeCtx(ClusterPtr cluster, RedisServerPtr serv, const std::string& mcmd,
            const std::string& cmd, std::string&& key, ::google::protobuf::Closure* done);
    Status init(const AccessOptions& options) {
        _acc_opt = options;
        return Status::OK();
    }
    void add_request(const std::string& key, Record* rec);
    void add_request(const std::string& key, const Record& rec, brpc::RedisResponse* resp);
    void send_req();
    brpc::RedisRequest* mutable_req() {
        return &_main_req;
    }
    brpc::RedisResponse* mutable_resp() {
        return &_main_resp;
    }
    const std::string& key() const {
        return _key;
    }
    uint64_t left_time_ms() const;
    bool big_key() const {
        return false;
    }
    void retry_in_sub(const RetryRequestList& to_retry, RedisServerPtr from);
    bool should_retry(const brpc::RedisReply& reply, RetryRequest* retry);
    void handle_err(const std::string& msg);
    void handle_resp(RedisServerPtr server);
    void finish_resp();

private:
    void add_request_impl(const std::string& key, const Record& rec);

private:
    bool _read_op = true;
    ClusterPtr _cluster = nullptr;
    RedisServerPtr _server;
    std::string _main_cmd;
    std::string _cmd;
    // only track first key
    std::string _key;
    AccessOptions _acc_opt;

    // two output type, use Record or brpc::RedisResponse for each key
    bool _use_rec_as_output = false;
    std::deque<brpc::RedisResponse*> _user_output_resp;
    std::deque<Record*> _user_output_records;

    // keep ref for retry, no need to manage life cycle
    std::deque<const Record*> _input_records;
    brpc::RedisRequest _main_req;
    brpc::RedisResponse _main_resp;

    // iff output type is Record, then RedisResponse used for rpc will be tracked here
    std::vector<brpc::RedisResponse> _retry_resp_list;

    ::google::protobuf::Closure* _user_cb = nullptr;
    AccessOptions _options;
    int64_t _start_time_us = 0;
    int64_t _end_time_us = 0;
};

class RetryClosure : public ::google::protobuf::Closure {
public:
    RetryClosure(int cnt, PipeCtx* req) : _counter(cnt), _pipe_req(req) {
    }
    bool unref() {
        return _counter.fetch_sub(1) == 1;
    }
    void Run();

private:
    std::atomic<int> _counter = {0};
    PipeCtx* _pipe_req = nullptr;
};

class SubReqClosure : public ::google::protobuf::Closure {
public:
    SubReqClosure(bool get, Record* rec, brpc::RedisResponse* resp,
                  ::google::protobuf::Closure* done) :
            _is_get(get),
            _rec(rec),
            _resp(resp),
            _done(done) {
    }
    void Run() override;

private:
    bool _is_get = true;
    Record* _rec = nullptr;
    brpc::RedisResponse* _resp = nullptr;
    ::google::protobuf::Closure* _done = nullptr;
};

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_PIPELINE_H
