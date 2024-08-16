#ifndef RSDK_API_INCLUDE_CLIENT_IMPL_H
#define RSDK_API_INCLUDE_CLIENT_IMPL_H

#include <condition_variable>
#include <map>
#include <string>
#include <vector>

#include <brpc/redis_command.h>
#include <butil/iobuf.h>

#include "redis_sdk/client.h"
#include "redis_sdk/include/cluster.h"
#include "redis_sdk/include/metric.h"
#include "redis_sdk/include/pipeline.h"

namespace rsdk {
namespace client {

void parse_get_reply(const brpc::RedisReply& reply, Record* rec);
class BatchClosure;
class ClientImpl : public Client {
public:
    void mget(RecordList* records, ::google::protobuf::Closure* done = nullptr) override;
    void mget(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
              ::google::protobuf::Closure* done = nullptr) override;
    void mset(RecordList* records, ::google::protobuf::Closure* done = nullptr) override;
    void mset(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
              ::google::protobuf::Closure* done = nullptr) override;

    void exec(const brpc::RedisRequest& req, brpc::RedisResponse* reply,
              ::google::protobuf::Closure* done = nullptr) override;
    void exec(const std::string& cmd, brpc::RedisResponse* reply,
              ::google::protobuf::Closure* done = nullptr) override;

private:
    friend class ClientManagerImpl;
    explicit ClientImpl(const AccessOptions& options, ClusterPtr cptr, uint64_t manager_id) :
            _options(options),
            _cluster(cptr),
            _manager_id(manager_id) {
    }
    ClientImpl(const ClientImpl& opt, ClusterMgrPtr cluster_mgr) = delete;
    ClientImpl& operator=(const ClientImpl& cli) = delete;

    void mget_impl(const RecordList& records, BatchClosure* batch,
                   ::google::protobuf::Closure* done);
    void mset_impl(const RecordList& records, BatchClosure* batch,
                   ::google::protobuf::Closure* done);
    bool valid_cmd(const std::string& cmd);
    bool check_and_parse(const brpc::RedisRequest& req, brpc::RedisCommandParser* parser,
                         butil::Arena* arena, std::string* cmd,
                         std::vector<butil::StringPiece>* args, std::string* msg, int* cmd_cnt,
                         int* arg_cnt);

    bool is_batch_command(const std::string& cmd_name);
    int get_standard_input_count(const std::string& cmd_name);

    AccessOptions _options;
    ClusterPtr _cluster;
    uint64_t _manager_id;
};

class ClientManagerImpl : public ClientManager {
public:
    ClientManagerImpl();
    explicit ClientManagerImpl(const ClientMgrOptions& options);
    ~ClientManagerImpl();

    ClusterMgrPtr get_cluster_mgr();
    ClientPtr new_client(const AccessOptions& opt) override;

    Status add_cluster(const ClusterOptions& options) override;
    bool has_cluster(const std::string& name) override;
    void set_cluster_state(const std::string& name, bool online) override;
    void stop() override;
    void join() override;
    void bg_work();
    void dump_big_key(ClusterKeys* cluster_big_keys);

private:
    ClientMgrOptions _options;
    std::atomic<bool> _reply_checked = {false};
    std::atomic<bool> _reply_passed = {false};
    ClusterMgrPtr _cluster_mgr;
    bool _is_inited = false;
    uint64_t _id = 0;
    bthread_t _bg_worker;
    bool _bg_worker_started = false;
    std::unique_ptr<ClientMetrics> _metrics;
};

class BatchClosure : public google::protobuf::Closure {
public:
    BatchClosure(uint64_t start_us, ClusterPtr cluster, const std::string& mcmd, int cmd_cnt,
                 google::protobuf::Closure* done) :
            _cluster(cluster),
            _main_cmd(mcmd),
            _cmd_cnt(cmd_cnt),
            _refs(_cmd_cnt),
            _done(done),
            _start_time_us(start_us) {
    }
    ~BatchClosure() override {
        *(_cluster->mutable_batch_cnt()) << _cmd_cnt;
    }
    virtual brpc::RedisResponse* mutable_response(size_t i) = 0;
    void set_refs(int cnt) {
        _refs.store(cnt);
    }
    bool unref() {
        return _refs.fetch_sub(1) == 1;
    }
    const std::string& main_cmd() const {
        return _main_cmd;
    }
    virtual void set_err(int /*errno*/, const std::string& /*msg*/, int /*index*/) {
    }
    void set_done(::google::protobuf::Closure* done) {
        _done = done;
    }

protected:
    ClusterPtr _cluster;
    std::string _main_cmd;
    int _cmd_cnt = 1;
    std::atomic<int> _refs;
    ::google::protobuf::Closure* _done = nullptr;
    uint64_t _start_time_us = 0;
    uint64_t _end_time_us = 0;
};

class RefCountClosure : public BatchClosure {
public:
    RefCountClosure(uint64_t start_us, ClusterPtr cluster, const std::string& mcmd, int cmd_cnt,
                    brpc::RedisResponse* resp, google::protobuf::Closure* done) :
            BatchClosure(start_us, cluster, mcmd, cmd_cnt, done),
            _resp(resp) {
        if (_cmd_cnt > 1) {
            _more_replys.resize(_cmd_cnt - 1);
        }
    }

    void set_merge_as_array(bool m) {
        _merge_as_array = m;
    }

    void Run() override {
        if (unref()) {
            std::unique_ptr<RefCountClosure> release_this(this);
            brpc::ClosureGuard done_guard(_done);
            this->merge_response();
            _end_time_us = get_current_time_us();
            uint64_t cost = _end_time_us - _start_time_us;
            *(_cluster->mutable_batch_latency()) << cost;
        }
    }

    void merge_as_array();

    void merge_response() {
        if (_main_cmd == "exists") {
            merge_exists_response();
            return;
        }
        if (_merge_as_array) {
            if (_cmd_cnt > 1) {
                merge_as_array();
            }
            return;
        }
        for (size_t i = 0; i < _more_replys.size(); ++i) {
            _resp->MergeFrom(_more_replys[i]);
        }
    }

    void merge_exists_response();

    virtual brpc::RedisResponse* mutable_response(size_t i) {
        if (i == 0) {
            return _resp;
        }
        if (i >= (_more_replys.size() + 1)) {
            return nullptr;
        }
        return &(_more_replys[i - 1]);
    }

protected:
    brpc::RedisResponse* _resp = nullptr;
    std::vector<brpc::RedisResponse> _more_replys;
    bool _merge_as_array = false;
};

void set_error_to_resp(brpc::RedisResponse* resp, const std::string& msg);
class BatchClosureV2 : public BatchClosure {
public:
    BatchClosureV2(uint64_t start_us, ClusterPtr cluster, const std::string& mcmd, int cnt,
                   const AccessOptions& opt, google::protobuf::Closure* done) :
            BatchClosure(start_us, cluster, mcmd, cnt, done),
            _acc_options(opt) {
    }
    void Run() override {
        if (unref()) {
            std::unique_ptr<BatchClosureV2> release_this(this);
            brpc::ClosureGuard done_guard(_done);
            _end_time_us = get_current_time_us();
            uint64_t cost = _end_time_us - _start_time_us;
            *(_cluster->mutable_batch_latency()) << cost;
        }
    }
    PipeCtx* split_to_sub_req(const std::string& cmd, const Record& rec);
    PipeCtx* add_or_get(ClusterPtr cluster, RedisServerPtr server, const std::string& cmd,
                        const Record& rec);
    void send_all_sub_req();

protected:
    brpc::RedisResponse* mutable_response(size_t) {
        return nullptr;
    }
    const AccessOptions& _acc_options;
    std::unordered_map<std::string, PipeCtx*> _server2req;
    std::vector<std::unique_ptr<PipeCtx>> _sub_reqs;
};

} // namespace client
} // namespace rsdk
#endif // RSDK_API_INCLUDE_CLIENT_IMPL_H
