#include <iostream>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>

#include "redis_sdk/client.h"
#include "redis_sdk/include/api_util.h"

namespace rsdk {
namespace client {

DEFINE_int32(req_depth, 1000, "pending request count");
DEFINE_int32(producer_cnt, 10, "producer thread cnt");
DEFINE_int32(key_per_thread, 1024 * 1024, "key per producer thread");
DEFINE_int32(batch_cnt, 10, "batch cnt");
DEFINE_bool(get_op, true, "get operation or not");
DEFINE_string(seed_nodes, "127.0.0.1:21010", "redis cluster node");

bvar::LatencyRecorder g_latency("put_latency");

class Example {
public:
    Example() {
    }
    ~Example() {
        _client_mgr->stop();
        _client_mgr->join();
        _client_mgr = nullptr;
    }
    bool init();
    void submit_in_thread(uint64_t start_key, uint64_t key_cnt);
    void submit();

private:
    std::atomic<int> _pending = {0};
    std::atomic<int> _finished = {0};
    std::string _cname = "cluster1";
    ClientManagerPtr _client_mgr;
};

bool Example::init() {
    _client_mgr = new_client_mgr();
    ClusterOptions coptions;
    coptions._seeds.push_back(FLAGS_seed_nodes);
    coptions._name = _cname;
    coptions._bg_refresh_interval_s = 1;
    coptions._server_options._conn_per_node = 1;
    Status ret = _client_mgr->add_cluster(coptions);
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init cluster";
        return false;
    }
    return _client_mgr != nullptr;
}

class PutClosure : public google::protobuf::Closure {
public:
    PutClosure(std::atomic<int>* in_queue) : _queue_size(in_queue) {
    }
    ~PutClosure() {
    }
    void Run() override {
        std::unique_ptr<PutClosure> release(this);
        int cnt = (int)_records.size();
        _queue_size->fetch_sub(cnt, std::memory_order_relaxed);
        if (cnt == 0) {
            return;
        }
        Record& record = _records[0];
        if (record._errno != 0) {
            std::string msg = record._err_msg;
            if (msg.find("TIMEOUT") != 0 && msg.find("CLUSTERDOWN") != 0) {
                LOG(ERROR) << "unexpect error msg:" << msg << " of set";
            }
            bthread_usleep(1 * 1000L);
        }
        g_latency << _cost.cost_us();
    }
    RecordList* mutable_records() {
        return &_records;
    }

private:
    RecordList _records;
    std::atomic<int>* _queue_size = nullptr;
    TimeCost _cost;
};

void log_response(const brpc::RedisResponse& resp);
void Example::submit_in_thread(uint64_t start_key, uint64_t key_cnt) {
    AccessOptions opts;
    opts._cname = _cname;
    TimeCost cost;

    auto client = _client_mgr->new_client(opts);
    if (!client) {
        LOG(ERROR) << "failed to get client";
        return;
    }
    uint64_t key = start_key;
    for (uint64_t index = 0; index < key_cnt;) {
        int cur_pending = _pending.load(std::memory_order_relaxed);
        int to_submit = FLAGS_req_depth > cur_pending ? (FLAGS_req_depth - cur_pending) : 0;
        to_submit = std::min(FLAGS_batch_cnt, to_submit);
        PutClosure* done = new PutClosure(&_pending);
        RecordList* records = done->mutable_records();
        for (int i = 0; i < to_submit; ++i) {
            Record record;
            record._key = fmt::format("{}", key);
            record._data = record._key;
            record._errno = 100;
            ++_pending;
            records->push_back(std::move(record));
            ++key;
        }
        if (FLAGS_get_op) {
            client->mget(records, done);
        } else {
            client->mset(records, done);
        }
        index += to_submit;
        if (0 == to_submit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ++_finished;
}

void Example::submit() {
    std::vector<std::thread> threads;
    uint64_t start_key = 0;
    uint64_t per_thread = (UINT64_MAX - 100) / FLAGS_producer_cnt;
    if (FLAGS_key_per_thread > 0) {
        per_thread = FLAGS_key_per_thread;
    }
    for (int i = 0; i < FLAGS_producer_cnt; ++i) {
        threads.emplace_back(&Example::submit_in_thread, this, start_key, per_thread);
        start_key += per_thread;
    }
    while (_finished.load(std::memory_order_relaxed) < FLAGS_producer_cnt) {
        std::cout << "async access at qps:" << g_latency.qps(1)
                  << " avg latency:" << g_latency.latency(1) << "us" << std::endl;
        sleep(1);
    }
    for (auto& th : threads) {
        th.join();
    }
}

} // namespace client
} // namespace rsdk

DECLARE_string(flagfile);
DECLARE_int32(logbufsecs);  // defined in glog
DECLARE_int32(minloglevel); // defined in glog
DECLARE_int32(v);           // defined in glog
DECLARE_string(log_dir);    // defined in glog

int main(int argc, char* argv[]) {
    // init base
    FLAGS_logbufsecs = 10;
    FLAGS_minloglevel = 0;
    FLAGS_v = 0;
    FLAGS_log_dir = "log";
    google::ParseCommandLineFlags(&argc, &argv, true);

    ::mkdir(FLAGS_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    google::InitGoogleLogging("api_async_example");
    rsdk::client::Example example;

    if (!example.init()) {
        LOG(INFO) << "init client failed";
        return -1;
    }
    example.submit();
    return 0;
}
