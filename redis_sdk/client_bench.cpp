#include <iostream>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "redis_sdk/client.h"
#include "redis_sdk/include/api_util.h"

namespace rsdk {
namespace client {

DEFINE_int32(thread_cnt, 10, "thread_cnt");
DEFINE_int32(conn_per_node, 1, "connection cnt per node");
DEFINE_int32(key_per_thread, 1024 * 1024, "key per thread");
DEFINE_string(seeds, "127.0.0.1:21010", "seed node");

bvar::LatencyRecorder g_latency("put_latency");

class Benchmark {
public:
    Benchmark() {
    }
    ~Benchmark() {
        _client_mgr->stop();
        _client_mgr->join();
    }
    bool init();
    void run();
    void start_bench();

private:
    std::atomic<int> _finished = {0};
    std::string _cname = "cluster1";
    ClientManagerPtr _client_mgr;
};

bool Benchmark::init() {
    _client_mgr = new_client_mgr();
    ClusterOptions coptions;
    coptions._seeds.push_back(FLAGS_seeds);
    coptions._name = _cname;
    coptions._server_options._conn_per_node = FLAGS_conn_per_node;
    Status ret = _client_mgr->add_cluster(coptions);
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init cluster";
        return false;
    }
    return _client_mgr != nullptr;
}
struct ThreadTask {
    uint64_t _start_key = 0;
    uint64_t _key_cnt = 0;
    ClientPtr _client;
    int _id = 0;
    std::atomic<int>* _counter = nullptr;
};

void log_response(const brpc::RedisResponse& resp);
void* run_in_thread(void* arg) {
    ThreadTask* task = static_cast<ThreadTask*>(arg);
    std::unique_ptr<ThreadTask> task_guard(task);
    uint64_t start_key = task->_start_key;
    for (uint64_t index = 0; index < task->_key_cnt; ++index) {
        TimeCost cost;
        uint64_t key = start_key + index % task->_key_cnt;
        RecordList records;
        Record record;
        record._key = fmt::format("{}", key);
        record._data = fmt::format("{}", key);
        records.push_back(std::move(record));
        task->_client->mset(&records, nullptr);
        if (records[0]._errno != 0) {
            bthread_usleep(1 * 1000L);
        }
        g_latency << cost.cost_us();
    }
    ++(*task->_counter);
    return nullptr;
}
void Benchmark::run() {
    std::thread th(&Benchmark::start_bench, this);
    while (_finished.load() < FLAGS_thread_cnt) {
        sleep(1);
        std::cout << "qps:" << g_latency.qps(1) << " avg latency:" << g_latency.latency(1) << "us"
                  << std::endl;
    }
    th.join();
}

void Benchmark::start_bench() {
    AccessOptions opts;
    opts._cname = _cname;
    TimeCost cost;
    std::vector<bthread_t> threads;
    auto client = _client_mgr->new_client(opts);
    if (!client) {
        LOG(ERROR) << "failed to get client";
        return;
    }
    uint64_t start_key = 0;
    uint64_t per_thread = (UINT64_MAX - 100) / FLAGS_thread_cnt;
    if (FLAGS_key_per_thread > 0) {
        per_thread = FLAGS_key_per_thread;
    }
    for (int i = 0; i < FLAGS_thread_cnt; ++i) {
        bthread_t th;
        ThreadTask* task = new ThreadTask();
        AccessOptions opts;
        task->_start_key = start_key;
        task->_key_cnt = per_thread;
        task->_client = client;
        task->_counter = &_finished;
        start_key += per_thread;
        bthread_start_background(&th, nullptr, run_in_thread, task);
        threads.push_back((th));
    }
    for (auto& th : threads) {
        bthread_join(th, nullptr);
    }
    LOG(INFO) << "all done, exit now";
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

    google::InitGoogleLogging("client_bench");
    rsdk::client::Benchmark benchmark;

    if (!benchmark.init()) {
        LOG(INFO) << "init client failed";
        return -1;
    }
    benchmark.run();
    return 0;
}
