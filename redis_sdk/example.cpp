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
    void crud();
    void mset_mget();

private:
    std::string _cname = "cluster1";
    ClientManagerPtr _client_mgr;
};

bool Example::init() {
    _client_mgr = new_client_mgr();
    ClusterOptions coptions;
    coptions._seeds.push_back("127.0.0.1:21010");
    coptions._name = _cname;
    coptions._bg_refresh_interval_s = 1;
    coptions._server_options._conn_per_node = 4;
    Status ret = _client_mgr->add_cluster(coptions);
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init cluster";
        return false;
    }
    return _client_mgr != nullptr;
}
void Example::mset_mget() {
    RecordList put_records;
    RecordList get_records;
    int cnt = 50;
    std::string prefix = "prefix";
    for (int i = 0; i < cnt; ++i) {
        Record rec;
        rec._key = fmt::format("{}_{}", prefix, i);
        rec._data = rec._key;
        rec._ttl_s = 5;
        Record get;
        get._key = rec._key;
        put_records.emplace_back(rec);
        get_records.emplace_back(get);
    }
    AccessOptions opts;
    opts._cname = _cname;
    TimeCost cost;

    auto client = _client_mgr->new_client(opts);
    if (!client) {
        LOG(ERROR) << "failed to get client";
        return;
    }
    std::cout << "-------start mget while expect empty--------" << std::endl;
    client->mget(&get_records, nullptr);
    for (int i = 0; i < cnt; ++i) {
        Record& rec = get_records[i];
        if (get_records[i]._errno != 0) {
            std::cout << "failed to get key:" << rec._key << " return err:" << rec._errno
                      << " msg:" << rec._err_msg << std::endl;
            continue;
        }
        if (rec._data.size() != 0) {
            std::cout << "key:" << rec._key << " value is:" << rec._data.size()
                      << " while expect nil" << std::endl;
            continue;
        }
        // std::cout << "succ to get key:" << rec._key << " with value size is 0" << std::endl;
    }
    std::cout << "-------start mset --------" << std::endl;
    client->mset(&put_records, nullptr);
    for (int i = 0; i < cnt; ++i) {
        Record& rec = put_records[i];
        if (rec._errno != 0) {
            std::cout << "failed to put key:" << rec._key << " return err:" << rec._errno
                      << " msg:" << rec._err_msg << std::endl;
            continue;
        }
        // std::cout << "succ to put key:" << rec._key << std::endl;
    }
    std::cout << "-------start mget, expect to get all --------" << std::endl;
    client->mget(&get_records, nullptr);
    for (int i = 0; i < cnt; ++i) {
        Record& rec = get_records[i];
        if (get_records[i]._errno != 0) {
            std::cout << "failed to get key:" << rec._key << " return err:" << rec._errno
                      << " msg:" << rec._err_msg << std::endl;
            continue;
        }
        std::string expect = fmt::format("{}_{}", prefix, i);
        if (rec._data != expect) {
            std::cout << "failed to get key:" << rec._key << " value is:" << rec._data
                      << " while expect:" << expect << std::endl;
            continue;
        }
        // std::cout << "succ to get key:" << rec._key << " with value:" << rec._data << std::endl;
    }
    std::cout << "-------wait 7 seconds to expire all --------" << std::endl;
    int wait_for = 7 * 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_for));
    get_records.clear();
    for (auto& rec : put_records) {
        Record get;
        get._key = rec._key;
        get_records.emplace_back(get);
    }
    std::cout << "-------start to mget, expect all empty --------" << std::endl;
    SyncClosure sync;
    client->mget(&get_records, &sync);
    sync.wait();
    for (int i = 0; i < cnt; ++i) {
        Record& rec = get_records[i];
        if (get_records[i]._errno != 0) {
            std::cout << "failed to get key:" << rec._key << " return err:" << rec._errno
                      << " msg:" << rec._err_msg << std::endl;
            continue;
        }
        std::string expect;
        if (!rec._data.empty()) {
            std::cout << "failed to get key:" << rec._key << " value is:" << rec._data
                      << " while expect ''" << std::endl;
            continue;
        }
        // std::cout << "succ to get key:" << rec._key << " with value is empty:" << std::endl;
    }
}

void log_response(const brpc::RedisResponse& resp);
void Example::crud() {
    AccessOptions opts;
    opts._cname = _cname;
    TimeCost cost;

    brpc::RedisResponse put_resp;
    std::string cmd = fmt::format("set {} {}", 1, 1000);
    auto client = _client_mgr->new_client(opts);
    if (!client) {
        LOG(ERROR) << "failed to get client";
        return;
    }
    client->exec(cmd, &put_resp, nullptr);
    log_response(put_resp);

    cmd = fmt::format("get {}", 1);
    brpc::RedisResponse get_resp;
    client->exec(cmd, &get_resp, nullptr);
    log_response(get_resp);

    cmd = fmt::format("hset site redis redis.com");
    SyncClosure sync1;
    brpc::RedisResponse hset_resp;
    client->exec(cmd, &hset_resp, &sync1);
    sync1.wait();
    log_response(hset_resp);

    cmd = fmt::format("hget site redis");
    brpc::RedisResponse hget_resp;
    SyncClosure sync2;
    client->exec(cmd, &hget_resp, &sync2);
    sync2.wait();
    log_response(hget_resp);

    // renamenx/rename/eval/msetnx is not support
    cmd = fmt::format("mset 1 2");
    brpc::RedisResponse mset_resp;
    SyncClosure sync3;
    client->exec(cmd, &mset_resp, &sync3);
    sync3.wait();
    log_response(mset_resp);
    {
        std::string key = std::string("key_") + std::to_string(butil::fast_rand() % 100000);
        cmd = fmt::format("smembers " + key);
        brpc::RedisResponse smember_resp;
        SyncClosure sync4;
        client->exec(cmd, &smember_resp, &sync4);
        sync4.wait();
        log_response(smember_resp);

        cmd = fmt::format("sadd {} x1 x2 123", key);
        brpc::RedisResponse resp2;
        SyncClosure sync5;
        client->exec(cmd, &resp2, &sync5);
        sync5.wait();
        log_response(resp2);

        cmd = fmt::format("smembers {}", key);
        brpc::RedisResponse resp3;
        SyncClosure sync6;
        client->exec(cmd, &resp3, &sync6);
        sync6.wait();
        log_response(resp3);
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
    FLAGS_logbufsecs = 0;
    FLAGS_minloglevel = 0;
    FLAGS_v = 3;
    FLAGS_log_dir = "log";
    google::ParseCommandLineFlags(&argc, &argv, true);

    ::mkdir(FLAGS_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    google::InitGoogleLogging("api_example");
    rsdk::client::Example example;

    if (!example.init()) {
        LOG(INFO) << "init client failed";
        return -1;
    }
    example.crud();
    example.mset_mget();
    return 0;
}
