#ifndef RSDK_API_INCLUDE_CLUSTER_H
#define RSDK_API_INCLUDE_CLUSTER_H

#include <map>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/containers/doubly_buffered_data.h>
#include <bvar/bvar.h>
#include <fmt/format.h>

#include "redis_sdk/client.h"
#include "redis_sdk/include/metric.h"
#include "redis_sdk/include/slot.h"

namespace rsdk {
namespace client {

enum class RedisStatus;

typedef std::unordered_map<uint64_t, std::string> KeySet;
class BigKeyMgr {
public:
    explicit BigKeyMgr(uint64_t th) : _threshold(th), _keys(nullptr) {
        _keys = new butil::DoublyBufferedData<KeySet>();
    }
    virtual ~BigKeyMgr() {
        delete _keys;
        _keys = nullptr;
    }
    BigKeyMgr(const BigKeyMgr&) = delete;
    BigKeyMgr& operator=(const BigKeyMgr&) = delete;

    bool has(uint64_t key);
    void add(uint64_t sign, const std::string& key);
    void dump(std::unordered_set<std::string>* keys);
    void remove(uint64_t sign);
    uint64_t count();
    uint64_t threshold() const;
    void update_threshold(uint64_t limit) {
        _threshold = limit;
    }

    static size_t add_key(KeySet& m, uint64_t sign, const std::string& key);
    static size_t remove_key(KeySet& m, uint64_t key);
    static size_t clear_all(KeySet& m);

private:
    uint64_t _threshold = 0;
    bthread::Mutex _ww_mutex;
    butil::DoublyBufferedData<KeySet>* _keys;
    std::unique_ptr<bvar::LatencyRecorder> _read_size;
};

struct ClusterNodeState {
    std::string _id;
    std::string _addr;
    ServerRole _role;
    std::string _leader_id;
    int64_t _config_epoch = -1;
    LinkState _link_state = LinkState::DIS_CONNECTED;
    std::vector<std::string> _slot_str;
    std::string _murmur_hash;
    //
    std::string to_str() const;
};

class Cluster {
public:
    Cluster(const ClusterOptions& options);
    virtual ~Cluster();
    void stop();
    void join();
    Status init();
    std::string name() const {
        return _options._name;
    }
    int64_t sn() const {
        return _sn.load();
    }

    std::string to_string() const {
        return "cluster:" + _options._name;
    }
    void set_online(bool on);

    SlotPtr get_slot_by_id(uint64_t slot_id) {
        return _slot_mgr->get_slot_by_id(slot_id);
    }
    RedisServerPtr add_and_get_node(const std::string& addr);

    std::vector<std::string> get_servers();
    std::vector<std::string> get_servers_for_cluster_nodes();
    RedisServerPtr get_node(const std::string& address);
    void update_sn(int64_t new_sn) {
        while (true) {
            int64_t cur_sn = _sn.load();
            if (new_sn <= cur_sn) {
                return;
            }
            bool ret = _sn.compare_exchange_strong(cur_sn, new_sn);
            if (ret) {
                return;
            }
        }
    }
    Status update_dist();
    Status refresh_dist(bool use_seed);
    bool may_update_dist(RedisStatus status, const std::string& msg);
    void may_triggle_update_dist();
    bthread::Mutex* mutable_bg_refresh_mutex() {
        return &_bg_refresh_mutex;
    }
    bthread::ConditionVariable* mutable_refresh_cond() {
        return &_refresh_cond;
    }
    ClusterOptions* mutable_options() {
        return &_options;
    }
    bvar::LatencyRecorder* mutable_read_bvar() {
        return _read_size.get();
    }
    bvar::LatencyRecorder* mutable_write_bvar() {
        return _write_size.get();
    }
    bvar::LatencyRecorder* mutable_latency_bvar() {
        return _latency.get();
    }

    bvar::LatencyRecorder* mutable_batch_latency() {
        return _batch_latency.get();
    }
    bvar::LatencyRecorder* mutable_batch_cnt() {
        return _batch_cnt.get();
    }
    bool is_big_key(uint64_t key);
    void add_big_key(uint64_t sign, const std::string& key, uint64_t size);
    void remove_big_key(uint64_t key);
    void dump_big_key(std::unordered_set<std::string>* keys);
    void print_and_auto_tune();
    uint64_t big_key_threshold() const {
        return _big_key_mgr.threshold();
    }
    void set_client_metrics(ClientMetrics* metrics) {
        _metrics = metrics;
    }
    void add_metrics(const std::string& method, const std::string& server, bool succ, uint64_t us,
                     int req_size, int resp_size, int cnt = 1);

private:
    // Status parse_cluster_slots(const brpc::RedisReply& resp, SlotMgrPtr slot_mgr);
    // Status parse_cluster_slots_sub_reply(const brpc::RedisReply& sub, SlotMgrPtr slot_mgr);
    Status parse_node(const std::string& data, ClusterNodeState* state);
    Status parse_cluster_nodes(const brpc::RedisResponse& resp, SlotMgrPtr slot_mgr);
    Status update_cluster_dist(const std::map<std::string, std::vector<ClusterNodeState>>& state,
                               SlotMgrPtr slot_mgr);

    Status init_cluster_dist();
    Status refresh_cluster_dist_by_cluster_nodes(const std::string& addr, SlotMgrPtr slot_mgr);
    // Status refresh_cluster_dist_by_cluster_slots(const std::string& addr, SlotMgrPtr slot_mgr);

    ClusterOptions _options;
    std::atomic<int64_t> _sn;
    std::string _cluster_digest;
    int64_t _last_refresh_time_ms = 0;
    int64_t _current_epoch = -1;

    bthread::Mutex _refresh_mutex;
    std::atomic<int64_t> _refresh_sn = {100};
    SlotMgrPtr _slot_mgr;

    bthread::Mutex _bg_refresh_mutex;
    bthread::ConditionVariable _refresh_cond;
    bthread_t _bg_update_dist_thread;
    bool _has_bg_update_dist_thread = {false};

    bthread::Mutex _nodes_mutex;
    std::unordered_map<std::string, RedisServerPtr> _ip2nodes;
    bool _use_cluster_slots_for_dist = false;
    uint64_t _init_time_ms = 0;

    bool _online = true;

    BigKeyMgr _big_key_mgr;

    // metrics
    std::unique_ptr<bvar::LatencyRecorder> _read_size;
    std::unique_ptr<bvar::LatencyRecorder> _write_size;
    std::unique_ptr<bvar::LatencyRecorder> _big_key_size;
    std::unique_ptr<bvar::LatencyRecorder> _latency;
    std::unique_ptr<bvar::LatencyRecorder> _batch_latency;
    std::unique_ptr<bvar::LatencyRecorder> _batch_cnt;
    uint64_t _last_print_time_ms = 0;
    uint64_t _last_tune_time_ms = 0;
    ClientMetrics* _metrics = nullptr;
};

using ClusterPtr = std::shared_ptr<Cluster>;
using ClusterMap = std::unordered_map<std::string, ClusterPtr>;

struct ClusterManagerOptions {};

class ClusterManager {
public:
    ClusterManager() : _metrics(nullptr) {
    }
    explicit ClusterManager(ClientMetrics* metric) : _metrics(metric) {
    }
    virtual ~ClusterManager() {
        _metrics = nullptr;
    }
    void stop();
    void join();

    ClusterPtr get(const std::string& cluster_name);
    bool remove(const std::string& cluster_name);
    Status add_cluster(const ClusterOptions& options);
    void clear_all();
    void get_all(std::vector<ClusterPtr>* all);

    bool has_cluster(const std::string& name);
    void set_online(const std::string& name);
    void set_offline(const std::string& name);
    void bg_work();
    void clear();
    void dump_big_key(ClusterKeys* cluster_big_keys);

    static size_t add_cluster_impl(ClusterMap& cm, ClusterPtr cluster);
    static size_t remove_cluster_impl(ClusterMap& cm, const std::string& name);
    static size_t clear_all_impl(ClusterMap& cm);

private:
    bthread::Mutex _clusters_lock;
    butil::DoublyBufferedData<ClusterMap> _clusters;
    ClientMetrics* _metrics = nullptr;
};

using ClusterMgrPtr = std::shared_ptr<ClusterManager>;

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_CLUSTER_H
