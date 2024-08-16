#include <map>
#include <random>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <butil/strings/string_split.h>
#include <butil/strings/string_util.h>
#include <fmt/format.h>

#include "redis_sdk/include/cluster.h"
#include "redis_sdk/include/ctx.h"
#include "redis_sdk/include/redis_sdk/client.h"

#ifdef WITH_PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#endif // WITH_PROMETHEUS

namespace butil {
void MurmurHash3_x86_128(const void* key, const int len, uint32_t seed, void* out);
} // namespace butil

namespace rsdk {
namespace client {

DEFINE_uint64(slow_log_threshold_ms, 50, "slow log threshold in ms");
DEFINE_int64(big_key_cnt_per_cluster, 1000, "big key count per cluster");
DEFINE_int64(big_key_threshold_bytes, 1052672L, "big key threshold in bytes, 1M+4K by default");
DEFINE_bool(feature_cluster_nodes_in_slow_channel, true,
            "use slow tcp channel to send cluster nodes cmd");
DEFINE_bool(print_slow_state_as_warn, true, "print slow state as warn or not");

DEFINE_bool(feature_export_to_prometheus, true, "export to prometheus or not");
DEFINE_bool(feature_cluster_nodes_to_leader_only, true, "send cluster nodes to leader only");
DEFINE_bool(use_digest_to_compare, true, "use digest to compare cluster change or not");
DEFINE_bool(feature_auto_tune_big_key_threshold, false, "auto calc big key threshold in runtime");
DEFINE_int64(cluster_change_check_interval_ms, 1000,
             "mini interval to check cluster dist changed or not");
DEFINE_int64(print_big_key_interval_ms, 60 * 1000L, "print big key by every minute");
DEFINE_int64(auto_tune_big_key_interval_ms, 5 * 60 * 1000L,
             "auto tune big key threshold every 5 minutes");
DEFINE_string(latency_metric_buckets, "5, 10, 25, 50, 100, 250",
              "latency ms buckets for prometheus");
DEFINE_bool(feature_collect_redis_server_metrics, true, "collect redis server metrics or not");

BRPC_VALIDATE_GFLAG(use_digest_to_compare, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(cluster_change_check_interval_ms, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(big_key_cnt_per_cluster, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(big_key_threshold_bytes, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(feature_cluster_nodes_in_slow_channel, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(print_slow_state_as_warn, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(print_big_key_interval_ms, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(feature_auto_tune_big_key_threshold, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(auto_tune_big_key_interval_ms, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(feature_cluster_nodes_to_leader_only, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(feature_export_to_prometheus, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(feature_collect_redis_server_metrics, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(slow_log_threshold_ms, brpc::PassValidate);

// static const int S_API_VERSION = 100;

// int64_t rand_logid() {
//     return (int64_t)butil::fast_rand();
// }
//
const std::vector<double> ClientMetrics::S_DEFAULT_LATENCY_BUCKETS_MS = {5,   10,  25,   50,  100,
                                                                         250, 500, 1000, 2500};

std::string to_string(const brpc::RedisResponse& resp) {
    const brpc::RedisReply& reply0 = resp.reply(0);
    if (reply0.is_error()) {
        std::string msg = fmt::format("error: cnt:{} first:{}", resp.reply_size(),
                                      reply0.error_message());
        return msg;
    }
    if (reply0.is_string()) {
        std::string msg = fmt::format("str: cnt:{} first:{}", resp.reply_size(), reply0.c_str());
        return msg;
    }
    if (reply0.is_integer()) {
        std::string msg = fmt::format("integer cnt:{} first:{}", resp.reply_size(),
                                      reply0.integer());
        return msg;
    }
    std::string msg = fmt::format("cnt:{} first type:{}", resp.reply_size(),
                                  brpc::RedisReplyTypeToString(reply0.type()));
    return msg;
}

uint64_t BigKeyMgr::threshold() const {
    return (_threshold > 0) ? _threshold : FLAGS_big_key_threshold_bytes;
}

size_t BigKeyMgr::add_key(KeySet& keys, uint64_t sign, const std::string& key) {
    uint64_t limit = FLAGS_big_key_cnt_per_cluster;
    if (keys.size() >= limit) {
        uint64_t per_time = 10;
        uint64_t index = 0;
        std::vector<uint64_t> to_drop;
        for (auto& iter : keys) {
            to_drop.push_back(iter.first);
            ++index;
            if (index >= per_time) {
                break;
            }
        }
        // TODO:
        // brpc 在调用add_key的时候，实际上是两个指针，分别执行一次。 下面的过程，有随机的过程，
        // 会导致2个对象里面存的key 并不一致。似乎没啥问题
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(to_drop.begin(), to_drop.end(), gen);
        uint64_t to_del = to_drop.front();
        keys.erase(to_del);
    }

    keys[sign] = key;
    return 1;
}

size_t BigKeyMgr::remove_key(KeySet& keys, uint64_t key) {
    keys.erase(key);
    return 1;
}

size_t BigKeyMgr::clear_all(KeySet& keys) {
    keys.clear();
    return 1;
}

uint64_t BigKeyMgr::count() {
    butil::DoublyBufferedData<KeySet>::ScopedPtr ptr;
    if (_keys->Read(&ptr) != 0) {
        return 0;
    }
    return ptr->size();
}

bool BigKeyMgr::has(uint64_t key) {
    butil::DoublyBufferedData<KeySet>::ScopedPtr ptr;
    if (_keys->Read(&ptr) != 0) {
        return false;
    }
    return ptr->count(key) > 0;
}

void BigKeyMgr::dump(std::unordered_set<std::string>* keys) {
    butil::DoublyBufferedData<KeySet>::ScopedPtr ptr;
    if (_keys->Read(&ptr) != 0) {
        return;
    }
    for (const auto& key : *ptr) {
        keys->insert(key.second);
    }
    return;
}

void BigKeyMgr::add(uint64_t sign, const std::string& key) {
    _keys->Modify(add_key, sign, key);
}

void BigKeyMgr::remove(uint64_t key) {
    _keys->Modify(remove_key, key);
}

Cluster::Cluster(const ClusterOptions& options) :
        _options(options),
        _big_key_mgr(options._big_key_threshold_bytes) {
    _init_time_ms = get_current_time_ms();
    _slot_mgr = std::make_shared<SlotMgr>();

    std::string read_metrix = fmt::format("read_msg_size_{}", _options._name);
    _read_size.reset(new bvar::LatencyRecorder(read_metrix));

    std::string write_metrix = fmt::format("write_msg_size_{}", _options._name);
    _write_size.reset(new bvar::LatencyRecorder(write_metrix));

    std::string big_metrix = fmt::format("big_key_metrix_{}", _options._name);
    _big_key_size.reset(new bvar::LatencyRecorder(big_metrix));

    std::string lname = fmt::format("latency_cluster_{}", _options._name);
    _latency.reset(new bvar::LatencyRecorder(lname));

    std::string bname = fmt::format("latency_inbatch_cluster_{}", _options._name);
    _batch_latency.reset(new bvar::LatencyRecorder(bname));

    std::string cname = fmt::format("batch_cnt_{}", _options._name);
    _batch_cnt.reset(new bvar::LatencyRecorder(cname));
}

std::string redis_status_to_str(const RedisStatus& s);
bool Cluster::may_update_dist(RedisStatus status, const std::string& msg) {
    VLOG(3) << "try to update cluster dist with status:" << redis_status_to_str(status)
            << " msg:" << msg;
    refresh_dist(false);
    return true;
}

void Cluster::may_triggle_update_dist() {
    _refresh_cond.notify_one();
}

void Cluster::stop() {
    if (_has_bg_update_dist_thread) {
        bthread_stop(_bg_update_dist_thread);
    }
    {
        std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
        for (auto& iter : _ip2nodes) {
            iter.second->stop();
        }
    }
}

void Cluster::join() {
    if (_has_bg_update_dist_thread) {
        bthread_join(_bg_update_dist_thread, nullptr);
    }
    std::vector<RedisServerPtr> servers;
    {
        std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
        servers.reserve(_ip2nodes.size());
        for (auto& iter : _ip2nodes) {
            servers.push_back(iter.second);
        }
    }
    for (auto& iter : servers) {
        iter->join();
    }
}

Cluster::~Cluster() {
    stop();
    join();
}

void* period_update_dist(void* args);

Status Cluster::init_cluster_dist() {
    Status status = refresh_dist(true);
    if (!status.ok()) {
        LOG(ERROR) << status;
        return status;
    }
    bool ret = _slot_mgr->all_slot_filled();
    if (!ret) {
        std::string msg = fmt::format("failed to init cluster:{}, some slot not filled", name());
        LOG(ERROR) << msg;
        return Status::SysError(msg);
    }
    return Status::OK();
}

Status Cluster::refresh_dist(bool use_seed) {
    int64_t init_sn = _refresh_sn.load();
    std::lock_guard<bthread::Mutex> lock(_refresh_mutex);
    int64_t cur_sn = _refresh_sn.load();
    if (init_sn != cur_sn) {
        VLOG(3) << "skip refresh cluster nodes as refresh sn changed from:" << init_sn
                << " to:" << cur_sn;
        return Status::OK();
    }
    int64_t cur_time_ms = get_current_time_ms();
    if (cur_time_ms < (_last_refresh_time_ms + FLAGS_cluster_change_check_interval_ms)) {
        VLOG(3) << "skip refresh cluster nodes as last refresh time:" << _last_refresh_time_ms
                << " cur time:" << cur_time_ms;
        return Status::OK();
    }
    _last_refresh_time_ms = cur_time_ms;
    std::vector<std::string> seeds;
    if (use_seed) {
        seeds = _options._seeds;
    } else {
        seeds = get_servers_for_cluster_nodes();
    }
    std::random_device rd;
    std::mt19937 rs(rd());
    std::shuffle(seeds.begin(), seeds.end(), rs);

    std::string try_msg;
    for (auto& addr : seeds) {
        Status ret = refresh_cluster_dist_by_cluster_nodes(addr, _slot_mgr);
        if (!ret.ok()) {
            try_msg += fmt::format("[{}, error:{}], ", addr, ret.to_string());
            continue;
        }
        ++_refresh_sn;
        return Status::OK();
    }
    std::string msg = fmt::format("failed to get cluster nodes when init cluster with retry:[{}]",
                                  try_msg);
    if (_online) {
        LOG(WARNING) << msg;
    }
    return Status::SysError(msg);
}

Status Cluster::init() {
    Status ret = init_cluster_dist();
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init redis cluster, from seeds:"
                   << JoinString(_options._seeds, ',');
        return ret;
    }
    if (bthread_start_background(&_bg_update_dist_thread, nullptr, period_update_dist, this) != 0) {
        return Status::SysError("fail to start period_update_dist thread");
    }
    _has_bg_update_dist_thread = true;
    return Status::OK();
}

RedisServerPtr Cluster::get_node(const std::string& addr) {
    std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
    auto iter = _ip2nodes.find(addr);
    if (iter != _ip2nodes.end()) {
        return iter->second;
    }
    return nullptr;
}

std::vector<std::string> Cluster::get_servers() {
    std::vector<std::string> ips;
    std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
    ips.reserve(_ip2nodes.size());
    for (auto& iter : _ip2nodes) {
        ips.push_back(iter.first);
    }
    return ips;
}

std::vector<std::string> Cluster::get_servers_for_cluster_nodes() {
    std::vector<std::string> ips;
    std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
    ips.reserve(_ip2nodes.size());
    if (FLAGS_feature_cluster_nodes_to_leader_only) {
        for (auto& iter : _ip2nodes) {
            if (iter.second->is_leader()) {
                ips.push_back(iter.first);
            }
        }
        if (!ips.empty()) {
            return ips;
        }
    }
    for (auto& iter : _ip2nodes) {
        ips.push_back(iter.first);
    }
    return ips;
}

RedisServerPtr Cluster::add_and_get_node(const std::string& addr) {
    RedisServerPtr server;
    std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
    auto iter = _ip2nodes.find(addr);
    if (iter != _ip2nodes.end()) {
        return iter->second;
    }
    server = std::make_shared<RedisServer>(addr);
    Status ret = server->init(_options._server_options);
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init redis server:" << addr;
        return nullptr;
    }
    VLOG(3) << "add server:[" << server->to_str() << "]";
    _ip2nodes[addr] = server;
    return server;
}

// Status Cluster::parse_cluster_slots_sub_reply(const brpc::RedisReply& sub, SlotMgrPtr slot_mgr) {
//     if (sub.size() < 3) {
//         std::string msg = fmt::format("[cluster slots] sub response expected to be has at least 5
//         "
//                                       "elements, but is current:{}",
//                                       sub.size());
//         LOG(WARNING) << msg;
//         return Status::SysError(msg);
//     }
//     int64_t begin = sub[0].integer();
//     int64_t end = sub[1].integer();
//     std::vector<RedisServerPtr> servers;
//     bool first = true;
//     std::string leader_id;
//     for (size_t i = 2; i < sub.size(); ++i) {
//         const brpc::RedisReply& one = sub[i];
//         if (!one.is_array()) {
//             std::string msg = fmt::format("[cluster slots] does not reply an array on one node");
//             LOG(WARNING) << msg;
//             return Status::SysError(msg);
//         }
//         if (one.size() < 3) {
//             std::string msg = fmt::format(
//                 "[cluster slots] does not reply enough reply on one node, only has:{}",
//                 one.size());
//             LOG(WARNING) << msg;
//             return Status::SysError(msg);
//         }

//         std::string ip = one[0].c_str();
//         int64_t port = one[1].integer();
//         std::string uuid = one[2].c_str();

//         std::string addr = fmt::format("{}:{}", ip, port);
//         RedisServerPtr server;
//         auto iter = _ip2nodes.find(addr);
//         if (iter != _ip2nodes.end()) {
//             server = iter->second;
//         }
//         if (!server) {
//             server = std::make_shared<RedisServer>(addr);
//             Status ret = server->init(_options._server_options);
//             if (!ret.ok()) {
//                 return ret;
//             }
//         }
//         if (first) {
//             server->set_server_role(ServerRole::LEADER);
//             first = false;
//             leader_id = uuid;
//         } else {
//             server->set_server_role(ServerRole::LEADER);
//             server->set_server_role(ServerRole::FOLLOWER);
//         }
//         server->set_leader_id(leader_id);
//         servers.push_back(server);
//     }
//     slot_mgr->fill_slot(begin, end, servers);
//     return Status::OK();
// }

// Status Cluster::parse_cluster_slots(const brpc::RedisReply& resp, SlotMgrPtr slot_mgr) {
//     if (!resp.is_array()) {
//         std::string type = brpc::RedisReplyTypeToString(resp.type());
//         std::string msg = fmt::format(
//             "[cluster slots] response expected to be a array, but is resp type is:{}", type);
//         LOG(WARNING) << msg;
//         return Status::SysError(msg);
//     }
//     for (size_t i = 0; i < resp.size(); ++i) {
//         const brpc::RedisReply& sub = resp[i];
//         if (!sub.is_array()) {
//             std::string type = brpc::RedisReplyTypeToString(sub.type());
//             std::string msg = fmt::format("[cluster slots] sub response expected to be a array, "
//                                           "but is resp type is:{}",
//                                           type);
//             LOG(WARNING) << msg;
//             return Status::SysError(msg);
//         }
//         parse_cluster_slots_sub_reply(sub, slot_mgr);
//     }
//     return Status::OK();
// }

std::string ClusterNodeState::to_str() const {
    std::string str =
        fmt::format("id:{} addr:{} role:{} leader_id:{} config_epoch:{} link_state:{}", _id, _addr,
                    role_to_str(_role), _leader_id, _config_epoch, link_state_to_str(_link_state));
    return str;
}

// flags may be:
// static struct redisNodeFlags redisNodeFlagsTable[] = {{CLUSTER_NODE_MYSELF, "myself,"},
//                                                       {CLUSTER_NODE_MASTER, "master,"},
//                                                       {CLUSTER_NODE_SLAVE, "slave,"},
//                                                       {CLUSTER_NODE_PFAIL, "fail?,"},
//                                                       {CLUSTER_NODE_FAIL, "fail,"},
//                                                       {CLUSTER_NODE_HANDSHAKE, "handshake,"},
//                                                       {CLUSTER_NODE_NOADDR, "noaddr,"},
//                                                       {CLUSTER_NODE_NOFAILOVER, "nofailover,"}};

Status Cluster::parse_node(const std::string& data, ClusterNodeState* state) {
    // data format
    // <id>                             f577054e5e5c415fa6f833069993c6bb0d569e6c
    // <ip:port>                        127.0.0.1:21031@31031    or  :0@0
    // <flags>                          master      slave      master,fail,noaddr
    // <master>                         - 1372c5914a8879909cf995097f51d92588ea6cb3
    // <ping-sent>                      0
    // <pong-recv>                      1673785700432
    // <config-epoch>                   3 6 2  // config epoch of the node or epoch of master
    // <link-state>                     connected   disconnected <slot> <slot> ...
    // <slot>                           10923-16383
    VLOG(3) << "start to parse cluster nodes resp of one node:" << data;
    std::vector<std::string> part;
    butil::SplitString(data, ' ', &part);
    if (part.size() < 8) {
        std::string msg = fmt::format("cluster nodes every response line expect to have at least 8 "
                                      "elements, but current:{}, str:[{}]",
                                      part.size(), data);
        LOG(WARNING) << msg;
        return Status::SysError(msg);
    }
    const std::string& uuid = part[0];
    std::string addr = part[1];
    auto at_pos = addr.find("@");
    if (at_pos != std::string::npos) {
        addr = addr.substr(0, at_pos);
    }
    if (addr.find(":0") == 0) {
        std::string msg = fmt::format("skip node:{} as address is ':0'", data);
        LOG(INFO) << msg;
        return Status::EAgain(msg);
    }
    std::string role_str = part[2];
    ServerRole role = RedisServer::parse_role_state(role_str);
    const std::string& leader_id = part[3];
    int64_t config_epoch = strtol(part[6].c_str(), nullptr, 10);
    if (std::to_string(config_epoch) != part[6]) {
        std::string msg = fmt::format("invalid epoch:{} in cluster nodes reponse", part[6]);
        LOG(WARNING) << msg;
        return Status::SysError(msg);
    }
    state->_id = uuid;
    state->_addr = addr;
    state->_role = role;
    state->_leader_id = leader_id == "-" ? uuid : leader_id;
    state->_config_epoch = config_epoch;
    state->_link_state = str_to_link_state(part[7]);
    int64_t mmhash[2];
    butil::MurmurHash3_x86_128((char*)data.data(), data.length(), 0, (void*)mmhash);
    state->_murmur_hash.append((char*)&(mmhash[0]), sizeof(int64_t));
    state->_murmur_hash.append((char*)&(mmhash[1]), sizeof(int64_t));
    if (part.size() > 8) {
        for (size_t x = 8; x < part.size(); ++x) {
            state->_slot_str.push_back(part[x]);
        }
    }
    return Status::OK();
}

Status Cluster::update_cluster_dist(
    const std::map<std::string, std::vector<ClusterNodeState>>& state, SlotMgrPtr slot_mgr) {
    std::set<std::string> all_nodes;
    for (auto& iter : state) {
        for (auto& node : iter.second) {
            all_nodes.insert(node._addr);
        }
    }
    std::vector<std::string> to_remove;
    for (auto& iter : _ip2nodes) {
        if (all_nodes.find(iter.first) == all_nodes.end()) {
            to_remove.push_back(iter.first);
        }
    }
    bool has_abnormal = false;
    for (auto& iter : state) {
        std::vector<RedisServerPtr> servers;
        RedisServerPtr leader;
        ClusterNodeState leader_state;
        for (auto& node : iter.second) {
            RedisServerPtr server = add_and_get_node(node._addr);
            if (!server) {
                std::string msg = fmt::format("failed to init connect to redis server:{}",
                                              node._addr);
                LOG(ERROR) << msg;
                return Status::SysError(msg);
            }
            if (node._link_state != LinkState::CONNECTED) {
                has_abnormal = true;
                continue;
            }
            if (node._config_epoch < server->config_epoch()) {
                LOG(WARNING) << "skip node:" << node._addr
                             << " as config epoch is smaller than current:"
                             << server->config_epoch() << " node epoch:" << node._config_epoch;
                continue;
            }
            server->update(node._id, node._role, node._leader_id, node._config_epoch,
                           node._link_state);

            {
                std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
                _ip2nodes[node._addr] = server;
            }
            servers.push_back(server);
            if (node._role == ServerRole::LEADER) {
                leader = server;
                leader_state = node;
            }
        }
        if (leader && leader_state._slot_str.size() > 0) {
            for (auto& sstr : leader_state._slot_str) {
                slot_mgr->update_slot(sstr, leader, servers, leader_state._config_epoch);
            }
            LOG(INFO) << "succeed to add/update redis server:[" << leader->to_str() << "]";
        }
    }
    if (!has_abnormal) {
        std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
        for (auto& ip : to_remove) {
            auto iter = _ip2nodes.find(ip);
            if (iter == _ip2nodes.end()) {
                continue;
            }
            LOG(INFO) << "succeed to remove redis server:[" << iter->second->to_str() << "]";
            _ip2nodes.erase(ip);
        }
    }
    return Status::OK();
}

Status Cluster::parse_cluster_nodes(const brpc::RedisResponse& resp, SlotMgrPtr slot_mgr) {
    if (resp.reply_size() < 1) {
        std::string msg = fmt::format("[cluster nodes] response expected to be a array, but is "
                                      "resp cnt:{}",
                                      resp.reply_size());
        LOG(WARNING) << msg;
        return Status::SysError(msg);
    }
    const brpc::RedisReply& reply0 = resp.reply(0);
    if (!reply0.is_string()) {
        std::string type = brpc::RedisReplyTypeToString(reply0.type());
        std::string msg = fmt::format("[cluster nodes] sub response expected to be a string, "
                                      "but is resp type is:{}",
                                      type);
        LOG(WARNING) << msg;
        return Status::SysError(msg);
    }
    std::vector<std::string> lines;
    std::string data = reply0.c_str();
    butil::SplitString(data, '\n', &lines);

    std::map<std::string, std::vector<ClusterNodeState>> state;
    std::string new_digest;
    new_digest.reserve(lines.size() * 2);
    for (size_t i = 0; i < lines.size(); ++i) {
        const std::string& str = lines[i];
        if (str == "") {
            continue;
        }
        ClusterNodeState node_state;
        Status ret = parse_node(str, &node_state);
        if (ret.is_eagain()) {
            continue;
        }
        if (!ret.ok()) {
            return ret;
        }
        // if (node_state._role != ServerRole::LEADER) {
        //     continue;
        // }
        new_digest.append(node_state._murmur_hash);
        state[node_state._leader_id].push_back(std::move(node_state));
    }
    if (FLAGS_use_digest_to_compare && !_cluster_digest.empty() && new_digest == _cluster_digest) {
        VLOG(3) << "cluster nodes response is same as before, skip it";
        return Status::OK();
    }
    if (state.empty()) {
        std::string msg = fmt::format("cluster nodes response is empty");
        LOG(WARNING) << msg;
        return Status::SysError(msg);
    }
    update_cluster_dist(state, slot_mgr);
    return Status::OK();
}

Status Cluster::refresh_cluster_dist_by_cluster_nodes(const std::string& addr,
                                                      SlotMgrPtr slot_mgr) {
    LOG(INFO) << "start to refresh cluster dist by cluster nodes, from addr:" << addr
              << ", sn:" << _refresh_sn;
    RedisServerPtr node = add_and_get_node(addr);
    if (!node) {
        std::string msg = fmt::format("failed to init connect to redis server:{}", addr);
        return Status::SysError(msg);
    }
    brpc::RedisResponse resp;
    int index = 0;
    if (FLAGS_feature_cluster_nodes_in_slow_channel) {
        index = -1;
    }
    Status ret = node->send_inner_cmd(S_CMD_CLUSTER_NODES, &resp, index);
    if (!ret.ok()) {
        if (_online) {
            LOG(WARNING) << ret;
        }
        return ret;
    }
    ret = parse_cluster_nodes(resp, slot_mgr);
    return ret;
}

void Cluster::set_online(bool on) {
    _online = on;
    std::lock_guard<bthread::Mutex> lock(_nodes_mutex);
    for (auto& iter : _ip2nodes) {
        iter.second->set_online(on);
    }
}

bool Cluster::is_big_key(uint64_t key) {
    return _big_key_mgr.has(key);
}

void Cluster::add_big_key(uint64_t sign, const std::string& key, uint64_t size) {
    _big_key_mgr.add(sign, key);
    (*_big_key_size) << size;
}

void Cluster::remove_big_key(uint64_t key) {
    _big_key_mgr.remove(key);
}

void Cluster::print_and_auto_tune() {
    uint64_t big_key_count = _big_key_mgr.count();
    uint64_t max_key_count = FLAGS_big_key_cnt_per_cluster;
    uint64_t threshold = _big_key_mgr.threshold();
    uint64_t ravg_len = _read_size->latency();
    uint64_t rlen_99 = _read_size->latency_percentile(0.99);
    uint64_t rlen_999 = _read_size->latency_percentile(0.999);

    uint64_t wavg_len = _write_size->latency();
    uint64_t wlen_99 = _write_size->latency_percentile(0.99);
    uint64_t wlen_999 = _write_size->latency_percentile(0.999);

    uint64_t cur = get_current_time_ms();
    if ((_last_print_time_ms + FLAGS_print_big_key_interval_ms) <= cur) {
        _last_print_time_ms = cur;
        std::string msg = fmt::format("cluster:[{}] big_key_count:{} local_max_big_key_count:{} "
                                      "threshold_in_bytes:{} ravg_len:{} rlen99:{} len999:{} "
                                      "wavg_len:{} wlen99:{} len999:{}",
                                      name(), big_key_count, max_key_count, threshold, ravg_len,
                                      rlen_99, rlen_999, wavg_len, wlen_99, wlen_999);
        if (FLAGS_print_slow_state_as_warn && max_key_count == big_key_count) {
            LOG(WARNING) << msg;
        } else {
            LOG(INFO) << msg;
        }
    }
    if (FLAGS_feature_auto_tune_big_key_threshold) {
        if ((_last_tune_time_ms + FLAGS_auto_tune_big_key_interval_ms) <= cur) {
            _last_tune_time_ms = cur;
            if (rlen_999 > threshold) {
                LOG(INFO) << "auto tune big key threshold of cluster:" << name()
                          << " from:" << threshold << " to: " << rlen_999;
                _big_key_mgr.update_threshold(rlen_999);
            }
        }
    }
}

void Cluster::dump_big_key(std::unordered_set<std::string>* keys) {
    _big_key_mgr.dump(keys);
}

void Cluster::add_metrics(const std::string& method, const std::string& server, bool succ,
                          uint64_t us, int req_size, int resp_size, int cnt) {
    if (!_metrics) {
        return;
    }
    _metrics->add_latency(_options._name, method, server, succ, us, cnt);
    uint64_t threshold = FLAGS_slow_log_threshold_ms;
    if (us >= threshold * 1000L) {
        LOG(INFO) << "slowlog in cluster=[" << _options._name << "] cmd=[" << method << "] from = ["
                  << server << "] cost = " << us << " us , succ = " << (succ ? "true" : "false")
                  << " req_size = " << req_size << " resp_size = " << resp_size;
    }
}

// Status Cluster::refresh_cluster_dist_by_cluster_slots(const std::string& addr,
//                                                       SlotMgrPtr slot_mgr) {
//     RedisServerPtr node = get_node(addr);
//     if (!node) {
//         node = std::make_shared<RedisServer>(addr);
//         Status ret = node->init(_options._server_options);
//         if (!ret.ok()) {
//             return ret;
//         }
//     }

//     RedisResp resp;
//     Status ret = node->send_cmd(S_CMD_CLUSTER_SLOTS, &resp);
//     if (!ret.ok()) {
//         LOG(WARNING) << ret;
//         return ret;
//     }
//     if (resp._resp.reply_size() != 1) {
//         std::string msg = fmt::format("expect cluster slots reply_size as 1, but got {}",
//                                       resp._resp.reply_size());
//         LOG(WARNING) << msg;
//         return Status::SysError(msg);
//     }
//     // VLOG(3) << "cluster slots, reply cnt:" << resp._resp.reply_size();

//     const brpc::RedisReply& reply = resp._resp.reply(0);
//     ret = parse_cluster_slots(reply, slot_mgr);
//     if (ret.ok()) {
//         ++_refresh_sn;
//     }
//     return ret;
// }

void* period_update_dist(void* args) {
    Cluster* cluster = static_cast<Cluster*>(args);
    int64_t pull_interval_s = std::max(1, cluster->mutable_options()->_bg_refresh_interval_s);
    int64_t interval_s = 5 + butil::fast_rand() % pull_interval_s;
    bthread::Mutex* bg_mutex = cluster->mutable_bg_refresh_mutex();
    bthread::ConditionVariable* cond = cluster->mutable_refresh_cond();
    while (true) {
        {
            std::unique_lock<bthread::Mutex> lock(*bg_mutex);
            cond->wait_for(lock, interval_s * 1000000L);
        }
        if (bthread_stopped(bthread_self())) {
            break;
        }
        cluster->refresh_dist(false);
    }
    LOG(INFO) << "cluster:[" << cluster->name() << "] bg refresh thread exit";
    return nullptr;
}

void ClusterManager::clear() {
    std::vector<ClusterPtr> clusters;
    get_all(&clusters);
    clear_all();

    for (auto& iter : clusters) {
        iter->stop();
    }
    for (auto& iter : clusters) {
        iter->join();
    }
}

void ClusterManager::stop() {
    std::vector<ClusterPtr> clusters;
    get_all(&clusters);
    for (auto& iter : clusters) {
        iter->stop();
    }
}

void ClusterManager::join() {
    std::vector<ClusterPtr> clusters;
    get_all(&clusters);

    for (auto& iter : clusters) {
        iter->join();
    }
}

void ClusterManager::get_all(std::vector<ClusterPtr>* all) {
    butil::DoublyBufferedData<ClusterMap>::ScopedPtr ptr;
    if (_clusters.Read(&ptr) != 0) {
        return;
    }
    for (auto& iter : *ptr) {
        all->push_back(iter.second);
    }
}

bool ClusterManager::remove(const std::string& cluster_name) {
    std::lock_guard<bthread::Mutex> lock(_clusters_lock);
    _clusters.Modify(remove_cluster_impl, cluster_name);
    return true;
}

ClusterPtr ClusterManager::get(const std::string& name) {
    butil::DoublyBufferedData<ClusterMap>::ScopedPtr ptr;
    if (_clusters.Read(&ptr) != 0) {
        return nullptr;
    }
    auto it = ptr->find(name);
    if (it == ptr->end()) {
        return nullptr;
    }
    return it->second;
}

Status ClusterManager::add_cluster(const ClusterOptions& options) {
    std::lock_guard<bthread::Mutex> lock(_clusters_lock);
    if (has_cluster(options._name)) {
        return Status::SysError("same cluster:" + options._name + " already exist");
    }
    ClusterPtr cluster = std::make_shared<Cluster>(options);
    cluster->set_client_metrics(_metrics);
    Status ret = cluster->init();
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init cluster:" << options._name << " with err:" << ret;
        return ret;
    }
    _clusters.Modify(add_cluster_impl, cluster);
    return Status::OK();
}

bool ClusterManager::has_cluster(const std::string& name) {
    butil::DoublyBufferedData<ClusterMap>::ScopedPtr ptr;
    if (_clusters.Read(&ptr) != 0) {
        return false;
    }
    return ptr->count(name) > 0;
}

void ClusterManager::set_online(const std::string& name) {
    auto cluster = get(name);
    if (!cluster) {
        return;
    }
    cluster->set_online(true);
}

void ClusterManager::set_offline(const std::string& name) {
    auto cluster = get(name);
    if (!cluster) {
        return;
    }
    cluster->set_online(false);
}

void ClusterManager::bg_work() {
    std::vector<ClusterPtr> clusters;
    get_all(&clusters);
    for (auto& iter : clusters) {
        iter->print_and_auto_tune();
    }
}

void ClusterManager::clear_all() {
    std::lock_guard<bthread::Mutex> lock(_clusters_lock);
    _clusters.Modify(clear_all_impl);
}

size_t ClusterManager::add_cluster_impl(ClusterMap& cm, ClusterPtr cluster) {
    if (cm.count(cluster->name()) > 0) {
        return 1;
    }
    cm[cluster->name()] = cluster;
    return 1;
}

size_t ClusterManager::remove_cluster_impl(ClusterMap& cm, const std::string& name) {
    cm.erase(name);
    return 1;
}

size_t ClusterManager::clear_all_impl(ClusterMap& cm) {
    cm.clear();
    return 1;
}

void ClusterManager::dump_big_key(ClusterKeys* key_sets) {
    std::vector<ClusterPtr> clusters;
    get_all(&clusters);
    for (auto& iter : clusters) {
        std::unordered_set<std::string> big_keys;
        iter->dump_big_key(&big_keys);
        key_sets->insert(std::make_pair(iter->name(), std::move(big_keys)));
    }
}

#ifdef WITH_PROMETHEUS
// NOTE: A simpler alternative is to using a plain `void*` context pointer
//       here, by casting it to `prometheus::Family<prometheus::Histogram>*`
//       whenever necessary. However, it needs complex transition when
//       multiple fields are required.
struct ClientMetrics::PrometheusContext {
    prometheus::Family<prometheus::Histogram>* _latency_histogram = nullptr;
    prometheus::Family<prometheus::Counter>* _error_counter = nullptr;
    prometheus::Family<prometheus::Counter>* _version = nullptr;
};
#endif // WITH_PROMETHEUS

ClientMetrics::ClientMetrics(prometheus::Registry* reg) : _registry(reg) {
#ifdef WITH_PROMETHEUS
    _context = new PrometheusContext();
#endif // WITH_PROMETHEUS
}

ClientMetrics::~ClientMetrics() {
#ifdef WITH_PROMETHEUS
    delete _context;
#endif // WITH_PROMETHEUS
}

void ClientMetrics::init() {
#ifdef WITH_PROMETHEUS
    if (!_registry || !_context) {
        return;
    }
    std::vector<std::string> parts;
    butil::SplitString(FLAGS_latency_metric_buckets, ',', &parts);
    std::vector<uint64_t> time_buckets;
    for (auto& one : parts) {
        if (one.empty()) {
            continue;
        }
        uint64_t cost = strtol(one.c_str(), nullptr, 10);
        std::string str = std::to_string(cost);
        if (str != one) {
            LOG(WARNING) << "skip empty time bucket in [" << FLAGS_latency_metric_buckets << "]";
            continue;
        }
        time_buckets.push_back(cost);
    }
    std::sort(time_buckets.begin(), time_buckets.end());
    if (time_buckets.empty()) {
        _latency_buckets_ms = S_DEFAULT_LATENCY_BUCKETS_MS;
    } else {
        for (auto& b : time_buckets) {
            _latency_buckets_ms.push_back((double)b);
        }
    }
    _context->_latency_histogram = &(prometheus::BuildHistogram()
                                         .Name("redis_client_requests_duration_ms")
                                         .Help("redis client duration ms histogram")
                                         .Register(*_registry));

    _context->_error_counter = &(prometheus::BuildCounter())
                                    .Name("redis_client_requests_error_total")
                                    .Help("redis error count for redis client")
                                    .Register(*_registry);

    _context->_version = &(prometheus::BuildCounter())
                              .Name("redis_client_cpp_version")
                              .Help("redis client version")
                              .Register(*_registry);
    auto& verison = _context->_version->Add({{"mode", "proxyless"}, {"version", "1.0.0"}});
    verison.Increment();
#endif // WITH_PROMETHEUS
}

void ClientMetrics::add_latency(const std::string& cluster, const std::string& method,
                                const std::string& server, bool succ, uint64_t us, int cnt) {
#ifdef WITH_PROMETHEUS
    if (!_context || !_context->_latency_histogram || !FLAGS_feature_export_to_prometheus) {
        return;
    }
    std::string addr = FLAGS_feature_collect_redis_server_metrics ? server : "default";

    auto& lable = _context->_latency_histogram->Add({{"name", cluster},
                                                     {"addr", addr},
                                                     {"command", method}},
                                                    _latency_buckets_ms);
    for (int i = 0; i < cnt; ++i) {
        lable.Observe(us / 1000);
    }

    if (!succ) {
        if (!_context->_error_counter) {
            return;
        }
        auto& error_counter_ref = _context->_error_counter->Add(
            {{"name", cluster}, {"addr", addr}, {"command", method}});
        for (int i = 0; i < cnt; ++i) {
            error_counter_ref.Increment();
        }
    }

#endif // WITH_PROMETHEUS
}

} // namespace client
} // namespace rsdk
