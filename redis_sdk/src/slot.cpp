#include <map>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <butil/strings/string_split.h>
#include <butil/strings/string_util.h>
#include <fmt/format.h>

#include "redis_sdk/include/cluster.h"

namespace rsdk {
namespace client {

// TODO: Avoid using non-trivial variables with static storage duration.
const std::string S_CMD_CLUSTER_NODES = "cluster nodes";
const std::string S_CMD_CLUSTER_SLOTS = "cluster slots";
const size_t S_SLOT_CNT = 16384;

static const int S_API_VERSION = 100;

std::string Slot::describe() const {
    std::string msg = fmt::format("id:{} sn:{} version:{} config_epoch:{} addr:{}", _id, _sn,
                                  _version, _config_epoch.load(), _leader->addr());
    return msg;
}

void Slot::update_servers(RedisServerPtr leader, const std::vector<RedisServerPtr>& servers,
                          int64_t cepoch) {
    if (servers.empty() || leader == nullptr) {
        return;
    }
    std::lock_guard<bthread::Mutex> lock(_servers_mutex);
    if (cepoch < _config_epoch.load()) {
        return;
    }
    _leader = leader;
    _servers = servers;
    _config_epoch.store(cepoch);
}

RedisServerPtr Slot::get_server_for_read(ReadPolicy policy) {
    std::lock_guard<bthread::Mutex> lock(_servers_mutex);
    if (_servers.empty()) {
        return _leader;
    }
    if (policy == ReadPolicy::LEADER_ONLY) {
        return _leader;
    }
    ++_acc_index;
    _acc_index %= _servers.size();
    // VLOG(3) << "retry index:" << _acc_index << " size:" << _servers.size() << " addr:"
    //         << _servers[_acc_index]->addr() << " leader:" << _leader->addr();
    return _servers[_acc_index];
}

RedisServerPtr Slot::get_leader() {
    std::lock_guard<bthread::Mutex> lock(_servers_mutex);
    return _leader;
}

SlotMgr::SlotMgr() {
    _slots.resize(S_SLOT_CNT, nullptr);
}

SlotPtr SlotMgr::get_slot_by_id(uint64_t slot_id) {
    return _slots[slot_id];
}
uint32_t key_to_slot_id(const char* key, int keylen);
SlotPtr SlotMgr::get_slot_by_key(const std::string& key) {
    uint32_t id = key_to_slot_id(key.c_str(), key.length());
    // NOTE: the slot count can not be modified
    return get_slot_by_id(id);
}

bool SlotMgr::add_slot(SlotPtr slot) {
    if (_slots[slot->id()] == nullptr) {
        _slots[slot->id()] = slot;
        ++_valid_slot_cnt;
        return true;
    }
    return false;
}

Status SlotMgr::update_slot(const std::string& data, RedisServerPtr leader,
                            const std::vector<RedisServerPtr>& servers, int64_t config_epoch) {
    if (leader == nullptr || servers.empty()) {
        std::string msg = fmt::format("bug, invalid slot:[{}] of cluster nodes on node:nullptr",
                                      data);
        LOG(ERROR) << msg;
        return Status::SysError(msg);
    }
    // 0-5460 [8->-15d9d385e01496cafac99b3013a4af46969b76e4]
    if (data.empty() || (data.at(0) == '[' && data.at(data.length() - 1) == ']')) {
        LOG(INFO) << "skip moving slot:" << data << ", just use current state";
        return Status::OK();
    }
    int64_t start_slot = 0;
    int64_t end_slot = 0;

    if (data.find("-") != std::string::npos) {
        std::vector<std::string> part;
        butil::SplitString(data, '-', &part);
        if (part.size() != 2) {
            std::string msg = fmt::format("invalid slot range:[{}] of cluster nodes on node:{}",
                                          data, leader->to_str());
            LOG(WARNING) << msg;
            return Status::SysError(msg);
        }
        start_slot = strtol(part[0].c_str(), nullptr, 10);
        end_slot = strtol(part[1].c_str(), nullptr, 10);
        if (std::to_string(start_slot) != part[0] || std::to_string(end_slot) != part[1]) {
            std::string msg = fmt::format("invalid slot range:[{}] of cluster nodes on node:{}",
                                          data, leader->to_str());
            LOG(WARNING) << msg;
            return Status::SysError(msg);
        }
    } else {
        start_slot = strtol(data.c_str(), nullptr, 10);
        if (std::to_string(start_slot) != data) {
            std::string msg = fmt::format("invalid slot:[{}] of cluster nodes on node:{}", data,
                                          leader->to_str());
            LOG(WARNING) << msg;
            return Status::SysError(msg);
        }
        end_slot = start_slot;
    }
    for (int id = start_slot; id <= end_slot; ++id) {
        bool fill_slot = false;
        SlotPtr slot = get_slot_by_id(id);
        if (!slot) {
            slot = std::make_shared<Slot>("", id, id);
            fill_slot = true;
        }

        slot->update_servers(leader, servers, config_epoch);

        if (fill_slot) {
            add_slot(slot);
        }
    }
    return Status::OK();
}

} // namespace client
} // namespace rsdk
