#ifndef RSDK_API_INCLUDE_SLOT_H
#define RSDK_API_INCLUDE_SLOT_H

#include <map>
#include <string>
#include <thread>

#include <brpc/channel.h>
#include <brpc/redis.h>
#include <bthread/bthread.h>
#include <bthread/mutex.h>
#include <bvar/bvar.h>
#include <fmt/format.h>

#include "redis_sdk/api_status.h"
#include "redis_sdk/include/api_util.h"
#include "redis_sdk/include/redis_server.h"

namespace rsdk {
namespace client {

extern const std::string S_CMD_CLUSTER_NODES;
extern const std::string S_CMD_CLUSTER_SLOTS;
extern const size_t S_SLOT_CNT;

class Slot {
public:
    Slot(const std::string& cluster_name, uint64_t id, uint64_t version, int partition_id) :
            _id(id),
            _cluster_name(cluster_name),
            _sn(0),
            _version(version) {
    }
    Slot(const std::string& cluster_name, uint64_t id, int partition_id) :
            Slot(cluster_name, id, 0, partition_id) {
    }

    uint64_t id() const {
        return _id;
    }
    const std::string& cluster_name() const {
        return _cluster_name;
    }
    std::string to_string() const {
        return "slot:" + _cluster_name + "#" + std::to_string(_id) + +":"
               + " version:" + std::to_string(_version);
    }
    std::string desc() const {
        return fmt::format("{}#{}", _cluster_name, _id);
    }

    int64_t get_sn() const {
        std::lock_guard<bthread::Mutex> lock(_servers_mutex);
        return _sn;
    }
    void set_sn(int64_t sn) {
        std::lock_guard<bthread::Mutex> lock(_servers_mutex);
        _sn = sn;
    }
    int64_t config_epoch() const {
        return _config_epoch.load();
    }

    int64_t version() {
        std::lock_guard<bthread::Mutex> lock(_servers_mutex);
        return _version;
    }
    Status fill_slot(int64_t begin, int64_t end, const std::vector<RedisServerPtr>& servers);
    RedisServerPtr get_leader();
    RedisServerPtr get_server_for_read(ReadPolicy policy);
    std::string describe() const;

    void update_servers(RedisServerPtr leader, const std::vector<RedisServerPtr>& servers,
                        int64_t cepoch);

private:
    uint64_t _id = 0;
    std::string _cluster_name;

    int64_t _sn = 0;
    int64_t _version = 0;
    int _start = 0;
    int _end = 0;
    std::string _server_sign;
    mutable bthread::Mutex _servers_mutex;
    RedisServerPtr _leader;
    std::vector<RedisServerPtr> _servers;
    int _acc_index = 0;
    std::atomic<int64_t> _config_epoch = {0};
};

using SlotPtr = std::shared_ptr<Slot>;

class SlotMgr {
public:
    SlotMgr();
    SlotPtr get_slot_by_id(uint64_t slot_id);
    SlotPtr get_slot_by_key(const std::string& key);
    Status update_slot(const std::string& data, RedisServerPtr server,
                       const std::vector<RedisServerPtr>& servers, int64_t epoch);
    bool add_slot(SlotPtr slot);

    void set_sn(int64_t sn) {
        _sn = sn;
    }
    int64_t sn() const {
        return _sn.load();
    }
    bool all_slot_filled() const {
        return _valid_slot_cnt.load() == (int)S_SLOT_CNT;
    }

private:
    std::atomic<int64_t> _sn = {0};
    std::vector<SlotPtr> _slots;
    std::atomic<int> _valid_slot_cnt = {0};
};
typedef std::shared_ptr<SlotMgr> SlotMgrPtr;

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_SLOT_H
