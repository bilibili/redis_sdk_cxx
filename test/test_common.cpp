#include <butil/rand_util.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "test/test_common.h"

namespace rsdk {
namespace client {
namespace test {

std::string build_slot_dist(const std::vector<int>& slot_ids) {
    std::vector<int> ids(slot_ids);
    std::sort(ids.begin(), ids.end());
    std::string dist;
    for (size_t i = 0; i < ids.size();) {
        int start = ids[i];
        int end = start;
        size_t end_pos = i;
        for (int j = i + 1; j < (int)ids.size(); ++j) {
            if (ids[j] == ids[j - 1] + 1) {
                end = ids[j];
                end_pos = j;
                continue;
            }
            break;
        }
        if (end_pos != i) {
            dist += std::to_string(start) + "-" + std::to_string(end);
            i = end_pos + 1;
        } else {
            dist += std::to_string(start);
            ++i;
        }
        if (i != ids.size()) {
            dist += " ";
        }
    }
    return dist;
}

void build_slot_id(std::vector<bool>* used_state, int cnt, std::vector<int>* slot_ids) {
    int start_pos = butil::fast_rand_less_than(used_state->size());
    int filled_cnt = 0;
    while (filled_cnt < cnt) {
        int pos = start_pos % used_state->size();
        if (!(*used_state)[pos]) {
            slot_ids->push_back(pos);
            (*used_state)[pos] = true;
            ++filled_cnt;
        }
        ++start_pos;
    }
}

int build_cluster(const std::vector<MockRedisServerPtr>& servers, int slave_cnt, std::string* desc,
                  ClusterDist* dist, std::unordered_map<int, std::string>* slot2addr) {
    int per_group = slave_cnt + 1;
    if (servers.size() % per_group != 0) {
        LOG(ERROR) << "invalid server count:" << servers.size() << ", slave_cnt:" << slave_cnt;
        return -1;
    }
    int index = 0;

    int group_cnt = servers.size() / per_group;
    std::map<int, int> slot_per_group;
    int free_cnt = 16384;
    int total_slot_cnt = free_cnt;
    for (int i = 0; i < group_cnt; ++i) {
        int expect = total_slot_cnt / group_cnt;
        int slot_cnt = butil::fast_rand_less_than(expect * 1.5);
        if (slot_cnt > free_cnt) {
            slot_cnt = free_cnt;
        }
        slot_per_group[i] = slot_cnt;
        free_cnt -= slot_cnt;
        if ((i + 1) == group_cnt && free_cnt > 0) {
            slot_per_group[i] += free_cnt;
        }
        VLOG(3) << "set group:" << i << " slot cnt:" << slot_per_group[i];
    }
    std::vector<bool> state;
    state.resize(total_slot_cnt, false);

    for (int i = 0; i < group_cnt; ++i) {
        LeaderAndSlaves group;
        MockRedisServerPtr leader = servers[index];
        std::vector<int> slot_ids;
        build_slot_id(&state, slot_per_group[i], &slot_ids);
        std::string slot_dist = build_slot_dist(slot_ids);
        for (int j = 0; j < slave_cnt; ++j) {
            MockRedisServerPtr slave = servers[index + j + 1];
            slave->set_leader_name(leader->name());
            group._slaves.push_back(slave);
            desc->append(slave->build_cluster_nodes() + "\n");
        }
        VLOG(3) << "set group:" << i << " slot dist:" << slot_dist;
        leader->set_slot_dist(slot_dist);
        leader->set_slot_ids(slot_ids);
        leader->set_leader_name(leader->name());
        group._leader = leader;
        dist->_leaders_and_slaves.push_back(std::move(group));
        index += per_group;
        desc->append(leader->build_cluster_nodes());
        if (i != group_cnt - 1) {
            desc->append("\n");
        }
        for (int slot_id : slot_ids) {
            (*slot2addr)[slot_id] = leader->addr();
        }
    }
    LOG(INFO) << "mock cluster nodes:" << *desc;
    return 0;
}

int setup_redis_server(int cnt, std::vector<MockRedisServerPtr>* servers) {
    for (int i = 0; i < cnt; ++i) {
        MockRedisServerPtr server = std::make_shared<MockRedisServer>(i);
        int ret = server->start();
        if (0 != ret) {
            LOG(ERROR) << "failed to start mock redis server";
            return ret;
        }
        servers->push_back(server);
    }
    return 0;
}

void update_slot_pointer(const std::string& cname, ClientManagerPtr mgr, int slot_id) {
    ClientManagerImpl* cli_mgr = dynamic_cast<ClientManagerImpl*>(mgr.get());
    ClusterPtr cluster = cli_mgr->get_cluster_mgr()->get(cname);
    if (!cluster) {
        LOG(ERROR) << "cluster:" << cname << " is not found";
        return;
    }
    SlotPtr slot = cluster->get_slot_by_id(slot_id);
    if (!slot) {
        LOG(ERROR) << "slot:" << slot_id << " is not found in cluster:" << cname;
        return;
    }
    VLOG(3) << "start to update slot:" << slot_id << " server address:" << slot->_leader->addr();
    std::lock_guard<bthread::Mutex> lock(cluster->_nodes_mutex);
    for (auto& iter : cluster->_ip2nodes) {
        RedisServerPtr server = iter.second;
        std::lock_guard<bthread::Mutex> lock(slot->_servers_mutex);
        if (server->is_leader() && server->addr() != slot->_leader->addr()) {
            VLOG(3) << "update slot:" << slot_id << " server addr from:" << slot->_leader->_addr
                    << " to:" << server->addr();
            slot->_leader = server;
            break;
        }
    }
}

void setup_cluster(const std::string& cname, int server_cnt, int slave_per_server,
                   std::vector<MockRedisServerPtr>* servers, std::vector<std::string>* seeds) {
    int ret = setup_redis_server(server_cnt, servers);
    ASSERT_EQ(0, ret);
    std::string cluster_nodes;
    ClusterDist dist;
    std::unordered_map<int, std::string> slot2addr;
    ret = build_cluster(*servers, slave_per_server, &cluster_nodes, &dist, &slot2addr);
    ASSERT_EQ(0, ret);

    for (auto& server : *servers) {
        seeds->push_back(server->addr());
        server->_service->_cluster_nodes = cluster_nodes;
        server->set_slot2addr(slot2addr);
    }
}

void rand_put_get(ClientPtr client, int cnt) {
    for (int i = 0; i < cnt; ++i) {
        brpc::RedisResponse resp;
        std::string key = std::to_string(butil::fast_rand_less_than(1000000));
        std::string cmd = "set " + key + " " + key;
        client->exec(cmd, &resp);
        ASSERT_TRUE(resp.reply(0).is_string());
        ASSERT_STREQ("OK", resp.reply(0).c_str());
        brpc::RedisResponse resp2;
        cmd = "get " + key;
        client->exec(cmd, &resp2);
        ASSERT_TRUE(resp2.reply(0).is_string());
        ASSERT_STREQ(key.c_str(), resp2.reply(0).c_str());
    }
}

void build_mset(int cnt, bool str_mode, brpc::RedisRequest* req) {
    std::vector<std::string> cmds;
    cmds.push_back("mSEt");
    for (int i = 0; i < cnt; ++i) {
        std::string key = std::to_string(i);
        std::string value;
        if (str_mode) {
            value = std::to_string(i);
        } else {
            value.append((char*)&i, sizeof(i));
        }
        cmds.push_back(key);
        cmds.push_back(value);
    }
    butil::StringPiece parts[cmds.size()];
    for (size_t i = 0; i < cmds.size(); ++i) {
        parts[i].set(cmds[i].c_str(), cmds[i].size());
    }
    req->AddCommandByComponents(parts, cmds.size());
}

void build_mset(int cnt, RecordList* recs) {
    for (int i = 0; i < cnt; ++i) {
        std::string key = std::to_string(i);
        std::string value;
        value.append((char*)&i, sizeof(i));
        Record rec;
        rec._key = key;
        rec._data = value;
        recs->push_back(rec);
        rec._errno = -1;
    }
}

void build_mget(int cnt, brpc::RedisRequest* req) {
    std::vector<std::string> cmds;
    cmds.push_back("MGET");
    for (int i = 0; i < cnt; ++i) {
        cmds.push_back(std::to_string(i));
    }
    butil::StringPiece parts[cmds.size()];
    for (size_t i = 0; i < cmds.size(); ++i) {
        parts[i].set(cmds[i].c_str(), cmds[i].size());
    }
    req->AddCommandByComponents(parts, cnt + 1);
}

void build_mget(int cnt, RecordList* recs) {
    for (int i = 0; i < cnt; ++i) {
        std::string key = std::to_string(i);
        Record rec;
        rec._key = key;
        recs->push_back(rec);
    }
}
void clear_value(RecordList* recs) {
    for (auto& rec : *recs) {
        rec._data.clear();
        rec._errno = -1;
    }
}

void set_crud_status(std::vector<MockRedisServerPtr>& servers, int cnt, const std::string& status) {
    for (auto& server : servers) {
        server->_service->_mock_status_cnt = cnt;
        server->_service->_crud_status = status;
    }
}
void set_sleep_time(std::vector<MockRedisServerPtr>& servers, int sleep_cnt, int sleep_ms) {
    for (auto& server : servers) {
        server->_service->_sleep_cnt = sleep_cnt;
        server->_service->_sleep_ms = sleep_ms;
    }
}

Status add_cluster(ClientManagerPtr mgr, const std::string& name,
                   const std::vector<std::string>& seeds) {
    ClusterOptions coptions;
    coptions._name = name;
    coptions._seeds = seeds;
    coptions._server_options._conn_per_node = 4;
    coptions._server_options._conn_hc_interval_ms = 10;
    Status status = mgr->add_cluster(coptions);
    return status;
}

} // namespace test
} // namespace client
} // namespace rsdk
