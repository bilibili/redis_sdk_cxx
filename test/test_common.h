#ifndef RSDK_TEST_TEST_COMMON_H
#define RSDK_TEST_TEST_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "redis_sdk/include/client_impl.h"
#include "redis_sdk/include/redis_sdk/client.h"
#include "test/mock_node.h"

namespace rsdk {
namespace client {
namespace test {

struct MockEnv {
    std::string _cname;
    std::vector<std::string> _seeds;
    std::vector<MockRedisServerPtr> _servers;
    AccessOptions _opt;
    ClientManagerPtr _mgr;
};

int build_cluster(const std::vector<MockRedisServerPtr>& servers, int slave_cnt, std::string* desc,
                  ClusterDist* dist, std::unordered_map<int, std::string>* slot2addr);
int setup_redis_server(int cnt, std::vector<MockRedisServerPtr>* servers);
void update_slot_pointer(const std::string& cname, ClientManagerPtr mgr, int slot_id);
void setup_cluster(const std::string& cname, int server_cnt, int slave_per_server,
                   std::vector<MockRedisServerPtr>* servers, std::vector<std::string>* seeds);
void rand_put_get(ClientPtr client, int cnt);
void build_mset(int cnt, bool str_mode, brpc::RedisRequest* req);
void build_mset(int cnt, RecordList* recs);
void build_mget(int cnt, brpc::RedisRequest* req);
void build_mget(int cnt, RecordList* recs);
void clear_value(RecordList* recs);
void set_crud_status(std::vector<MockRedisServerPtr>& servers, int cnt, const std::string& status);
void set_sleep_time(std::vector<MockRedisServerPtr>& servers, int sleep_cnt, int sleep_ms);
Status add_cluster(ClientManagerPtr mgr, const std::string& name,
                   const std::vector<std::string>& seeds);

} // namespace test
} // namespace client
} // namespace rsdk

#endif // RSDK_TEST_TEST_COMMON_H
