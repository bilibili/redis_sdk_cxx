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
#include "redis_sdk/include/client_impl.h"
#include "redis_sdk/include/cluster.h"
#include "redis_sdk/include/slot.h"

namespace rsdk {
namespace client {

DEFINE_string(cluster_seeds, "127.0.0.1:21010", "seed of cluster");
DEFINE_bool(print_slot, true, "print log slot details");
DEFINE_bool(print_cluster_node_from_every_node, true, "print cluster node resp");

class ClientTool {
public:
    ClientTool() {
    }
    bool init();
    void print_nodes();
    void print_slot();
    void print_cluster_nodes();
    void run();

private:
    std::string _cname = "cluster1";
    ClientManagerPtr _client_mgr;
    ClusterPtr _cluster;
};

bool ClientTool::init() {
    _client_mgr = new_client_mgr();
    ClusterOptions coptions;
    coptions._seeds.push_back(FLAGS_cluster_seeds);
    coptions._name = _cname;
    coptions._bg_refresh_interval_s = 1;
    coptions._server_options._conn_per_node = 4;
    Status ret = _client_mgr->add_cluster(coptions);
    if (!ret.ok()) {
        LOG(ERROR) << "failed to init cluster from seed node:" << FLAGS_cluster_seeds;
        return false;
    }
    ClientManagerImpl* cli_mgr = dynamic_cast<ClientManagerImpl*>(_client_mgr.get());
    if (!cli_mgr) {
        LOG(ERROR) << "bug, failed to cast client_mgr to ClientManagerImpl";
    }
    _cluster = cli_mgr->get_cluster_mgr()->get(_cname);
    if (!_cluster) {
        LOG(ERROR) << "bug, failed to get cluster from seed node:" << FLAGS_cluster_seeds;
        return false;
    }
    return true;
}

void print_reply(const brpc::RedisReply& reply) {
    if (reply.is_string()) {
        std::cout << "reply is str:[" << reply.c_str() << "]" << std::endl;
        return;
    }
    if (reply.is_error()) {
        std::cout << "reply is error:[" << reply.error_message() << "]" << std::endl;
        return;
    }
    if (reply.is_integer()) {
        std::cout << "reply is integer:[" << reply.integer() << "]" << std::endl;
        return;
    }
    if (reply.is_array()) {
        for (size_t j = 0; j < reply.size(); ++j) {
            const brpc::RedisReply& sub = reply[j];
            print_reply(sub);
        }
        return;
    }
    std::cout << "unknown reply:[" << brpc::RedisReplyTypeToString(reply.type()) << "]"
              << std::endl;
}

void print_response(const brpc::RedisResponse& resp) {
    for (int i = 0; i < resp.reply_size(); ++i) {
        print_reply(resp.reply(i));
    }
}

void ClientTool::print_nodes() {
    std::vector<std::string> ips = _cluster->get_servers();
    for (auto& ip : ips) {
        auto server = _cluster->get_node(ip);
        if (!server) {
            std::cout << "server:" << ip << " is not found, maybe removed" << std::endl;
        } else {
            std::cout << "server:[" << server->to_str() << "]" << std::endl;
        }
    }
}

void ClientTool::print_slot() {
    if (!FLAGS_print_slot) {
        return;
    }
    for (int id = 0; id < (int)S_SLOT_CNT; ++id) {
        SlotPtr slot = _cluster->get_slot_by_id(id);
        if (!slot) {
            std::cout << "slot:" << id << " is not found in cluster" << std::endl;
            continue;
        }
        std::cout << "slot:" << slot->describe() << std::endl;
    }
}
void ClientTool::print_cluster_nodes() {
    if (!FLAGS_print_cluster_node_from_every_node) {
        return;
    }
    std::vector<std::string> ips = _cluster->get_servers();
    for (auto& ip : ips) {
        auto server = _cluster->get_node(ip);
        if (!server) {
            std::cout << "server:" << ip << " is not found, maybe removed" << std::endl;
        } else {
            std::string cmd = "cluster nodes";
            brpc::RedisResponse resp;
            Status ret = server->send_inner_cmd(cmd, &resp, 0);
            if (!ret.ok()) {
                std::cout << "failed to send cmd:" << cmd << "] to " << ip << ", got error:" << ret
                          << std::endl;
                continue;
            }
            print_response(resp);
        }
    }
}

void ClientTool::run() {
    bool ret = init();
    if (!ret) {
        return;
    }
    print_nodes();
    print_slot();
    print_cluster_nodes();
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

    google::InitGoogleLogging("client_tool");
    rsdk::client::ClientTool tool;

    tool.run();
    return 0;
}
