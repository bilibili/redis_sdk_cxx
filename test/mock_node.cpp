#include "test/mock_node.h"
#include "redis_sdk/include/api_util.h"

namespace rsdk {
namespace client {

uint32_t key_to_slot_id(const char* key, int keylen);

namespace test {

bool Value::timeout() const {
    if (_ttl_at <= 0) {
        return false;
    }
    return ((uint64_t)_ttl_at) <= get_current_time_s();
}

bool RedisServiceImpl::set(const std::string& key, const std::string& value, int ttl,
                           brpc::RedisReply* output) {
    may_sleep();
    int slot = key_to_slot_id(key.c_str(), key.length());
    std::lock_guard<butil::Mutex> lock(_mutex);
    if (_mock_status_cnt > 0) {
        --_mock_status_cnt;
        if (_crud_status == "OK") {
            output->SetStatus(_crud_status);
        } else {
            output->SetError(_crud_status);
        }
        VLOG(3) << "set resp status as:" << _crud_status << " and return directly";
        return true;
    }
    _crud_status = "OK";

    std::string addr = _slot2addr[slot];
    if (addr != _addr) {
        VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                << " when process put on key:" << key;
        output->SetError(fmt::format("MOVED {} {}", slot, addr));
        return true;
    }
    if (key.find("timeout_key_") != std::string::npos) {
        output->SetError("timeout");
        return false;
    }
    if (key.find("moved_key_") != std::string::npos) {
        if (_moved_cnt % 2 == 0 || _always_moved) {
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            ++_moved_cnt;
            return true;
        }
    }
    VLOG(3) << "succeed to set key:" << key << " with status:" << _crud_status;
    _kv[key] = {value, ttl};

    output->SetStatus(_crud_status);
    return true;
}

bool RedisServiceImpl::exists(const std::string& key, brpc::RedisReply* output) {
    may_sleep();
    int slot = key_to_slot_id(key.c_str(), key.length());
    std::lock_guard<butil::Mutex> lock(_mutex);
    if (_mock_status_cnt > 0) {
        --_mock_status_cnt;
        output->SetError(_crud_status);
        return true;
    }
    --_mock_status_cnt;
    if (_mock_status_cnt == 0) {
        _mock_status_cnt = -1;
        _crud_status = "OK";
    }

    std::string addr = _slot2addr[slot];
    if (!_enable_follow_read) {
        if (addr != _addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    } else {
        if (addr != _leader_addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key << " any leader is:" << _leader_addr;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    }
    if (key.find("timeout_key_") != std::string::npos) {
        output->SetError("timeout");
        return false;
    }
    if (key.find("moved_key_") != std::string::npos) {
        if (_moved_cnt % 2 == 0) {
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            ++_moved_cnt;
            return true;
        }
    }
    VLOG(3) << "start to handle get key:" << key << " on server:" << _addr;
    if (_mock_exist_ret != "") {
        output->SetString(_mock_exist_ret);
        return true;
    }
    ++_get_cnt;
    auto it = _kv.find(key);
    if (it == _kv.end()) {
        output->SetInteger(0);
        return true;
    }
    if (it->second.timeout()) {
        output->SetInteger(0);
        return true;
    }
    output->SetInteger(1);
    return true;
}

bool RedisServiceImpl::get(const std::string& key, brpc::RedisReply* output) {
    may_sleep();
    int slot = key_to_slot_id(key.c_str(), key.length());
    std::lock_guard<butil::Mutex> lock(_mutex);
    if (_mock_status_cnt > 0) {
        --_mock_status_cnt;
        output->SetError(_crud_status);
        return true;
    }
    --_mock_status_cnt;
    if (_mock_status_cnt == 0) {
        _mock_status_cnt = -1;
        _crud_status = "OK";
    }

    std::string addr = _slot2addr[slot];
    if (!_enable_follow_read) {
        if (addr != _addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    } else {
        if (addr != _leader_addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key << " any leader is:" << _leader_addr;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    }
    if (key.find("timeout_key_") != std::string::npos) {
        output->SetError("timeout");
        return false;
    }
    if (key.find("moved_key_") != std::string::npos) {
        if (_moved_cnt % 2 == 0 || _always_moved) {
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            ++_moved_cnt;
            return true;
        }
    }
    VLOG(3) << "start to handle get key:" << key << " on server:" << _addr;
    ++_get_cnt;
    auto it = _kv.find(key);
    if (it == _kv.end()) {
        output->SetNullString();
        return true;
    }
    if (it->second.timeout()) {
        output->SetNullString();
        return true;
    }
    output->SetString(it->second._value);
    return true;
}

bool RedisServiceImpl::cluster_nodes(brpc::RedisReply* output) {
    may_sleep();
    std::lock_guard<butil::Mutex> lock(_mutex);
    VLOG(3) << "start to process cluster_nodes with status:" << _cluster_nodes_status
            << " resp:" << _cluster_nodes;
    output->SetString(_cluster_nodes);
    return true;
}

bool RedisServiceImpl::handle_asking(brpc::RedisReply* output) {
    may_sleep();
    std::lock_guard<butil::Mutex> lock(_mutex);
    VLOG(3) << "start to process asking with status:" << _asking_status;
    output->SetStatus(_asking_status);
    return true;
}

void RedisServiceImpl::may_sleep() {
    if (_sleep_ms == 0) {
        return;
    }
    if (_sleep_cnt <= 0) {
        return;
    }
    --_sleep_cnt;
    LOG(INFO) << "server at:" << _addr << " sleep " << _sleep_ms << "ms";
    for (int i = 0; i < _sleep_ms; ++i) {
        bthread_usleep(1000);
        if (!_running) {
            break;
        }
    }
}

bool RedisServiceImpl::handle_sadd(const std::string& key, const std::vector<std::string>& params,
                                   brpc::RedisReply* output) {
    may_sleep();
    int slot = key_to_slot_id(key.c_str(), key.length());
    std::lock_guard<butil::Mutex> lock(_mutex);
    if (_mock_status_cnt > 0) {
        --_mock_status_cnt;
        output->SetError(_crud_status);
        return true;
    }
    --_mock_status_cnt;
    if (_mock_status_cnt == 0) {
        _mock_status_cnt = -1;
        _crud_status = "OK";
    }

    std::string addr = _slot2addr[slot];
    if (!_enable_follow_read) {
        if (addr != _addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    } else {
        if (addr != _leader_addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key << " any leader is:" << _leader_addr;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    }
    VLOG(3) << "start to handle sadd key:" << key << " on server:" << _addr;

    auto& it = _sets[key];
    for (auto& m : params) {
        it.insert(m);
        VLOG(3) << "start to add member:" << m << " to set on key:" << key;
    }
    output->SetInteger(params.size());
    return true;
}

bool RedisServiceImpl::handle_smember(const std::string& key, brpc::RedisReply* output) {
    may_sleep();

    int slot = key_to_slot_id(key.c_str(), key.length());
    std::lock_guard<butil::Mutex> lock(_mutex);
    if (_mock_status_cnt > 0) {
        --_mock_status_cnt;
        output->SetError(_crud_status);
        return true;
    }
    --_mock_status_cnt;
    if (_mock_status_cnt == 0) {
        _mock_status_cnt = -1;
        _crud_status = "OK";
    }

    std::string addr = _slot2addr[slot];
    if (!_enable_follow_read) {
        if (addr != _addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    } else {
        if (addr != _leader_addr) {
            VLOG(3) << "redirect slot:" << slot << " to:" << addr << " from:" << _addr
                    << " when process get on key:" << key << " any leader is:" << _leader_addr;
            output->SetError(fmt::format("MOVED {} {}", slot, addr));
            return true;
        }
    }
    VLOG(3) << "start to handle set members key:" << key << " on server:" << _addr;

    auto& one = _sets[key];
    if (one.empty()) {
        output->SetArray(0);
    } else {
        output->SetArray(one.size());
        int index = 0;
        for (auto& m : one) {
            (*output)[index].SetString(m);
            ++index;
        }
    }
    return true;
}

brpc::RedisCommandHandlerResult GetCommandHandler::Run(const std::vector<butil::StringPiece>& args,
                                                       brpc::RedisReply* output,
                                                       bool /*flush_batched*/) {
    if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'get', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    const std::string key(args[1].data(), args[1].size());
    _rsimpl->get(key, output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult ExistsCommandHandler::Run(
    const std::vector<butil::StringPiece>& args, brpc::RedisReply* output, bool /*flush_batched*/) {
    if (args.size() != 2ul) {
        output->FormatError("Expect 1 arg for 'get', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    const std::string key(args[1].data(), args[1].size());
    _rsimpl->exists(key, output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult SetCommandHandler::Run(const std::vector<butil::StringPiece>& args,
                                                       brpc::RedisReply* output,
                                                       bool /*flush_batched*/) {
    if (args.size() != 3ul && args.size() != 5ul) {
        output->FormatError("Expect 2 or 5 args for 'set', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    const std::string key(args[1].data(), args[1].size());
    const std::string value(args[2].data(), args[2].size());
    int ttl = 0;
    if (args.size() == 5) {
        std::string ex(args[3].data(), args[3].size());
        std::string str(args[4].data(), args[4].size());
        if (ex == "EX") {
            ttl = (int)strtol(str.c_str(), nullptr, 10);
            ttl += get_current_time_s();
        }
    }
    _rsimpl->set(key, value, ttl, output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult ClusterCommandHandler::Run(
    const std::vector<butil::StringPiece>& args, brpc::RedisReply* output, bool /*flush_batched*/) {
    if (args.size() != 2ul) {
        output->FormatError("Expect 1 args for 'cluster', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    const std::string node(args[1].data(), args[1].size());
    _rsimpl->cluster_nodes(output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult AskingCommandHandler::Run(
    const std::vector<butil::StringPiece>& args, brpc::RedisReply* output, bool /*flush_batched*/) {
    if (args.size() != 1ul) {
        output->FormatError("Expect 0 args for 'asking', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }

    _rsimpl->handle_asking(output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult PingCommandHandler::Run(const std::vector<butil::StringPiece>& args,
                                                        brpc::RedisReply* output,
                                                        bool /*flush_batched*/) {
    output->SetStatus("Pong");
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult SAddCommandHandler::Run(const std::vector<butil::StringPiece>& args,
                                                        brpc::RedisReply* output,
                                                        bool /*flush_batched*/) {
    if (args.size() <= 2ul) {
        output->FormatError("Expect more than 1 args for 'sadd', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    std::vector<std::string> params;
    for (size_t i = 2; i < args.size(); ++i) {
        params.push_back(args.at(i).as_string());
    }

    _rsimpl->handle_sadd(args.at(1).as_string(), params, output);
    return brpc::REDIS_CMD_HANDLED;
}

brpc::RedisCommandHandlerResult SMembersCommandHandler::Run(
    const std::vector<butil::StringPiece>& args, brpc::RedisReply* output, bool /*flush_batched*/) {
    if (args.size() != 2ul) {
        output->FormatError("Expect more than 1 args for 'sadd', actually %lu", args.size() - 1);
        return brpc::REDIS_CMD_HANDLED;
    }
    const std::string key(args[1].data(), args[1].size());
    _rsimpl->handle_smember(key, output);
    return brpc::REDIS_CMD_HANDLED;
}

int MockRedisServer::start() {
    static int start_port = 0;
    if (start_port == 0) {
        int base = rand() % 3000;
        start_port = 9000 + base;
    }
    ++start_port;
    _service = new RedisServiceImpl();
    _service->AddCommandHandler("get", new GetCommandHandler(_service));
    _service->AddCommandHandler("set", new SetCommandHandler(_service));
    _service->AddCommandHandler("mget", new GetCommandHandler(_service));
    _service->AddCommandHandler("mset", new SetCommandHandler(_service));
    _service->AddCommandHandler("exists", new ExistsCommandHandler(_service));
    _service->AddCommandHandler("cluster", new ClusterCommandHandler(_service));
    _service->AddCommandHandler("asking", new AskingCommandHandler(_service));
    _service->AddCommandHandler("ping", new PingCommandHandler());
    _service->AddCommandHandler("sadd", new SAddCommandHandler(_service));
    _service->AddCommandHandler("smembers", new SMembersCommandHandler(_service));

    brpc::ServerOptions server_options;
    server_options.redis_service = _service;

    brpc::PortRange range(start_port, start_port + 1000);
    _port = 0;
    int ret = _server.Start(range, &server_options);
    if (0 != ret) {
        VLOG(1) << "failed to start mock metaserver on port:" << _port;
    }
    _port = _server.listen_address().port;
    start_port = _port;
    _service->_desc = std::to_string(_port);
    _service->_addr = addr();
    VLOG(1) << "mock node start to work at:" << addr();
    return 0;
}

void MockRedisServer::stop() {
    if (!_runing) {
        return;
    }
    _service->_running = false;
    _runing = false;
    int ret = _server.Stop(0);
    if (0 != ret) {
        LOG(ERROR) << "failed to stop server at:" << addr();
    }
    _server.Join();
    VLOG(1) << "succeed to stop server at:" << addr();
}

void MockRedisServer::set_slot_ids(const std::vector<int>& slot_ids) {
    _slot_ids.clear();
    for (auto slot_id : slot_ids) {
        _slot_ids.insert(slot_id);
    }
}

std::string MockRedisServer::build_cluster_nodes() {
    // 15d9d385e01496cafac99b3013a4af46969b76e4 127.0.0.1:21020@31020 master - 0 1677932078214 3
    // connected 10923-16383
    std::string role = "master";
    std::string leader_addr = "-";
    std::string desc = fmt::format("{} {}@{} ", name(), addr(), _index);
    std::string part;
    if (_leader_name != name()) {
        role = "slave";
        part = fmt::format("{} {} {} {} {} {}", role, _leader_name, 0, 0, _config_epoch, _state);
    } else {
        part = fmt::format("{} {} {} {} {} {} {}", role, "-", 0, 0, _config_epoch, _state,
                           _slot_dist);
    }
    return desc + part;
}

} // namespace test
} // namespace client
} // namespace rsdk
