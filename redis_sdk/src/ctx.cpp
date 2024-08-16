#include "redis_sdk/include/ctx.h"

#include <butil/logging.h>
#include <butil/strings/string_split.h>
#include <xxhash.h>

#include "redis_sdk/client.h"
#include "redis_sdk/include/pipeline.h"
#include "redis_sdk/include/redis_server.hpp"

namespace rsdk {
namespace client {

DEFINE_bool(use_indepent_channel_for_big_key, true, "use indepent channel for big key or");
BRPC_VALIDATE_GFLAG(use_indepent_channel_for_big_key, brpc::PassValidate);

// TODO: Use `absl::Status` instead.
// TODO: Avoid using non-trivial variables with static storage duration.
const std::string S_STATUS_OK = "OK";
const std::string S_STATUS_MOVED = "MOVED";
const std::string S_STATUS_ASK = "ASK";
const std::string S_STATUS_TRYAGAIN = "TRYAGAIN";
const std::string S_STATUS_CROSSSLOT = "CROSSSLOT";
const std::string S_STATUS_CLUSTERDOWN = "CLUSTERDOWN";

std::string redis_status_to_str(const RedisStatus& s);
std::string to_string(const brpc::RedisResponse& resp);

IOCtx::~IOCtx() {
    if (_user_cb) {
        _user_cb->Run();
    }
    add_or_remove_big_key();
}

bool read_op(const std::string& cmd) {
    return (cmd == "get" || cmd == "hget" || cmd == "zcount" || cmd == "zcard" || cmd == "scard"
            || cmd == "mget");
}
// put xx yyy
// get xx
uint32_t key_to_slot_id(const char* key, int keylen);
uint64_t xxhash(const std::string& key) {
    XXH64_hash_t seed = 2147483647;
    return (uint64_t)XXH64(key.data(), key.size(), seed);
}

Status IOCtx::init(const AccessOptions& opt) {
    _options = opt;
    _start_time_us = get_current_time_us();
    Status ret;
    _read_op = read_op(_main_cmd);
    _slot_id = key_to_slot_id(_key.c_str(), _key.length());
    _slot = _cluster->get_slot_by_id(_slot_id);
    assert(_slot != nullptr);
    if (_read_op && FLAGS_use_indepent_channel_for_big_key) {
        _big_key_threshold = _cluster->big_key_threshold();
        _key_sign = xxhash(_key);
        if (_cluster->is_big_key(_key_sign)) {
            _big_key = true;
        }
    }
    if (!_read_op) {
        _req_size_bytes = _req.ByteSize();
        (*(_cluster->mutable_write_bvar())) << _req_size_bytes;
    }
    VLOG(3) << "succeed to init io ctx, cmd:[" << _sub_cmd << "], key:[" << encode_key(_key)
            << "], slot:" << _slot->describe();
    return Status::OK();
}

int64_t IOCtx::left_time_ms() {
    int64_t used = (get_current_time_us() - _start_time_us) / 1000;
    return _options.timeout_ms > used ? _options.timeout_ms - used : 0;
}

bool IOCtx::should_retry(RedisStatus status) {
    if (get_current_time_us() >= uint64_t(_start_time_us + _options.timeout_ms * 1000)) {
        return false;
    }
    if (_has_ask_head) {
        return false;
    }
    if (status >= RedisStatus::MOVED && status <= RedisStatus::CLUSTER_DOWN) {
        return true;
    }
    return false;
}
void IOCtx::add_ask_head() {
    if (_has_ask_head) {
        return;
    }
    std::string ask_cmd = "ASKING";
    brpc::RedisRequest tmp_req;
    tmp_req.AddCommand(ask_cmd);
    tmp_req.MergeFrom(_req);
    _req.Swap(&tmp_req);
    _has_ask_head = true;
}

// put resp
// OK
// (error) MOVED 15983 127.0.0.1:21020

// get resp
// MOVED 4038 127.0.0.1:21010
// (nil)
// "yyy"
// (error) ERR wrong number of arguments for 'get' command
void IOCtx::retry(RedisStatus status, RedisServerPtr from, const std::vector<std::string>& err_list,
                  const std::string& msg) {
    VLOG(3) << "start to retry cmd[" << _sub_cmd << "] key:" << encode_key(_key)
            << " to slot:" << _slot_id << ", prev redis status:" << redis_status_to_str(status)
            << ", msg:" << msg;
    ++_retry_cnt;
    RedisServerPtr server;
    if (status == RedisStatus::ASK || status == RedisStatus::MOVED) {
        if (err_list.size() >= 3) {
            // int64_t slot_id = strtol(part[1].c_str(), nullptr, 10);
            std::string addr = err_list[2];
            server = _cluster->add_and_get_node(addr);
            LOG(INFO) << "redis server:[" << from->to_str()
                      << "] return:" << redis_status_to_str(status) << " on cmd:[" << _sub_cmd
                      << "] key:" << encode_key(_key) << ", for slot:" << _slot_id << " retry to:["
                      << (server ? server->addr() : addr) << "] now";
            _cluster->may_triggle_update_dist();
        }
        if (status == RedisStatus::ASK) {
            add_ask_head();
        }
        if (!server) {
            // 可能其他线程已经更新过了，直接用新的
            server = get_server();
            if (server->id() == _server_id) {
                _cluster->may_update_dist(status, msg);
                server = get_server();
            }
        }
    }
    if (!server) {
        server = get_server();
    }

    _resp->Clear();

    _slot_config_epoch = _slot->config_epoch();
    _server_id = server->id();
    VLOG(3) << "retry, start to send cmd:[" << _sub_cmd << "] key:" << encode_key(_key)
            << " to server:" << server->addr() << " with resp:" << to_string(*_resp);
    server->send_cmd(this);
}

RedisServerPtr IOCtx::get_server() {
    return _slot->get_leader();
}

void IOCtx::send_cmd() {
    RedisServerPtr server = get_server();
    _slot_config_epoch = _slot->config_epoch();
    _server_id = server->id();
    if (FLAGS_v >= 3) {
        VLOG(3) << "send cmd [" << _sub_cmd << "] key:" << encode_key(_key)
                << " to slot:" << _slot_id << ", to:" << server->addr() << " id:" << _server_id
                << ", epoch:" << _slot_config_epoch;
    }
    server->send_cmd(this);
}

void set_error_to_resp(brpc::RedisResponse* resp, const std::string& msg) {
    butil::IOBuf err_buf;
    err_buf.append("-");
    err_buf.append(msg);
    err_buf.append("\r\n");
    if (resp->reply_size() != 0) {
        resp->Clear();
    }
    brpc::ParseError perr = resp->ConsumePartialIOBuf(err_buf, 1);
    if (perr != brpc::PARSE_OK) {
        LOG(ERROR) << "bug of redis api, failed to parse:[" << err_buf.to_string() << "]";
        return;
    }
    return;
}

void IOCtx::handle_err(const std::string& msg) {
    std::unique_ptr<IOCtx> release_this(this);
    set_error_to_resp(_resp, msg);
}

void log_response(const brpc::RedisResponse& resp);
void IOCtx::may_skip_ask_head() {
    if (!_has_ask_head) {
        return;
    }
    VLOG(3) << "resp reply size:" << _resp->reply_size() << ", may need to skip ask head"
            << " on cmd:" << _sub_cmd << " key:" << encode_key(_key);
    if (_resp->reply_size() < 2) {
        LOG(ERROR) << "bug of redis api, reply size:" << _resp->reply_size()
                   << ", expect to have ask head";
        return;
    }
    int cnt = _resp->reply_size();
    butil::IOBufAppender appender;
    for (int i = 1; i < cnt; ++i) {
        if (_resp->reply(i).is_nil()) {
            std::string nil_resp = "$-1\r\n";
            appender.append(nil_resp.c_str(), nil_resp.size());
        } else {
            brpc::RedisReply reply(nullptr);
            reply.CopyFromSameArena(_resp->reply(i));
            bool ret = reply.SerializeTo(&appender);
            if (!ret) {
                std::string err_msg = "ERR bug of redis api, failed to serialize response to iobuf";
                set_error_to_resp(_resp, err_msg);
                return;
            }
        }
    }
    butil::IOBuf buf;
    appender.move_to(buf);

    _resp->Clear();

    brpc::ParseError err = _resp->ConsumePartialIOBuf(buf, cnt - 1);
    if (err != brpc::PARSE_OK) {
        std::string err_msg = "ERR bug of redis api, failed to parse serialized result";
        set_error_to_resp(_resp, err_msg);
        return;
    }
    // VLOG(3) << "succeed to skip ask header from user response";
}

void IOCtx::add_or_remove_big_key() {
    if (_add_to_key_cache) {
        _cluster->add_big_key(_key_sign, _key, _resp_size_bytes);
        return;
    }
    if (_remove_from_key_cache) {
        _cluster->remove_big_key(_key_sign);
        return;
    }
}

void IOCtx::handle_read_resp() {
    if (!_read_op) {
        return;
    }
    _resp_size_bytes = _resp->ByteSize();
    (*_cluster->mutable_read_bvar()) << _resp_size_bytes;
    if (!FLAGS_use_indepent_channel_for_big_key) {
        return;
    }
    uint64_t threshold = _big_key_threshold;
    if (_big_key && (_resp_size_bytes < threshold)) {
        LOG(INFO) << "big key:[" << encode_key(_key) << "] size:" << _resp_size_bytes
                  << " is less than threshold:" << threshold << ", will remove from local cache";
        _remove_from_key_cache = true;
        return;
    }
    if (!_big_key && threshold > 0 && (_resp_size_bytes >= threshold)) {
        LOG(INFO) << "key:[" << encode_key(_key) << "] size:" << _resp_size_bytes
                  << " is greater than threshold:" << threshold << ", will add to local cache";
        _add_to_key_cache = true;
        return;
    }
}

RedisStatus parse_redis_error(const std::string& str);
void IOCtx::handle_resp(RedisServerPtr from) {
    std::unique_ptr<IOCtx> release_this(this);
    const brpc::RedisReply& reply0 = _resp->reply(0);
    VLOG(3) << "start to handle resp for cmd:[" << _sub_cmd << "], from:" << from->to_str()
            << " key:" << encode_key(_key) << ", resp size:" << _resp->reply_size()
            << ", resp:" << to_string(*_resp);
    _end_time_us = get_current_time_us();
    int64_t cost = _end_time_us - _start_time_us;
    if (!reply0.is_error()) {
        may_skip_ask_head();
        handle_read_resp();
        // 这里可能对_resp的内容进行了swap，故不能再使用reply0
        VLOG(2) << "succeed to get resp of cmd:[" << _sub_cmd << "] from " << from->to_str()
                << " key:" << encode_key(_key) << " in " << cost
                << "us, resp:" << to_string(*_resp);
        (*(_cluster->mutable_latency_bvar())) << cost;
        _cluster->add_metrics(_main_cmd, from->addr(), true, cost, _req.ByteSize(),
                              _resp->ByteSize());
        return;
    }
    const std::string& err_msg_str = reply0.error_message();
    std::vector<std::string> err_list;
    butil::SplitString(err_msg_str, ' ', &err_list);
    if (err_list.empty()) {
        LOG(ERROR) << "invalid error response:" << err_msg_str << " when send cmd[" << _sub_cmd
                   << "] from" << from->to_str();
        std::string msg = fmt::format("invalid response:[{}] from:{}", err_msg_str, from->to_str());
        set_error_to_resp(_resp, msg);
        _cluster->add_metrics(_main_cmd, from->addr(), false, cost, _req.ByteSize(),
                              _resp->ByteSize());
        return;
    }
    RedisStatus rs = parse_redis_error(err_list[0]);
    if (!should_retry(rs)) {
        VLOG(3) << "no need to retry, cmd[" << _sub_cmd << "] to " << from->to_str()
                << " key:" << encode_key(_key) << " err msg:" << err_msg_str
                << " redis status:" << redis_status_to_str(rs);

        _cluster->add_metrics(_main_cmd, from->addr(), false, cost, _req.ByteSize(),
                              _resp->ByteSize());
        return;
    }
    _err_msg += err_msg_str + " ";
    if (_retry_cnt >= _options.max_redirect) {
        LOG(WARNING) << "failed to send cmd[" << _sub_cmd << "] to " << from->to_str()
                     << " key:" << encode_key(_key) << ", got resp:" << err_msg_str
                     << " redis status:" << redis_status_to_str(rs) << ", retry cnt:" << _retry_cnt
                     << " >= max:" << _options.max_redirect;
        std::string msg = _err_msg;
        msg += ", retry cnt:" + std::to_string(_retry_cnt)
               + " exceed max:" + std::to_string(_options.max_redirect);
        set_error_to_resp(_resp, msg);
        _cluster->add_metrics(_main_cmd, from->addr(), false, cost, _req.ByteSize(),
                              _resp->ByteSize());
        return;
    }

    release_this.release();
    retry(rs, from, err_list, err_msg_str);
    return;
}
RpcCallback::RpcCallback(IOCtx* ctx, std::shared_ptr<RedisServer> server) :
        _ctx(ctx),
        _server(server) {
    _cntl.set_timeout_ms(ctx->left_time_ms());
}
RpcCallback::RpcCallback(PipeCtx* req, std::shared_ptr<RedisServer> server) :
        _pipe_req(req),
        _server(server) {
    _cntl.set_timeout_ms(req->left_time_ms());
}
void RpcCallback::Run() {
    std::unique_ptr<RpcCallback> release_this(this);
    if (_cntl.Failed()) {
        int err = _cntl.ErrorCode();
        std::string msg = fmt::format("TIMEOUT, failed to access redis server:{}, got err:{} "
                                      "code:{}",
                                      _server->to_str(), _cntl.ErrorText(), err);
        VLOG(3) << msg;
        if (_ctx) {
            _ctx->handle_err(msg);
        } else {
            _pipe_req->handle_err(msg);
        }
        return;
    }
    // VLOG(3) << "succeed to access redis server:" << _server
    //         << ", got resp:" << to_string(*_ctx->mutable_resp());
    if (_ctx) {
        _ctx->handle_resp(_server);
    } else {
        _pipe_req->handle_resp(_server);
    }
}

} // namespace client
} // namespace rsdk
