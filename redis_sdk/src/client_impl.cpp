#include <brpc/channel.h>
#include <brpc/redis_command.h>
#include <butil/base64.h>
#include <butil/crc32c.h>
#include <butil/rand_util.h>

#include <fmt/format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <cstdlib>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "redis_sdk/include/client_impl.h"
#include "redis_sdk/include/pipeline.h"

namespace rsdk {
namespace client {

static bvar::LatencyRecorder s_parse_recorder("redis_req_parser");

DEFINE_int64(client_mgr_bg_work_interval_ms, 10 * 1000L, "client mgr bg worker interval in ms");
DEFINE_bool(check_bug_in_brpc_replica_reply, true, "check bug of brpc in method RedisReply::Swap");
DEFINE_bool(replace_sub_cmd, true, "replace mget/mset to get/set in sub request");

BRPC_VALIDATE_GFLAG(client_mgr_bg_work_interval_ms, brpc::PositiveInteger);
BRPC_VALIDATE_GFLAG(check_bug_in_brpc_replica_reply, brpc::PassValidate);
BRPC_VALIDATE_GFLAG(replace_sub_cmd, brpc::PassValidate);

// static uint32_t compute_crc(const butil::IOBuf& buf) {
//     uint32_t crc = 0;
//     for (size_t i = 0; i < buf.backing_block_num(); ++i) {
//         const butil::StringPiece blk = buf.backing_block(i);
//         crc = butil::crc32c::Extend(crc, blk.data(), blk.size());
//     }
//     return crc;
// }

#ifndef START_SYNC
#define START_SYNC                      \
    std::unique_ptr<SyncClosure> sync;  \
    if (!done) {                        \
        sync.reset(new SyncClosure(1)); \
        done = sync.get();              \
        batch->set_done(done);          \
    }                                   \
    do {                                \
    } while (0)
#endif // START_SYNC

#ifndef WAIT_SYNC
#define WAIT_SYNC     \
    if (sync) {       \
        sync->wait(); \
    }                 \
    do {              \
    } while (0)
#endif // WAIT_SYNC

void* client_mgr_bg_worker(void* arg) {
    ClientManagerImpl* mgr = (ClientManagerImpl*)arg;
    mgr->bg_work();
    return nullptr;
}

ClientManagerImpl::ClientManagerImpl(const ClientMgrOptions& options) : _options(options) {
#ifdef WITH_PROMETHEUS
    _metrics.reset(new ClientMetrics(options._registry));
#else
    _metrics.reset(new ClientMetrics());
#endif // WITH_PROMETHEUS
    _metrics->init();
    _cluster_mgr = std::make_shared<ClusterManager>(_metrics.get());
    if (bthread_start_background(&_bg_worker, nullptr, client_mgr_bg_worker, this) == 0) {
        _bg_worker_started = true;
    }
}

ClientManagerImpl::ClientManagerImpl() {
    _metrics.reset(new ClientMetrics());
    _metrics->init();
    _cluster_mgr = std::make_shared<ClusterManager>(_metrics.get());
    if (bthread_start_background(&_bg_worker, nullptr, client_mgr_bg_worker, this) == 0) {
        _bg_worker_started = true;
    }
}

ClientManagerImpl::~ClientManagerImpl() {
    stop();
    join();
}

void ClientManagerImpl::stop() {
    _cluster_mgr->stop();
    if (_bg_worker_started) {
        bthread_stop(_bg_worker);
    }
}

void ClientManagerImpl::join() {
    _cluster_mgr->join();
    if (_bg_worker_started) {
        bthread_join(_bg_worker, nullptr);
    }
}

ClientPtr ClientManagerImpl::new_client(const AccessOptions& opt) {
    ClusterPtr cluster = _cluster_mgr->get(opt._cname);
    if (!cluster) {
        return nullptr;
    }
    return ClientPtr(new ClientImpl(opt, cluster, _id));
}

void ClientManagerImpl::bg_work() {
    while (true) {
        if (bthread_stopped(bthread_self())) {
            break;
        }
        uint64_t sleep_time = FLAGS_client_mgr_bg_work_interval_ms * 1000L;
        bthread_usleep(sleep_time);
        _cluster_mgr->bg_work();
    }
}

void ClientManagerImpl::dump_big_key(ClusterKeys* cluster_big_keys) {
    _cluster_mgr->dump_big_key(cluster_big_keys);
}

ClusterMgrPtr ClientManagerImpl::get_cluster_mgr() {
    return _cluster_mgr;
}

Status check_brpc_redis_reply_bug() {
    if (!FLAGS_check_bug_in_brpc_replica_reply) {
        return Status::OK();
    }
    std::string msg = "+";
    uint64_t msg_len = 200;
    msg.resize(msg_len, 'x');
    msg.append("\r\n");
    butil::ArenaOptions options;
    options.initial_block_size = 4096;
    options.max_block_size = 40960;
    butil::Arena arena1(options);
    butil::Arena arena2(options);
    std::unique_ptr<brpc::RedisReply> reply1(new brpc::RedisReply(&arena1));
    std::unique_ptr<brpc::RedisReply> reply2(new brpc::RedisReply(&arena2));

    butil::IOBuf buf;
    buf.append(msg);
    auto err = reply1->ConsumePartialIOBuf(buf);
    if (err) {
        std::string emsg = fmt::format("failed to parse from str[{}] for RedisReply", msg);
        return Status::SysError(msg);
    }
    const void* buf1 = reply1->c_str();

    // expect to use same arena
    reply1.get()->Swap(*reply2.get());
    buf.clear();
    buf.append(msg);
    // alloc from stack to make sure heap pointer changed:w
    std::unique_ptr<char[]> tmp_buf(new char[10 * 1024]);
    err = reply1->ConsumePartialIOBuf(buf);
    if (err) {
        std::string emsg = fmt::format("failed to parse from str[{}] for RedisReply", msg);
        return Status::SysError(msg);
    }
    const void* buf2 = reply1->c_str();
    uint64_t ptr1 = (uint64_t)buf1;
    uint64_t ptr2 = (uint64_t)buf2;
    uint64_t diff = (ptr1 > ptr2) ? (ptr1 - ptr2) : (ptr2 - ptr1);
    if (diff <= (msg_len + 10)) {
        return Status::OK();
    }
    std::string err_msg = "bug of brpc::RedisReply.Swap, _arena of RedisReply should not be "
                          "swapped";
    return Status::SysError(err_msg);
}

Status ClientManagerImpl::add_cluster(const ClusterOptions& options) {
    if (_reply_checked) {
        if (!_reply_passed) {
            std::string msg = "bug of brpc::RedisReply.Swap, _arena of RedisReply should not be "
                              "swapped";
            return Status::SysError(msg);
        }
    } else {
        Status ret = check_brpc_redis_reply_bug();
        if (!ret.ok()) {
            _reply_passed = false;
        } else {
            _reply_passed = true;
        }
        _reply_checked = true;
        if (!ret.ok()) {
            return ret;
        }
    }
    return _cluster_mgr->add_cluster(options);
}

bool ClientManagerImpl::has_cluster(const std::string& name) {
    return _cluster_mgr->has_cluster(name);
}

void ClientManagerImpl::set_cluster_state(const std::string& name, bool online) {
    if (online) {
        _cluster_mgr->set_online(name);
    } else {
        _cluster_mgr->set_offline(name);
    }
}

void set_error_to_resp(brpc::RedisResponse* resp, const std::string& msg);

bool ClientImpl::is_batch_command(const std::string& cmd_name) {
    static const std::unordered_set<std::string> batch_commands = {"mset", "mget", "del", "exists"};
    return batch_commands.count(cmd_name) > 0;
}

int ClientImpl::get_standard_input_count(const std::string& cmd_name) {
    static const std::unordered_map<std::string, int> inputCounts = {
        {"mget", 1},
        {"del", 1},
        {"exists", 1},
        {"mset", 2},
    };
    auto it = inputCounts.find(cmd_name);
    if (it != inputCounts.end()) {
        return it->second;
    } else {
        return 0;
    }
}

bool ClientImpl::valid_cmd(const std::string& cmd) {
    if (cmd == "renamenx" || cmd == "rename" || cmd == "msetnx" || cmd == "eval") {
        return false;
    }
    return true;
}

bool ClientImpl::check_and_parse(const brpc::RedisRequest& req, brpc::RedisCommandParser* parser,
                                 butil::Arena* arena, std::string* cmd,
                                 std::vector<butil::StringPiece>* args, std::string* msg,
                                 int* cmd_cnt, int* arg_cnt) {
    if (req.has_error()) {
        *msg = fmt::format("client side error, has error in req");
        return false;
    }
    if (req.command_size() != 1) {
        *msg = fmt::format("empty request or more than one cmd, cnt:{}", req.command_size());
        return false;
    }
    butil::IOBuf data;
    req.SerializeTo(&data);

    auto ret = parser->Consume(data, args, arena);
    if (ret != brpc::ParseError::PARSE_OK) {
        *msg = fmt::format("client side error, failed to parse cmd and args, got err:{}", int(ret));
        return false;
    }
    if (args->size() <= 1) {
        *msg = fmt::format("client side error, no cmd in req, arg cnt:{}", args->size());
        return false;
    }
    *cmd = (*args)[0].as_string();
    if (!valid_cmd(*cmd)) {
        *msg = fmt::format("client side error, error: unsupport cmd [{}]", *cmd);
        return false;
    }
    *arg_cnt = args->size() - 1;
    *cmd_cnt = 1;
    if (is_batch_command(*cmd)) {
        *arg_cnt = get_standard_input_count(*cmd);
        if (*arg_cnt == 0) {
            *msg = fmt::format("client side err, BUG, arg cnt is 0 for cmd:[{}]", *cmd);
            return false;
        }
        if (((args->size() - 1) % *arg_cnt) != 0) {
            *msg = fmt::format("client side error, invalid args count:{} for cmd:[{}]",
                               args->size() - 1, *cmd);
            return false;
        }
        *cmd_cnt = (args->size() - 1) / *arg_cnt;
    }
    return true;
}

void ClientImpl::mget(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
                      ::google::protobuf::Closure* done) {
    if (records.empty() || !resps) {
        brpc::ClosureGuard done_guard(done);
        return;
    }
    resps->clear();
    resps->resize(records.size());
    std::string main_cmd = "mget";
    uint64_t start_us = get_current_time_us();
    BatchClosureV2* batch = new BatchClosureV2(start_us, _cluster, main_cmd, (int)records.size(),
                                               _options, done);

    START_SYNC;
    std::string sub_cmd = "get";

    for (size_t index = 0; index < records.size(); ++index) {
        std::string key = records[index]._key;
        PipeCtx* pipe_req = batch->split_to_sub_req(sub_cmd, records[index]);
        pipe_req->add_request(key, records[index], &(*resps)[index]);
    }
    batch->send_all_sub_req();
    WAIT_SYNC;
}

void ClientImpl::mget(RecordList* records, ::google::protobuf::Closure* done) {
    if (records == nullptr || records->empty()) {
        brpc::ClosureGuard done_guard(done);
        return;
    }
    std::string main_cmd = "mget";
    uint64_t start_us = get_current_time_us();
    BatchClosureV2* batch = new BatchClosureV2(start_us, _cluster, main_cmd, (int)records->size(),
                                               _options, done);
    START_SYNC;
    std::string sub_cmd = "get";

    for (size_t index = 0; index < records->size(); ++index) {
        std::string key = (*records)[index]._key;
        PipeCtx* pipe_req = batch->split_to_sub_req(sub_cmd, (*records)[index]);
        pipe_req->add_request(key, &(*records)[index]);
    }
    batch->send_all_sub_req();
    WAIT_SYNC;
}

uint32_t key_to_slot_id(const char* key, int keylen);
PipeCtx* BatchClosureV2::split_to_sub_req(const std::string& cmd, const Record& rec) {
    std::string key = rec._key;
    uint32_t slot_id = key_to_slot_id(key.c_str(), key.length());
    SlotPtr slot = _cluster->get_slot_by_id(slot_id);
    RedisServerPtr server = slot->get_leader();
    return add_or_get(_cluster, server, cmd, rec);
}

PipeCtx* BatchClosureV2::add_or_get(ClusterPtr cluster, RedisServerPtr server,
                                    const std::string& cmd, const Record& rec) {
    auto iter = _server2req.find(server->addr());
    if (iter == _server2req.end()) {
        std::string key = rec._key;
        std::unique_ptr<PipeCtx> pipectx(
            new PipeCtx(_cluster, server, _main_cmd, cmd, std::move(key), this));
        pipectx->init(_acc_options);
        _server2req[server->addr()] = pipectx.get();
        _sub_reqs.push_back(std::move(pipectx));
    }
    return _server2req[server->addr()];
}

void BatchClosureV2::send_all_sub_req() {
    _refs.store((int)_sub_reqs.size());
    for (auto& sub_req : _sub_reqs) {
        sub_req->send_req();
    }
}

void ClientImpl::mset(const RecordList& records, std::vector<brpc::RedisResponse>* resps,
                      ::google::protobuf::Closure* done) {
    if (records.empty() || !resps) {
        brpc::ClosureGuard done_guard(done);
        return;
    }
    resps->clear();
    resps->resize(records.size());
    std::string main_cmd = "mset";
    uint64_t start_us = get_current_time_us();
    BatchClosureV2* batch = new BatchClosureV2(start_us, _cluster, main_cmd, (int)records.size(),
                                               _options, done);
    START_SYNC;
    std::string sub_cmd = "set";

    for (size_t index = 0; index < records.size(); ++index) {
        std::string key = (records)[index]._key;
        PipeCtx* pipe_req = batch->split_to_sub_req(sub_cmd, records[index]);
        pipe_req->add_request(key, records[index], &((*resps)[index]));
    }
    batch->send_all_sub_req();
    WAIT_SYNC;
}
void ClientImpl::mset(RecordList* records, ::google::protobuf::Closure* done) {
    if (records == nullptr || records->empty()) {
        brpc::ClosureGuard done_guard(done);
        return;
    }
    std::string main_cmd = "mset";
    uint64_t start_us = get_current_time_us();
    BatchClosureV2* batch = new BatchClosureV2(start_us, _cluster, main_cmd, (int)records->size(),
                                               _options, done);
    START_SYNC;
    std::string sub_cmd = "set";

    for (size_t index = 0; index < records->size(); ++index) {
        std::string key = (*records)[index]._key;
        PipeCtx* pipe_req = batch->split_to_sub_req(sub_cmd, (*records)[index]);
        pipe_req->add_request(key, &(*records)[index]);
    }
    batch->send_all_sub_req();
    WAIT_SYNC;
}

void ClientImpl::exec(const brpc::RedisRequest& req, brpc::RedisResponse* resp,
                      ::google::protobuf::Closure* done) {
    uint64_t start_us = get_current_time_us();
    std::vector<butil::StringPiece> args;
    std::string main_cmd;
    std::string err_msg;
    brpc::RedisCommandParser parser;
    butil::Arena arena;

    int cmd_cnt = 1;
    int arg_cnt = 0;
    if (!check_and_parse(req, &parser, &arena, &main_cmd, &args, &err_msg, &cmd_cnt, &arg_cnt)) {
        brpc::ClosureGuard done_guard(done);
        set_error_to_resp(resp, err_msg);
        return;
    }
    uint64_t parse_end = get_current_time_us();
    s_parse_recorder << (parse_end - start_us);

    std::string sub_cmd = main_cmd;
    if (FLAGS_replace_sub_cmd) {
        if (main_cmd == "mget") {
            sub_cmd = "get";
        } else if (main_cmd == "mset") {
            sub_cmd = "set";
        }
    }
    std::unique_ptr<SyncClosure> sync;
    if (!done) {
        sync.reset(new SyncClosure(1));
        done = sync.get();
    }
    RefCountClosure* ref = new RefCountClosure(start_us, _cluster, main_cmd, cmd_cnt, resp, done);
    ref->set_merge_as_array(_options._merge_batch_as_array);

    butil::StringPiece arg_list[arg_cnt + 1];
    arg_list[0].set(sub_cmd.c_str(), sub_cmd.size());
    for (int cmd_index = 0; cmd_index < cmd_cnt; ++cmd_index) {
        brpc::RedisRequest sub_req;
        if (cmd_cnt > 1) {
            for (int x = 0; x < arg_cnt; ++x) {
                arg_list[x + 1] = args[1 + cmd_index * arg_cnt + x];
            }
            sub_req.AddCommandByComponents(arg_list, arg_cnt + 1);
        } else {
            sub_req.CopyFrom(req);
        }
        std::string key = args[1 + cmd_index * arg_cnt].as_string();
        IOCtx* ctx = new IOCtx(_cluster, main_cmd, sub_cmd, std::move(key), std::move(sub_req),
                               ref->mutable_response(cmd_index), ref);

        Status ret = ctx->init(_options);
        if (!ret.ok()) {
            std::string msg = fmt::format("client side error, error: {}", ret.to_string());
            set_error_to_resp(ref->mutable_response(cmd_index), msg);
            delete ctx;
            continue;
        }
        ctx->send_cmd();
    }
    if (sync) {
        sync->wait();
    }
}

void ClientImpl::exec(const std::string& cmd, brpc::RedisResponse* resp,
                      ::google::protobuf::Closure* done) {
    brpc::RedisRequest req;
    bool ret = req.AddCommand(cmd);
    if (!ret) {
        brpc::ClosureGuard done_guard(done);
        std::string err_msg = fmt::format("client side error, invalid request cmd, len:{}",
                                          cmd.size());
        set_error_to_resp(resp, err_msg);
        return;
    }
    exec(req, resp, done);
    return;
}

void RefCountClosure::merge_exists_response() {
    int64_t total = 0;
    std::string error_msg;
    if (_resp->reply(0).type() == brpc::REDIS_REPLY_ERROR) {
        error_msg = std::string("[") + _resp->reply(0).error_message() + std::string("]");
    } else if (_resp->reply(0).type() == brpc::REDIS_REPLY_INTEGER) {
        total = _resp->reply(0).integer();
    } else {
        error_msg = fmt::format("bug, invalid response from redis server, type:[{}]",
                                (int)_resp->reply(0).type());
        set_error_to_resp(_resp, error_msg);
        return;
    }

    for (auto& resp : _more_replys) {
        switch (resp.reply(0).type()) {
        case brpc::REDIS_REPLY_ERROR:
            if (error_msg.size() <= 300) {
                error_msg += std::string("[") + resp.reply(0).error_message() + std::string("]");
            }
            break;
        case brpc::REDIS_REPLY_INTEGER:
            total += resp.reply(0).integer();
            break;
        default:
            error_msg = fmt::format("bug, invalid response type from redis server, [{}]",
                                    (int)resp.reply(0).type());
            set_error_to_resp(_resp, error_msg);
            return;
        }
    }
    if (error_msg != "") {
        set_error_to_resp(_resp, error_msg);
    } else {
        const brpc::RedisReply& reply0 = _resp->reply(0);
        brpc::RedisReply* mutable_reply0 = const_cast<brpc::RedisReply*>(&reply0);
        mutable_reply0->SetInteger(total);
    }
}

std::string to_string(const brpc::RedisResponse& resp);
void RefCountClosure::merge_as_array() {
    butil::IOBufAppender appender;
    int cnt = 1 + (int)_more_replys.size();
    appender.push_back('*');
    appender.append_decimal(cnt);
    appender.append("\r\n", 2);
    brpc::RedisReply& tmp = const_cast<brpc::RedisReply&>(_resp->reply(0));
    if (tmp.is_nil()) {
        std::string nil_resp = "$-1\r\n";
        appender.append(nil_resp.c_str(), nil_resp.size());
    } else {
        bool ret = tmp.SerializeTo(&appender);
        if (!ret) {
            LOG(ERROR) << "failed to serialize reply to iobuf, on resp:" << to_string(*_resp);
        }
    }
    for (auto& resp : _more_replys) {
        const brpc::RedisReply& reply = resp.reply(0);
        brpc::RedisReply& tmp = const_cast<brpc::RedisReply&>(reply);
        if (tmp.is_nil()) {
            std::string nil_resp = "$-1\r\n";
            appender.append(nil_resp.c_str(), nil_resp.size());
            continue;
        }
        bool ret = tmp.SerializeTo(&appender);
        if (!ret) {
            LOG(ERROR) << "failed to serialize reply to iobuf, on resp:" << to_string(resp);
        }
    }
    _resp->Clear();
    _resp->ConsumePartialIOBuf(appender.buf(), 1);
}

ClientManagerPtr new_client_mgr(const ClientMgrOptions& options) {
    auto mgr = std::make_shared<ClientManagerImpl>(options);
    return mgr;
}

} // namespace client
} // namespace rsdk
