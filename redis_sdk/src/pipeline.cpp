#include "redis_sdk/include/pipeline.h"

#include <butil/logging.h>
#include <xxhash.h>
#include "redis_sdk/client.h"
#include "redis_sdk/include/client_impl.h"
#include "redis_sdk/include/redis_server.hpp"

#include <butil/strings/string_split.h>

namespace rsdk {
namespace client {
void reply_of_set_to_record(const brpc::RedisReply& reply, Record* rec);
void reply_of_get_to_record(const brpc::RedisReply& reply, Record* rec);
void reply_to_resp(const brpc::RedisReply& reply, brpc::RedisResponse* resp);

PipeCtx::PipeCtx(ClusterPtr cluster, RedisServerPtr serv, const std::string& mcmd,
                 const std::string& cmd, std::string&& key, ::google::protobuf::Closure* done) :
        _cluster(cluster),
        _server(serv),
        _main_cmd(mcmd),
        _cmd(cmd),
        _key(std::move(key)),
        _user_cb(done) {
    _start_time_us = get_current_time_us();
    _read_op = (cmd == "get" || cmd == "mget");
}

void record_to_redis_req(const std::string& cmd, const Record& rec, bool read_op,
                         brpc::RedisRequest* req) {
    butil::StringPiece arg_list[5];
    arg_list[0].set(cmd.c_str(), cmd.length());
    arg_list[1].set(rec._key.c_str(), rec._key.length());
    if (read_op) {
        req->AddCommandByComponents(arg_list, 2);
        return;
    }
    // write operation
    arg_list[2].set(rec._data.c_str(), rec._data.size());
    if (rec._ttl_s == 0) {
        req->AddCommandByComponents(arg_list, 3);
        return;
    }
    // has ttl
    std::string ttl_str = fmt::format("{}", rec._ttl_s);
    static const std::string S_EX = "EX";
    arg_list[3].set(S_EX.c_str(), S_EX.length());
    arg_list[4].set(ttl_str.c_str(), ttl_str.length());
    req->AddCommandByComponents(arg_list, 5);
    return;
}

void PipeCtx::add_request_impl(const std::string& key, const Record& rec) {
    record_to_redis_req(_cmd, rec, _read_op, &_main_req);
    if (_key.empty()) {
        _key = key;
    }
}

void PipeCtx::add_request(const std::string& key, Record* rec) {
    add_request_impl(key, *rec);
    _input_records.push_back(rec);
    _user_output_records.push_back(rec);
    _use_rec_as_output = true;
}

void PipeCtx::add_request(const std::string& key, const Record& rec, brpc::RedisResponse* resp) {
    add_request_impl(key, rec);
    _input_records.push_back(&rec);
    _user_output_resp.push_back(resp);
    _use_rec_as_output = false;
}

void PipeCtx::send_req() {
    _server->send_cmd(this);
}

void PipeCtx::handle_err(const std::string& msg) {
    VLOG(3) << "handle err from:" << _server->addr() << " with err:" << msg << " on cmd:" << _cmd
            << " request cnt:" << _main_req.command_size();
    brpc::ClosureGuard user_callback(_user_cb);
    for (size_t i = 0; i < _user_output_resp.size(); ++i) {
        set_error_to_resp(_user_output_resp[i], msg);
    }
    for (size_t i = 0; i < _user_output_records.size(); ++i) {
        _user_output_records[i]->_errno = -1;
        _user_output_records[i]->_err_msg = msg;
    }

    _end_time_us = get_current_time_us();
    int64_t cost = _end_time_us - _start_time_us;
    _cluster->add_metrics(_main_cmd, _server->addr(), false, cost, _main_req.ByteSize(),
                          _main_resp.ByteSize(), int(_user_output_records.size()));
}

RedisStatus parse_redis_error(const std::string& str);
bool PipeCtx::should_retry(const brpc::RedisReply& reply, RetryRequest* retry) {
    const std::string& err_msg_str = reply.error_message();
    butil::SplitString(err_msg_str, ' ', &(retry->_err_list));
    if (retry->_err_list.empty()) {
        return false;
    }
    retry->_rs = parse_redis_error(retry->_err_list[0]);
    uint64_t cur_time = get_current_time_us();
    if (cur_time >= uint64_t(_start_time_us + _acc_opt.timeout_ms * 1000)) {
        return false;
    }
    if (retry->_rs >= RedisStatus::MOVED && retry->_rs <= RedisStatus::CLUSTER_DOWN) {
        return true;
    }
    return false;
}

void reply_to_resp(const brpc::RedisReply& reply, brpc::RedisResponse* resp) {
    butil::IOBufAppender appender;
    if (reply.is_nil()) {
        std::string nil_resp = "$-1\r\n";
        appender.append(nil_resp.c_str(), nil_resp.size());
    } else {
        brpc::RedisReply new_reply(nullptr);
        new_reply.CopyFromSameArena(reply);
        bool ret = new_reply.SerializeTo(&appender);
        if (!ret) {
            std::string err_msg = "ERR bug of redis api, failed to serialize response to iobuf";
            set_error_to_resp(resp, err_msg);
            return;
        }
    }
    butil::IOBuf buf;
    appender.move_to(buf);
    brpc::ParseError err = resp->ConsumePartialIOBuf(buf, 1);
    if (err != brpc::PARSE_OK) {
        std::string err_msg = "ERR bug of redis api, failed to parse serialized result";
        set_error_to_resp(resp, err_msg);
        return;
    }
}

void reply_of_get_to_record(const brpc::RedisReply& reply, Record* rec) {
    if (reply.is_error()) {
        rec->_errno = -1;
        rec->_err_msg = reply.error_message();
        return;
    }
    if (reply.is_array()) {
        rec->_errno = -1;
        rec->_err_msg = fmt::format("bug, 'get' got array resp");
        return;
    }
    if (reply.is_integer()) {
        rec->_errno = 0;
        int64_t v = reply.integer();
        rec->_data.resize(sizeof(v));
        memcpy(const_cast<char*>(rec->_data.data()), &v, sizeof(v));
        return;
    }
    if (reply.is_nil()) {
        rec->_errno = 0;
        rec->_data.clear();
        return;
    }
    if (reply.is_string()) {
        rec->_errno = 0;
        butil::StringPiece v = reply.data();
        v.CopyToString(&(rec->_data));
        return;
    }
    rec->_errno = -1;
    rec->_err_msg = fmt::format("bug, unknown resp type:{}", (int)reply.type());
}

void reply_of_set_to_record(const brpc::RedisReply& reply, Record* rec) {
    if (reply.is_error()) {
        rec->_errno = -1;
        rec->_err_msg = reply.error_message();
        return;
    }
    if (reply.is_nil() || reply.type() == brpc::REDIS_REPLY_STATUS) {
        rec->_errno = 0;
        return;
    }
    // impossible
    if (reply.type() == brpc::REDIS_REPLY_STRING) {
        rec->_errno = -1;
        std::string err = reply.data().as_string();
        rec->_err_msg = fmt::format("bug, got STRING in 'set' resp, content:{}", err);
        return;
    }
    rec->_errno = -1;
    rec->_err_msg = fmt::format("bug, unknown reply type:{} in 'set'", (int)reply.type());
}

void record_to_redis_req(const std::string& cmd, const Record& rec, bool read_op,
                         brpc::RedisRequest* req);
void PipeCtx::retry_in_sub(const RetryRequestList& to_retry, RedisServerPtr from) {
    VLOG(3) << "retry to send " << to_retry.size()
            << " keys after pipeline mode as has err from:" << from->addr();
    if (_use_rec_as_output) {
        _retry_resp_list.resize(to_retry.size());
    }
    RetryClosure* retry_done = new RetryClosure(to_retry.size(), this);
    for (size_t i = 0; i < to_retry.size(); ++i) {
        brpc::RedisRequest sub_req;
        const RetryRequest& retry = to_retry.at(i);
        const Record* rec = retry._input_rec;
        std::string key = rec->_key;
        record_to_redis_req(_cmd, *rec, _read_op, &sub_req);
        brpc::RedisResponse* resp = retry._output_resp;
        if (_use_rec_as_output) {
            resp = &_retry_resp_list[i];
        }
        VLOG(4) << "start to send retry request on key:[" << encode_key(key) << "] on cmd:[" << _cmd
                << "]"
                << " prev server:" << from->addr();
        SubReqClosure* sub_done = new SubReqClosure(_read_op, retry._output_rec, resp, retry_done);
        IOCtx* ctx = new IOCtx(_cluster, _main_cmd, _cmd, std::move(key), std::move(sub_req), resp,
                               sub_done);
        Status ret = ctx->init(_options);
        if (!ret.ok()) {
            std::string msg = fmt::format("client side error, error: {}", ret.to_string());
            set_error_to_resp(resp, msg);
            delete ctx;
            continue;
        }
        ctx->set_start_time(_start_time_us);
        ctx->set_server_id(from->id());
        ctx->retry(retry._rs, from, retry._err_list, retry._err_msg);
    }
}

void PipeCtx::handle_resp(RedisServerPtr from) {
    VLOG(3) << "start to handle resp for cmd:[" << _cmd << "], from:" << from->addr()
            << ", first_key:" << encode_key(_key) << ", resp cnt:" << _main_resp.reply_size();
    int reply_cnt = (int)_main_resp.reply_size();
    if (reply_cnt != (int)_input_records.size()) {
        std::string msg = fmt::format("bug, reply_cnt:{} != input_user_records.size:{}", reply_cnt,
                                      _input_records.size());
        handle_err(msg);
        return;
    }
    _end_time_us = get_current_time_us();
    int64_t cost = _end_time_us - _start_time_us;

    RetryRequestList to_retry;
    int success_cnt = 0;
    int failed_cnt = 0;
    for (int i = 0; i < reply_cnt; ++i) {
        const brpc::RedisReply& reply = _main_resp.reply(i);
        if (reply.is_error()) {
            RetryRequest retry;
            retry._input_rec = _input_records[i];
            VLOG(3) << "redis server:" << from->addr() << " return err:" << reply.error_message()
                    << " on key:" << encode_key(retry._input_rec->_key)
                    << ", may need to retry on it";
            if (should_retry(reply, &retry)) {
                if (_use_rec_as_output) {
                    retry._output_rec = _user_output_records[i];
                } else {
                    retry._output_resp = _user_output_resp[i];
                }
                to_retry.push_back(retry);
            } else {
                failed_cnt++;
                if (_use_rec_as_output) {
                    Record* rec = _user_output_records[i];
                    rec->_errno = -1;
                    rec->_err_msg = reply.error_message();
                } else {
                    set_error_to_resp(_user_output_resp[i], reply.error_message());
                }
            }
        } else {
            success_cnt++;
            VLOG(5) << "redis server:" << from->addr() << " return succ"
                    << " on key:" << encode_key(_input_records[i]->_key) << " on cmd:[" << _cmd
                    << "]"
                    << " reply type:" << brpc::RedisReplyTypeToString(reply.type());
            if (_use_rec_as_output) {
                if (_read_op) {
                    reply_of_get_to_record(reply, _user_output_records[i]);
                } else {
                    reply_of_set_to_record(reply, _user_output_records[i]);
                }
            } else {
                brpc::RedisResponse* resp = _user_output_resp[i];
                reply_to_resp(reply, resp);
            }
        }
    }
    if (success_cnt > 0) {
        _cluster->add_metrics(_main_cmd, from->addr(), true, cost, _main_req.ByteSize(),
                              _main_resp.ByteSize(), success_cnt);
    }
    if (failed_cnt > 0) {
        _cluster->add_metrics(_main_cmd, from->addr(), false, cost, _main_req.ByteSize(),
                              _main_resp.ByteSize(), failed_cnt);
    }

    if (!to_retry.empty()) {
        retry_in_sub(to_retry, from);
        return;
    }
    VLOG(2) << "succeed to handle resp from:" << from->addr() << " cmd:" << _cmd
            << " resp cnt:" << _main_resp.reply_size() << ", no need to do retry";
    finish_resp();
}

uint64_t PipeCtx::left_time_ms() const {
    int64_t used = (get_current_time_us() - _start_time_us) / 1000;
    return _acc_opt.timeout_ms > used ? _acc_opt.timeout_ms - used : 0;
}
void PipeCtx::finish_resp() {
    brpc::ClosureGuard user_callback(_user_cb);
}

void RetryClosure::Run() {
    if (!unref()) {
        return;
    }
    std::unique_ptr<RetryClosure> release_this(this);
    _pipe_req->finish_resp();
}

void SubReqClosure::Run() {
    std::unique_ptr<SubReqClosure> release_this(this);
    brpc::ClosureGuard done(_done);
    if (_rec) {
        if (_is_get) {
            reply_of_get_to_record(_resp->reply(0), _rec);
        } else {
            reply_of_set_to_record(_resp->reply(0), _rec);
        }
    }
}

} // namespace client
} // namespace rsdk
