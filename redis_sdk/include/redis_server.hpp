#ifndef RSDK_API_INCLUDE_REDIS_SERVER_HPP
#define RSDK_API_INCLUDE_REDIS_SERVER_HPP

#include <brpc/policy/hasher.h>
#include "redis_sdk/include/redis_server.h"

namespace brpc {
DECLARE_bool(redis_verbose_crlf2space);
} // namespace brpc

namespace rsdk {
namespace client {

DECLARE_bool(log_request_verbose);

template <typename CtxType>
void RedisServer::send_cmd(CtxType* ctx) {
    if (FLAGS_log_request_verbose) {
        std::stringstream ss;
        brpc::FLAGS_redis_verbose_crlf2space = true;
        ctx->mutable_req()->Print(ss);
        std::string cmd = ss.str();
        LOG(INFO) << "start to send cmd to redis server: " << _addr << ", cmd:[" << cmd << "]";
    }
    brpc::RedisResponse* resp = ctx->mutable_resp();
    const std::string& key = ctx->key();
    uint32_t conn_id = brpc::policy::MurmurHash32(key.c_str(), key.size()) % _channels.size();
    RpcCallback* cb = new RpcCallback(ctx, shared_from_this());
    if (ctx->big_key()) {
        _slow_channel->CallMethod(nullptr, cb->mutable_cntl(), ctx->mutable_req(), resp, cb);
    } else {
        _channels[conn_id]->CallMethod(nullptr, cb->mutable_cntl(), ctx->mutable_req(), resp, cb);
    }
}

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_REDIS_SERVER_HPP
