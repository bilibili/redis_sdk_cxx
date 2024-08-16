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
#include "redis_sdk/include/ctx.h"

uint16_t crc16(const char* buf, int len);

namespace rsdk {
namespace client {

uint32_t key_to_slot_id(const char* key, int keylen) {
    int s = 0;
    for (s = 0; s < keylen; ++s) {
        if (key[s] == '{') {
            break;
        }
    }

    if (s == keylen) {
        return crc16(key, keylen) & 0x3FFF;
    }

    int e = 0;
    for (e = s + 1; e < keylen; ++e) {
        if (key[e] == '}') {
            break;
        }
    }

    if (e == keylen || e == (s + 1)) {
        return crc16(key, keylen) & 0x3FFF;
    }

    return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

void log_reply(const brpc::RedisReply& reply) {
    if (reply.is_string()) {
        VLOG(3) << "reply is str:" << reply.c_str();
        return;
    }
    if (reply.is_error()) {
        VLOG(3) << "reply is error:" << reply.error_message();
        return;
    }
    if (reply.is_integer()) {
        VLOG(3) << "reply is integer:" << reply.integer();
        return;
    }
    if (reply.is_array()) {
        VLOG(3) << "reply is array, size:" << reply.size() << ", content:";
        for (size_t j = 0; j < reply.size(); ++j) {
            const brpc::RedisReply& sub = reply[j];
            log_reply(sub);
        }
        return;
    }
    if (reply.is_nil()) {
        VLOG(3) << "reply is nil";
        return;
    }
    VLOG(3) << "unknown reply:" << brpc::RedisReplyTypeToString(reply.type());
}
void log_response(const brpc::RedisResponse& resp) {
    for (int i = 0; i < resp.reply_size(); ++i) {
        log_reply(resp.reply(i));
    }
}

uint64_t get_current_time_us() {
    struct timeval current;
    gettimeofday(&current, nullptr);
    uint64_t ts = current.tv_sec * 1000000ULL + current.tv_usec;
    return ts;
}

uint64_t get_current_time_s() {
    return get_current_time_us() / 1000000L;
}

uint64_t get_current_time_ms() {
    struct timeval current;
    gettimeofday(&current, nullptr);
    uint64_t ts = current.tv_sec * 1000ULL + current.tv_usec / 1000ULL;
    return ts;
}

std::string encode_key(const std::string& key) {
    std::string buf;
    buf.reserve(key.size() + (key.size() >> 1));
    butil::Base64Encode(key, &buf);
    return buf;
}

} // namespace client
} // namespace rsdk
