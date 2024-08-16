#ifndef RSDK_API_INCLUDE_API_UTIL_H
#define RSDK_API_INCLUDE_API_UTIL_H

#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <bthread/countdown_event.h>
#include <butil/time.h>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thread>

#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

namespace rsdk {
namespace client {

class SyncClosure : public google::protobuf::Closure {
public:
    SyncClosure(int initial_count = 1) : _event(initial_count) {
    }
    ~SyncClosure() {
    }
    void Run() override {
        _event.signal();
    }
    void wait() {
        _event.wait();
    }
    void incr(int v = 1) {
        _event.add_count(v);
    }
    void reset(int count = 1) {
        _event.reset(count);
    }

private:
    bthread::CountdownEvent _event;
};

class TimeCost {
public:
    TimeCost() {
        _start = butil::cpuwide_time_ns();
    }
    // return time cost since last call, in mili seconds
    uint64_t cost_ms() {
        return cost_us() / 1000;
    }
    uint64_t cost_us() {
        uint64_t now = butil::cpuwide_time_ns();
        uint64_t cost = now - _start;
        _start = now;
        return cost / 1000;
    }

    virtual ~TimeCost() {
    }

private:
    uint64_t _start;
};

uint64_t get_current_time_us();
uint64_t get_current_time_s();
uint64_t get_current_time_ms();
std::string encode_key(const std::string& key);

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_API_UTIL_H
