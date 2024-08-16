#ifndef RSDK_TEST_MOCK_MGR_HTTP_H
#define RSDK_TEST_MOCK_MGR_HTTP_H

#include <memory>
#include <vector>

#include <brpc/controller.h> // brpc::Controller
#include <brpc/redis.h>
#include <brpc/server.h>
#include <bthread/mutex.h>
#include <butil/crc32c.h>
#include <butil/strings/string_split.h>
#include <gflags/gflags.h>
#include <unordered_map>

#include <cpptoml.h>
#include <fmt/format.h>

#include <butil/time.h>
#include "test/http_mgr.pb.h"

namespace rsdk {
namespace client {
namespace test {

class MockHttpServiceImpl : public HttpService {
public:
    MockHttpServiceImpl() {
    }
    virtual ~MockHttpServiceImpl() {
    }
    void get(google::protobuf::RpcController* cntl_base, const HttpRequest* req, HttpResponse* resp,
             google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        std::lock_guard<bthread::Mutex> lock(_mutex);
        if (_root) {
            std::stringstream ss;
            ss << *_root;
            std::string value = ss.str();
            cntl->response_attachment().append(value);
        }
    }
    void set_root(std::shared_ptr<cpptoml::table> root) {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        _root = root;
    }

public:
    bool _running = true;
    bthread::Mutex _mutex;
    std::shared_ptr<cpptoml::table> _root;
};

class MockHttpServer {
public:
    MockHttpServer() : _port(0) {
    }
    ~MockHttpServer() {
        stop();
    }
    int start() {
        _service = new MockHttpServiceImpl();
        int ret = _server.AddService(_service, brpc::SERVER_OWNS_SERVICE,
                                     "/overlord/api/v3/app/toml  => get");

        if (ret != 0) {
            LOG(ERROR) << "Fail to add mock metaserver service to rpc server";
            return -1;
        }
        int start_port = 5000;
        int end_port = 5999;
        brpc::PortRange range(start_port, end_port);
        if (0 != _server.Start("127.0.0.1", range, nullptr)) {
            LOG(ERROR) << "failed to start server from port:" << start_port << " to:" << end_port;
            return -1;
        }
        _port = _server.listen_address().port;
        _ip_port = "127.0.0.1:" + std::to_string(_port);
        VLOG(1) << "mock node start to work at:" << addr();
        return 0;
    }
    void stop() {
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
    int port() {
        return _port;
    }
    std::string addr() {
        return _ip_port;
    }
    std::string name() {
        return addr();
    }

public:
    brpc::Server _server;
    MockHttpServiceImpl* _service = nullptr;
    int _port = 0;
    std::string _ip_port;
    bool _runing = true;
};

} // namespace test
} // namespace client
} // namespace rsdk

#endif // RSDK_TEST_MOCK_MGR_HTTP_H
