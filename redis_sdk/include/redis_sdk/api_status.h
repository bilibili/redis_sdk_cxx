#ifndef REDIS_SDK_INCLUDE_CLIENT_STATUS_H
#define REDIS_SDK_INCLUDE_CLIENT_STATUS_H

#include <string>

namespace rsdk {

class Status {
public:
    enum class Code {
        // OK
        OK = 0,
        BAD_REQUEST = 400,

        // Network Errors
        UNKNOWN_ERROR = 1001,
        SYS_ERROR = 1002,
        INVALID_PARAM = 1003,
        TIMEOUT = 1004,
        NET_CONNECTION_ERROR = 1005,
        E_AGAIN = 1006,
        // Resouce Not Found
        NOT_FOUND = 2000,
        SLOT_NOT_FOUND = 2001,
        CLUSTER_NOT_FOUND = 2002,
    };

public:
    Status() : _code(Code::OK), _msg() {
    }
    Status(const Code& code, const std::string& msg) : _code(code), _msg(msg) {
    }
    explicit Status(Code code) : _code(code) {
    }
    explicit Status(int code) : _code(Code::UNKNOWN_ERROR) {
        _code = Code(code);
    }
    Status(int code, const std::string& msg) : _code(Code::UNKNOWN_ERROR), _msg(msg) {
        _code = Code(code);
    }
    ~Status() {
    }
    Status(const Status& s) {
        this->_code = s._code;
        this->_msg = s._msg;
    }
    Status& operator=(const Status& s) {
        this->_code = s._code;
        this->_msg = s._msg;
        return *this;
    }
    // OK
    static Status OK(const std::string& msg = "OK") {
        return Status(Code::OK, msg);
    }
    // Net Errors
    static Status SysError(const std::string& msg = "SysError") {
        return Status(Code::SYS_ERROR, msg);
    }
    static Status UnknownError(const std::string& msg = "UnknownError") {
        return Status(Code::UNKNOWN_ERROR, msg);
    }
    static Status InvalidParam(const std::string& msg = "InvalidParam") {
        return Status(Code::INVALID_PARAM, msg);
    }
    static Status Timeout(const std::string& msg = "Timeout") {
        return Status(Code::TIMEOUT, msg);
    }
    static Status NetConnectionError(const std::string& msg = "NetConnectionError") {
        return Status(Code::NET_CONNECTION_ERROR, msg);
    }
    static Status EAgain(const std::string& msg = "eagain") {
        return Status(Code::E_AGAIN, msg);
    }
    // Resource Not Found
    static Status NotFound(const std::string& msg = "NotFound") {
        return Status(Code::NOT_FOUND, msg);
    }
    static Status ClusterNotFound(const std::string& msg = "ClusterNotFound") {
        return Status(Code::CLUSTER_NOT_FOUND, msg);
    }
    // IO Errors
    // TODO Business Errors
    static bool check_err_no(int err_no, Code checked) {
        return err_no == static_cast<int>(checked);
    }

    // Returns true iff the status indicates success.
    inline bool ok() const {
        return code() == Code::OK;
    }

    bool is_eagain() const {
        return code() == Code::E_AGAIN;
    }

    inline bool is_cluster_not_found() const noexcept {
        return code() == Code::CLUSTER_NOT_FOUND;
    }
    bool is_sys_error() const {
        return code() == Code::SYS_ERROR;
    }
    std::string to_string() const {
        return "status:" + std::to_string((int)_code) + ", msg:" + _msg;
    }
    std::string to_json() const {
        return "{\"code\":" + std::to_string((int)_code) + ",\"msg\":\"" + _msg + "\"}";
    }
    friend std::ostream& operator<<(std::ostream& os, const Status& s) {
        return os << s.to_string();
    }
    bool operator!=(const Status& status) const {
        return _code != status.code();
    }
    bool operator==(const Status& status) const {
        return _code == status.code();
    }
    Code code() const {
        return _code;
    }
    const std::string& msg() const {
        return _msg;
    }

private:
    Code _code;
    std::string _msg;
}; // class Status

} // namespace rsdk
#endif // REDIS_SDK_INCLUDE_CLIENT_STATUS_H
