#ifndef RSDK_API_INCLUDE_METRIX_H
#define RSDK_API_INCLUDE_METRIX_H

namespace prometheus {
class Registry;
} // namespace prometheus

namespace rsdk {
namespace client {

class ClientMetrics {
public:
    explicit ClientMetrics(prometheus::Registry* reg = nullptr);
    virtual ~ClientMetrics();
    void init();
    void add_latency(const std::string& cluster, const std::string& method,
                     const std::string& server, bool succ, uint64_t us, int cnt);

private:
    struct PrometheusContext;

    prometheus::Registry* _registry = nullptr;
    PrometheusContext* _context = nullptr;
    std::vector<double> _latency_buckets_ms;

    static const std::vector<double> S_DEFAULT_LATENCY_BUCKETS_MS;
};

} // namespace client
} // namespace rsdk

#endif // RSDK_API_INCLUDE_METRIX_H
