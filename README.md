# 优点
- 基于brpc, 支持异步模式
- 支持配置后端节点连接数
- 支持prometheus上报埋点信息
- 解决以下常见问题
  1. 随机初始化种子节点，避免集中发版时导致的部分redis 节点压力过大的问题
  2. 业务超时和后端连接超时解耦
  3. ask/move 命令重试上限
  4. 限制单Redis 节点(server)的连接数量
  5. cluster node/slot 单飞，避免过度刷新导致后端节点压力过大
  6. 连接主动保活


# 使用限制
- 只支持cluster模式
- 只支持读主
- 不支持dump/keys/scan/msetnx/sdiff/siner/sunion/pipeline/eval 命令
- exec 命令中不支持多余的空格(使用空格进行命令切分)
