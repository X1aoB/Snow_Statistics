# Kafka 与 ZooKeeper 故障专题

2026-09-12 已在一台 3 GiB 的 snow-analysis VM 顺序完成两组真实进程实验。控制、计算 VM 关闭，两组服务不同时运行。证据见 [ha.json](evidence/ha.json)。这些是同宿主进程故障与协议实验，不提供物理机容灾。

| 实验 | 故障与观察 | 恢复结果 |
|---|---|---|
| Kafka 3.9.1 KRaft，RF=3 / minISR=2 / acks=all | 停实际 leader 2 后选出 3，ISR 从 3 降至 2，仍可确认写入 | ISR 恢复 3；三个已确认消息按原 offset 读回 |
| Kafka 严格副本主题，minISR=3 | 单节点停止即返回 NotEnoughReplicasError | 只有之前确认的 strict-before；失败探针没有新增可见消息 |
| Kafka 丢两个 broker/controller | 写入返回 NotEnoughReplicasError，没有确认 | 恢复后完整读取三个已确认消息；本次未确认消息可见数为 0 |
| ZooKeeper 3.9.3 | 停 leader 3 后选出 2；持久节点更新触发 CHANGED；旧 version=0 写入被拒 | 持久值 one-down、version=1 保留 |
| ZooKeeper 客户端会话 | 强杀持有临时节点的独立 Python 客户端，等待服务端会话到期 | 临时节点自动消失；此处与服务器丢多数分别测试 |
| ZooKeeper 丢多数 | 剩一个节点无法建立可写会话，KazooTimeoutError；没有在无会话时提交写入 | 三节点恢复后持久值不变，临时节点没有复活 |

Kafka 两次故障都停止了 combined broker/controller，实验没有把控制器选主与数据 ISR 的影响单独归因。未确认写入一般需要读回核对，不能从一个错误就推论消息绝不落地。[Kafka 3.9 主题配置](https://kafka.apache.org/39/configuration/topic-level-configs/)规定了 min.insync.replicas 与 acks=all 的配合。

ZooKeeper Watch 是通知机制，本次验证一次修改通知；没有声明完整变更日志或跨断连永不漏通知。临时节点由 session 生命周期控制。[Kazoo 使用说明](https://kazoo.readthedocs.io/en/latest/basic_usage.html)说明了会话状态、版本检查和 Watch 行为。收据中的主客户端 LOST 包括主动 stop，不把每个 LOST 都解释为被强杀的客户端。

## 复现

1. 按 [运行手册](runbook.md)校验源码与锁文件，确认所有实验容器和三台 VM 停止，检查当前资源余量。配置 `uv run python tools/vmware_lab.py configure --node snow-analysis --profile ha` 后只启动分析 VM。该 profile 为 3072 MiB。
2. 将索引源码 bundle 同步至分析节点，保留所有既有 runtime 和私有配置。`lab/secrets/ha.env` 只需 `HA_IP=<实际分析 VM NAT 地址>`，不能填公网或默认全接口地址。使用 `vmware_lab.py ip --node snow-analysis` 核对；私有配置不入 Git。
3. 在该 VM 的 `/home/snow/Snow_Statistics` 执行 `bash tools/start_ha_node.sh kafka-ha`。工具要求没有任何运行容器，锁定 digest，内存/guest 磁盘检查后才启动三个 broker。
4. 宿主执行 `uv run --extra lab python tools/smoke_kafka_ha.py --lane ha03`，选未用过的 lane。脚本发现实际 leader 后注入故障，读回确认，并在 finally 中停止本项目 HA 容器。首次创建主题可能暂时没有 partition 元数据，工具会有界等待；ha01 的发送前失败材料保留。
5. 所有 Kafka 容器停止后，在 VM 执行 `bash tools/start_ha_node.sh zookeeper`；宿主执行 `uv run --extra lab python tools/smoke_zookeeper.py --lane zk02`。新 lane 不覆盖旧 znode 或收据。
6. 检查 `runtime/ha/<类型>-<lane>/shutdown.json` 和真实 Docker 状态，再执行 `uv run python tools/vmware_lab.py stop --node snow-analysis`。工具停止服务后会暂留 VM 供下一阶段使用，不自动删除数据。

旧 `kafka_ha_experiment.sh` 只提示迁移并退出，避免固定停止 broker1 的旧探针被误认为 leader 故障验收。

## 资源与保留

Kafka 容器各 768 MiB、堆最大 384 MiB；ZooKeeper 各 512 MiB、堆最大 256 MiB。客户端仅使用 VM NAT 19092..19094 / 12181..12183。它们是隔离实验端口，无生产认证配置，不加入任何业务服务部署。

所有观察容器 OOM=0。cgroup peak 从最近一次启动计，不代表跨重启的全程峰值，也不能将各自峰值相加作为同一时刻占用。具体采样和预算随证据保留。Broker 数据、ZooKeeper data/datalog 均采用有名字的项目卷，日志上限 5 MiB × 2；关闭不删除这些卷或 synthetic topic/znode。清单登记在 `deploy/resources.json`。

HDFS 两副本及 NameNode 重启已验证，见 [湖仓恢复](lake.md)。自动 HDFS HA / ZKFC / fencing 仍是可选拓展，当前没有开启，不能把本页 ZooKeeper 实验当成 HDFS 自动故障切换。
