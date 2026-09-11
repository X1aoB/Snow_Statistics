# 本地合成验收收据

2026-09-11。这里保存实际工具输出的汇总 JSON；无用户数据、凭据或原始事件。首轮引擎及服务代码为 `98f217e`；轻量镜像 ID 记录在其收据内。适配器最新浏览器验收为 Project_Snow `db02fb7`。

| 收据 | 生成工具与运行条件 |
|---|---|
| `ingest-receipt.json` | `tools/smoke_ingest.py`；snow-control 6 GiB，真实 Kafka/MySQL/Connect 容器，合成事件 |
| `cdc-receipt.json` | `tools/cdc_receipt.py`；归档 CDC 的表、删除和事务元数据计数 |
| `sync-receipt.json` | `tools/smoke_sync.py`；Kafka 5 次确认后故障注入，归档重放及位点恢复 |
| `spark.json` | `tools/smoke_spark.sh` → `warehouse/spark/verify.py`；Spark 3.5.7 local[2]、driver 1 GiB、容器上限 3 GiB；实际 Parquet 对账 |
| `iceberg.json` | `tools/smoke_iceberg.sh`；相同 Spark + Iceberg 1.10.0，本地文件系统 catalog |
| `lite-container.json` | `tools/smoke_lite_container.py`；512 MiB/0.5 CPU、非 root 容器、只读根目录、真实 2 GiB quota 文件系统及回环端口；仅隔离临时测试库使用 real 标签测试公开指标 |
| `oracle-million.json` | `snow-stats benchmark --events 1000000`；宿主 Python 正确性基准，seed 42；不是 Spark/Flink 吞吐测试 |

这些收据不代表尚未启动的集群、生产流量、并发负载或端到端实时延迟通过验收。持久化状态、原始合成归档、Spark 输出和运行日志位于被 Git 忽略的 runtime；仓库工具及固定输入可用于重建。
