# 验收证据索引

## 2026-09-19宿主故障与单机检查

| 证据 | 实际范围与边界 |
|---|---|
| [宿主中断与诊断](host-interruption-20260919.json) | 0xF7后精确临时残留处理及WinDbg离线诊断；根因未定，不证明来宾恢复 |
| [第一次单analysis窗口](postcrash-single-vm-20260919.json) | 768 MiB检查超时、无guest sample，随后软关读回通过；失败历史保留 |
| [V2单analysis窗口](postcrash-single-vm-v2-20260919.json) | 同授权范围的基础元数据检查和软关通过；不证明HDFS/数据库/Checkpoint恢复或长期稳定，三机及引擎暂停仍保持 |
| [POSIX作业收尾](offline-session-cleanup-20260919.json) | 功能196c的Linux合成测试与CI；VM仍安装de406，不能当作蓝屏修复 |

## 历史合成验收收据

2026-09-11。这里保存实际工具输出的汇总 JSON；无用户数据、凭据或原始事件。首轮引擎及服务代码为 `98f217e`；轻量镜像 ID 记录在其收据内。适配器最新浏览器验收为 Project_Snow `cf13565`。

| 收据 | 生成工具与运行条件 |
|---|---|
| `production-integration.json` | 2026-09-13 独立 off 生产安装；140 条合成样例走 real 代码分支并绑定采集源代际，实际 Kafka/HDFS/YARN/Iceberg、ODS 到期、私有看板和 Marquez 读回；另有两个小容器物理清理 fixture，不能代替实时引擎验收 |
| `ingest-receipt.json` | `tools/smoke_ingest.py`；snow-control 6 GiB，真实 Kafka/MySQL/Connect 容器，合成事件 |
| `cdc-receipt.json` | `tools/cdc_receipt.py`；归档 CDC 的表、删除和事务元数据计数 |
| `sync-receipt.json` | `tools/smoke_sync.py`；Kafka 5 次确认后故障注入，归档重放及位点恢复 |
| `spark.json` | `tools/smoke_spark.sh` → `warehouse/spark/verify.py`；Spark 3.5.7 local[2]、driver 1 GiB、容器上限 3 GiB；实际 Parquet 对账 |
| `iceberg.json` | `tools/smoke_iceberg.sh`；相同 Spark + Iceberg 1.10.0，本地文件系统 catalog |
| `doris.json` | `tools/smoke_doris.py`；Doris 3.0.6.2，独立 snow_acceptance 数据库，FE 2.5 GiB/BE 5 GiB 上限，重复执行及业务版本验证；校时后结果 |
| `lite-container.json` | `tools/smoke_lite_container.py`；512 MiB/0.5 CPU、非 root 容器、只读根目录、真实 2 GiB quota 文件系统及回环端口；仅隔离临时测试库使用 real 标签测试公开指标 |
| `oracle-million.json` | `snow-stats benchmark --events 1000000`；宿主 Python 正确性基准，seed 42；不是 Spark/Flink 吞吐测试 |
| `clock-snow-*.json` | `tools/guest_clock.py --set`；只在无运行容器时校正，保存相对宿主的校正前后偏差；不等同于生产 NTP 精度认证 |
| `yarn.json` | `tools/smoke_yarn.sh` 及故障注入日志整理；4/6/2 GiB 三 VM，真实 YARN、Hive 3.1.3、HDFS 双副本，56 条黄金输入；停止 analysis DN 后校验读取 SHA256 |
| `publication.json` | `tools/smoke_publication.py`；实际 YARN 输出发布到 Doris，小配置 FE 2 GiB/BE 3 GiB；失败前后、重放、空日期及版本冲突验证 |
| `airflow.json` | `tools/airflow_receipt.py` 从专用元数据库只读提取状态，并附计算导出的 manifest；4/4/2 GiB 计算阶段、4/关闭/6 GiB 发布阶段；所有任务成功，发布重试一次 |
| `incremental-ods.json` | `tools/smoke_ods.py` 及连续增量运行日志；真实 Kafka/WebHDFS 双副本/YARN/Hive，原始 81 条及追加 3 条重放；HDFS 提交后、Kafka ACK 后恢复，SCD2/工单黄金对账；包含实际阶段资源与中断记录 |
| `behavior-models.json` | 31 行手算边界 → Spark 五表逐行对账；固定 ODS → 两次 Airflow `snow_models` → 相同发布哈希；独立汇总基准、日指标回归、Hive 读回、HDFS 双副本；3.5/4/1 GiB VM，252 个预装 JAR，Streamlit / Chromium 归档验证 |
| `lineage.json` | 四个实际 Airflow DAG，24 条协议校验事件/12 次运行；Marquez 输入输出读回、停机补发、HTTP 接收后本地确认前重放、容器重建；16 节点/16 边，3.5/3/1 GiB 压缩资源 DAG 与本地 SVG 图验证 |
| `realtime.json` | 单 4.5 GiB VM 实际 Flink TaskManager / session 恢复，17 条边界与累计 20 条输入；Doris DUPLICATE KEY 2PC 探针；单控制 VM Spark local[2] 校正为 12 条事实、四行汇总，Doris 重复发布一致；记录容量失败及未验收范围 |
| `freshness.json` | 宿主真实 HTTP 合成采集 → 增量归档 → 单分析 VM Kafka/Flink/Doris；300/300 可见，发送至查询 P95 12.016 秒，三方日指标一致；关闭全部 VM 后新增 5 条仍由原有 60 秒聚合线程处理 |
| `scale-100k.json` | 2/2/1 GiB 三 VM，实际 Spark/YARN 单远端 executor；56 行回归后，10 万行 → 9 万有效 + 1 万重复，14 行指标逐行一致、HDFS 输入输出双副本；事件日志应用时长 41.851 秒。百万条输入已保存，RAM 门禁拒绝落地，未计算 |
| `scale-1m.json` | 宿主 RAM 恢复后，同一 2/2/1 GiB 配置实际完成百万行 → 90 万有效 + 10 万重复，14 行指标一致；双副本、应用时长 115.023 秒、50 task 成功，保留 spill、执行计划、cgroup 与宿主监测；原 60 GiB 上限内通过 |
| `lite-small.json` | 独立 512 MiB ext4、256 MiB / 0.25 CPU 容器；真实满盘、kill/重启、full/lite/off 及追加 1 万条 HTTP；容器记账峰值 68.39 MiB、状态文件 6.75 MiB；记录未节流首轮失败和节流后通过，保留测试数据 |

这些收据不代表尚未启动的集群、生产流量、并发负载或端到端实时延迟通过验收。持久化状态、原始合成归档、Spark 输出和运行日志位于被 Git 忽略的 runtime；仓库工具及固定输入可用于重建。
