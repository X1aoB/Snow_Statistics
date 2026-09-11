# 实施状态

本记录只列实际证据，不把技术栈配置文件计作集群验收。2026-09-11 首轮实现。

本次交付是可运行首版；完整计划仍有后续实施和集群验收。轻量服务、产品候选适配、可靠同步及离线正确性样例已落地。

## 已验证

- 独立公开仓库 `X1aoB/Snow_Statistics` 已创建。
- Python 58 项测试通过：原有轻量/同步/建模/发布/网关 38 项，加增量归档、HDFS 故障恢复、校验和/位点门禁及运营快照截止日期的 20 项。
- 浏览器 7 项测试通过：默认关闭无标识/网络副作用、隐私信号、缺失/不安全的采集地址、重复入口点击更新归因标识、有限队列和重试、统计故障不伪造零值、归档/文本渲染。
- MyWebsite 独立工作树构建与 Astro 检查通过，60 项测试通过；Project_Snow 前端类型检查和 7 项 Node 测试通过，两个产品采集仍默认关闭。
- Project_Snow 两项实际 Chromium 测试通过：默认关闭无统计标识/请求；生产 CSP 下采集 503、断连、超时均不阻断假后端聊天，撤销后统计标识及回调清除。7 项静态发布包验证通过；没有付费模型调用。
- 轻量 Docker 容器在独立 2 GiB ext4 文件系统中通过实际验收：无数仓运行仍刷新、强制 kill 后恢复去重、full→lite 指标不变、真实 ENOSPC 后拒收且已确认事件保留、off 显示归档且不删除历史。测试容器已移除，测试数据库镜像保留在忽略的 runtime。
- Flink Java 作业用 Maven 3.9.9 编译成功，2 项归一化/来源隔离测试通过。Java 11 字节码、Flink 1.20.3、Doris connector 25.1.0、Jackson BOM 2.17.3。
- 三台 VMware VMX/薄置备磁盘/seed 已生成；Ubuntu release-20260826 SHA256 已验证。6/6/10 GiB 节点已逐台通过实际启动、cloud-init、受验证 SSH 和 Docker 初始化；未把逐台启动当作三节点并发负载验收。
- 控制节点 Kafka 3.9.1、MySQL 8.4.5、Debezium 3.0.8.Final 启动。真实 Kafka 收发 28 条合成事件；连接器及任务 RUNNING，两个模拟工单写入成功。
- CDC 归档覆盖 contents/campaigns/tickets，首轮取得 19 条唯一变更，包含 1 条删除和 15 条有事务元数据的变更。
- 真 Kafka 中断恢复：接收 28 条，5 条确认后模拟失败；恢复重放后共 33 次 Kafka 确认，最终源位点 28，公开合成行数 0。
- Spark 3.5.7/Java 11/Python 3.8 容器实际运行：56 条含重放输入 → 28 条有效 + 28 条重复，隔离 0；事件分层和运营 SCD2/工单作业完成。模式为单虚拟机 local[2]，不是 YARN 集群成绩。
- 实际 Spark Parquet 与正确性基准对账通过：6 行日指标、4 个工单轮次、4 个内容有效版本。
- Iceberg 1.10.0 在实际 Spark 容器中通过 MERGE、历史快照读取、字段新增、分区从月到日演进、数据文件重写；独立本地文件系统 catalog，未冒充 HDFS 湖仓集群。
- Doris 3.0.6.2 在分析节点受限容器中通过建表、稳定主键重放、旧业务版本不覆盖新版本、实时/离线整数对账，并在重建容器后重复通过。FE 上限 2.5 GiB、BE 上限 5 GiB，首次小样本后观察约 889 MiB/1.105 GiB 使用量；这是单键功能验收，不代表负载测试峰值。
- Python 正确性基准：10 万条约 6.618 秒；100 万条约 89.844 秒；均完整对账且隔离 0。固定 seed 42，不能作为分布式吞吐或真实用户规模。
- 14 个第三方镜像已从实际 registry 解析并固定 digest；Ubuntu、Python lock 与 Java 直接版本可检查。

## 后续离线闭环验收（2026-09-11）

- HDFS 3.4.1 两个 DataNode、YARN RM/NM、Hive 3.1.3 元数据库实际联通；元数据库持久卷重建容器后校验通过。Hive 客户端 334 个 JAR 由固定镜像提取并逐一 SHA256 锁定。
- Spark 3.5.7 在真实 YARN 上运行，执行器位于 compute，输入/输出位于 HDFS，Hive 元数据注册及查询成功。56 条输入 → 28 有效 + 28 重复，0 隔离，6 行日指标与基准一致。
- 重跑指标相同；故意制造基准不一致时不生成 accepted 标记，旧结果仍存在，新的 run ID 重算恢复。输出 17 个非空块均有两个有效副本；停止一个 DataNode 后读回输入 SHA256 相同。
- Doris 正式发布改为不可变日快照及原子日期指针。实际通过读回对账、重复发布、提交前失败保留旧报表、空日期修正、较旧 cutoff 拒绝覆盖和相同 cutoff 冲突拒绝。
- Streamlit 已查询 Doris 已发布视图；AppTest 验证实际 6 行指标、基准切换及不可用提示，真实 Chromium 验证两张表和趋势图渲染。高级分析基准仍可独立查看。
- Airflow 2.10.5 实际 DAG `synthetic-e2e-20260911` 成功：window 成功，YARN 计算首次成功，Doris 停止时发布进入重试，资源切换后第二次发布成功；计算没有重跑。默认手动模式，仅常驻 scheduler，受限 SSH 网关复用固定 Spark 运行时。
- 初始独立 YARN 样例使用 VM 4/6/2 GiB、executor 1 GiB；Airflow 链路已使用 4/4/2 GiB、executor 768 MiB。发布阶段 control 4 GiB、compute 关闭、analysis 6 GiB，Doris FE/BE 容器上限 2/3 GiB。资源切换由操作者完成，Airflow 不管理 VMware。

收据见 `evidence/yarn.json`、`evidence/publication.json`、`evidence/airflow.json`；操作见 [离线闭环手册](offline-pipeline.md)。这是固定小样本离线闭环，尚不是全部技术栈或连续线上链路的验收。

原始验收工具输出的汇总收据见 [evidence](evidence/README.md)。业务适配为独立草稿 PR：[MyWebsite #2](https://github.com/X1aoB/MyWebsite/pull/2)、[Project_Snow #59](https://github.com/X1aoB/Project_Snow/pull/59)。未合并或部署。

## 增量 ODS 验收（2026-09-11）

- Kafka 历史范围捕获 81 条合成原始消息：61 行行为、19 行 CDC、1 条 tombstone、隔离 0。四个分区独立记录边界、cluster/Topic ID；Kafka group 在 HDFS 验证前保持未提交。
- WebHDFS 原子目录落地、SHA256 读回、两个副本确认、固定输入清单已实际通过。注入 HDFS 批次提交后失败，重试仍为相同清单；注入 Kafka ACK 后本地 checkpoint 前失败，恢复后位点相同、空增量不生成新批次。
- 第二次追加 3 条重放事件，只捕获 offset 61..63；清单累积两个批次，原清单保持不变。
- 第一份 Kafka 输入清单已接入真实 YARN/Hive：61 行 → 28 有效 + 33 重复，0 隔离，6 行日指标与基准一致。
- 两批累计输入再次在 YARN/Hive 计算：64 行 → 28 有效 + 36 重复，日指标与第一轮完全一致；Doris 发布及重复发布、公开实验视图读回一致。第二批位点已提交，之后捕获无新数据，pending 已清除。
- CDC 运营模型已接入 HDFS/YARN：19 条变更形成 4 个 SCD2 内容版本、4 个工单处理轮次、8 行每日状态（截至 2026-01-04），Parquet 读回与相同日期的基准一致。首次严格校验发现旧基准只生成到末次变更次日，现提供显式 `operations_as_of` 并测试未来变更排除；旧失败运行不生成 accepted 标记。
- 首轮资源 4/4/2 GiB。某次重算时临时 JAR 触发 60 GiB 容量门禁，作业已中断；增加 `--reserve-mib 1024` 作业启动检查，新增 `ods` 3.5/4/1 GiB 配置与分析 DataNode 512 MiB 容器。容量阈值保持原值，没有清理已接收数据。
- 3.5/4/1 GiB 配置实际完成增量复算；日指标 17 个非空块、运营模型 4 个非空块均至少双副本、fsck HEALTHY。结束时项目文件约 58.91 GiB，1 GiB 分析 VM 的 DataNode 观察到约 216.8 MiB 容器用量、434 MiB guest 可用内存，非压力峰值。Doris 发布阶段仅保留 6 GiB 分析 VM，由 Windows 验证客户端执行，控制机关闭。

操作与恢复边界见[增量 ODS 手册](incremental-ods.md)。实际运行收据在 `evidence/incremental-ods.json`。这是合成数据、单写者、显式位点及业务去重的验收，不是跨系统事务或持续实时 SLA。

## 尚未完成的验收

- 分布式 Spark 留存/会话/渠道漏斗及自动血缘生命周期接入仍需实现；CDC 运营模型已通过 YARN，尚未接入 Airflow DAG。Kafka/CDC 增量 ODS 命令已实现，资源阶段切换和调度编排仍由操作者执行。
- 三 VM 同时运行时的内存预算；两台 6 GiB 节点同时运行已验证，追加 10 GiB 节点时可用 13819 MiB，低于 10240+4096 MiB 门禁要求，拒绝启动。需分阶段验证或释放宿主内存后重试；没有绕过余量限制。
- Doris 扩样/负载测试、Flink checkpoint 恢复与端到端 P95≤60 秒；离线/实时的分布式 10 万及 100 万规模测试尚未完成。
- Airflow 当前为单调度器/SQLite/SequentialExecutor 的实验部署；持续定时运行和多执行器不是本轮验收范围。Marquez 运行与故障验收、Iceberg 集群并发与长期文件维护待验证；列级血缘未声明完成。
- 三 Kafka/ZooKeeper 专题配置已加入；HDFS 自动 HA 因 fencing 尚未配置而保持关闭。
- 真实日志跟随、线上 quota mount、代理/CSP 和业务生产发布；当前没有启用生产采集，也未修改生产服务或业务数据库。
- 7 天离线积压与 2 GiB 在线容量的真实容量测试、实时/离线/Doris 一致性、物理资源测量和退出部署演练。

首轮结束时项目文件长度约 31.19 GiB。离线闭环增加 Hadoop/Hive/Airflow 镜像和 HDFS/YARN 数据，运行期间已接近 60 GiB 门禁；文件长度是保守近似，不等于精确磁盘分配量。后续扩样须先处理受管理的临时缓存和磁盘回收，不能直接增加数据量或绕过门禁。

## 本轮修复的实际兼容问题

VMware e1000e 缺少 PCIe 插槽导致启动失败，生成配置改为有 PCI bridge 的 e1000；Windows 生成 env 文件强制 LF；MySQL 8.4 RSA 认证补齐 PyMySQL rsa 依赖；Kafka offset 元数据显式提供 leader_epoch；旧 Maven 3.6.1 改用校验下载的 3.9.9；Jackson 版本用 BOM 对齐；Spark 镜像内 Python 3.8 使用 timezone.utc。

虚拟机初始 UTC 快 8 小时，而 NTP 包计数为 0。新增只允许空闲实验机校时的工具和 2 秒偏差门禁；保留早期日志的原始时间，不将它们用于新鲜度测量。固定时钟的合成正确性样例不依赖这些日志墙钟。

Doris 官方入口需要可写运行配置，不能直接以只读配置覆盖；保留镜像默认配置并注入小内存参数，单独覆盖 JDK 17 堆大小。已有元数据启动时还需明确选择 NAT 地址，否则可能选到 Docker bridge 地址而无法恢复选主。上述修复已通过持久卷重建容器后的再次验收。Doris 镜像及工作数据促使分析节点系统盘由 18 增为 24 GiB 薄置备，项目物理预算不变。

MyWebsite 原有 npm 依赖审计报告 10 个问题（含 1 critical）；本次没有改动其依赖版本或将无关升级混入统计适配。生产候选应结合原项目维护处理。
