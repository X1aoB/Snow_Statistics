# 实施状态

本记录只列实际证据，不把技术栈配置文件计作集群验收。2026-09-11 首轮实现。

## 已验证

- 独立公开仓库 `X1aoB/Snow_Statistics` 已创建。
- Python 20 项测试通过：持久化、重复/冲突、事务回滚、UV 与跨天、保留期/过期位点、满盘拒收、公开字段白名单、模式切换、合成数据隔离、归因及轻量/离线整数对账。
- 浏览器 5 项测试通过：默认关闭无标识/网络副作用、隐私信号、有限队列和重试、统计故障不伪造零值、归档/文本渲染。
- MyWebsite 独立工作树构建通过，59 项原有测试通过；Project_Snow 前端类型检查和 6 项原有测试通过，两个产品采集仍默认关闭。
- Flink Java 作业用 Maven 3.9.9 编译成功，2 项归一化/来源隔离测试通过。Java 11 字节码、Flink 1.20.3、Doris connector 25.1.0、Jackson BOM 2.17.3。
- 三台 VMware VMX/薄置备磁盘/seed 已生成；Ubuntu release-20260826 SHA256 已验证。控制节点 6 GiB 实际启动、cloud-init 和受验证 SSH 通过。
- 控制节点 Kafka 3.9.1、MySQL 8.4.5、Debezium 3.0.8.Final 启动。真实 Kafka 收发 28 条合成事件；连接器及任务 RUNNING，两个模拟工单写入成功。
- CDC 归档覆盖 contents/campaigns/tickets，首轮取得 19 条唯一变更，包含 1 条删除和 15 条有事务元数据的变更。
- 真 Kafka 中断恢复：接收 28 条，5 条确认后模拟失败；恢复重放后共 33 次 Kafka 确认，最终源位点 28，公开合成行数 0。
- Spark 3.5.7/Java 11/Python 3.8 容器实际运行：56 条含重放输入 → 28 条有效 + 28 条重复，隔离 0；事件分层和运营 SCD2/工单作业完成。模式为单虚拟机 local[2]，不是 YARN 集群成绩。
- 实际 Spark Parquet 与正确性基准对账通过：6 行日指标、4 个工单轮次、4 个内容有效版本。
- Iceberg 1.10.0 在实际 Spark 容器中通过 MERGE、历史快照读取、字段新增、分区从月到日演进、数据文件重写；独立本地文件系统 catalog，未冒充 HDFS 湖仓集群。
- Python 正确性基准：10 万条约 6.618 秒；100 万条约 89.844 秒；均完整对账且隔离 0。固定 seed 42，不能作为分布式吞吐或真实用户规模。
- 14 个第三方镜像已从实际 registry 解析并固定 digest；Ubuntu、Python lock 与 Java 直接版本可检查。

## 尚未完成的验收

- 三 VM 同时运行时的内存预算；当前第二台 6 GiB 启动曾被 4 GiB 宿主余量门禁拒绝。需分阶段验证或释放宿主内存后重试。
- HDFS 双副本/YARN/Hive 3.1.3 组合、Doris 在受限内存下的运行、Flink checkpoint 恢复与端到端 P95≤60 秒。
- Airflow、Marquez 配置/作业已加入，运行与故障验收仍需记录；Iceberg 集群并发与长期文件维护待验证；列级血缘未声明完成。
- 三 Kafka/ZooKeeper 专题配置已加入；HDFS 自动 HA 因 fencing 尚未配置而保持关闭。
- 真实日志跟随、线上 quota mount、代理/CSP 和业务生产发布；当前没有启用生产采集，也未修改生产服务或业务数据库。
- 7 天离线积压与 2 GiB 在线容量的真实容量测试、实时/离线/Doris 一致性、物理资源测量和退出部署演练。

## 本轮修复的实际兼容问题

VMware e1000e 缺少 PCIe 插槽导致启动失败，生成配置改为有 PCI bridge 的 e1000；Windows 生成 env 文件强制 LF；MySQL 8.4 RSA 认证补齐 PyMySQL rsa 依赖；Kafka offset 元数据显式提供 leader_epoch；旧 Maven 3.6.1 改用校验下载的 3.9.9；Jackson 版本用 BOM 对齐；Spark 镜像内 Python 3.8 使用 timezone.utc。

MyWebsite 原有 npm 依赖审计报告 10 个问题（含 1 critical）；本次没有改动其依赖版本或将无关升级混入统计适配。生产候选应结合原项目维护处理。
