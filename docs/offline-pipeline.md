# 离线数仓闭环

当前主线：合成 ODS → HDFS 双副本 → Spark on YARN → Hive 元数据/ADS Parquet → 校验包 → Doris 已发布视图 → Streamlit。Kafka/CDC 归档接入遵循同一 ODS 契约；本次端到端调度使用固定合成文件，不能据此宣称 Kafka 到报表的连续生产链路已验收。

## 资源阶段

| 阶段 | control | compute | analysis | 常驻组件 |
|---|---:|---:|---:|---|
| batch | 4 GiB | 4 GiB | 2 GiB | NN/RM/Hive、Airflow scheduler、DN/NM、第二 DN |
| olap | 4 GiB | 关闭 | 6 GiB | Airflow scheduler、Doris FE/BE；计算完成的发布包保存在 control |

最初 4/6/2 GiB 通过独立 Spark 对账；加入 Airflow 后再验证 4/4/2 GiB，单 executor 768 MiB + 256 MiB overhead，AM 512 MiB + 256 MiB overhead。NodeManager 可分配 2048 MiB，容器 2816 MiB；Doris 小配置 FE 2 GiB/BE 3 GiB，内部堆/内存上限分别 1 GiB/2 GiB。这些是小样本功能预算，不是负载测试结论。

`standard` 保留原始 6/6/10 GiB 上限。VM 只能完全关机后修改配置；工具检查实际 VMX 内存，不再按默认值误算。启动保留 4 GiB 宿主可用内存、35 GiB 空闲磁盘，项目文件长度加新 `.vmem` 不超过 60 GiB。镜像和 YARN 缓存占用较大，扩样前必须再次检查；不要同时启动所有 profile。

## 首次配置与独立 Spark 验收

在 Windows 仓库目录按节点配置，再启动：

```powershell
uv run python tools/vmware_lab.py configure --node snow-control --profile batch
uv run python tools/vmware_lab.py configure --node snow-compute --profile batch
uv run python tools/vmware_lab.py configure --node snow-analysis --profile batch
uv run python tools/vmware_lab.py start --node snow-control
uv run python tools/vmware_lab.py start --node snow-compute
uv run python tools/vmware_lab.py start --node snow-analysis
```

将 Git 已跟踪源码通过 `tools/bundle.py` 和 `tools/lab_remote.py --upload` 分发至三台 `/home/snow/Snow_Statistics`。新文件先加入 Git 索引；打包不包含 runtime、凭据或数据库。`lab/.env` 中的三节点地址应与 `vmware_lab.py ip` 一致；地址变更时只更新地址字段，保留原实验凭据。

在各 VM 仓库执行 `bash tools/start_batch_node.sh`。配置首次变化后需在无作业时重建对应服务，单纯更新挂载 XML 不代表守护进程已重新加载。禁止对已有 NameNode 卷重新格式化。

control 上执行：

```sh
bash tools/prepare_hive_client.sh
bash tools/smoke_yarn.sh yarn-demo-001
```

每次计算使用新 run ID，旧目录禁止覆盖。相同合成输入重复出现只增加 duplicates，不增加有效日指标。`--expected` 用实际 Parquet 与 Python 基准比对；验收脚本会等待输入的两个副本，并保存输出 `fsck` 报告和可发布 JSON。首次启动应等待 SSH、NameNode、两个 DN、RM/NM 和 Hive 就绪后运行。

依赖：Hadoop 镜像提供 3.4.1 守护进程；NM 镜像将该发行目录复制到固定 Spark 镜像，使用 Java 11/Python 3.8。Hive 客户端的 334 个 JAR 从固定 Hive 镜像提取，逐个 SHA256 锁定；不在作业启动时解析 Maven。Spark 与 Hive 3.1.3 的客户端配置依据 [Spark 3.5.7 官方文档](https://spark.apache.org/docs/3.5.7/sql-data-sources-hive-tables.html)，实际组合另有 YARN 收据验证。

## Airflow 调度

在 control 执行 `bash tools/start_airflow.sh`。默认只运行调度器，SQLite 元数据保存在独立 Compose 卷，SequentialExecutor 每次一个任务；Web UI 不常驻。受限 SSH 公钥只能启动本项目的合成计算命令，禁用转发与任意 shell，私钥只读挂载给 Airflow。它不挂载 Docker socket，不复制 Spark/JDK，也不访问业务仓库。

检查导入并手动触发：

```sh
sudo docker exec snow-lab-orchestration-airflow-1 airflow dags list-import-errors --output json
sudo docker exec snow-lab-orchestration-airflow-1 airflow dags unpause snow_daily
sudo docker exec snow-lab-orchestration-airflow-1 airflow dags trigger snow_daily \
  --run-id synthetic-demo-001 \
  --conf '{"date_from":"2026-01-01","date_to":"2026-01-04","cutoff":"2026-01-05T00:00:00Z"}'
sudo docker exec snow-lab-orchestration-airflow-1 python /opt/snow/tools/airflow_receipt.py --run-id synthetic-demo-001
```

`window` 校验香港业务日期及 UTC cutoff；`compute_and_validate` 通过 YARN 产生实际结果和可发布包；`publish_doris` 再校验并发布。重试有独立尝试编号；已成功导出的同一计算请求可复用。所有发布者必须共享 control 的 `runtime/publication` 锁目录，不能跨主机并发启动第二个发布者。

计算成功后，在 Windows 正常关闭 compute 和 analysis，把 analysis 改为 `olap` 并启动，再在 analysis 执行 `bash tools/start_olap_node.sh`。不要在计算任务仍运行时切换资源阶段。Doris 未启动时发布会失败并按 5 分钟间隔重试两次，旧报表保持有效；超过重试次数时恢复 Doris 后只重试发布任务，无需重复已完成的计算。

首次初始化 Doris 表，在 analysis 仓库执行 `PYTHONPATH=src .venv/bin/python tools/smoke_publication.py /home/snow/yarn-package.json`，其中输入为 control 导出的固定合成发布包。该验收还会在合成 2026-01-10 日期生成空修正指针。普通调度使用已经初始化的表，不自动执行 DDL。

验收配置 `SNOW_AIRFLOW_SCHEDULE=manual` 没有定时运行。基础设施长期可用后，改为 `daily` 并重建调度器，即启用香港时间每天 03:00 的最近七天校正；DAG 保持默认暂停，启用由显式 unpause 完成。历史补数使用上面的日期参数，最长 366 个日期；cutoff 也可显式固定，方便重放对账。

## 发布与报表

`daily_snapshots` 保存内容寻址的不可变结果；写入后全量读回日指标校验；一次 INSERT 更新整个日期范围的 `offline_releases` 指针。`daily_published` 提供指标视图；看板通过 `report_published` 在一条查询内同时读取指标和对应 cutoff，避免发布期间元数据与指标错配。相同 cutoff 的不同结果拒绝发布，较旧 cutoff 不得覆盖新版；要发布修正必须使用新的、经过审核的数据截止时间。空日期也有指针，因此“当天结果被修正为空”能够移除原来的可见行。

在 Windows：

```powershell
$env:SNOW_DORIS_HOST='192.168.216.133' # 使用实际 NAT 地址
uv run --extra lab streamlit run dashboard/app.py --server.address 127.0.0.1
```

选择“Doris 已发布数仓结果”可查看真实查询结果和逐日 cutoff；连接失败明确显示不可用，不伪造零值。选择“Python 正确性基准”可离线查看原有会话/漏斗/SCD2/工单演示；这些高级指标尚未全部迁入分布式主线。看板仅供本地实验，不加入线上公开代理。

## 故障及证据边界

- YARN 固定样例：56 条输入、28 有效、28 重复、0 隔离，6 行日指标与基准一致；这是功能样例，没有分布式吞吐承诺。
- 故意提供错误基准时，失败目录没有 accepted 标记，旧 accepted 结果保持；改用新 run ID 重算后恢复。
- 输出块有两个有效副本，停掉一个 DN 后仍可读回同一输入 SHA256；两台 VM 位于同一物理机，不代表物理容灾。
- Doris 实际验证重放、提交前失败、空日期修正、旧 cutoff 与同 cutoff 冲突；UI 已查询实际 Doris 日指标。
- 资源切换由操作者或本地主机脚本执行，Airflow 本身不管理 VMware；当前验证的是分阶段运行的离线链路。
- 停用只停止实验服务和 VM。数据库、HDFS 结果、检查点、SSH 授权等清理按资源清单另行审核；不执行全局 prune。
