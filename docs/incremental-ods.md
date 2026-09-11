# Kafka / CDC 增量 ODS

本阶段把 Kafka 行为事件与 Debezium 变更纳入同一套可恢复的 HDFS 输入。入口仅允许本项目四个 `snow.synthetic.*` Topic；不接触生产采集配置。轻量服务、公开汇总接口及两个业务产品均没有新增依赖。

资源更新：治理阶段曾通过 `ods-compact`（3.5/3/1 GiB）；下文原始 `ods` 数字保留为当时的实验记录。实时镜像与数据增长后曾超过初始 60 GiB 预算；当前按用户授权调整为 64 GiB，重启前仍须检查动态资源及 1 GiB 作业余量，见[最新运行手册](runbook.md)。

后续 `scale`（2/2/1 GiB）已完成合成文件输入的 10 万及百万条日指标 YARN 验证，见[扩样手册](scale.md)。那次关闭 Kafka、Hive、Airflow，仅运行 HDFS/YARN/Spark，未把增量 CDC 及全部模型链路扩到相同规模。

## 提交边界

```text
Kafka 固定分区起止位点
  → 本地 fsync 原始消息、规范化文件、SHA256 manifest、pending
  → HDFS 临时目录写入、读回校验、目录 rename
  → 确认非空文件两副本、发布不可变输入清单
  → 保存已落地收据
  → Kafka 提交各分区下一条 offset
  → 保存本地 checkpoint、清除 pending
```

`capture`、`land`、`ack` 可分阶段执行。Kafka 不需要与 Hadoop/YARN 同时常驻；`land` 完成并保存收据后，允许停止 HDFS 再启动 Kafka 执行 `ack`。这依赖已经确认的持久卷及本地收据，不声称 ACK 时重新在线检查 HDFS。暂停服务不会删除数据。

每个 lane 的 consumer group 为 `snow-ods-<lane>`，只有控制机上同一个工作目录的单写者可使用它。所有阶段共用操作系统文件锁；进程死亡会释放锁。不提供跨主机租约，也不允许多个工作目录/机器共用同一 group 或 HDFS lane。

默认一次最多 10,000 条、原始编码消息 32 MiB；上限 100,000 条/64 MiB。分区独立冻结范围，保存 cluster ID、Topic ID 和 group。主题删除重建、分区拓扑变化、位点倒退、保留期导致的缺口、外部修改 group 位点均拒绝继续，不能静默 reset earliest。首次启动要求历史从 offset 0 可读；已有截断历史须单独设计迁移/补数，当前命令不自动跳过。

目前以不压缩、不使用事务生产消息的实验 Topic 为输入，严格检查每个数据 offset 连续；存在 Kafka 事务控制记录或 compacted offset 缺口时会拒绝，未声明支持这些输入模式。

## 文件与数据质量

- `runtime/ods/<lane>/batches/<sha>/` 保存原始 bytes 的 Base64、key、headers、Kafka 时间及 topic/partition/offset，tombstone 保持 `value_b64=null`。
- `events.jsonl` 是经过事件契约校验的采集信封，保留 Kafka 位点；`changes.jsonl` 包含 CDC 前后状态的规范化业务变更、事务元数据和源日志位置。完整 Debezium 信封仍可从 `raw.jsonl` 还原。
- `quarantine.jsonl` 只列位点和通用原因。原始数 = 行为数 + CDC 变更数 + tombstone 数 + 隔离数。原始批次落地后允许提交消费位点；含隔离记录的快照不能进入模型发布，需修复/重建处理版本，不会当作有效数据跳过。
- HDFS 目录为 `/snow/ods/synthetic/kafka/<lane>/batches/<sha>/`；应用级输入清单为 `snapshots/<sha>/_snapshot.json`。这是内容寻址的应用清单，不是 HDFS 原生快照功能。
- 清单累计列出已接收的批次和分区结束位点。Spark 仅读清单指定的文件，排除 `_staging`、清单文件和随后到达的数据。日指标包记录清单 ID 与位点；事件 `accepted_at` cutoff 和香港业务日期继续分别控制数据截止时间与报表范围。
- CDC 删除与 tombstone 分别保留；删除生成一条业务删除变更，tombstone 不重复生成业务变更。事件 ID/请求 ID 的业务去重仍由 DWD 负责，Kafka 重放不会因新增 offset 而被当作新用户行为。

归档文件不可覆盖为不同内容。HDFS 已完成 rename 但客户端未收到确认时，重试读回已有目录；部分上传遗留在 `_staging`，不对读者可见。`pending.json` 存在时必须恢复原批次。Kafka ACK 后本地 checkpoint 前故障、部分分区 ACK 成功、本地 checkpoint 后 pending 未清除，均有恢复测试。

## 分阶段运行

Windows 用 `tools/vmware_lab.py status` 检查容量。首轮使用 `batch` 的 4/4/2 GiB，接近磁盘上限后增加 `ods` 的 3.5/4/1 GiB 配置。只能在虚拟机完全停止后用 `configure --node <name> --profile ods` 修改内存；1 GiB 分析节点只运行 DataNode，须执行 `bash tools/start_batch_node.sh --ods-small`，对应 512 MiB 容器/256 MiB Java 堆。Doris 继续使用独立 6 GiB 分析节点配置。

通过 `tools/bundle.py`、`tools/lab_remote.py` 上传已纳入 Git 的源文件。运行 Spark 的远程脚本必须加 `--reserve-mib 1024`，在作业启动前为临时 JAR/文件预留 1 GiB 项目容量，同时检查宿主 RAM；失败时不会执行 SSH 脚本。运行期间仍要观察容量，预留量不是硬磁盘配额。

```powershell
uv run python tools/lab_remote.py --node snow-control --reserve-mib 1024 --script runtime/my-ods-job.sh
```

下列命令在控制机 `/home/snow/Snow_Statistics` 执行；工作环境是 `.venv`（Python 3.12 + lab 依赖）。本机三台 VM 启动/停止方法见[离线手册](offline-pipeline.md)。

先只启动控制机 Kafka。不能把 Kafka/MySQL/Connect、Hadoop、Airflow 和 Spark 全部同时塞进 4 GiB 控制机。

```bash
set -a
. lab/.env
set +a
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.control.yaml up -d kafka
# 等待 broker API 可用后执行；不要把容器 Started 当作服务已就绪。
.venv/bin/python tools/land_kafka.py capture --lane main-v1 --directory runtime/ods/main-v1
```

捕获期间不提交消费位点。停止 Kafka，按原手册启动三个 batch 节点并等待 HDFS 退出安全模式、两个 DataNode 可用：

```bash
timeout 120 sudo docker exec snow-lab-control-namenode-1 hdfs dfsadmin -safemode wait
.venv/bin/python tools/land_kafka.py land --lane main-v1 --directory runtime/ods/main-v1
input=$(.venv/bin/python -c 'import json; print(json.load(open("runtime/ods/main-v1/landed.json"))["input"])')

# run ID 每次新计算都不同；本例日期对应固定合成样例。
bash tools/spark_yarn.sh /opt/snow/warehouse/spark/batch.py \
  --input "$input" --output /snow/warehouse --run-id example-ods-daily-01 \
  --date-from 2026-01-01 --date-to 2026-01-04 --cutoff 2026-01-05T00:00:00Z \
  --source synthetic --register-hive \
  --package-file /opt/snow/runtime/publication/example-ods-daily-01.json

# 再次检查宿主容量，才启动下一作业。
bash tools/spark_yarn.sh /opt/snow/warehouse/spark/ops.py \
  --input "$input" --output /snow/warehouse/operations/example-ods-ops-01 --as-of 2026-01-04
```

运营作业读回实际 Parquet，输出 `accepted` manifest；先用 `tools/prepare_spark_fixture.py --as-of 2026-01-04` 生成相同截止日期的基准，再加 `--expected /opt/snow/runtime/spark-fixture/expected.json` 校验 SCD2 区间/属性、各轮工单时间及每日状态。业务发生日在 `--as-of` 之后的变更不参与本次模型。

关闭控制机 Hadoop 服务后重新启动 Kafka、等待就绪，提交已确认的分区位点：

```bash
.venv/bin/python tools/land_kafka.py ack --lane main-v1 --directory runtime/ods/main-v1
# 新数据才产生下一批；没有新数据输出 batch_id=null。
.venv/bin/python tools/land_kafka.py capture --lane main-v1 --directory runtime/ods/main-v1
```

日指标发布继续使用 `tools/publish_daily.py` 和原 Doris 发布协议。当前容量紧张时，可先下载已接受的日指标包并关闭控制/计算 VM，只启动 6 GiB 分析 VM，由宿主 Python 客户端设置 `SNOW_DORIS_HOST` 后发布；此时必须保证原控制机发布者已停止，仍是单写者。

Airflow 网关接受这条固定的 HDFS 清单路径：将其设为 `SNOW_ODS_PATH` 再执行日任务，无需改变公开页面。当前没有自动化 VMware 切换，也没有把 CDC 运营作业加入 Airflow DAG；上述阶段由操作者执行。

## 验收与资源

`tools/smoke_ods.py land` 注入 HDFS 批次提交后故障，再恢复并重复落地；`ack` 注入 Kafka ACK 后故障，再恢复并验证无新数据时不产生新批次。工具使用独立 `acceptance-v1` lane；这是一次性验收序列，已完成后不要覆盖其历史运行记录。

实际收据见 [incremental-ods.json](evidence/incremental-ods.json)，已验证结果与未完成项见[实施状态](status.md)。小样本验收不能作为大规模吞吐、7 天积压恢复或实时新鲜度成绩。

本轮曾观察项目文件约 59.92 GiB，后续一次重算期间 60 GiB 门禁实际失败，停止了对应 Spark 容器和数据 VM，未把中断作业记作通过。此前门禁仅检查当前文件/VM 启动，未预留 YARN 临时 JAR；现在新增作业前 1 GiB 预留检查，并降低只承载基础服务的 VM 内存以减少 `.vmem` 占用。未修改 60 GiB 门禁值。

停机压缩 compute 的稀疏 VMDK 仅回收约 8 MiB；没有用填零占满磁盘。确认三台 system.vmdk 都是完整独立磁盘（`parentCID=ffffffff`，无父盘引用）后，只删除可重新下载的 Ubuntu 源镜像缓存约 566 MiB，保留 SHA256 锁、VM 系统盘、数据库、HDFS、归档及检查点。不能依靠这次小额回收继续扩样，后续仍需评估临时 JAR 分发与存储布局。

原理接口参考：[Hadoop 3.4.1 WebHDFS](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)、[Kafka Consumer 位点管理](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)。本实验采用单写者、显式提交和业务去重，不声明跨 Kafka/HDFS 的分布式事务。
