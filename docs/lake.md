# HDFS 上的 Iceberg DWD 与恢复

2026-09-12 使用固定 Iceberg 1.10.0、Spark 3.5.7 / YARN，实际迁移 `scale-100k-01` 的 **90,000 行合成 DWD**。Hadoop catalog 位于独立 `/snow/lake/lake-01`，表为 `snow.synthetic.dwd`。源 Parquet、既有报表和轻量服务不变。

## 实际操作与校验

1. 创建按业务月份分区的 Iceberg v2 表，读回后与原 DWD 做双向 `exceptAll`，90,000 行逐项相同。
2. 新增 `classification_revision`，按事件 ID MERGE 一条修订记录；总行数保持 90,000，只有一条修订为 v2。
3. 分区从月演进到日，追加两条明确的实验事件，再删除其中一条新增事件；最终 90,001 行。
4. 执行文件重写，与重写前的固定快照做双向逐行比较，结果不变。最初的 90,000 行快照仍与原始 Parquet 相同。
5. 正常停止并重启项目 NameNode，停止期间 HTTP 不可用，恢复后 36 个非空 HDFS 块全部至少双副本、HEALTHY。
6. 用第二个实际 YARN 应用重新打开 catalog，读回相同最终快照、90,001 行和 v2 修订；原始快照仍可读且逐行一致。

初始快照 `1622158363550742658`，最终快照 `2077898771536498172`，两个应用分别为 `application_1789144357700_0002` 和 `_0003`。详细快照、操作 summary、执行统计与校验值见[运行证据](evidence/lake.json)。所有旧快照和源数据都保留。

文件重写将 2 个数据文件替换为 8 个，改写约 4.98 MB。因为输出采用新的日分区规范，7 个一月日期加一个二月日期形成 8 个分区，不能将这次成功描述为“文件数减少”。本实验用于验证演进与一致性，低流量表实际维护时应避免过细分区。Iceberg 的[DDL 演进](https://iceberg.apache.org/docs/1.10.0/spark-ddl/)和[重写操作](https://iceberg.apache.org/docs/1.10.0/spark-procedures/)提供具体语法约定。

## 运行和中断边界

三 VM 仍为 2/2/1 GiB，Spark 使用与扩样相同的受限配置。Hive metastore、Kafka、Doris、Airflow 不需同时运行；catalog 通过 HDFS 保存。额外 Iceberg runtime JAR 的 47,130,096 bytes 和 SHA256 `70f9471870b2d456167d703c829f098a446b86bb1379eee338c0efa03a341026` 在提交前验证，随 YARN 分发。

```powershell
# 新 attempt，已完成 HDFS/YARN 启动和 10 万条源 DWD 验收
uv run python tools/run_scale_guarded.py --events 100000 --workload lake --attempt lake-02
# 通过受信 SSH 在 control 执行 tools/restart_lake_namenode.sh lake-02 后
uv run python tools/run_scale_guarded.py --events 100000 --workload lake-verify --attempt lake-02
```

工具会拒绝覆盖已有创建或恢复收据。NameNode RPC 绑定实际 NAT 地址，重启后按生成配置探测该地址，再等待安全模式退出。实验 driver 以 root 创建的收据只在明确实验路径内使用 sudo 导出，不修改共享权限。初版观察工具的权限及回环地址错误没有导致源数据删除；正式重启和第二会话验收通过。

还实际执行了 `guard-stop-01`：在新合成扩样尝试运行 25 秒后显式注入停止条件，监测器停止 `snow-spark-yarn`，随后逐节点停止指定服务及 VM。driver 返回 143，完整流程 46.890 秒，所有停止步骤成功；这次预期中断不作为成功计算结果。已观察 VM 全关，旧成果保留。该证据验证自动停止分支，不声称真的耗尽了 RAM 或磁盘。

单 NameNode 重启期间服务暂不可用。本次没有配置自动 HA/fencing，没有多写入者并发、快照到期或长期文件维护结论，不将同宿主虚拟节点描述为物理容灾。
