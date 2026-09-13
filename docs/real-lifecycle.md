# 真实链路的生命周期入口与按需执行

本文描述当前代码入口。单元测试中的内存后端只证明状态机行为；真实 HDFS、Kafka、Doris、Checkpoint 和卷回收的通过情况，应以对应运行回执为准。

## 三类控制文件

1. `src/snow_statistics/lifecycle.py` 管理本地真实文件。先登记，再写入；读取前清理。`registry.json` 在数据文件之外，记录原始到期时间。
2. `src/snow_statistics/real_remote_lifecycle.py` 管理精确 HDFS 子目录以及其他后端的登记清单。旧数据复制、备份和恢复不能延长寿命。任何清理失败均留下 `cleanup.json`，禁止生成新的可用读取许可。
3. `tools/airflow_real_gateway.py` 接受经过校验的真实作业请求。许可绑定 ODS 快照、覆盖文件校验和、辅助状态文件校验和、全部输出目录，最长有效 15 分钟。它与 synthetic SSH 网关分开部署。

未经初始化的空后端记录为 `scope=not_initialized, certified=false`，不代表已验证该引擎。已经登记过资源的后端必须进行真实检查；无法连接时不会自动当作空资源。

## Linux 控制机操作示例

以下命令在 **snow-control 的 `/home/snow/Snow_Statistics`** 执行。`<...>` 为必须替换的参数；不要将凭据直接写在命令行。所有配置、回执和包含行数据的文件留在 Git 忽略的 `runtime` 下。使用已配置的 Python 3.12 / `uv` 环境。

```bash
cd /home/snow/Snow_Statistics

# 只对全新的元数据目录初始化；实例与代际来自私有 status API。
uv run python tools/real_lifecycle.py --registry runtime/real/lifecycle/<lane> init \
  --warehouse-root hdfs://<control-ip>:9000/snow/warehouse/real/<lane> \
  --auxiliary-root hdfs://<control-ip>:9000/snow/auxiliary/real/<lane> \
  --ods-root hdfs://<control-ip>:9000/snow/ods/real/kafka/<lane> \
  --instance-id <collector-instance-uuid> --generation <collector-generation-uuid>

# 在 Spark 写出任何内容之前登记整个作业可能写出的子目录。
uv run python tools/real_lifecycle.py --registry runtime/real/lifecycle/<lane> reserve-job \
  --job-file runtime/real/jobs/<run-id>.json \
  --original-raw-at <原始批次最早accepted_at> \
  --original-auxiliary-at <旧辅助状态与新输入的最早accepted_at>

# Kafka/Doris/Checkpoint 在首次写入前单独登记，下面仅示范 Kafka。
uv run python tools/real_lifecycle.py --registry runtime/real/lifecycle/<lane> register-backend \
  --backend kafka --resource snow.real.<event-lane>.events.v1 \
  --kind raw --original-at <此资源包含的最早原始accepted_at>

# 私有 datanodes.json 是 hostname -> 本实验室IP 的映射；backend配置需0600。
uv run python tools/real_lifecycle.py --registry runtime/real/lifecycle/<lane> cleanup \
  --ods-directory runtime/real/ods/<lane> --datanodes-file runtime/real/config/datanodes.json \
  --backend-config runtime/real/config/backends.json

uv run python tools/real_lifecycle.py --registry runtime/real/lifecycle/<lane> permit \
  --job-file runtime/real/jobs/<run-id>.json --coverage-file runtime/real/coverage/<coverage-id>.json \
  --permit-file runtime/real/permits/<run-id>.json \
  --ods-directory runtime/real/ods/<lane> --datanodes-file runtime/real/config/datanodes.json \
  --backend-config runtime/real/config/backends.json
```

如果尚无已初始化的 Kafka/Doris/Checkpoint 资源，可不提供 `--backend-config`，回执会明确哪些后端不在本次认证范围。初始化后不能通过省略配置绕过检查。启用真实 Hive 注册需要相应实际后端适配器；当前网关默认不注册 Hive，不声明未验证的元存储覆盖。

已有辅助状态时，`permit` 还需 `--auxiliary-file runtime/real/auxiliary/<previous-run>.json`。该文件是元数据，包含源代际、HDFS 路径、原始时间和行数。Parquet 文件不能借复制重新计算到期。

## 作业请求与覆盖文件

`runtime/real/jobs/<run-id>.json` 的示意结构如下。`kind` 由真实 DAG 对两次计算分别设置为 `daily` / `behavior`。网关不接受 `operations`。

```json
{
  "run_id": "real-day-001",
  "kind": "daily",
  "source": "real",
  "input": "hdfs://CONTROL:9000/snow/ods/real/kafka/LANE/snapshots/SNAPSHOT_HASH/_snapshot.json",
  "warehouse_root": "hdfs://CONTROL:9000/snow/warehouse/real/LANE",
  "auxiliary_root": "hdfs://CONTROL:9000/snow/auxiliary/real/LANE",
  "date_from": "2026-09-11",
  "date_to": "2026-09-12",
  "cutoff": "2026-09-12T16:00:00Z",
  "coverage_file": "coverage-001.json",
  "auxiliary_file": null,
  "permit_file": "real-day-001.json",
  "register_hive": false
}
```

上例日期、地址和 hash 均为说明占位，不能直接用于执行。coverage 文件的字段为 `schema_version=1`、`source=real`、`instance_id`、`generation`、`continuous_from`、`through` 和 `gaps`。`through` 必须等于作业 cutoff。缺口元素仅包含 `from`、`to` 和短原因代码 `reason`。不能根据事件发生日期倒填接收覆盖起点，不能将缺口当作零事件。

网关在宿主机读取 `runtime/real/coverage`、`runtime/real/auxiliary`、`runtime/real/permits` 中的精确 JSON 文件名，不接收任意绝对路径。它用单独的强制 SSH 命令和密钥，不能复用 synthetic 网关的授权范围。

## 到期后的辅助状态重写

`warehouse/spark/real_auxiliary.py` 提供 `prune_auxiliary_hdfs(spark, manifest, output, now)`。这是**清理权限下**的读取：检查固定字段和来源，按原始 `accepted_at + 30 天` 过滤，写入事先登记的新目录，双向全行比对，然后只删除旧的精确 real 目录并确认消失。

调用前由协调器检查存活行最早原始时间并登记新目录，且记录替换计划；之后更新辅助元数据指针。如果任一步失败，不启动模型读取者。不能把过期元数据传给正常 `real_behavior.py` 来“顺便清理”。关闭电脑不会执行这些步骤；重新启动后必须先清理。

Kafka 删除记录和 Doris DELETE 的逻辑不可见性，不等同于日志段或存储文件已经物理擦除。涉及原始数据的物理到期边界由独立 real epoch 的停止、精确卷回收和不存在性回读补充，回执中需分开说明。

## 真实行为模型

`warehouse/spark/real_behavior.py` 输入真实不可变 ODS 快照，不订阅模拟 CDC。辅助状态仅保留 `source/seq/event_id/app/event_type/occurred_at/accepted_at/anonymous_id/jump_id/channel/request_id/success`；不保留页面路径、角色、耗时、聊天或完整事件正文。

留存 cohort 的定义是 **保留观察窗口中首次观察到的匿名标识**，不是自然人的首次访问，也不是全历史新用户。D1/D7 状态分别为：

- `complete_accepted_prefix`：目标日期已闭合，观察起点和已确认的接收前缀足够，没有影响该指标的接收缺口；此时才给分母和返回人数。
- `pending`：目标日期尚未闭合，返回人数为 null，分母为 0。
- `incomplete`：输入覆盖或辅助状态不足，返回人数为 null，分母为 0。

较迟事件可修正内部模型。这里的“完整”限定为声明截止时间内已确认的接收前缀，不代表每一次用户操作都被采集。

## 真实聚合 Iceberg 与血缘

仅 `validate_real_pair` 通过的 daily / session_daily / retention / funnel 聚合可进入 Iceberg。准备步骤先登记本地副本和独立 HDFS Iceberg 子目录；原始统计日期决定 90 天到期，迁移不重置。

```bash
uv run python tools/run_real_iceberg.py \
  --daily runtime/real/publication/<run-id>.daily.json \
  --behavior runtime/real/publication/<run-id>.behavior.json \
  --registry runtime/real/lifecycle/<lane> --runtime-root runtime/real/lake/<lake-run> \
  --warehouse hdfs://<control-ip>:9000/snow/warehouse/real/<lane>/iceberg/<lake-run> \
  --run-id <lake-run> --prepare-only

# 此时再次运行上文 cleanup，保证新增登记后的清理回执是新的。
# 再运行相同 Iceberg 命令，去掉 --prepare-only，并可添加：
# --lineage-db runtime/real/lineage/events.sqlite

uv run python tools/real_lineage.py --journal runtime/real/lineage/events.sqlite
# 仅在已建立受控回环隧道时投递：
uv run python tools/real_lineage.py --journal runtime/real/lineage/events.sqlite --url http://127.0.0.1:5000
```

Iceberg 使用既有 1.10.0 JAR 校验锁，在真实 Spark/YARN 中进行当前值与历史快照读回。空表若没有快照会如实记录 null。新增 `model_revision` 列属于实际字段演进，不为数据填充假值。工具不会声称执行了 Hive 注册或列级血缘。

真实血缘使用独立 `snow-statistics.real` 命名空间及单独 journal。只有实际运行的 wrapper 才记录 START，成功校验产物后记录 COMPLETE，失败记录 FAIL。过去已完成且未采集的作业不会补造启动或成功事件；未实际验证的引擎、表和列关系不进入回执。
