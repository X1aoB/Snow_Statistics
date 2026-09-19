# 独立合成输入的 Hive / Iceberg 验收

这是开发验收工具，不是正式数据接入入口。正式数据继续使用 `real_lab` 和已冻结的 writer。`lake_fixture` 只接受新建的 `fixture-lake-*` 名称，配置必须为 `source=real`、`input_origin=synthetic fixtures`、analysis 权威节点，无生产隧道、无 Kafka/Doris 凭据。这里的 `real` 表示被测试的代码分支；输入来源明确是合成数据。

## 证据边界

工具在 analysis 启动一次独立 loopback 采集服务，使用固定种子生成 28 条事件，经真实 HTTP 接收、聚合和私有读取验证。匿名标识和事件内容来自本次固定生成器，不读取业务事件或旧 fixture 的数据。事件发生时间设为创建日前的已闭日期；接收时间使用采集服务的实际时钟，创建后不可重置。留存观察覆盖从实际首次接收开始，不把合成事件的历史发生日期当作已经观察过的历史。

`FixtureSource` 是显式的确定性测试适配器，提供 capture/land/ack 所需的接口。它的集群和 Topic 代际使用 `fixture-adapter-*` 标记；**没有运行 Kafka，不证明 Kafka 吞吐、恢复或消费行为**。Kafka、Doris、Checkpoint 后端始终为 `not_initialized`，清理回执对应 `certified=false`。

真实引擎范围是 HDFS 两副本、Spark/YARN 日指标和行为模型、Hive Catalog、Iceberg 写入及独立读取验证。必须取得每个实际阶段的回执后才能把该项标为通过。单元测试里的内存 HDFS 与模拟引擎结果不构成引擎证据。

## 精确资源范围

以下 `<lane>` 只能为 `fixture-lake-` 加 2–10 个小写字母或数字；初次验收使用 `fixture-lake-01`。模型 run 固定为 `<lane>-r1`。

- analysis 独有：`runtime/real/lake-fixture/<lane>/` 的创建意图、不可变生成参数、临时采集库及导出；`runtime/real/ods/<lane>/` 的 capture/land/ack；`runtime/real/lifecycle/<lane>/` 的真实资源注册表。
- 各节点配置：`runtime/real/config/<lane>.json`，mode `0600`。随机测试令牌只写 analysis 的 `runtime/real/secrets/<lane>.token`，不向 control 复制。
- HDFS：`/snow/ods/real/kafka/<lane>/`、`/snow/warehouse/real/<lane>/`、`/snow/auxiliary/real/<lane>/`。所有新模型输出由 `reserve_job` 预先登记。
- 元数据：`runtime/real/{jobs,coverage,permits}/<lane>-r1.json`；control 的模型包与日志在 `runtime/real/runs/<lane>-r1/data/`。
- 聚合传递：`runtime/real/transfers/<lane>/<lane>-r1/`，使用原有 `export/reserve/accept` 库 API 校验源代际、不可变输入、哈希和原始期限。
- 后续 Hive、Iceberg 仍使用其原有权威注册与执行入口，不导入或复制旧 registry，不手写通过回执。

临时采集库、适配器输入和本地原始批次适用原接收时间起 7 天；辅助明细与日聚合仍分别适用原有 30 天和 90 天规则。新复制品不能刷新期限。没有新增容器、镜像、卷、Kafka Topic 或 Doris 数据库。停止不删除这些数据。

## 操作顺序

实际执行前先冻结通过 CI 的完整源码包，并逐字确认正式 writer 的 12 个已登记文件未改变。正式 epoch 必须已受控暂停，全部真实引擎停止。使用已验收小样本的 2048/1920/768 MiB 离线配置，保持 64 GiB 项目、35 GiB 宿主空闲磁盘、4 GiB 宿主可用内存硬界，256 MiB 启动预留，63.75 GiB 提前停止线；每台来宾至少保留 128 MiB 可用内存、384 MiB 磁盘。

以下 Linux 命令通过受信任的 `lab_remote.py` 和外层资源监测执行；它们不代替 Windows 启停及失败收尾。工具拒绝在其他 checkout 或错误节点运行。

1. analysis 执行 `.venv/bin/python tools/lake_fixture.py initialize --lane fixture-lake-01`。初始化要求所有本次路径为空；失败保留现场，使用新的 lane 重新验收，不能改旧时钟后重试。
2. 固定离线服务正常、HDFS 两台 DataNode 就绪后，analysis 执行 `.venv/bin/python tools/lake_fixture.py land-permit --lane fixture-lake-01`。实际 HDFS 文件校验和两副本通过后才确认**模拟适配器**的位点；随后真实注册输出并清理，签发最长 15 分钟的读许可。
3. 只把新 fixture 的配置和 `manifest.json` 元数据复制到 Windows/control，不复制采集库、原始输入、令牌或 authority registry。配置 mode 保持 `0600`。使用原 `real_lab stage-compute --run-id fixture-lake-01-r1` 传递经过验证的 job/coverage/permit/ODS 状态元数据。
4. 在 control 依次通过冻结的 `real_lab daily`、`behavior`、`validate`、`publish-private` 执行实际模型；参数均绑定 `runtime/real/config/fixture-lake-01.json` 与 run `fixture-lake-01-r1`。每个计算阶段之前在 analysis 重新运行 `land-permit` 并重新传递计算元数据，不能沿用过期 permit。
5. control 执行 `.venv/bin/python tools/lake_fixture.py export --lane fixture-lake-01`。它先检查实际 Spark 日整数结果与独立采集器 oracle 一致，再调用正常托管导出库。
6. 只复制导出的 `manifest.json` 到 analysis 的固定 `incoming-manifest.json`，执行 helper `reserve`；再复制原 `pair.json` 到固定 `data/pair.json.tmp`，执行 helper `accept`。这两个阶段均使用同一个 `--lane`；复制和读回保留原聚合期限。不得调用拒绝合成来源的生产 `stage-release`，也不得把配置改成真实来源绕过。
7. 按 `real-hive-dispatch.md` 和 `real-lake-authority.md` 调用相同 Hive/Lake 协调器，把配置换为本次明确合成配置，并填写固定 run 与新 attempt。先做实际 Catalog 注册/读回，再做 Iceberg 执行/独立验证/确认；仅由协调器在同一 VM 拓扑内切换已登记的 Hive 与 YARN 服务。
8. 结束时关闭固定离线服务并软关虚拟机，保存来源标签、actual application ID、回执哈希和资源最低余量。任何门禁失败都保留失败记录，不能输出成功状态或扩大数据规模。

本页为实现与操作约束；实际引擎验收结果另存证据文件。在尚未取得回执前，工具可运行和测试通过不代表整条引擎路径已通过。
