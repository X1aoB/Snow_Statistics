# 运营与行为模型

`snow_models` 在一份固定的 Kafka ODS 清单上依次计算 CDC 运营模型和行为模型，完成 Hive 读回校验后原子发布私有汇总。日常公开 JSON 仍由独立轻量服务计算；此 DAG、Hive 表和实验看板均不属于产品依赖。

## 口径

| 输出 | 粒度与边界 |
|---|---|
| 会话事实 | 来源、应用、匿名标识、会话开始时间。相邻活动间隔达到 30 分钟开始新会话，跨午夜不拆分；按香港开始日期进入报表。截止时仍未满 30 分钟静默的会话标记未关闭。 |
| 会话日汇总 | 来源、应用、开始日期；会话数、当日开始会话的去重人数、事件数、持续秒数、关闭会话数。不能当作轻量每日 UV。 |
| D1 / D7 留存 | 固定输入全部历史中首次观察到该应用匿名标识的香港日期。观察日结束后才有分母及回访人数；尚未成熟时分母为 0、回访人数为 null，不能显示成 0% 留存。 |
| 渠道转化 | 点击后 30 分钟内的成功服务端完成事件，经该应用匿名标识上的有效到达及前端请求观察关联。每次成功归于最近一次有效点击，每次点击最多转化一次；已消费的最近点击不回退到更早点击。恰好 30 分钟有效。 |
| 角色选择 | 入口后的辅助诊断计数；使用默认角色直接成功对话也算转化，因此不能把选择数当作所有转化的必经漏斗步骤。 |
| CDC 运营 | 内容 SCD2、工单重开处理轮次、报表日期范围的每日末次状态；当前分类汇总按本次业务截止日重组，不替代历史有效区间。 |

各应用匿名标识分别分组；服务端请求完成按 request ID 去重。行为作业先按接收截止时间读取并去重，再按事件时间截止计算，输入质量计数描述前者。会话与留存读取报表开始前的全部清单历史，然后过滤输出日期，避免七天修正窗口制造“新用户”。首次观察不等于账号注册，也不解决已过保留期的缺失历史；输入完整性须由上游清单与归档管理保证。

## 计算与发布

- `warehouse/spark/event_input.py` 统一日指标与行为作业的 ODS 校验、事件及请求去重。
- `warehouse/spark/behavior.py` 使用 Spark Window、聚合和 Join 计算五张 Parquet 表；`src/snow_statistics/behavior.py` 是独立的小数据循环基准。
- `warehouse/spark/ops.py` 生成三张运营表，支持固定截止时间、日期窗口、Hive 注册与私有验收包。
- `orchestration/dags/snow_models.py`：`window → compute_operations → compute_behavior → publish`。首次任务把输入清单与日期写入 XCom；两次计算沿用同一清单。默认手动、单活动运行、串行执行。
- 受限 SSH 网关仅接受白名单作业；模型输入必须是本项目的内容寻址 `_snapshot.json`。每次尝试使用独立输出路径；失败的部分文件不生成验收包。已验收的相同作业可直接复用，窗口或输入冲突则拒绝。
- 发布只读取验收包，检查来源、粒度、字段白名单、指标不等式、留存成熟期、输入质量及明细/汇总计数。两个模型的清单、日期、截止时间必须一致。先写不可变发布历史，再原子替换 `runtime/publication/models-latest.json`；旧截止时间或同截止时间的不同结果不会覆盖当前发布。
- 私有包仅含聚合、模型计数与运行溯源，不含匿名标识、跳转标识或请求标识。Hive 明细依然私有。这里的 JSON 不是公开汇总 v1 接口。

## 重现

沿用[增量 ODS 手册](incremental-ods.md)的三台 `ods` VM：3.5 / 4 / 1 GiB。开启 batch 服务时，分析节点必须使用 `tools/start_batch_node.sh --ods-small`。先运行主机 `tools/vmware_lab.py status`，并通过 `tools/lab_remote.py --reserve-mib 1024` 进行作业前容量检查。所有输入均为合成数据；不扩样、不绕过 60 GiB 门禁。

治理阶段曾通过相同 DAG 的 `ods-compact`（3.5 / 3 / 1 GiB）。后续实时镜像与数据增长后，该组合已不能直接满足当前 60 GiB 预算；这里的配置保留为历史验收条件，重启前须按[最新资源门禁](runbook.md)重新检查。分析节点仍使用 `--ods-small`。

后续 `scale`（2/2/1 GiB）已通过日指标 10 万条 YARN 验证，见[扩样手册](scale.md)。该阶段关闭 Hive 和 Airflow，未验证本页完整模型 DAG，不能直接将本页命令套入小配置。

新源码需在控制和计算节点同步，然后重建 NodeManager 容器，使两端使用相同 JAR 锁。`tools/spark_yarn.sh` 使用 `spark.yarn.jars=local:/opt/spark/jars/*`；两个入口逐一校验 `lab/locks/spark-jars.sha256` 的 252 个 JAR。`local:` 文件由节点预装，不通过 YARN 逐次分发，参见 [Spark 3.5.7 YARN 文档](https://spark.apache.org/docs/3.5.7/running-on-yarn.html)及[对应提交客户端源码](https://github.com/apache/spark/blob/v3.5.7/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/Client.scala)。Hive 334 个客户端 JAR 的独立锁保持有效。

控制节点启动 `bash tools/start_airflow.sh`，检查 `airflow dags list-import-errors --output json` 为空后，显式触发：

```bash
airflow dags unpause snow_models
airflow dags trigger snow_models --run-id <新的实验名称> --conf '{
  "date_from":"2026-01-01", "date_to":"2026-01-04", "cutoff":"2026-01-05T00:00:00Z",
  "input_snapshot":"hdfs://<CONTROL_IP>:9000/snow/ods/synthetic/kafka/<lane>/snapshots/<sha256>/_snapshot.json"
}'
```

在 Airflow 容器内使用 `tools/airflow_receipt.py --dag-id snow_models --run-id <名称>` 读取任务收据。将控制节点验收包下载到本机 `runtime/publication/models-latest.json` 后，Streamlit 选择“Spark 已发布运营与行为模型”；虚拟机关闭时仍可展示带日期的历史归档。固定两人模拟器输入可用 `tools/verify_model_release.py <发布包>` 对照独立基准。

边界测试使用 `tools/prepare_behavior_fixture.py` 生成 31 条含重放记录及独立预期结果，单独写入测试 HDFS 路径，再执行 Spark behavior 作业并传入 `--expected`。它覆盖跨午夜、恰好 30 分钟、晚 1 毫秒、默认角色、重复完成、最近点击已转化以及留存观察期。其单文件输入仅供直接测试，不满足 Airflow 成对发布所需的固定 ODS 清单条件。

当前不包含每日状态快照的增量维护、全历史身份保留策略、列级血缘和持续生产调度；重算仍需读取固定清单的历史。运行规模与真实收据以[实施状态](status.md)为准。
