# 真实 Hive 接入交接（暂停点，2026-09-13）

> **2026-09-19 更新：** 用户已恢复任务。已从忽略草稿继续实现独立模块、实际 catalog driver/shell、`tools/real_hive.py` 和合成测试；规范序列化已统一。当前操作与验证边界见 [独立真实聚合 Hive 入口](real-hive.md)。下文保留 9 月 13 日暂停时的历史状态；不代表当前源码仍只是草稿。实际 Hive 引擎验收仍待离线窗口。

**状态：只完成源码审计，新增代码是未接线草稿，不能执行验收或用于正式读取。**

本轮没有操作虚拟机，没有部署、修改冻结 writer 文件、提交或推送代码。用户要求在安全处停止后，停止新增实现。两个草稿已从 `src/` 安全移到本机忽略目录 `runtime/hive-draft/20260913-220517/`，不属于签名提交 `be591517`。既有 `real_lab` 仍禁止 `register_hive=true`，维持原先的失败关闭边界。

## 已确认的现状

- 实际日聚合入口是 `warehouse/spark/batch.py --source real`；没有 `real_daily.py`。它和 `warehouse/spark/real_behavior.py` 支持 `--register-hive`。
- 现有 inline 注册使用共享 `snow_real` 数据库和按 run ID 生成的表名。只有创建和行数检查，缺少表所有者、采集源代际、精确外表位置及分区位置的独立读回。不同 lane 复用 run ID 可能碰撞。
- `RealRemoteLifecycle` 支持登记 Hive 表并要求真实后端检查；`real_backend_lifecycle.backend_adapters` 目前只实现 Kafka、Doris 和 Checkpoint。登记 Hive 后缺少适配器时清理会明确失败；不能用成功 JSON 绕过。
- 现有 Hive 服务是锁定镜像的 Hive 3.1.3 metastore，复用 Derby 元数据卷。`tools/prepare_hive_client.sh` 与 `lab/locks/hive-client.sha256` 已有版本锁定客户端准备流程。
- `tools/spark_yarn.sh` 显式配置 Hive 3.1.3 客户端；`tools/spark_yarn_scale.sh` 未配置该外部客户端，且明确排斥 Hive 与规模计算阶段并行。缺少远端 metastore 配置时，`enableHiveSupport()` 本身不能证明使用了该 Hive 服务。
- Spark/Hive 外表的目录数据仍由 HDFS 生命周期负责。删除外表元数据不等于删除 Parquet；目录删除也不等于清除表/分区目录引用。

## 已保存的未完成草稿

| 文件 | 当前内容 | 明确缺口 |
|---|---|---|
| `runtime/hive-draft/20260913-220517/real_hive.py` | 聚合发布物到表描述符的规划；metadata-only 登记意图；Hive 生命周期适配器与固定 Spark 子进程接口草稿 | 没有实际子进程驱动、shell 或 CLI；未测试；`cleanup` 接线尚未完成；实际 catalog 回执尚不存在 |
| `runtime/hive-draft/20260913-220517/real_hive_contract.py` | Python 3.8 可导入的字段、表名、来源、原截止、外表位置和分区白名单草稿 | 未测试；尚未与真实 Spark 元数据格式核对；其 `canonical()` 使用 `ensure_ascii=False`，与主服务的 `ensure_ascii=True` 不一致，恢复工作时必须先统一，否则非 ASCII 聚合维度的哈希会不一致 |

这两个草稿已移出 Python 包，不会被稳定生产入口导入。移动前只通过 `py_compile` 语法检查，没有运行功能测试。草稿提到的 `tools/spark_hive_catalog.sh` **尚不存在**；不要运行 `SparkCatalog.execute()`，不要将文件存在作为能力已实现的证明。忽略目录不会跟随 Git 克隆；在另一台电脑继续时应以本文缺口清单为准重新实现，不能假定草稿已同步。

## 建议继续的最小范围

1. 先统一序列化契约并补合成单测。继续前阅读两份草稿，不直接接入生产。
2. 用 `read_real_release` 读取受生命周期管理的已验收发布物，再调用 `validate_real_pair`。仅登记四组已有 Parquet：`ads_daily`、`session_daily`、`retention`、`funnel`；不复制行、不登记会话/归因匿名辅助明细。
3. 四组均继承原报表起始香港日期加 90 天的截止；不能按登记、备份或重跑时间续期。必须匹配 `RealRemoteLifecycle` 已登记的精确 HDFS 聚合路径与截止。
4. 实现固定 Spark 3.5.7 + Hive 3.1.3 客户端 catalog 驱动。只允许明确动作和严格描述符，不接受任意 SQL、任意 shell 或外部成功回执。建立明确远端 metastore 绑定；实际检查 external 类型、Parquet provider、所有者属性、精确位置、分区 source 和分区位置。
5. 先完成真实生命周期清理，再建立/读取表。清理适配器只对登记且所有权读回一致的到期外表执行 DROP，并实际确认表不存在；不执行 DROP DATABASE/CASCADE/PURGE，不删除共享目录。HDFS 数据仍由现有生命周期精确删除并读回。
6. 对全部已登记 Hive 表进行完整目录检查，并处理「预登记后创建失败」与「已验证表意外消失」的区别。失败保留登记意图和失败状态；不可丢失已有范围或伪造空范围。
7. 新增独立 `cleanup / register / verify / permit` 操作入口，组合现有实际后端检查或受真实 epoch 约束的 stopped-storage 证明。初始化 Hive 后，旧统一入口没有该适配器仍会阻断；后续需显式接适配器 hook，不能仅改 `register_hive` 布尔值。
8. 资源安排需重新核验。可考虑计算完成后停 RM，复用既有 NameNode + Hive metastore，独立 Spark local[1] 只核验小聚合。analysis 当前 1 GiB 还承担 DataNode，不能直接再叠加 Spark；需在实际离线窗口确认可用 RAM、HDFS 副本可读性和项目磁盘，不先扩大虚拟机或新增常驻服务。

## 尚须完成的验证

合成测试至少覆盖：混源、代际变化、未登记路径、原截止续期、表名碰撞、SQL/路径注入、managed 表误用、分区位置外逃、稀疏/空组、不完整登记、重试幂等、过期表删除读回失败、HDFS 删除失败、表元数据丢失、实际 backend 缺失和伪造回执拒绝。

之后在独立离线窗口实际验证：锁定 jars 检查 → 远端 metastore 实际读回 → 四组外表与成对发布物逐值/哈希一致 → metastore 重启后注册保持 → 到期目录/表分别精确清理 → 对业务资源和 synthetic 范围没有变更。**本轮没有执行这些引擎验证。**

## 已核对的官方依据

- [Spark 3.5.7 Hive Tables](https://spark.apache.org/docs/3.5.7/sql-data-sources-hive-tables.html)：远端 Hive 配置、客户端版本，以及未配置 metastore 时创建本地目录的行为。
- [Spark 3.5.7 CREATE DATASOURCE TABLE](https://spark.apache.org/docs/3.5.7/sql-ref-syntax-ddl-create-table-datasource.html)：带 LOCATION 的数据源表定义。
- [Spark 3.5.7 DROP TABLE](https://spark.apache.org/docs/3.5.7/sql-ref-syntax-ddl-drop-table.html)：外表元数据与实际数据删除边界。实际版本/所有权读回仍须本项目验收，文档不是引擎证据。
