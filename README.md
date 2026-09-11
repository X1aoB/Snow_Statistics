# Snow Statistics

独立、可移除的基础统计服务与数据开发实验项目。业务来源为个人网站与小吉终端；开发、CI 和面试演示完全使用合成数据。

**公开统计由轻量服务独立维护；关闭完整数仓不会中断统计页。**

```text
可选浏览器适配器 / 脱敏完成日志
              ↓
Python + SQLite WAL → 基础汇总 JSON → MyWebsite 统计页面
              ↓ 私有增量接口
校验和归档 → Kafka → Spark / Hive / Flink → Doris / Iceberg
                          ↑
Python 运营状态流转 → MySQL → Debezium
```

## 立即运行

需要 Python 3.12、[uv](https://docs.astral.sh/uv/)；浏览器测试需要 Node.js 22+。

```sh
uv sync --extra dev
uv run pytest -q
node --test adapters/browser/analytics.test.mjs
uv run snow-stats demo
```

演示在 `runtime/demo` 生成固定种子、固定时钟的运营事件及模型结果，包括 SCD2 历史/当前分类、工单重开轮次、每日快照、会话、留存、漏斗和整数指标。原始及有效/重复/隔离数据可对账。

```sh
uv sync --extra dev --extra lab
uv run streamlit run dashboard/app.py --server.address 127.0.0.1
```

无虚拟机时在侧栏选择“Python 正确性基准”。“Doris 已发布数仓结果”查询实际离线发布视图，需设置 `SNOW_DORIS_HOST` 并启动分析节点。已经验证的 HDFS/YARN/Hive → Doris → Streamlit 链路及分阶段 Airflow 调度，见[离线闭环手册](docs/offline-pipeline.md)。

“Spark 已发布运营与行为模型”读取 `runtime/publication/models-latest.json`：展示实际 Spark / YARN 计算的会话、成熟期留存、渠道转化和工单状态，虚拟机关闭后仍可查看带日期的归档。计算、重跑和发布见[运营与行为模型](docs/behavior-models.md)。

轻量 HTTP 服务：复制 `.env.example` 为本地 `.env`，配置路径/角色允许列表和独立令牌。命令行运行时需要将变量导入进程环境；Compose 会读取 `.env`。

```sh
# Linux example; PowerShell: $env:SNOW_MODE='lite'
export SNOW_MODE=lite
uv run snow-stats serve
```

默认监听 `127.0.0.1:8100`，默认模式 **off**，不读取生产数据。

低流量部署默认 256 MiB 内存、0.25 CPU、512 MiB 独立存储，已通过 1 万条 HTTP 合成事件及满盘/重启验收；已有卷不自动缩容。实际配置和容量边界见[资源手册](docs/resources.md)。

## 三种模式

| 模式 | 运行范围 | 页面数据源 |
|---|---|---|
| full | 轻量服务 + 手动启动的实验组件 | 轻量汇总 JSON |
| lite | 采集、SQLite、聚合 | 相同汇总 JSON |
| off | 拒收新事件，已有结果可作为归档读取 | 标注时间的归档或移除页面 |

`SNOW_MODE` 控制采集行为；它不会启动/删除虚拟机，不会远程更改业务开关。完全关闭必须先关闭产品适配器，按退出手册处理已接收数据。

## 接口与代码

| 目录 | 职责 |
|---|---|
| `src/snow_statistics` | 事件契约、轻量 API、SQLite、同步、模拟器、模型正确性基准 |
| `adapters/browser` | 可复制且默认关闭的采集、同意控制和公开汇总显示 |
| `warehouse` | Spark 分层与 SCD2/工单作业、Flink Java、Doris SQL、Iceberg 实验 |
| `lab` | 三节点配置、镜像 digest 锁、独立运行 profile |
| `orchestration`, `governance` | Airflow 调度、版本化指标字典、显式表级血缘 |
| `deploy`, `tools` | 轻量部署、资源清单、VMware 创建、合成 CDC、退出范围检查 |

HTTP：`POST /analytics/v1/events`、私有 `GET /analytics/private/v1/events?after=…`、公开 `GET /analytics/public/v1/summary.json`。公开接口不提供用户标识、原始事件、SQL 或错误诊断。

浏览器访问是可丢失的匿名行为估计，不代表经过认证的自然人。请求量仅覆盖已有服务端生成完成日志，不宣称覆盖所有入口失败。`request_observed` 只关联漏斗，不影响请求量及成功率。

## 文档与验证边界

- [架构与口径](docs/architecture.md)
- [本地及虚拟机运行](docs/runbook.md)
- [离线闭环、发布与 Airflow](docs/offline-pipeline.md)
- [Kafka / CDC 增量落地与恢复](docs/incremental-ods.md)
- [Spark 会话、留存、归因与 Airflow 模型发布](docs/behavior-models.md)
- [OpenLineage 本地日志、补发与 Marquez](docs/lineage.md)
- [Flink 恢复、Doris 2PC 与 Spark 迟到校正](docs/realtime.md)
- [HTTP 接收至 Doris 新鲜度及数仓停机验收](docs/freshness.md)
- [2/2/1 GiB 三 VM 的 Spark/YARN 10 万及百万条扩样](docs/scale.md)
- [10 万条 Flink/Doris、轻量与 YARN 对账及恢复](docs/realtime-scale.md)
- [固定输入的分区、Join、倾斜及小文件优化](docs/optimization.md)
- [Iceberg DWD 迁移、演进与 HDFS 恢复](docs/lake.md)
- [Kafka / ZooKeeper 选主、丢多数和会话实验](docs/ha.md)
- [低流量部署与按需实验资源](docs/resources.md)
- [接入和发布](docs/integrations.md)
- [退出与清理](docs/retirement.md)
- [实施记录及未通过的验收](docs/status.md)
- [后续实验与性能取证](docs/experiments.md)
- [交付验收矩阵](docs/completion-checklist.md)
- [离线演示与面试讲解](docs/demo.md)

性能数字必须附带输入、代码修订、资源及执行证据。Python 小数据正确性基准不代表分布式吞吐；虚拟节点不等于物理容灾。当前实测状态以实施记录为准，不把配置文件的存在当作完整集群验收。
