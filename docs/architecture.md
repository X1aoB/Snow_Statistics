# 架构与口径 v1

## 独立边界

Snow_Statistics 独立仓库、数据库、部署和配置。两业务仓库只包含复制的可选浏览器适配器；MyWebsite 另包含静态统计页面。产品不导入本包，不访问 Kafka/Doris，不将采集结果加入业务健康检查或构建条件。

浏览器开关默认关闭；开启后还需用户选择允许。GPC/DNT 会禁用标识。队列最多 20 项，每批 10 项，每批 1.5 秒超时，最多重试一次；无正文、查询参数、IP、密钥、工具输入。失败允许丢弃未确认事件。允许的路径和角色必须在产品配置及采集器同时配置。

统计命名空间为 `snow.statistics.v1.<app>.`；匿名 ID 30 天到期、会话闲置 30 分钟更新。统计撤销只删除该命名空间的匿名标识、会话及同意记录，业务 IndexedDB、Cookie、凭据和签名状态保持原样。

## 事件契约

事件 v1 统一 `event_id`、`app`、`event_type`、带时区的 `occurred_at`。服务端决定 `source`（线上固定 real），客户端不能提交该字段。SQL/Topic/存储表名不进入产品契约。

- page_view：允许列表路径；去查询参数；用于 PV。
- character_select：公开角色 ID；热度以选择次数计。
- entry_click / entry_arrival：一次性 jump_id；通过 fragment 传递并在目标页消费，避免进入 HTTP 查询日志。
- request_observed：前端仅发 request_id 及匿名关联信息，用于漏斗，不计完成或成功。
- request_complete：独立服务端令牌，request_id、公开角色 ID、是否成功及耗时；来源为脱敏完成日志。

日志适配器将已有 `stage=complete`、无 terminal_error/exception_type 识别为成功；只返回白名单字段，使用日志封装时间，不用重放时间。前端不能提交可信完成事件。入口认证/限流/排队前失败没有完成日志，因此统计明确称“完成请求”，不称全部 HTTP 请求。

## 接收、聚合与同步

SQLite 单服务进程、单写连接互斥锁，WAL + synchronous FULL，完整批次提交后返回 202。事件 ID 内容冲突拒绝整批；完全重复不重复追加。聚合计数和游标同事务提交，原始暂存仅删除已聚合前缀。正常刷新周期 60 秒，积压处理结束才发布快照；超过 3 分钟标记过期。

时间统一 UTC 存储；业务日期 Asia/Hong_Kong。接收过去 7 天至未来 5 分钟的事件，避免任意历史写入破坏已过期去重状态。原始保留按接收时间计算 7 天，标识辅助/去重保留 30 天，日聚合 90 天。独立文件系统提供硬边界，低流量新部署默认 512 MiB，可在创建时选择 1 或 2 GiB；已有卷不自动调整。应用保守预留空间并明确拒收，不因消费者离线删除已确认保留期数据。7 天是待容量验证的目标，拒收会形成采集缺口，汇总完整性只承诺已接收事件。小配置实测及新旧数据边界见[资源手册](resources.md)。

增量接口只读且需要独立 reader token；游标过期返回 410，不静默跳过。同步先写带 SHA256 的原始归档，再确认 Kafka 写入，最后原子推进本地游标。消费者崩溃可能重放，绝不通过先提交游标跳过数据。已推进游标而 pending 清理前崩溃也可恢复。单个同步目录只允许一个进程。

显式 `--follow` 支持持续轮询并复用连接；错误退出且保留 pending，不依赖业务服务启动。目录绑定源 URL、Kafka 地址、lane 和来源，避免直接复用另一条链路的位点；没有绑定的旧归档须先显式核对迁移。约束及实际端到端测量见[新鲜度验收](freshness.md)。

## 数仓建模

事件及 CDC 原样 ODS；DWD 先按来源/应用/事件 ID 去重，再按来源/应用/请求 ID 去重完成请求。DIM 内容 SCD2 使用半开有效区间并保留删除；按当前归属分析使用单独视图。

工单 open → in_progress → resolved，resolved → open 开始下一处理轮次，保留首次/最近解决与轮次耗时，并输出每日状态。模拟事务标记与业务修改同事务提交，重试不会重复推进状态。

主转化链路：点击 → 到达 → 前端请求关联 → 服务端成功。角色选择是辅助诊断事件，默认角色成功对话同样可转化。成功必须在入口点击后 30 分钟内，归于最近有效点击，每个点击最多一次；最近入口已转化不回退到更早入口。缺少前端同意或必要关联事件的成功不做猜测归因。两个应用不合并长期身份。

会话跨午夜不拆分，归入开始日期。留存按固定输入历史中首次观察到匿名标识的日期分群；D1 / D7 观察日结束后才有有效分母，未成熟人数为 null。详细日期、去重和成对发布口径见[运营与行为模型](behavior-models.md)。

Spark 写入版本化运行目录，校验后生成 accepted manifest，旧结果保留。Airflow 默认最近 7 个业务日期，历史补数显式提供范围；同一截止时间对账，不能把持续变化中的实时结果直接与昨日快照比较。

Flink Java：30 秒 watermark、10 分钟迟到侧输出、8 天去重状态 TTL、10 秒 checkpoint；晚到记录仍由原始归档参与离线修正。历史回放使用独立 lane/group/label，不能将恢复检查点和业务唯一键视为同一个保证。Doris 使用唯一键及版本字段，实时事实和离线日表分开写。

真实/合成 source 在 Topic、分区及看板分开。公开 JSON 永远只选择 real，由轻量模块产生。OpenLineage 初版只显式记录实际任务与表；未验证的列级依赖不作声明。

## 官方参考

- [SQLite WAL](https://www.sqlite.org/wal.html)：FULL 提交和 checkpoint；数据库留在本机文件系统。
- [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/)：服务生命周期内的聚合任务。
- [Spark 3.5.7 Hive compatibility](https://spark.apache.org/docs/3.5.7/sql-data-sources-hive-tables.html)：Hive metastore 3.1.3。
- [Flink Doris connector](https://doris.apache.org/docs/3.x/ecosystem/flink-doris-connector/)：checkpoint/事务提交与批处理语义。
- [Iceberg evolution](https://iceberg.apache.org/docs/latest/evolution/)：字段和分区演进实验。
