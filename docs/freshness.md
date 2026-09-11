# 端到端新鲜度与数仓停机验收

使用本机合成采集实例，通过真实 HTTP 202 接收、SQLite FULL WAL、私有增量 HTTP、带校验和归档、Kafka、Flink Checkpoint 和 Doris 查询完成验收。采集器及观察客户端位于 Windows 宿主回环接口，数仓位于单台 NAT 分析 VM；未接入生产网络或两个业务产品。

2026-09-11 实测：300/300 条可见，发送至首次查询返回 P50 **7.047 秒**、P95 **12.016 秒**、P99/最大值 **14.047 秒**；202 返回至查询 P95 为 12.000 秒。60 批发送阶段实际耗时 84.218 秒，包含同步查询及每十批一次资源检查开销，不能将它标记为严格恒定 5 条/秒的负载。Flink 完成 11 次 Checkpoint，失败 0 次。

两张应用日汇总与轻量库及基准一致：网站 PV 60 / UV 12，小吉终端 PV 60 / UV 10 / 完成请求 60 / 成功 48。关闭数仓后新增 5 条事件，聚合位点从 300 推进至 305，公开合成行仍为 0。运行期间项目文件约 58.38 GiB，关机后约 53.88 GiB、宿主空闲约 55.55 GiB。

## 测量口径

- 开始前要求实验表为空、Flink 正常运行且已有一个完成的空 Checkpoint。启动、镜像下载和历史积压不计入本轮测量窗口。
- 连续 60 批，每批 5 条事件：两个应用的页面访问、角色选择、请求观察和可信请求完成。每批目标间隔 1 秒，共 300 条事件；同批事件共享一次 HTTP 事务，因此不把它们描述为 300 次独立网络试验。
- 每批记录宿主单调时钟的请求发起、202 返回时刻。约每秒查询 Doris，以首次查询返回为可见时间上界。同时统计发送至查询、202 返回至查询两组时长。
- **验收使用发送至查询返回的 P95≤60 秒**。数据库持久化发生在 HTTP 请求发起与 202 返回之间，这个区间包含 HTTP、轮询及查询开销，是接收持久化至可见耗时的保守上界。没有扣除轮询时间，也没有拿 Checkpoint 时长冒充端到端延迟。
- P50/P95/P99 使用 nearest-rank；全部 300 条必须可见。缺失、重复观察标识、非法时长会使验收失败；未观察到的消息不能从分母中删除。
- 起止耗时使用同一宿主单调时钟，避免两个机器的时钟差污染计算。仍检查 VM 与宿主偏差小于 2 秒，并核对测试期间宿主墙钟与单调时钟差异；此检查不等于生产 NTP 精度认证。

测量后对账：HTTP 已接收、归档、Kafka 和 Doris 各 300 条；隔离、重复和超迟到均为零。轻量服务按原有 60 秒周期自行聚合，日 PV、UV、完成请求数和成功数逐项与 Doris、独立 Python 基准比较。

随后停止同步、取消合成 Flink 作业、保存 Checkpoint 身份、停止明确列出的容器并关闭分析 VM。宿主采集服务继续运行，再通过 HTTP 接收 5 条事件，等待原有聚合线程推进到 305。公开 JSON 只筛选真实来源，因此本轮合成事件始终不产生公开指标行；验收的是合成隔离和快照继续更新。

## 持续同步

`snow-stats sync` 默认仍只读取一个批次。新增显式 `--follow` 复用 HTTP/Kafka 连接按秒轮询；`--lane` 将实验写入独立主题，`--source` 拒绝来源混入。它不会随业务服务自动启动，也不创建计划任务。

```powershell
# SNOW_READER_TOKEN 在本地进程环境中设置，不写入命令或仓库。
uv run --extra lab snow-stats sync --url http://127.0.0.1:8100 --bootstrap 192.168.216.133:9092 --directory runtime/sync/new-lane --lane new_lane --source synthetic --follow --poll-seconds 1
```

目标主题和对应 Flink lane 必须提前配置。每次仍先保存原始归档及 SHA256，再等待 Kafka ACK，最后提交位点；网络失败采用有限重试后退出，pending 保留，由操作者或独立监督进程决定恢复时机。不循环吞掉认证、过期位点或校验错误。

同步目录新增 `target.json`，绑定 URL、bootstrap、lane 和预期来源，不包含凭据。更换这些字段时拒绝沿用该目录；旧目录有游标或归档而没有绑定文件时，也拒绝静默接管。迁移前应核对旧目的地、已确认位点和归档，选择新的独立目录明确重放与对账。这个绑定不能识别同一地址背后的数据库重建或 Kafka 集群更换；单采集序列代际约束仍适用。

## 复现与资源

使用[实时手册](realtime.md)的单分析 VM 4.5 GiB 配置，关闭另两台 VM。复用已有 Doris 容器，避免复制其大型可写层。首次或新增镜像不能沿用本轮的空间估计；作业前仍保留 60 GiB 总预算、35 GiB 宿主余量和 1 GiB 作业空间。

1. 选择全新 lane，在分析 VM 忽略的 `lab/secrets/realtime.env` 设置对应 `SNOW_REPLAY_LANE` 和 `DORIS_TABLE=snow_realtime_<lane>.events_realtime`；只调整本项目实验配置。
2. 同步当前源码与已校验的 JAR，启动本项目服务，检查 VM 时钟。
3. 在宿主执行以下命令。第二个命令成功后会停止本项目数仓容器和分析 VM；宿主临时采集服务完成停机验收后也退出，数据库与归档保留。

```powershell
uv run --extra lab python tools/smoke_realtime.py --action init --lane freshness01
uv run --extra lab python tools/smoke_freshness.py --lane freshness01
```

已验收的 `freshness01` 不得再次初始化；换 lane 重现。采集器通过 Python Settings 固定 synthetic，两个短期令牌只在进程内生成。随机回环端口与绑定关系写入私有同步目录；不把这个测试服务作为生产采集部署。

原始时长、归档、SQLite、Checkpoint 和实际工具收据位于忽略的 `runtime/freshness/<lane>`，Kafka 读回在 `runtime/realtime/<lane>`。公开证据只保留分位数、聚合计数、配置、哈希和限制，见 [freshness.json](evidence/freshness.json)。

这项实验覆盖本地完整路径的小样本新鲜度和停机后的轻量独立性。公网代理/部署、持续负载、故障期间 SLA、三 VM 并发、10 万/100 万规模与长期运行仍需独立验收。
