# 10 万条历史输入的三方对账

本实验复用 [Spark/YARN 扩样](scale.md)中已验收的四个 gzip 分片，比较实际轻量 Store、Flink/Doris 与 YARN 日指标。数据全部为 synthetic，公开汇总不包含这些样本。实时组件只在分析 VM 中按需启动，另两台 VM 关闭。

## 输入和顺序

原始样本跨七个业务日期，按生成序号交替排列。直接向带 10 分钟迟到窗口的实时作业发送这些历史数据，会把其中部分有效事件送入迟到主题。因此新工具先验证四片 SHA256，再按事件时间、原始序号排序，保持信封内容、事件 ID、请求 ID、接收时间及原始 seq 不变；对每个事件和请求检查首次接收的胜出记录不变，发现改变则拒绝回放。

这是历史数据按事件时间重放，不是原始到达顺序的乱序测试。Kafka 位点与原始 seq 属于不同进度，不能互相替代，也不能将本轮耗时当作生产新鲜度。Flink 的 30 秒 watermark、10 分钟迟到修正与 8 天处理时间去重 TTL 没有调整。

日指标按应用和 Asia/Hong_Kong 日期比较，共 14 行；总 PV 30,000、完成请求 10,000、成功 7,500。每日 UV 单独比较，不相加冒充跨日期去重人数。

计数分为两个层次：轻量接收端按事件 ID 去重，100,000 次输入中 95,000 条持久化、5,000 条事件重复；聚合另排除 5,000 条请求重复。Flink 明细同时经过事件与请求去重，期望得到 90,000 条有效事实及 10,000 条重复诊断。不能直接把两个阶段的 accepted 数相等作为验收条件。

## 实测结果（2026-09-12）

`scale100k01` 使用 Kafka 3.9.1 / Flink 1.20.3 / Doris 3.0.6.2，沿用新鲜度验收的相同 JAR。以下均来自实际运行，收据为 [realtime-scale100k.json](evidence/realtime-scale100k.json)。

| 核验 | 实际结果 |
|---|---|
| Kafka 首次写入及完整读回 | 100,000 条，规范化字节流 SHA256 一致 |
| Doris 全明细 | 90,000 行，12 列规范化摘要与独立基准完全相同 |
| 日指标 | 14 行，轻量 Store、YARN 与 Doris 逐项相同 |
| 初始旁路 | 10,000 重复、0 迟到、0 隔离 |
| TaskManager 强制终止后恢复 | 实际恢复 Checkpoint 4，恢复计数 1 |
| 恢复后追加 1,000 条旧消息 | Kafka 总计 101,000，重复诊断总计 11,000；事实与指标不变 |
| 最终 Checkpoint | 最新完成 ID 8，状态 36,108,302 bytes |
| 轻量来源隔离 | 公开 synthetic 行数 0 |

最终 REST 计数为 7 次完成、7 次失败、0 次进行中；历史中失败 Checkpoint 5 的原始信息为 `Trigger checkpoint failure.`，恢复后 6、7、8 连续完成。计数保留在证据中，不把“恢复成功”写成“Checkpoint 零失败”；没有逐次日志证据的触发失败不推断更具体原因。

Kafka 预装载 26.703 秒；作业提交开始到全部事实首次查询可见为 32.922 秒。后者包含提交和检查开销，排除预装载及之后的全明细读回、故障恢复，不是每条事件 P95 或稳定吞吐。历史对账无需公网采集，不能替代生产部署验收。

分析 VM 仍为 4608 MiB。18 次宿主采样中项目最大 59.60 GiB，宿主最少可用 RAM 10,326 MiB、磁盘 48.30 GiB，未触发 64 GiB / 4 GiB / 35 GiB 门禁。三次 VM 观察最少可用 1051 MiB；观察到的容器生命周期内存峰值如下：

| 容器 | 上限 MiB | 观察峰值 MiB |
|---|---:|---:|
| Kafka | 640 | 429.84 |
| JobManager | 896 | 456.04 |
| TaskManager | 1536 | 750.80 |
| Doris FE | 1280 | 722.59 |
| Doris BE | 1792 | 1690.40 |

OOM/OOM kill 计数均为 0。cgroup 包含文件缓存，TaskManager 重启重置其峰值，表中取三次观察的最大值；它不是全过程进程 RSS 或同时峰值。BE 已较接近容器上限，本轮保留 4.5 GiB VM，不继续缩容。控制/计算 VM 始终关闭。

冷启动阶段曾在发送前遇到 Windows 文本 stdin 换行、Doris 未就绪及 SSH 尚未启动的问题；改为 LF 字节输入、显式等待 Doris/Flink，以及在启动服务前确认 SSH/Docker。前三次没有产生 Kafka 输入或提交作业。一次取消脚本因 REST 未启动而失败，但独立 VM 关机步骤仍执行，并逐次确认 VM 全关；失败收据保留。

正式验收正常停止指定容器并关闭 VM，收尾错误为空。冷态项目约 55.10 GiB，宿主空闲磁盘 52.77 GiB。轻量本机固定时钟测试库及文件约 64.20 MiB，保留作对账证据；它的大小不能外推为线上保留期容量。

## 重现与保留

先完成 `runtime/scale/input-100k-v1` 与 `runtime/scale/scale-100k-01.json` 的 YARN 验收，保留原输入清单。每次需要重新实验时选择新的 lane 和目录，以下 `scale100k02` 只是示例：

```powershell
uv run python tools/prepare_realtime_scale.py --output runtime/realtime-scale/scale100k02
uv run python tools/verify_scale_lite.py --directory runtime/realtime-scale/scale100k02
```

轻量比较直接调用实际 Store/aggregate，将测试时钟随批次推进到事件发生时间；它不模拟原始接收时刻，也不是 HTTP、保留期或容器资源验收。HTTP 与实际限额证据分别见[新鲜度](freshness.md)和[轻量资源](resources.md)。已有测试库、输入或提交收据均不覆盖。

依据[实时手册](realtime.md)关闭其他 VM，将分析 VM 配置为 `realtime` 4608 MiB，检查当前容量、受信 SSH、Docker 和 VM 时钟。备份分析 VM 中私有 `lab/secrets/realtime.env`，只将 lane 与目标表分别改为 `scale100k02`、`snow_realtime_scale100k02.events_realtime`；其余私有配置保留。不要把历史 Checkpoint 用于新 lane。

通过受验证 SSH 运行 `tools/start_realtime_node.sh`，等 SSH/Docker 启动成功后再执行下面命令。命令使用实际 VM NAT IP；当前初始化工具默认 `192.168.216.133`，IP 变化时传入 `--host`。规模工具自行解析本项目 VM 地址。

```powershell
uv run --extra lab python tools/smoke_realtime.py --action init --lane scale100k02
uv run --extra lab python tools/smoke_realtime_scale.py --lane scale100k02
```

规模工具核对隔离配置和空表，等待 Doris/Flink 就绪，验证 JAR 与既有新鲜度验收的 SHA256 相同。100,000 条 Kafka 消息全部确认并读回校验后才提交作业；对全部有效明细的 12 个字段排序规范化并比较 SHA256，再逐项比较 14 行日指标。有效结果可见且新的 Checkpoint 完成后，强制终止并启动本项目 TaskManager，确认恢复身份，再追加最早的 1,000 条历史消息；明细和指标应保持不变。

输入和三个旁路主题分别归档，旁路投递语义仍为至少一次。本工具严格核验本次观察到的诊断条数，超出预期会保留状态并失败，不能为了通过检查删除重复诊断。它不声明所有故障窗口都有恰好一次的旁路消息。

收尾尝试保存 Checkpoint、取消合成作业、停止指定容器，并始终另行尝试正常关闭分析 VM。必须检查 `shutdown.json` 和实际 VM 状态；REST 不可用时不能声称取消及收据导出成功。仅关闭计算服务，不删除数据。原始事件、数据库、主题、Checkpoint、私有 env 与完整收据留在忽略路径中，资源范围见 `deploy/resources.json`。
