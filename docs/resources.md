# 按实际流量配置资源

个人网站与小吉终端预期流量较小，日常使用轻量服务即可；百万条合成数据用于按需开展求职实验。2026-09-11 已实际验证更小的轻量配置，默认值调整如下，采集仍默认 off。

| 使用方式 | 内存 / CPU | 持久化 |
|---|---|---|
| 关闭统计 | 无统计进程、无运行 VM | 已有历史保留，不自动删除 |
| 线上轻量统计 | 容器上限 **256 MiB / 0.25 CPU** | 新卷默认 **512 MiB**，SQLite、WAL、辅助表和日聚合共享限额 |
| 本地离线实验 | 三 VM **2/2/1 GiB**，按需启动 | 含镜像、VM 磁盘、历史及合成数据；当前执行 **72 GiB** 项目门禁，启动前重新测量 |
| 本地实时 / 治理 | 实时单 VM 4.5 GiB；治理单 VM 2 GiB | 与离线重算分阶段运行，结束后关机 |

本地镜像和实验成果的磁盘占用不等于线上统计库的需要。轻量服务无需任何虚拟机、Kafka、Spark、Flink 或 Doris 在线；对两个产品的接口和开关没有新增依赖。

## 2026-10-13 项目容量调整

用户授权“如果碰到空间上限不足可适当扩容并标注相应操作”。本次将 `deploy/resources.json` 的项目文件预算从 **64 GiB（68,719,476,736 bytes）提高到 72 GiB（77,309,411,328 bytes）**。运行中仍提前 **256 MiB** 停止，因此停止线为 **71.75 GiB**；宿主空闲磁盘 **35 GiB**、可用 RAM **4 GiB** 的门槛不变。

调整依据：2026-10-13 冷态项目约 **59.23 GiB**，宿主磁盘约 **198.82 GiB** 空闲，RAM 约 **14.47 GiB** 可用，三台项目 VM 全关。旧 64 GiB 限额叠加 analysis 4608 MiB 内存后备及 256 MiB 启动预留后不足；4096 MiB 候选的来宾可用内存又未达到 storage 启动要求。新增 8 GiB 项目预算为沿用 4608 MiB 配置和后续分阶段离线计算提供空间。这是容量规划，尚不代表本轮引擎运行验收通过；每阶段仍按实际字节检查，不能同时启动全部服务。

上述预算调整只修改项目容量门禁，**当时没有扩展 VMDK、来宾文件系统或线上统计卷**，也不删除任何历史数据；同日后续 control 实际扩盘另记于下节。`tools/vmware_lab.py` 及其启动、监测、磁盘维护调用者读取同一资源清单；`src/snow_statistics/real_offline_small.py` 已移除固定 64/63.75 GiB 门限，改为读取该清单并计算停止线。运行入口需重新启动才能读取新配置，VM 上的源码包需重新核验并安装；旧签名包及历史回执仍绑定原有源码、限额和测量值。

在 Windows PowerShell、仓库根目录检查当前门禁（只读，不启动 VM）：

```powershell
.\.venv\Scripts\python.exe tools/vmware_lab.py status
.\.venv\Scripts\python.exe tools/real_offline_small.py --help
```

实际增加来宾虚拟磁盘或文件系统时，另行记录原容量、新容量、目标 VM、校验结果；预算变更本身不是该操作的完成证明。

本轮后续实测：analysis 4608 MiB 单节点完成真实同步、同截止整数对账及整机软关/开后的 Checkpoint 恢复。切换 HDFS 阶段时实时引擎保持暂停，三台 VM 配置为 2048/2048/4608 MiB；宿主测得项目 67.74 GiB、VM 文件 66.93 GiB、空闲磁盘 182.83 GiB，低于新停止线。该阶段保留 analysis 原内存配置，以减少重复关机调配；不能把这组观察当作所有引擎同时运行的容量保证，也不改写上表的日常小配置。

## 2026-10-13 control 虚拟盘实际扩容

离线窗口中 control 根分区空闲降到约 371 MiB，低于 384 MiB 门槛，先停止整个窗口，再把该节点独立薄盘从 **18 GiB 扩到 20 GiB**。所有 VM 关闭后，依据用户的适度扩容授权，项目门禁临时设为 **82 GiB，仅限冷维护**；按 [VMware 官方工作空间说明](https://knowledge.broadcom.com/external/article?legacyId=2041556)，为原 18 GiB monolithic 盘、2 GiB 增长和 1 GiB 余量共预留 **21 GiB**。宿主 35 GiB 磁盘及 4 GiB RAM 门槛保持不变。

工具运行时确实观察到临时盘及锁文件，项目文件大小合计的**采样峰值为 75.2358 GiB**，不是连续监测的精确峰值，也不是文件系统分配块数。完成后未知残余为 0，项目约 59.41 GiB，原资源文件按字节恢复至 72 GiB 后才启动 guest。薄 VMDK 最终只增加 131,072 字节，不代表扩盘过程中只需要这点空间。

首次开机检查时，guest 根分区和 ext4 已自动增长；随后 `growpart` 返回 `NOCHANGE`，`resize2fs` 确认无需操作。根分区实际增加 2 GiB，原起点、身份及其他分区不变；文件系统可用空间核验为 **2,702,868,480 字节（约 2.52 GiB）**。没有删除数据、重建文件系统或调整留存期限。该结果只证明容量维护完成，后续 Hive/Iceberg 仍以各自引擎回执验收。公开容量数据及四份私有回执摘要见 [control-disk-growth-20261013.json](evidence/control-disk-growth-20261013.json)。

公开工具的新建磁盘默认值随后对齐为 **control 20 / compute 18 / analysis 29 GiB**，并记入资源清单的 `vm_disk_capacity`。`prepare` 检测到已有 VMDK 时跳过创建与扩盘，所以这次默认值变化不会修改任何已有环境。压缩检查也会按 control 至少 20 GiB 逻辑容量加 1 GiB 余量预留临时副本，当前文件更大时按更大者计算；这是容量估算，没有授权或执行压缩。82 GiB 仅作为本次已完成维护的历史元数据，运行门禁仍为 72/35/4。

## 2026-10-13 小资源离线配置的 analysis 内存调整

V15 的合成 Hive 验证在监测到 analysis `MemAvailable` **127 MiB** 时停止，低于原定 **128 MiB** 门槛；三次 Spark 清理调用已正常退出，但本轮注册尚未完成。这次停止不是新的 Snappy 加载错误。该轮收尾时三个 VM 已停止，原数据与失败回执保留；这不是随后新窗口的持续运行状态。

依据已授权的适度扩容，现有 `real-small-1792` 配置只把 analysis 从 **768 增至 1024 MiB**；control **2048 MiB**、compute **1792 MiB** 不变。两个启动入口与资源清单同步使用 **2048/1792/1024 MiB**，不新增配置名称，默认 `real-small-1920` 仍为原配置。三台 VM 的内存后备预算相应从 **4608 增至 4864 MiB**，冷启动还须另留 **128 MiB** 作业写入预算；每次启动的 **256 MiB** 预留不变。

阶段工具接受 analysis 的两组内核可见 `MemTotal` 范围：历史 768 MiB 分配对应 **640–768 MiB**，新增 1024 MiB 分配对应 **896–1024 MiB**。这两个范围用于检查明确的配置，不能把中间大小或更大 VM 当作本轮配置。DataNode 原容器上限仍为 **768 MiB**。可用内存 **128 MiB**、来宾磁盘 **384 MiB**、项目 **72 GiB**、停止线 **71.75 GiB**、宿主磁盘 **35 GiB** 和 RAM **4 GiB** 门槛均未降低。

**香港21:38后准备阶段的历史状态：**当时仅完成源码配置和合成测试，实际 VMX 尚未修改。该 pending 状态保留为当时截点，后续事实如下。

**香港22:06–22:11实际容量结果：**签名 `2434ab6bac6710becd81ed4f8714536d0b5a27c1` 已通过两轮全部 CI 作业／步骤，并在三节点各完成447文件实际安装和独立回读；12个冻结 writer、依赖及安装时原配置／恢复元数据保持。22:06:32在全VM关闭时，既有 `configure` 流程完成analysis **768→1024 MiB**的VMX读回，其余配置和虚拟盘不变。22:11:07来宾实际 `MemTotal` 为 **983516 KiB**，`MemAvailable` 为 **385 MiB**，进入上述新内存区间且高于原128 MiB门槛。这是一次采样，不是连续最低余量、峰值或整轮引擎稳定性证明。

本次容量维护通过的独立元数据见 [analysis-memory-1024-20261013.json](evidence/analysis-memory-1024-20261013.json)。Hive登记、独立查询和Iceberg仍须新attempt的实际验收；不能复用V15已消耗的阶段记录宣称成功。此次未修改容器限制、冻结writer、数据或保留期，历史回执保留原768 MiB条件。

## 小配置的实际验证

使用新建的隔离 512 MiB ext4 文件系统，实际 Docker 容器限制为 256 MiB、0.25 CPU。真实 HTTP 测试先验证两个事件，再追加 10,000 个合成事件，总计 10,002 个。测试库使用 real 标签验证公开汇总，但监听仅在 VM 回环，未接触生产数据或生产采集配置。

- 总 PV 5001、匿名 UV 501、完成请求 5001、成功 4001；重启和切换 off 后指标不变。
- 追加阶段 cgroup v2 内存峰值 **68.39 MiB**，OOM / OOM kill 均为 0；这是包含文件缓存的容器记账值，不是纯进程 RSS。
- 重启后数据库及同目录辅助文件合计 **6.75 MiB**。它是一种事件结构和一天数据的样本，不代表任意事件、7 天原始数据及 30 天辅助数据的固定容量。
- 真实填满专用文件系统后返回 503，已确认数据可读；强制 kill / restart 后事件去重有效，full→lite 保持汇总，off 保留归档。
- 50 个事件一批，批间暂停 0.55 秒，追加阶段 113.801 秒。测试聚合间隔 0.2 秒，生产默认仍为 60 秒，不能将这个时间写成最大吞吐或持续生产负载能力。

首次未节流批量尝试未通过，保留了 5952 个已确认事件和失败镜像；当时断言没有保存 HTTP 状态码，因此只能记录失败位置，不能把原因写成已证实的资源故障。第二次工具加入低于现有限流的提交节奏和响应诊断后通过。服务端每分钟 120 次请求的限制没有改动。

原 2 GiB / 512 MiB / 0.5 CPU 小样本收据仍保留。新证据见 [lite-small.json](evidence/lite-small.json)，实际工具为 `tools/smoke_lite_container.py`；已移除临时测试容器并卸载测试文件系统，两个本轮镜像均保留。没有缩小或重建任何已有数据卷。

## 新部署与已有数据

复制 `.env.example`，其中 `SNOW_MODE=off`、`SNOW_BUDGET_BYTES=536870912`、`SNOW_COLLECTOR_MEMORY=256m`、`SNOW_COLLECTOR_CPUS=0.25`。`Settings` 的无配置默认存储预算也为 512 MiB。以下创建命令只用于没有既有统计数据的新目录：

```sh
sudo bash deploy/prepare-state.sh
```

脚本默认创建 512 MiB 专用文件系统；可在创建新卷时通过 `SNOW_STATE_SIZE_MIB=1024` 或 `2048` 选择更大容量，并将 `.env` 中的 `SNOW_BUDGET_BYTES` 对应设为 `1073741824` 或 `2147483648`。检测已有镜像、挂载、符号链接或非空状态目录会拒绝格式化。它不对现有卷进行缩容或扩容。

已有 2 GiB 部署继续保留原卷和 `SNOW_BUDGET_BYTES=2147483648`。不把新示例整份覆盖到已有 `.env`。如果主动把应用预算调到低于已存数据，它会拒收新增事件并保留已确认历史；这一行为通过了实际 SQLite 重开测试。调整应用预算不会改变文件系统大小。

`deploy/start-lite.sh` 和 systemd 单元同时读取镜像锁及本地 `.env`，使 Compose CPU/内存变量与容器环境一致。`tools/check_lite_compose.py` 已实际解析公开示例，确认 off、256 MiB、0.25 CPU、512 MiB，不读取私人 `.env` 或启动容器。挂载依赖、回环端口、只读根目录及进程限制仍保持原设置。

历史记录：2026-09-27 容量复查显示当时项目实占 63,597,863,100 bytes；4608 MiB storage 加 256 MiB 预留会超出当时 63.75 GiB 停止线约 236 MiB。新增 realtime-4096 作为显式低流量候选后，静态余量约 276 MiB；它尚未经过真实引擎验证，不改变默认 profile，也不降低宿主 35 GiB 磁盘和 4 GiB RAM 门槛。私有容量回执为 runtime/real/operator/storage-memory-capacity-candidate-20260927.json。此次 72 GiB 调整不追溯改写该历史门禁或实验结果。

## 本地实验的资源取舍

### 2026-10-13 Iceberg driver 的有限进程数调整

合成湖仓 attempt `lake20261013` 在实际执行阶段失败。私有诊断记录了线程创建返回 `EAGAIN` 和 `unable to create native thread`；当时 driver 容器的 PID 上限为 256。旧 attempt 和失败回执保留，不能沿用该 attempt 重新宣称成功。尚未采到失败瞬间的 `pids.current`、`pids.events` 或完整内存事件，因此不能只凭这段日志断言唯一原因是 PID 上限。

Apache Spark **3.5.7** 官方源码在 [BlockManagerStorageEndpoint](https://github.com/apache/spark/blob/v3.5.7/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L35-L37) 与 [BlockManagerMasterEndpoint](https://github.com/apache/spark/blob/v3.5.7/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L86-L88) 中，各以常量 100 创建对应异步线程池。这两处不是可直接调小的 SparkConf 配置；源码核对本身也不等于对正在运行的镜像 JAR 做过字节码测量。

依据适度扩容授权，`tools/real_lake_spark.sh` 仅把独立 driver 容器的 **PID 上限从 256 提高至 512**。内存仍为 **1280 MiB**、CPU **2**、Java driver 堆 **640 MiB**，executor 与 VM 配置、所有权标签和精确 CID 清理逻辑不变。该改动没有取消进程上限，也不增加业务服务依赖。Bash 合成测试检查实际参数传递，以及成功、失败和所有权不匹配时的清理范围；它不证明 Docker 或 Spark 引擎已经通过。

后续实际运行必须使用新的固定 attempt，保留原聚合到期时间，并采集该精确容器的 `pids.current/max/events` 和 `memory.current/events`。需同时确认独立执行、独立读回和最终确认通过，才能验收 Iceberg；PID 候选配置和采样工具都不替代引擎证据。项目 72 GiB、71.75 GiB 停止线、宿主 35 GiB 空闲磁盘和 4 GiB 可用 RAM 均保持不变。

[10 万条与百万条 Spark/YARN](scale.md)均已在 2/2/1 GiB 完成。百万条应用时长 115.023 秒，累计磁盘 spill 约 423.17 MiB；NodeManager 容器（包含 AM / executor）记录的生命周期内存峰值约 1327.33 MiB。计算节点仍需容纳 DataNode 与操作系统，因此本轮保留 2 GiB，不再压缩。

监测中项目文件最大 59.67 GiB、宿主磁盘最少空闲 48.27 GiB、RAM 最少可用 10459 MiB。历史保留后，下一次启动要按最新文件占用重新检查；两项验证完成后冷态文件约 54.79 GiB；按用户授权将初始 60 GiB 实验上限适度调整为 64 GiB，覆盖历史文件、5 GiB VM 内存后备和 1 GiB 作业余量，宿主 35 GiB 磁盘及 4 GiB RAM 余量保持不变。启动、监测和磁盘维护共同读取资源清单中的限额；成功一次不代表可以无界保留重复输出。日常让 VM 停止，离线查看已导出的模型、血缘和性能报告；优化或恢复实验按主题启动并使用新 attempt，始终保留宿主资源余量。

2026-09-12 的[实时 10 万条对账和恢复](realtime-scale.md)也已通过原 4.5 GiB 分析 VM 配置。TaskManager 观察到的 cgroup 生命周期峰值约 750.80 MiB，Doris BE 约 1690.40 MiB，OOM 计数为 0；BE 已接近 1792 MiB 上限，当前保留该配置。正式运行项目最大 59.60 GiB；全关后的冷态文件约 55.10 GiB、宿主空闲 52.77 GiB。三次 VM 内存快照不代表连续负载峰值。轻量默认值保持 256 MiB / 0.25 CPU / 512 MiB。
