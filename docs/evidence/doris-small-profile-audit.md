# Doris 3.0.6.2 小资源 BE 启动配置审计

本记录是源码核查与候选参数建议，不是新配置的引擎验收回执。没有为本次核查启动、停止或修改虚拟机。实际配置与验收由独立 real epoch 流程管理。

## 版本和证据绑定

- 锁定镜像：`apache/doris:be-3.0.6.2`，摘要 `sha256:4d3bb3b70568b044f36035b9f0b6337fafa0c06ffae89bfd39387bcfdc302517`，见 `lab/locks/images.env`。
- 官方 tag `3.0.6.2/bin/start_be.sh` 的 SHA-256 为 `d901a0cb6f13b3016d65fe3ccfdb3832a0dd88352f96cea99516bfc1076d906b`，与 `deploy/real-epoch/prepare-be-start.sh` 中经过镜像核验的原脚本摘要一致。
- 官方 `3.0.6.2/conf/be.conf` 的 SHA-256 为 `ad3b9f4efe1a73152ab72b584a6878384492367fdacbd08395a363be18c9186f`。
- 本机只读参考副本在忽略目录 `runtime/doris-tuning-research/`，文件名用双下划线替换源码路径中的 `/`。它们是公开源码，不包含真实事件或凭据。

## 为什么普通 JAVA_OPTS 没有生效

[锁定版本启动脚本](https://github.com/apache/doris/blob/3.0.6.2/bin/start_be.sh)先从 be.conf 导出大写配置，随后检查实际 Java 版本。JDK 17 分支选择 `JAVA_OPTS_FOR_JDK_17`，最终将这个值赋给 `LIBHDFS_OPTS` 和 `JAVA_OPTS`。因此仅在追加配置中设置 `JAVA_OPTS=-Xmx256m`，不能改变该分支采用的参数。

[同版本默认配置](https://github.com/apache/doris/blob/3.0.6.2/conf/be.conf)中的 `JAVA_OPTS_FOR_JDK_17` 含 `-Xmx2048m` 及一组兼容性参数。候选修正应保留这组原始参数，仅把堆配置替换为 `-Xms64m -Xmx256m`；不要只写一个短 `-Xmx` 行而丢失模块开放参数。是否使用 Serial GC 或修改 JVM 活跃处理器数，应另行实测，不能把 C++ 线程池问题全部归因于 JVM。

验证时应读取实际 `doris_be` 进程的环境并只投影 `JAVA_OPTS`/`LIBHDFS_OPTS`，确认两者都采用 256 MiB 堆。仅查看 Compose 环境或宿主 shell 的变量不够。不得打印整个进程环境。

## 为什么 scanner 处触及 pids_limit

[同版本 scanner 初始化](https://github.com/apache/doris/blob/3.0.6.2/be/src/vec/exec/scan/scanner_scheduler.cpp)使用两个固定大小的本地/受限扫描池；默认扫描线程数为 `max(48, 2 × CPU 核数)`，另外还有远端扫描池。[线程池实现](https://github.com/apache/doris/blob/3.0.6.2/be/src/util/threadpool.cpp)会在初始化时立即创建最小线程数。设置 `cpus=2` 并不会把默认扫描线程数降为 2。

扫描器之前还会创建 SendBatch、预取、上传、关闭等池。[执行环境初始化顺序](https://github.com/apache/doris/blob/3.0.6.2/be/src/runtime/exec_env_init.cpp)能够解释为什么尚未进行业务查询就可能发生线程创建失败。扫描器之后仍有服务线程：[主程序](https://github.com/apache/doris/blob/3.0.6.2/be/src/service/doris_main.cpp)继续启动 Thrift、BRPC、HTTP 及心跳服务。因此修复 scanner 的第一处错误，不等于整个 BE 已有足够线程余量。

以下都是[锁定 tag 实际定义的参数](https://github.com/apache/doris/blob/3.0.6.2/be/src/common/config.cpp)。建议用于单 BE、小样本、分阶段实验；降低并发能力是明确的取舍，不能称为一般生产推荐。

| 参数 | 原默认值 | 候选值 |
|---|---:|---:|
| brpc_num_threads | 256 | 8 |
| be_service_threads | 64 | 8 |
| webserver_num_workers | 128 | 4 |
| doris_scanner_thread_pool_thread_num | 自动，至少 48 | 4 |
| doris_scanner_min_thread_pool_thread_num | 8 | 1 |
| doris_max_remote_scanner_thread_pool_thread_num | 自动，至少 512 | 4 |
| send_batch_thread_pool_thread_num | 64 | 4 |
| fragment_mgr_asynic_work_pool_thread_num_min | 16 | 2 |
| fragment_mgr_asynic_work_pool_thread_num_max | 512 | 8 |
| pipeline_executor_size | CPU 核数 | 2 |
| spill_io_thread_pool_thread_num | 自动，至少 48 | 2 |
| num_buffered_reader_prefetch_thread_pool_min_thread | 16，另按 CPU 计算 | 1 |
| num_buffered_reader_prefetch_thread_pool_max_thread | 64，另按 CPU 计算 | 4 |
| num_s3_file_upload_thread_pool_min_thread | 16，另按 CPU 计算 | 1 |
| num_s3_file_upload_thread_pool_max_thread | 64，另按 CPU 计算 | 4 |
| min_nonblock_close_thread_num | 12 | 1 |
| max_nonblock_close_thread_num | 64 | 4 |
| min_s3_file_system_thread_num | 16 | 1 |
| max_s3_file_system_thread_num | 64 | 4 |

上述最小值满足线程池构造的非负最小值、正数最大值及最小值不大于最大值约束。扫描数显式为 4 时，不再执行 `-1` 才触发的自动 48 下界；远端最大线程数会与本地扫描数取较大值，所以远端也设为 4、最小值为 1。参数合法只代表代码能够接受，是否足以完成实际导入、查询和 Checkpoint 恢复仍需实验。

还存在事务发布、删除位图、刷盘、压缩、快照等后台池。本次不根据名字随意把所有池降为 1；若有限样本验收仍触及线程限制，应保留失败位置与实际线程计数，再核查具体初始化路径。

## 下一轮必须记录的实际结果

1. 使用全新、明确声明 synthetic fixtures 的 epoch；保留原失败实验身份，不修改旧回执来冒充新配置。
2. 除原来的 JVM 堆和内存证据外，记录本 BE 的实际 `pids.max`、`pids.current`、`pids.events`、`doris_be` 进程 `Threads` 及容器 PIDs，分别在启动、导入、查询和恢复后检查。
3. 实际查询 `SHOW BACKENDS`，核验本次 BE 的 Alive、启动时间和错误状态；保留内存峰值、无 OOM、没有线程创建错误的证据。
4. 对每个改动的 C++ 配置使用精确名称的 `SHOW BACKEND CONFIG LIKE '<参数>' FROM <实际 backend ID>`，结果再检查 Key 与期望完全一致。该语句需要管理员权限，不能把管理凭据给浏览器。[语法说明](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/cluster-management/instance-management/SHOW-BACKEND-CONFIG/)
5. 重启同一个候选 BE 后再次核验实际参数，防止启动包装脚本的重复追加或镜像默认值覆盖。之后再进入原有 Flink/Doris 合成恢复与对账验收。

本次核查不宣称 256 的 pids 限额一定足够，也不自动提高它。若实际数据表明仍需增加，必须作为独立、有限的本项目配置调整记录，继续受 VM 内存和磁盘边界约束。

## 2026-09-19：补齐 BRPC 构造阶段及其后的启动审计

05 的失败位置是 AgentServer 工作池，06 在只提高 BE `pids_limit=512` 后已越过该位置，随后失败于 PInternalService 的 WorkThreadPool 构造。两者都没有进入本 fixture 的 SQL、Topic 或事件初始化；这些是失败实验，不能作为完整引擎容量验收。06 的配置与卷仍作为原代际保留，不修改旧 manifest 来套用新参数。

本轮按 BE 实际日志中的编译 commit `910c4249c521b5e68453336bf8859acd1c0c3a24` 重新取得下列 9 个官方源码文件，并与 tag `3.0.6.2` 的副本逐字节比较，结果全部相同。哈希记录在忽略目录 `runtime/doris-tuning-research/pinned-source-check.json`，文件只含公开源码：internal_service.cpp、doris_main.cpp、config.cpp、brpc_service.cpp、http_service.cpp、heartbeat_server.cpp、daemon.cpp、flight_sql_service.cpp、work_thread_pool.hpp。

[PInternalService 构造函数](https://github.com/apache/doris/blob/910c4249c521b5e68453336bf8859acd1c0c3a24/be/src/service/internal_service.cpp#L182)无条件构造三个工作池：heavy/light 默认各为 `max(128, 4 × cores)`，Arrow Flight 工作池默认 `max(512, 2 × cores)`。因此仅修复前两个池仍然会创建至少 512 个 Arrow Flight 工作者，三个池的默认合计至少 768。这里的 Arrow Flight 工作者和后面可关闭的网络监听服务是两件事。[WorkThreadPool](https://github.com/apache/doris/blob/910c4249c521b5e68453336bf8859acd1c0c3a24/be/src/util/work_thread_pool.hpp#L59)在构造时直接创建指定数量的线程，并非等到请求到来才创建。三个正数 4 都可以直接进入该循环，不触发自动计算分支。

07 候选将 `brpc_heavy_work_pool_threads`、`brpc_light_work_pool_threads`、`brpc_arrow_flight_work_pool_threads` 都显式设为 4，同时显式保留 `arrow_flight_sql_port=-1`。三个池合计 12 个工作线程；这不是整个 BE 进程的线程总数。保留前文已缩小的 19 项配置、JNI 256 MiB、BE 容器 1792 MiB 内存上限及 512 的 pids 限额；其他容器 pids 仍为 256。不扩大 VM 内存、磁盘或宿主资源硬边界。

[main 后续启动顺序](https://github.com/apache/doris/blob/910c4249c521b5e68453336bf8859acd1c0c3a24/be/src/service/doris_main.cpp#L524)已继续检查至进入主循环：BRPC server 使用已显式限制的 `brpc_num_threads=8`；HTTP 服务把已限定的 `webserver_num_workers=4` 交给 EvHttpServer；心跳 Thrift 使用锁定默认 `heartbeat_service_thread_count=1`；[Arrow Flight 网络服务](https://github.com/apache/doris/blob/910c4249c521b5e68453336bf8859acd1c0c3a24/be/src/service/arrow_flight/flight_sql_service.cpp#L116)在端口为 -1 时直接返回，不启动 gRPC；[Daemon::start](https://github.com/apache/doris/blob/910c4249c521b5e68453336bf8859acd1c0c3a24/be/src/common/daemon.cpp#L541)创建 9 个固定任务线程和至多 2 个条件启用的监控线程，未在这里按 CPU 派生大型工作池。这些代码范围内的自动池已覆盖，不把源码检查当作第三方库内部线程或整个进程峰值的证明。

新的实际验收会在 storage、TaskManager 恢复后、整个 session 恢复后分别读取每个冻结整数配置的精确 `/api/show_config?conf_item=...` 结果；结果键和值必须与配置一致。还会记录 cgroup `pids.current/max/events`、内存 current/peak/max、实际 BE 进程 Threads，只投影实际进程环境中的 `JAVA_OPTS` 与 `LIBHDFS_OPTS` 来验证两者的堆上限。不会打印整个环境。07 尚需实际通过这些读回和事件对账后，才可补记集成结论。

## 2026-09-19：07 存储阶段实际通过

`fixture-engine-07` 的 generation 为 `8f120c96-2a51-4ddf-b029-bf9cdce9c426`，原窗口从 `2026-09-19T05:28:41+00:00` 到 `2026-09-26T05:28:41+00:00`。模式始终为 `synthetic_engine_test`，输入来源说明为 `synthetic fixtures`。VM 保持 4608 MiB、29 GiB 薄置备盘，控制/计算 VM 关闭；没有扩大三项宿主资源边界。

最终 storage 使用源包 SHA256 `95a36f361b2d40a9eae5b0b40d552b6858a233aac8b0778295844f769dac5a9c`，JAR 仍为 `ef25755bf473610b47e62c4392b720c00d84fef23bb3ee5c567d7d21dbba0913`。FE/BE Alive、23 个冻结整数配置的逐项 API 读回、有效 JNI 256 MiB、启动脚本及二进制未发生大文件 copy-up 的检查均通过。该次 storage 监测的 BE pids 峰值为 379/512，没有额度命中；guest 可用 RAM 最低 2271 MiB、磁盘余量最低 2517 MiB，宿主项目实占最高 63.36 GiB。

精确七个 schema 对象、四张基础表全零、原角色与用户状态、四个新 Kafka 主题、输入主题 offset 0、主题身份和保留配置均经过实际检查。初始化先后遇到 CREATE ROLE 语法、SHOW FULL TABLES 四列结构和 PyMySQL `%` 主机参数替换的问题；各次失败证据与源码快照保留，同一代际的两段显式恢复分别绑定原失败、精确权限读回和新的脚本哈希，未重置期限或重建已有角色。原失败记录不改写为成功。

私有回执：`runtime/epoch-acceptance/fixture-engine-07-storage.json`、`fixture-engine-07-account-bootstrap-retry.json` 以及各次 `*-host-resources-attemptN.json`；VM 中的 `engine-test/storage-attempt-N/` 保留各次原始回执。此处只说明存储阶段，实时事件、TaskManager 和整个 session 的恢复验收仍需各自的实际结果。

07 随后的实时阶段通过了两个容器的 JAR 哈希和 Flink 会话就绪检查，但在作业主入口的时间解析处失败：锁定 Java 11 的 `Instant.parse` 不接受本次 `+00:00` 窗口表示。独立 `flink info` 重现同一 `RealtimeJob.main:185` 错误，REST 作业清单仍为空，临时 collector 从未启动；这不是已通过的 Kafka→Flink→Doris 验收。契约中已有兼容 Java 11 的 `EventContract.instant`（OffsetDateTime→Instant），新 JAR 应统一外部时间解析后重新冻结为新实验身份，旧 manifest/JAR/失败记录保持原样。

## 2026-09-19：08 实际实时链路与完整 session 恢复通过

Java 11 CI 通过后创建 `fixture-engine-08`，generation `cb56027a-17a0-4b98-ac04-d3f84c709c95`，原窗口 `2026-09-19T06:11:19+00:00` 至 `2026-09-26T06:11:19+00:00`。源包 SHA256 为 `57350a85aed1866d5e0e3cf72d051556c0c306c0bb8a1c14b216b87afc260b29`，新 JAR 为 `95dd8e0243fea1e403ad41842d68d003fed2b21c74dd54585cc4237dde108144`。BE 配置与07已通过的配置相同；未增加 VM 内存、磁盘或容器 pids 上限。

全新 storage 一次完成。随后20条合成事件通过临时 HTTP collector 持久化确认，进入独立 Kafka/Flink/Doris：网站 PV5/UV3，小吉 PV5/UV2、请求5、成功3，轻量聚合、独立计算和实时表整数完全一致。TaskManager 被明确 kill 后从 Checkpoint 恢复，重放不重复计数；迟到、重复、错误来源旁路及原始 accepted_at 时间戳检查通过，Doris 汇总发布与读回也通过。全部20条的 ack→首次查询完成 P95 为27.948470秒，包含本地受控TM故障、HTTP/轮询和查询开销，只代表这次合成小样本，不是线上生产 SLA。

进一步取消旧 Job `e5b8f9d894aaca1e211bfca694a26cb5`，冻结全5个subtask已ACK的 chk-6，停止全部五个引擎，然后启动全新 session。使用精确原 Checkpoint 路径和 `NO_CLAIM`，未启用允许丢失状态；新 Job `1c13fbf29ae828ec875ec9bde3fd93e0` 的 REST 恢复记录与原 checkpoint 身份相符。再次发送两个原事件后，duplicate 旁路数从3变5、整数指标保持不变，原有三份 Checkpoint 共59687字节的哈希集合保持，原到期时间未延长。

| 阶段 | guest可用RAM最低 MiB | guest空闲磁盘最低 MiB | BE pids峰值 |
|---|---:|---:|---:|
| storage | 2294 | 2466 | 368/512 |
| realtime及TM恢复 | 1550 | 2227 | 382/512 |
| 整个session恢复 | 1543 | 2221 | 376/512 |

三个阶段均无pids额度命中、资源监测中止或宿主硬门禁触发；宿主项目实占最高63.36GiB。最终五个容器全部停止、五个专属卷保留。可审查的无事件/凭据元数据汇总为 `runtime/epoch-acceptance/fixture-engine-08-acceptance-metadata.json`；`-08-session-pause.json`、`-08-session-resume.json`、各阶段host资源文件及VM内原始回执可逐项复核。这些结果证明锁定版本与小数据条件下该实验链路实际通过，仍不等同于真实生产数据、物理机容灾或更大规模容量验证。
