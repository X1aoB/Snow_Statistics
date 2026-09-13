# 真实引擎的有限运行周期

本目录提供候选机制，不能把配置文件当作 Kafka、Doris 或 Flink 已通过集成验收的证据。真正的物理回收验收必须运行下面的合成 fixture，并保存其 Docker 删除回读；真正的引擎接入还要分别完成健康、SQL/JAR、Checkpoint 恢复和汇总对账。

2026-09-13 已在仅 1 GiB RAM 的 analysis VM 实际完成两项物理回收验收：`fixture-physical-01` 模拟到期并执行实际 Docker 删除；`fixture-deadline-01` 使用约 45 秒真实时钟截止，PID1 实际退出 78，再执行到期回收。每项都验证五卷合成标记、精确容器和卷消失、已退休重启被拒绝。其他容器/卷名称清单保持不变，临时暂停的 DataNode 已恢复。私有元数据回执在 `runtime/epoch-acceptance/`，源包 SHA256 为 `648912390453232e91c6031b1313147bd2f2e1a3e05e453d7a16c2d0f0a83d30`。真实大引擎的启动与容量仍未由这些 fixture 证明。

一个 epoch 固定原始最早接收时间，截止时间严格为该时间加 7 天。复制、同步、重放、重启都不延长截止。每个 epoch 使用独立 Compose 项目和五个具备 owner/source/epoch/generation/expiry 标签的本地卷：Kafka、Doris FE、Doris BE、Flink Checkpoint、Flink 临时状态。容器的可写层和 Docker 日志也随容器移除。日汇总由已登记的 HDFS/本地发布包按 90 天保存，不放在 epoch 中作为唯一副本。

运行中，容器的 PID1 守护脚本到期终止引擎；`watch` 进程在截止时间运行精确物理回收。所有容器 `restart=no`；VM 关机后，下次启动必须先清理到期 epoch，再启动未到期配置。监督进程正常退出或 systemd 停止时只停止本项目容器，保留卷。开关关闭或手动 `stop` 均不删除数据。过期资源的清理失败会保留关闭的 gate，不能重启这个 epoch。

删除顺序为：读取全部精确容器/卷清单 → 验证全部标签、generation、禁止自动重启、固定截止守护和可写挂载范围 → 停止所属容器并回读 → 移除所属容器 → 移除所属 local 卷 → 再列举并确认这些精确名字均不存在。未知卷、外部卷驱动、可写宿主目录、匿名卷、被其他容器占用的卷以及不匹配标签都会阻断。没有 `prune`、`down -v`、全局 rm、业务服务器路径或线上 SQLite 清理接口。

停止读者单独验证精确容器归属；即使挂载审计失败，仍停止这些归属已核对的进程，防止关闭失败后继续读取。删除依然执行完整挂载审计。锁定 Kafka 镜像自动声明的 `/etc/kafka/secrets` 和 `/mnt/shared/config` 显式覆盖为 4 MiB/16 MiB 的 tmpfs，避免生成匿名卷；实际临时挂载范围与限制也需通过审计。

这是保守的整 epoch 回收：晚于 epoch 首次接收的本地明细也会随周期一起退休。需要保留的数据应在原始时间仍有效时从同一 ODS/线上窗口重新构建到一个新的、时间范围明确的周期；不能修改事件 `accepted_at`，不能把已过期的数据迁入新周期。线上已确认事件的 7 天保留期与轻量聚合完全独立，不受本地提前结束周期影响。

Docker 对象及卷不存在是可以回读的实际存储边界，不等于对 VMDK/SSD 残余扇区实施取证级擦除。不得把 VM 文件、卷目录、容器可写层另做未登记的备份；任何真实数据备份仍须进入原时间约束的保留期注册表。

## 最小物理回收 fixture

在 `snow-analysis` 中使用安装了本仓库依赖的 Python。以下命令只有 `fixture-*` 模式能调用模拟时钟；它创建真实 Docker 容器及卷，只写无身份的合成标记，不启动大数据引擎、不读取真实事件。Python 镜像取自现有 `lab/locks/images.env`。

```sh
umask 077
export PYTHONPATH="$PWD/src"
python tools/real_epoch.py --root "$PWD/runtime/real/epochs" prepare \
  --epoch fixture-physical-01 --fixture --original-at "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
python tools/real_epoch.py --root "$PWD/runtime/real/epochs" start --epoch fixture-physical-01
python tools/real_epoch.py --root "$PWD/runtime/real/epochs" expire-fixture --epoch fixture-physical-01
```

回执在该目录的 `retirement.json`。它绑定 owner manifest 的 SHA256、原接收/截止时间、实际处理的容器和卷、最终按资源类型分开的缺失回读清单，以及合成输入声明。删除失败应返回非零，不能生成完成回执；修复明确故障后可重跑同一命令。已完成回执重跑保持不变。尝试重启已退休 epoch 必须失败。

需要单独验证 PID1 真实截止时，可创建另一个 fixture，把原始时间设为当前时间减 7 天再加足够启动时间（例如 60 秒）；镜像提前存在后启动，等待实际截止再读取容器退出状态和 `cleanup` 回执。这和 `expire-fixture` 的模拟时钟测试应分别标注。

## 真正的引擎周期

`prepare` 去掉 `--fixture`，指定 `--analysis-ip` 和 `--jar warehouse/flink/target/snow-realtime-0.1.0.jar`。四类镜像取既有摘要锁，JAR 字节哈希也冻结在 manifest。配置生成到忽略的 `runtime/real/epochs/<epoch>/compose.json`；改动配置/JAR后不能直接重新启动，必须建立新周期。

此首版复用分析 VM 的标准引擎端口及先前小样本验收的 JVM 参数，各容器使用独立卷和名字。启动前要求其他分析 profile 的容器均停止；程序不会停止别的 profile。沿用原实时实验约 4.5 GiB 的分析 VM，先用 `start --stage storage` 启动 Kafka/FE/BE，验证健康和实际内存余量，再用 `start --stage realtime` 启动 Flink。首次存储阶段检查约 3.5 GiB 可用 RAM（为验收进程的 Python 模块导入预留 256 MiB；这是准入预算调整，尚不代表新峰值已测） 和 1.5 GiB 可用磁盘；新增实时阶段再检查 1.5 GiB RAM 和 256 MiB 磁盘。已经运行的阶段仅要求安全余量，不重新把完整初始预算计一次。

各容器上限沿用原实验：Kafka 640 MiB、FE 1280 MiB、BE 1792 MiB、JobManager 896 MiB、TaskManager 1536 MiB。上限之和不是实际峰值，不能据此要求大幅扩容，也不能把既有 synthetic 配置的结果当成本次 real epoch 已测容量。04 的实际失败验收已验证修补后的 BE 可写层仅 724,992 字节；新存储阶段仍保留 1.5 GiB guest 磁盘准入，必须在启动前另做宿主项目 64 GiB/空闲 35 GiB 等资源门禁，不能仅靠 guest 检查。fixture 只需要 64 MiB 容器限额。这里是小样本准入阈值和配置上限，不是磁盘硬配额；实际 OOM、内存余量或空间不足时停止当前阶段和扩样，保存资源测量后再调整。

首次实际引擎验收还必须核对锁定 Kafka 镜像中的 `/etc/kafka/docker/run`、Flink 的 `/docker-entrypoint.sh` 以及 Doris 原有入口在守护包装后正常运行。Flink 空集群启动不自动提交 JAR；提交前提供真实 Kafka cluster/topic identity、起始 offset、原时间可读窗口、到期禁止恢复时间及独立 Doris 用户，完成事件/诊断/迟到主题登记。缺少这些配置时 Java 作业会拒绝启动。不要把旧 synthetic 数据库凭据、Topic 或 Checkpoint 复制过来。

2026-09-13 的首次引擎验收实际发现：锁定 BE 镜像的 `start_be.sh` 无条件执行 `chmod 550`，使原镜像中已经是 755、大小 2,867,606,656 字节的 `lib/doris_be` 被完整复制到每个容器可写层。失败 fixture 的数据卷几乎为空，但 BE 可写层达到约 2.67 GiB。`prepare-be-start.sh` 只接受原脚本 SHA256 `d901a0cb6f13b3016d65fe3ccfdb3832a0dd88352f96cea99516bfc1076d906b`，把唯一一整行改成“不可执行时才 chmod”；修补后必须为 `dcb0d8265e282cc1deec46ac039ea913df0833b877fe2a703f7e314a65954107`。重启只接受这两个明确哈希，不改镜像摘要、启动参数或其余脚本内容；原二进制的大小、755/550 模式和可执行位都要先校验。修补脚本本身也冻结在 epoch manifest 中。实际 storage 验收另外读回容器内启动脚本哈希、二进制 mode/size 及可写层大小，不能以配置宣称节省已实现。

`snow-statistics-real-epochs.service.in` 中的 `@CHECKOUT@` 与 `@PYTHON@` 需替换为分析 VM 实际路径，再安装为独立 systemd 服务。真实周期启用前要验证该监督服务随开机运行，`ExecStopPost` 能停止自己所有 epoch，以及挂载/引擎进程故障时 gate 关闭。源代码模板没有自动安装系统服务。

逻辑表/Topic 级清理仍使用 `real_backend_lifecycle.py` 的实际后端检查，物理 epoch 回执是额外边界，不能以一个外部 JSON 的 `passed=true` 取代。Kafka 前缀删除、Doris DELETE 和 Flink TTL 都不单独证明引擎旧磁盘文件已经移除。

统一真实作业入口应先调用 `Epoch.readable()` 验证实际容器/卷、原始截止时间和完整阶段 gate，再调用逻辑后端的 permit。fixture 的 gate 明确不服务真实数据，不能用于真实读许可。epoch_id 最长 24 个小写字母/数字/连字符，事件 lane 固定由连字符替换为下划线得到，不截断、不用当前时间重命名同一代数据。

## 实际引擎的合成验收入口

`synthetic_engine_test` 是第三种、显式命名的验收模式：运行 Kafka、Doris、Flink 的真实进程，输入全部由程序生成；事件走 `source=real` 分支来检查真实分支的校验条件。manifest 和回执同时写明 `input_origin="synthetic fixtures"`，epoch 名必须以 `fixture-` 开始。它与上面的 64 MiB 物理回收 fixture 不同，也不能取得生产 `Epoch.readable()` 许可。不能将其中事件导入生产 collector、真实 ODS 或面向用户的统计接口。

运行前必须取得分析 VM 空闲交接，停止其他实验 profile，并执行宿主资源门禁。此脚本不会关停 DataNode、改变 VM 内存或清理旧实验。Python 环境需要仓库锁定的基础和 `lab` 依赖；JAR 必须是当前源码通过 Java 检查后产生的文件。以下命令是在分析 VM 内执行的模板，`ANALYSIS_IP` 必须取当前实际私网地址。新建的 fixture 名不能重复使用，准备阶段固定 JAR、入口脚本、配置哈希和原始截止时间。

```sh
umask 077
export PYTHONPATH="$PWD/src"
python tools/real_epoch.py --root "$PWD/runtime/real/epochs" prepare \
  --epoch fixture-engine-01 --synthetic-engine-test \
  --original-at "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" \
  --analysis-ip "$ANALYSIS_IP" --jar warehouse/flink/target/snow-realtime-0.1.0.jar
python tools/smoke_real_epoch.py --root "$PWD/runtime/real/epochs" \
  --epoch fixture-engine-01 --action storage
# 读取 storage.json，重新检查 guest 与宿主实际空间和内存余量，再进入下一阶段。
python tools/smoke_real_epoch.py --root "$PWD/runtime/real/epochs" \
  --epoch fixture-engine-01 --action run --events 20
```

存储阶段实际检查 Doris FE/BE 健康，创建唯一的 `snow_real_fixture_engine_01` 数据库、同名用户和独立角色，只授予该库的 SELECT/LOAD 权限，再用该用户读回空表。角色和用户语法依据 [Doris CREATE USER](https://doris.apache.org/docs/3.0/sql-manual/sql-statements/account-management/CREATE-USER/) 与 [Doris 权限说明](https://doris.apache.org/docs/3.0/admin-manual/auth/authentication-and-authorization)。四个主题分别为 `snow.real.fixture_engine_01.events.v1`、`late.v1`、`duplicates.v1`、`quarantine.v1`；后面三个沿用相同完整前缀。它们是单分区、有限保留期的新主题，初始输入 offset 必须为 0。已有同名数据库、主题或未完成的初始化文件会阻止直接覆盖。

实时阶段先校验两个 Flink 容器内实际挂载 JAR 的 SHA256，绑定 Kafka cluster/topic ID、起始位点、原接收时间窗口和截止恢复时间，再提交新作业。程序只启动一个回环地址的临时 collector，将 10–100 条合成事件经 HTTP 持久化确认后发送到新主题，记录原始 `accepted_at`。默认 20 条的手算口径为：网站 PV 5/UV 3；小吉 PV 5/UV 2、请求 5、成功 3。

程序等待第一批数据和后续 Checkpoint，明确杀死本 epoch 的 TaskManager，在故障期间接收第二批，再重放两条已接收事件并启动同一个 TaskManager。通过标准包括：实际恢复计数增加、恢复 Checkpoint 不早于记录位点、轻量库/独立口径计算/Doris 三者整数指标一致；随后在单独的负例范围向 Kafka 提交请求重复、事件 ID 冲突、迟到和错误来源样例，验证三个旁路主题以及原始接收时间戳，主指标保持不变。负例不伪装成 collector 已接收的唯一输入。最后用新数据库实际验证汇总发布与读回。

新鲜度回执覆盖全部唯一 HTTP 输入，报告包含受控 TaskManager 故障的本地合成 P95；明确标记 `freshness_is_production_sla_evidence=false`。正常结束和可处理失败均取消/停止本 epoch 读者并保留卷，不自动删除数据。硬中断时使用 `tools/real_epoch.py ... stop --epoch fixture-engine-01` 只停当前周期；需要重新验收时保留失败证据，创建新的 fixture 名。

新增资源均归该 epoch 所有，必须纳入精确资源清单：

| 资源 | 位置与处理 |
|---|---|
| 引擎和磁盘对象 | manifest 中五个精确容器、五个带完整标签的卷；关闭保留，到期按物理回收流程删除并读回 |
| SQL/主题/作业 | 新 fixture 数据库、用户、角色、四主题、记录的 Flink JobID；不复用旧 synthetic/real 对象 |
| 私有凭据 | `runtime/real/epochs/<fixture>/engine-test/secrets.json` 与 `job.env`，权限 0600；只在 VM 内使用，不输出、不下载到公共材料 |
| 临时轻量库 | 同目录 `local-collector/statistics.db` 及 WAL/SHM，预算 8 MiB，全部输入为合成；只绑定 127.0.0.1 |
| 检查和结果 | 同目录 `storage.json`、`run-in-progress.json`、`job.json`、`fixture-input.json`、`publication/`、`acceptance.json` 或 `failure.json`；保持合成来源说明 |
| 固定配置与入口 | epoch 根目录中的 manifest、compose、守护脚本和 FE/BE 配置；JAR 引用与 SHA 在 manifest 中；不得修改后原地重启 |

本入口截至编写时只通过本地约束和回环 collector 测试，实际大引擎运行、角色权限兼容性及此 profile 的资源峰值仍待分析 VM 验收；成功回执只能在这些实际步骤完成后生成。

05 候选缩小 Doris 3.0.6.2 中实际初始化的扫描、RPC、HTTP 和文件读写线程池，保留容器 pids_limit=256；生效依据为锁定版本 config.cpp。JNI 使用该版本 start_be.sh 实际读取的 JAVA_OPTS_FOR_JDK_17，保留原兼容 flag 并将堆上限设为 256 MiB；仅写 JAVA_OPTS 会被 JDK17 分支覆盖。实际 storage 验收检查 JNI 日志、pids.current、pids.events 及 BE 健康，配置本身不算通过。

合成引擎验收分 storage、run、resume 三步；run 在 TaskManager 故障、整数对账和三类诊断之后取消作业、冻结最终外部 Checkpoint 元数据与文件哈希，再停止所有本 epoch 引擎。resume 要求全体已停止、同代际/原截止和同 JAR，先启动存储再启动新 Flink session，使用精确 --fromSavepoint 与 --claimMode no_claim；实际 REST 必须确认新 JobID 从原 Checkpoint 恢复，重放两个原事件后诊断增加但整数不变。最终 acceptance 只有 resume 实测通过才设置 full_session_checkpoint_restore_verified。

05 实际运行已越过 ScannerScheduler，但 AgentServer 后续常驻 worker 仍触及 pids=256。06 仅将 BE pids_limit 调整为 512，其他容器仍 256；已有19项小线程池和 JNI256 保持，内存上限、guest磁盘准入和host三项硬门槛不变。storage实际读取pids.max并校验等于冻结配置，记录pids.current及pids.events，不能以新上限宣称容量已测。
