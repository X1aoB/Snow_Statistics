# 真实引擎的有限运行周期

本目录提供候选机制，不能把配置文件当作 Kafka、Doris 或 Flink 已通过集成验收的证据。真正的物理回收验收必须运行下面的合成 fixture，并保存其 Docker 删除回读；真正的引擎接入还要分别完成健康、SQL/JAR、Checkpoint 恢复和汇总对账。

一个 epoch 固定原始最早接收时间，截止时间严格为该时间加 7 天。复制、同步、重放、重启都不延长截止。每个 epoch 使用独立 Compose 项目和五个具备 owner/source/epoch/generation/expiry 标签的本地卷：Kafka、Doris FE、Doris BE、Flink Checkpoint、Flink 临时状态。容器的可写层和 Docker 日志也随容器移除。日汇总由已登记的 HDFS/本地发布包按 90 天保存，不放在 epoch 中作为唯一副本。

运行中，容器的 PID1 守护脚本到期终止引擎；`watch` 进程在截止时间运行精确物理回收。所有容器 `restart=no`；VM 关机后，下次启动必须先清理到期 epoch，再启动未到期配置。监督进程正常退出或 systemd 停止时只停止本项目容器，保留卷。开关关闭或手动 `stop` 均不删除数据。过期资源的清理失败会保留关闭的 gate，不能重启这个 epoch。

删除顺序为：读取全部精确容器/卷清单 → 验证全部标签、generation、禁止自动重启、固定截止守护和可写挂载范围 → 停止所属容器并回读 → 移除所属容器 → 移除所属 local 卷 → 再列举并确认这些精确名字均不存在。未知卷、外部卷驱动、可写宿主目录、匿名卷、被其他容器占用的卷以及不匹配标签都会阻断。没有 `prune`、`down -v`、全局 rm、业务服务器路径或线上 SQLite 清理接口。

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

此首版复用分析 VM 的标准引擎端口及先前小样本验收的 JVM 参数，各容器使用独立卷和名字。启动前要求其他分析 profile 的容器均停止；程序不会停止别的 profile。沿用原实时实验约 4.5 GiB 的分析 VM，先用 `start --stage storage` 启动 Kafka/FE/BE，验证健康和实际内存余量，再用 `start --stage realtime` 启动 Flink。首次存储阶段检查约 3.75 GiB 可用 RAM 和 1.5 GiB 可用磁盘；新增实时阶段再检查 1.5 GiB RAM 和 256 MiB 磁盘。已经运行的阶段仅要求安全余量，不重新把完整初始预算计一次。

各容器上限沿用原实验：Kafka 640 MiB、FE 1280 MiB、BE 1792 MiB、JobManager 896 MiB、TaskManager 1536 MiB。上限之和不是实际峰值，不能据此要求大幅扩容，也不能把既有 synthetic 配置的结果当成本次 real epoch 已测容量。首次新 FE/BE 可写层预计还需要约 2 GiB 空间，必须在启动前另做宿主项目 64 GiB/空闲 35 GiB 等资源门禁，不能仅靠 guest 检查。fixture 只需要 64 MiB 容器限额。这里是小样本准入阈值和配置上限，不是磁盘硬配额；实际 OOM、内存余量或空间不足时停止当前阶段和扩样，保存资源测量后再调整。

首次实际引擎验收还必须核对锁定 Kafka 镜像中的 `/etc/kafka/docker/run`、Flink 的 `/docker-entrypoint.sh` 以及 Doris 原有入口在守护包装后正常运行。Flink 空集群启动不自动提交 JAR；提交前提供真实 Kafka cluster/topic identity、起始 offset、原时间可读窗口、到期禁止恢复时间及独立 Doris 用户，完成事件/诊断/迟到主题登记。缺少这些配置时 Java 作业会拒绝启动。不要把旧 synthetic 数据库凭据、Topic 或 Checkpoint 复制过来。

`snow-statistics-real-epochs.service.in` 中的 `@CHECKOUT@` 与 `@PYTHON@` 需替换为分析 VM 实际路径，再安装为独立 systemd 服务。真实周期启用前要验证该监督服务随开机运行，`ExecStopPost` 能停止自己所有 epoch，以及挂载/引擎进程故障时 gate 关闭。源代码模板没有自动安装系统服务。

逻辑表/Topic 级清理仍使用 `real_backend_lifecycle.py` 的实际后端检查，物理 epoch 回执是额外边界，不能以一个外部 JSON 的 `passed=true` 取代。Kafka 前缀删除、Doris DELETE 和 Flink TTL 都不单独证明引擎旧磁盘文件已经移除。
