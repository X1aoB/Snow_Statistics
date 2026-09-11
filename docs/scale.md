# 受限资源下的 Spark / YARN 扩样

2026-09-11，在同一物理宿主的三台 VMware VM 上，**2 / 2 / 1 GiB** 配置实际完成 10 万及百万条合成事件计算。Spark driver 位于 control，唯一 executor 位于 compute；HDFS 两个 DataNode 位于 compute 和 analysis。先通过 56 行黄金回归，再扩到 10 万行。百万行第一次被 RAM 门禁阻止，宿主资源恢复后同配置完成，没有增加 VM 内存。

汇总收据见 [scale-100k.json](evidence/scale-100k.json) 和 [scale-1m.json](evidence/scale-1m.json)。全部输入为 synthetic，产品和公开接口未接入实验数据。轻量服务资源调整另见[低流量资源配置](resources.md)。

## 10 万条结果

| 项目 | 实测 |
|---|---|
| 引擎 / 应用 | Spark 3.5.7，YARN client，`application_1789131936007_0002` |
| 输入与质量 | 100,000 原始 = 90,000 有效 + 10,000 重复；隔离、截止时间后记录均为 0 |
| 日指标 | 14 行，PV 30,000、请求 10,000、成功 7,500；逐行匹配独立循环/集合基准 |
| 输入文件 | 4 个确定性 gzip 分片，共 5,693,277 bytes；HDFS 读回 SHA256 相同 |
| HDFS | 输入 4 个、输出 38 个非空块，均至少两个有效副本，fsck HEALTHY |
| Spark 应用时长 | **41.851 秒**，48 个 task 均成功，25 个 Spark job 均成功 |
| 任务计数 | executor run 合计 15.015 秒；CPU 11.710 秒；JVM GC 1.375 秒 |
| Spill / shuffle | 内存及磁盘 spill 均为 0；shuffle write 27,892,228 bytes |
| 输出 | Parquet 等结果共 10,203,441 logical bytes；路径 `/snow/warehouse/scale/runs/scale-100k-01` |

应用时长取事件日志的 ApplicationStart/End，包含执行器分配，不包含 HDFS 落地和 SparkContext 建立前的 JVM 启动。任务输入计数 200,104 行 / 201,463,974 bytes 包含重复扫描等工作，不能当作源数据条数。任务 `Peak Execution Memory` 最大值 44 MiB 只覆盖 Spark 报告的执行内存，不能写成进程峰值 RSS。每个规模只有一次运行，没有优化前后加速比，也没有持续吞吐结论。

## 百万条结果

`scale-1m-01` / `application_1789138789213_0001` 处理 1,000,000 原始记录，得到 **900,000 有效 + 100,000 重复**，隔离和截止时间后记录均为 0。14 行日指标逐行匹配独立基准：PV 300,000、请求 100,000、成功 75,000。

| 项目 | 实测 |
|---|---|
| Spark 应用时长 | **115.023 秒**，50 个 task、25 个 job 均成功 |
| executor 计数 | run 78.270 秒、CPU 69.337 秒、JVM GC 4.573 秒 |
| 内存 / 磁盘 spill | 累计 1,321,205,760 / 443,725,078 bytes |
| shuffle write | 254,317,685 bytes |
| HDFS 输入 | 4 个 gzip，56,914,743 bytes，SHA256 读回一致 |
| HDFS 输出 | 88,776,063 logical bytes；输入 4、输出 38 个非空块均双副本，fsck HEALTHY |
| 宿主监测 | 26 次采样，项目文件最大 59.67 GiB；磁盘最少空闲 48.27 GiB、RAM 最少可用 10459 MiB |

资源监测包装器总耗时 139.844 秒，包含检查、SSH、提交和结束验证，不能与 Spark 应用时间混用。Spark spill 是累计任务指标，不是同一时刻的磁盘占用；百万条在相同小内存下发生 spill 是这次实测取舍，没有扩大 executor。保留 34 次物理计划观察。NodeManager（含 AM/executor）cgroup 生命周期峰值 1,391,808,512 bytes，无 OOM；driver 自动移除，只保留运行中两次 peak-to-date 采样，没有完整峰值。cgroup 内存包含文件缓存，不等同 RSS。

本轮实际运行仍遵守原 **60 GiB** 上限，自动监测没有触发提前停止。全部结果导出后，计算/分析 VM 关闭，控制 VM 完成轻量配置测试后也关闭。当前本地资源上限随后按用户授权调整为 **64 GiB**，由 `deploy/resources.json` 统一管理；这不是将旧实验的越界运行改记为成功。

两种规模都没有运行 Hive metastore 或注册新表，没有将结果发布到 Doris，也没有做同规模 Flink 对账。既有 Hive / Airflow 模型 DAG 的小样本成绩保留在此前收据中，不能据此声称它们通过了本轮配置或规模。

## 资源与停止边界

| 节点 | VM 内存 | 本阶段服务与容器上限 |
|---|---|---|
| control | 2048 MiB | NameNode 512、RM 384、临时 driver 1280 MiB |
| compute | 2048 MiB | DataNode 384、NodeManager 1536 MiB（内部运行 AM 和 executor） |
| analysis | 1024 MiB | DataNode 512 MiB |

control 容器上限之和高于 VM 内存，实际服务不会各自预留全部上限；这仍是需要监测的共享边界，不能保证峰值负载可用。黄金回归期间观察到 control / compute 可用 770 / 800 MiB，driver 容器约 417.4 MiB；这些是采样值，没有进程峰值证明。

`scale` profile 将旧 `ods-compact` 的 VM 内存后备文件预算从 7.5 GiB 降到 5 GiB。Hive、Airflow、Kafka、Doris、Marquez 均停止，原有卷和数据保留；本轮没有删除数据、下载新镜像或压缩 VMDK。运行结束观察项目文件约 58.91 GiB、宿主磁盘空闲 50.04 GiB；关机后约 53.97 / 54.51 GiB。文件长度是保守预算近似，不等于底层精确物理分配量。

YARN 可分配内存 1280 MiB，最小分配 256 MiB。AM heap 256 + overhead 128 MiB，实际申请按最小单位取整为 512 MiB；executor heap 512 + overhead 256 = 768 MiB；一共 1280 MiB。driver heap 640 MiB。两端复用 252 个 SHA256 锁定的 Spark JAR。client 模式下 driver 位于提交端，AM 承担资源申请；设置依据见 [Spark 3.5.7 YARN](https://spark.apache.org/docs/3.5.7/running-on-yarn.html) 和 [内存配置](https://spark.apache.org/docs/3.5.7/configuration.html)。这些显式小值通过了本次测试，不是组件默认推荐值。

百万条第一次尝试时，控制 VM 已接收四个 gzip 文件共 56,914,743 bytes；落地前 `capacity(1024)` 检测宿主可用 RAM 仅 **740 MiB**，立即拒绝。停止本项目 VM 后一次 RAM 观察为 5895 MiB。随后恢复到 16469 MiB，重新过门禁才完成本页百万条结果。宿主其他应用的动态使用会改变余量，本项目没有修改或停止它们。

重新启动前，冷态可用 RAM 应至少约 **10 GiB**，覆盖 5 GiB VM、4 GiB 宿主余量及 1 GiB 作业检查，并考虑虚拟化额外开销。两项验证和轻量测试结束后冷态文件约 54.79 GiB，再加 5 + 1 ≈ 60.79 GiB，因此当前实验文件预算适度调整为 64 GiB；仍保留宿主 35 GiB 磁盘和 4 GiB RAM。每次重新检查实际余量，空间或内存不足就停止扩样。

## 按需重现

前置条件是按[离线手册](offline-pipeline.md)完成实验机、HDFS、镜像及 JAR 初始化，控制 VM 已安装本项目 Python 环境，56 行固定输入已在 HDFS。以下只适用于这三台专用实验机。先在关机状态配置各节点，逐个执行启动门禁；不要在资源不足时批量尝试开启。

```powershell
uv run python tools/vmware_lab.py status
uv run python tools/vmware_lab.py configure --node snow-control --profile scale
uv run python tools/vmware_lab.py configure --node snow-compute --profile scale
uv run python tools/vmware_lab.py configure --node snow-analysis --profile scale
# 确认冷态 RAM/磁盘余量后，对每个节点分别执行 start，并检查时钟。
uv run python tools/vmware_lab.py start --node snow-control
```

将最新索引源码用 `tools/bundle.py` 打包，通过受验证 SSH 分发并解包至各 VM 的 `/home/snow/Snow_Statistics`。不要上传 runtime 或密钥；输入文件另行复制。每台 VM 当前容器均应停止，再用 `lab_remote.py --node <节点> --script tools/start_scale_node.sh` 启动本阶段服务。该脚本渲染 `--scale-small` Hadoop 配置，使用固定镜像并禁止拉取和构建，复用已有 HDFS/YARN 卷。

首次扩样前执行 `lab_remote.py --node snow-control --script tools/run_scale_golden.sh --reserve-mib 1024`，将 `runtime/scale/golden-01.json` 下载归档。它检查本地与 HDFS 黄金输入 SHA256，随后实际计算；已有收据会拒绝覆盖。本机已完成这一步，不要覆盖保留的黄金结果。

```powershell
# 新环境只生成一次；已存在的本机目录保持不动。
uv run python tools/prepare_scale_fixture.py --events 100000 --output runtime/scale/input-100k-v1
```

将该目录的四个分片、`manifest.json`、`expected.json` 逐文件复制到控制 VM 同名 runtime 目录。准备本机 LF 脚本，内容如下，并通过 `lab_remote.py --reserve-mib 1024` 执行。百万条改参数为 `1000000`，对应目录 `input-1m-v1`。

```sh
set -euo pipefail
cd /home/snow/Snow_Statistics
bash tools/land_scale_input.sh 100000
```

落地脚本先验证分片字节数与 SHA256，不覆盖已有 HDFS 文件；已存在文件也逐一读回校验，确认双副本。随后在 Windows 提交带监测的单次计算：

```powershell
uv run python tools/run_scale_guarded.py --events 100000 --attempt scale-100k-02
```

每次使用新 attempt，避免覆盖已接受的结果。`run_scale_batch.sh` 在计算前重新检查输入 fsck，计算后检查质量、14 行日指标和输出双副本；指标基准由 Spark 作业逐行比对。黄金包、manifest 和输出 fsck 应一起归档；仅有计算包不能替代副本检查成功。

在本机持有已验证的 `runtime/scale/scale-100k-01.json` 后，生成器才允许百万条输入。本机两种输入和计算结果都已保留，不要重复生成；确需重跑时先校验落地，再使用新 attempt：

```powershell
# 仅新环境需要生成；当前输入已保留。
uv run python tools/prepare_scale_fixture.py --events 1000000 --output runtime/scale/input-1m-v1
uv run python tools/run_scale_guarded.py --events 1000000 --attempt scale-1m-02
```

`run_scale_guarded.py` 在提交前检查 1 GiB 作业余量，运行中每轮检查后间隔 2 秒采样。项目文件达到预算减 256 MiB（当前 63.75 GiB）、宿主 RAM 低于 4096 MiB、检查失败或超过 1000 秒就记录原因，停止专用 driver，再依次停止 compute、analysis、control 的指定服务和 VM。一次停止失败仍尝试后续节点，并把失败写进收据；发现失败需核查实际 VM 状态。成功或普通作业失败后仍需正常关闭 VM。该监测不是硬磁盘配额，无法阻止采样间隙增长。历史百万条运行的提前停止阈值为 59.75 GiB，未触发。

真实观察到了旧 preflight 拒绝、手动关闭和随后完整的百万条监测；“拒绝不启动”和“停止失败仍继续其他节点”通过模拟进程单元测试，自动停止分支尚未做真实故障注入。

## 原始证据与退出

Spark event log 位于 control 的 `runtime/scale/eventlogs/<application-id>`，容器以 root 写入。通过 sudo 复制为 snow 可读的独立导出文件，再用 `lab_remote.py --download` 下载到本机。以下命令提取汇总和独立计划文件：

```powershell
uv run python tools/summarize_spark_events.py --input runtime/scale/events-100k.jsonl --output runtime/scale/performance-100k.json
```

已保留 34 次 SQL 初始/自适应物理计划观察，原始日志与计划文件均有 SHA256。解析器拒绝不完整应用和遗漏/重复任务结束，不从日志导出环境属性。原始文件仅在忽略的 runtime，公开仓库保留汇总收据、输入哈希、代码哈希和可复现工具。收据分别记录本机实测文件字节哈希和 Git 规范化为 LF 后的代码哈希，避免 Windows 换行符导致复现核对歧义。

结束时逐台经 `lab_remote.py` 执行 `tools/stop_scale_node.sh`，再 `vmware_lab.py stop --node <节点>`，最后 `status` 确认没有运行的项目 VM。不要删除 HDFS、YARN 日志、数据库卷、输入或历史 Checkpoint。资源归属见 `deploy/resources.json`，退出方式见[移除手册](retirement.md)。
