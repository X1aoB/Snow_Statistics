# 独立真实聚合 Hive 入口

2026-09-19：实现和合成测试完成，**实际 Hive 引擎验收待离线窗口执行**。本轮没有操作虚拟机或生产服务器。不要把配置、单测或历史 synthetic Hive 结果当作这条新链路的实测证据。

## 范围与数据流

Hive 保存的是「表名对应哪份数据、有哪些列和分区」的目录信息。原始 Parquet 已由实际 Spark/YARN 作业计算并存放在 HDFS；本入口不重复写入数据，也不把匿名明细搬进另一个系统。

```mermaid
flowchart LR
  P[受管 daily + behavior 发布物] --> V[validate_real_pair / 原截止校验]
  R[远端生命周期登记] --> C[实际 HDFS / 已初始化后端清理]
  V --> C
  C --> H[Hive 外表登记与目录读回]
  H --> Q[Spark local 1 小聚合查询]
  Q --> E[与原发布物逐组哈希和行数一致]
```

只登记 `daily / session_daily / retention / funnel` 四组，指向已有 `ads_daily / session_daily / retention / funnel` 目录。会话、归因、匿名标识、请求级明细不进入这个入口。表名包含注册所有者和发布物的哈希，避免仅按 run ID 命名造成跨 lane 或代际冲突；完整哈希和 collector identity 仍在表属性内核验。

四组沿用原报表起始香港日期加 90 天的期限。登记、复制、重跑或服务重启不会延期。真实来源混入 synthetic、代际变化、表位置或分区位置变化、未知字段、原期限变化都会阻断。

## 文件与命令

- `tools/real_hive.py`：Linux 操作入口；读取与 `real_lab` 相同的严格配置，配置必须是私有 regular file，不可通过符号链接跳转。
- `src/snow_statistics/real_hive.py`：受管发布物验证、登记意图、生命周期适配器、固定子进程调用。
- `src/snow_statistics/real_hive_contract.py`：主机 Python 3.12 与 Spark 镜像 Python 3.8 共用的规范序列化和白名单。
- `src/snow_statistics/real_hive_spark.py`、`warehouse/spark/real_hive_catalog.py`：实际 Spark catalog/Parquet 操作；不接受任意 SQL。
- `tools/spark_hive_catalog.sh`：锁定镜像与 JAR 的临时客户端，结束或失败时仅收尾自身容器。

所有以下命令均在**配置指定的本地 Linux transport VM**，工作目录 `/home/snow/Snow_Statistics` 执行。生产 real 配置指定 `snow-analysis`；合成验收可使用已有明确标为 `synthetic fixtures` 的独立 transport 配置。不要在正式业务服务器运行，也不要在 Windows 直接调用执行阶段。

```bash
cd /home/snow/Snow_Statistics
.venv/bin/python tools/real_hive.py --help

# 替换成该 VM 上已有的私有 runner 配置；不是凭据正文。
.venv/bin/python tools/real_hive.py --config runtime/real/config/production.json cleanup

# RUN_ID 必须已计算、validate_real_pair 验收并正式进入受管私有发布物。
.venv/bin/python tools/real_hive.py --config runtime/real/config/production.json register --run-id RUN_ID
.venv/bin/python tools/real_hive.py --config runtime/real/config/production.json verify --run-id RUN_ID

# 给已 prepare 的新真实 Spark 作业颁发既有短期 permit；仍做全部后端检查。
.venv/bin/python tools/real_hive.py --config runtime/real/config/production.json permit --run-id NEXT_RUN_ID

# 只清理 Hive 到期目录引用，不读取行、不颁发读权限，也不声称 HDFS 已清理。
.venv/bin/python tools/real_hive.py --config runtime/real/config/production.json catalog-cleanup
```

生产模式的发布物必须先用既有 `real_lab stage-release` 路径转移到 analysis，位于 `runtime/real/transfers/<lane>/<run_id>/published/`，有原 90 日期限登记和哈希。合成候选使用 `runtime/real/publication/`。不允许传任意「成功 JSON」或选择未登记目录来跳过检查。

没有 ODS 输入且从未初始化生命周期时，入口明确返回 `no_computable_input`，不创建零值报表。已经有登记资源却丢失 ODS 头时会失败关闭；不会把丢失当作没有数据。

## 运行前置条件

1. 已有锁定 Hive 3.1.3 metastore、原元数据卷及 HDFS；沿用 `tools/prepare_hive_client.sh` 准备的客户端和 `lab/locks/hive-client.sha256`。本入口不会下载镜像或自动启动服务。
2. 未到期 epoch 先暂停真实同步和 writer，保留完整、合法的恢复状态；工具使用实际 `StoppedStorage` 检查容器/卷、源、窗口和 ledger。已到期 epoch 改走下述实际物理退役检查；不能以「服务停了」冒充未初始化或 SQL 清理通过。
3. 在运行客户端的 VM 腾出独占窗口。analysis 不允许其他运行容器；control 仅允许已有 NameNode 和 Hive。因此 analysis 的 DataNode 需要在确认其他副本可读后按既有服务管理流程暂停。不要为本工具盲目停止它，HDFS 副本不足时先解决可用性问题。
4. Shell 要求 `MemAvailable >= 768 MiB`。客户端硬限制内存 768 MiB、1 CPU、128 PID，JVM 堆 512 MiB、`local[1]`、shuffle 1；这些是候选配置，尚未实测证明在当前 VM 组合下足够。整体仍遵守 64 GiB 项目、35 GiB 宿主空闲和 4 GiB 宿主可用 RAM 门槛。
5. `lab/.env` 只按四个允许值解析，不作为 shell 执行。容器只挂载代码、锁、Hadoop 配置、Hive client 和本次 metadata request；不挂载 reader token、SSH key 或完整 runtime。临时目录使用受限 tmpfs，事件日志关闭，根文件系统只读。

这是阶段安排要求，不是新的常驻 profile。工具自己只运行临时 Spark 客户端，不自动停业务或其他实验容器，不改变现有 VM 容量。

## 受限容量下的 Hive-only 候选窗口

2026-09-19 新增 `vmware_lab.py configure --profile hive-only`：control 2048 MiB、compute 1024 MiB、analysis 1536 MiB，总计 4608 MiB。**当前仅新增配置、合成参数测试和本操作说明，尚未实际验证该组合下的 Hive、内存峰值和阶段恢复。** `real_lab.py` 的离线编排没有新增 Hive 阶段，不能把 `start-offline` 当作保留这个 profile 的操作；它会按自己的 scale / real-small 选择重新配置。

| VM | 此窗口允许的持久容器 | 容器上限 / JVM 配置 | 必须停止 |
|---|---|---|---|
| control 2048 MiB | 已有 NameNode、Hive metastore | NameNode 使用 control-scale 的 512 MiB / heap256；Hive 沿用 1024 MiB / `HADOOP_CLIENT_OPTS=-Xmx512m` | ResourceManager、Airflow、Kafka及其他控制面实验 |
| compute 1024 MiB | 已有 DataNode | 下方使用 compute-scale 的 384 MiB / heap192；不启动NodeManager | NodeManager、Flink及其他实验 |
| analysis 1536 MiB | 无 | 临时Spark Hive客户端 768 MiB / heap512、1CPU、128PID；每次请求前实际MemAvailable至少768 MiB | analysis DataNode、所有epoch引擎和治理服务 |

容器上限不是可相加的实测峰值；analysis剩余内存还要容纳Linux、Docker、Python协调器和监督进程。客户端本身会检查空容器窗口与可用内存，失败时停止，不通过减小门槛绕开。三台VM的4.5 GiB总内存与实时analysis单机相同，启动每台仍额外预留256 MiB，宿主项目64 GiB、空闲磁盘35 GiB、可用RAM4 GiB不变。

若某次观测项目63.42 GiB已经包含运行中analysis的4.5 GiB `.vmem`，那么在该文件确实随软关机消失后，冷态约为58.92 GiB，Hive-only重新启动后仍约63.42 GiB；这是容量推算，**必须以实际关机后的status重新检查**。剩下约0.58 GiB还要承受VMDK增长、日志、源包及临时文件，256 MiB预留不会制造更多空间。禁止删除历史证据或放宽门槛；缺失镜像/JAR时先算完整安装增长，不在这个余量里盲目拉镜像。

### 阶段图与输入条件

```mermaid
flowchart TD
    A[已验证且日期闭合的受管聚合pair] --> B[停止同步 正常pause真实writer]
    B --> C[双DataNode检查 所需块均在compute且可读]
    C --> D[停本项目服务 softoff三VM 保留所有卷]
    D --> E[hive-only配置 逐台带256MiB预留启动]
    E --> F[control NN+Hive compute仅DN analysis无容器]
    F --> G[实际cleanup register verify或私有view]
    G --> H[停精确服务 softoff]
    H --> I[恢复离线双DN 实际两副本读回]
```

输入必须已经通过 `validate_real_pair` 并按 `stage-release` 到达analysis受管发布目录。当前只有当天未闭合访问事件时，不能制造昨日零值、未来cutoff或空行为包来通过前置检查；等待符合当前模型要求的闭合输入，或先对明确的独立合成候选执行验收。

在双DN离线阶段，先对登记范围执行 `hdfs fsck`，确认所有本次Hive待读聚合和ODS清理所需元数据块在compute有副本、没有missing/corrupt blocks。Hive窗口保留两个DN原卷，但只运行compute DN，因此**不能声称这一阶段两副本在线**。如果某个必需块只在analysis上，就停止切换，不复制到未登记目录或修改副本计数凑条件。`land`要求两副本读回，不能放在Hive-only窗口执行；这里不跑YARN daily/behavior/Iceberg重计算。

### 1. 镜像、JAR与原元数据卷的只读准备

analysis上必须已有与 `lab/locks/images.env` 一致的 Spark 镜像、`runtime/hive-client` 中完整且匹配 `lab/locks/hive-client.sha256` 的JAR。Hive客户端使用`--pull=never`。`tools/prepare_hive_client.sh`会从锁定Hive镜像复制依赖；它可能新增大量文件或触发缺失镜像下载，不是本窗口可无条件执行的检查。先核对analysis是否已有缓存、实际大小和剩余预算，需要准备时另设受控安装步骤，不复制旧raw/Checkpoint或额外整盘快照。

control沿用已有的 `snow-lab-control_hive` metastore卷、`snow-lab-control_namenode`卷；compute沿用 `snow-lab-compute_datanode`。必须核对原容器挂载的精确卷名、持久位置及镜像摘要，不能在旧卷缺失时让Compose创建空元数据库并宣称原目录恢复。Hive仅保存表级元数据，原聚合Parquet仍在登记的HDFS路径。

### 2. 停止并配置三台项目VM

先在Windows仓库完成现有正常暂停路线；如果已经paused，不重复首次初始化。离线HDFS仍在运行时，先完成上述块检查，再用 `real_lab ... stop-offline --power-off` 停五个离线服务并软关三台VM。若仍处于实时阶段，先 `pause-writer`、`stop-epoch --power-off`，并确认另外两台没有残留任务。**所有服务已经正常停止，VM完全关机**后才能执行以下配置；不支持对运行或挂起VM直接改内存。

```powershell
# Windows PowerShell，Snow_Statistics仓库根目录
uv run python tools/vmware_lab.py status
uv run python tools/vmware_lab.py configure --node snow-control --profile hive-only
uv run python tools/vmware_lab.py configure --node snow-compute --profile hive-only
uv run python tools/vmware_lab.py configure --node snow-analysis --profile hive-only
# 每台成功后再次检查实际容量和状态；任一步失败不要继续启动其他节点
uv run python tools/vmware_lab.py start --node snow-control --reserve-mib 256
uv run python tools/vmware_lab.py start --node snow-compute --reserve-mib 256
uv run python tools/vmware_lab.py start --node snow-analysis --reserve-mib 256
```

这些底层VM命令不自动协调整组失败收尾；若第三台启动失败，检查并软关本次已启动的control/compute，不执行全局VM停止。只改本项目VMX的内存字段，不重建磁盘、seed、身份或epoch。监督服务开机应先处理到期副本，不能被禁用来取得Hive读取许可。

### 3. 只启动精确的已有服务

每段均在注明的Linux VM，工作目录 `/home/snow/Snow_Statistics`。先检查 `sudo docker ps --format '{{.Names}}'`，确认没有其他阶段容器；遇到不认识的进程先核实，不扩展停止范围。以下up使用明确服务、`--no-deps --no-build --pull never`，不会顺带启动全部batch profile或下载新镜像。

```bash
# control：沿用scale的NameNode限额，Hive沿用既有配置和卷
cd /home/snow/Snow_Statistics
test "$(hostname)" = snow-control
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.control.yaml -f lab/compose.control-scale.yaml \
  up -d --no-deps --no-build --pull never namenode hive
```

```bash
# compute：仅启动384MiB/heap192的DataNode；NodeManager继续停止
cd /home/snow/Snow_Statistics
test "$(hostname)" = snow-compute
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.compute.yaml -f lab/compose.compute-scale.yaml \
  up -d --no-deps --no-build --pull never datanode
```

analysis不执行Compose up。确认analysis没有运行容器、实际 `MemAvailable >= 786432 kB`，再确认control的Hive通过原Derby元数据库schema检查并监听9083、NameNode可读。由NameNode观察compute DN已live，且本次所有必需块可通过它实际读取；刚关掉analysis DN时不能只凭进程状态断言NameNode已经停止选择旧副本。读回失败保持关闭，等待确定的可读窗口，不改HDFS网络或保留策略。

### 4. Hive验收、私有看板和收尾

在analysis使用本页前文的 `real_hive cleanup`、`register --run-id`、`verify --run-id`。预期四组外表类型、位置、分区、期限和聚合哈希均实际一致；没有足够条件时失败，不把只读配置当成功。已有Hive登记后，`real_lab view --run-id` 的周期性生命周期检查也会运行同一临时客户端，因此只能在这个资源窗口打开；view本身不启动Hive或更改VM内存。

view使用宿主Python/Streamlit，有额外内存；1536 MiB analysis是否能在它运行时继续满足每次Hive客户端768 MiB可用门槛，需要单独实测。若不足，显示暂停，不能延长旧60秒读取许可或跳过catalog检查。可先完成CLI Hive登记/验证，私有view只在实际余量检查通过后运行。

停止view和临时客户端后，只停以下拥有的服务，不删除卷：

```bash
# control，/home/snow/Snow_Statistics
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.control.yaml -f lab/compose.control-scale.yaml stop hive namenode
```

```bash
# compute，/home/snow/Snow_Statistics
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.compute.yaml -f lab/compose.compute-scale.yaml stop datanode
```

回Windows逐台 `vmware_lab.py stop --node ...` 软关机并检查状态。然后才能按既有离线入口恢复scale/real-small，读回两台DN在线、原输入/聚合文件可读且两副本恢复。不能在compute仅1GiB时启动NodeManager，不能在analysis1536MiB时启动实时epoch；切阶段要先停再重新配置。整个过程保留Hive元数据、两份DN卷和所有历史实验，不提供prune或`down -v`。

Hive登记之后，后续cleanup/permit也需要实际catalog检查。现有Spark计算入口排斥control同时运行Hive，且768MiB analysis排斥Hive客户端，因此不能把Hive注册后继续重算简单拼接成一段“全部up”。应在Hive窗口完成相应实际清理/短期许可，再在其期限内受控切换所需计算窗口；任何绑定、登记或期限改变均重新验收。Iceberg正式registry由analysis持有而作业在control执行，尚需独立受控交接核对，不能复制一份可随意变更的registry到control绕过真实登记。

## 检查与失败行为

显式 `real-small-1792` 候选使用2048/1792/768 MiB，不改变Hive目录、表、原期限、Compose身份或control的768 MiB实际可用内存要求；客户端仍使用control已有镜像/JAR，analysis不复制或拉取镜像。Lake/Hive元数据检查只给compute新增1664–1792 MiB候选 `MemTotal` 带，原1800–2048带保留；control仍1800–2048，analysis仍640–768，所有guest的128 MiB实际可用余量保持。

这只是首次受控启动的准入范围，1792分配后的准确MemTotal及Hive/Iceberg峰值仍待实测。Windows统一入口和私有首次过渡工具必须显式选择同一profile，收尾也保持一致；不能只改VMX后用默认1920入口。冷启动须留足三台后备文件及128 MiB写入预算，见[小内存入口](real-offline-small.md)。已有1920 golden不能证明新候选通过，且不能扩大同时运行的服务。

`register` 先持久化元数据登记意图，再登记 Hive 生命周期资源；即使中断，重试也不会丢失待清理范围。未实际创建的预登记表可以暂时不存在；已经验证的存活表消失则报错。`verify` 不创建缺失表。

完整清理要求实际执行 HDFS/ODS 处理和所有已初始化后端检查。Hive adapter 验证外表类型、Parquet provider、固定原始列、source、所有者属性、表位置、每个分区的位置及日期。分区详情的两条 `Location` 分别解析，不能用整表位置掩盖分区外逃。查询结果与原发布物按规范 ASCII JSON 排序哈希比较，同时检查精确行数；null 留存和空聚合组保留原语义。

到期清理仅对登记且所有权一致的外表执行精确 `DROP TABLE`，随后读回不存在。它不使用 `PURGE / CASCADE / DROP DATABASE`，也不负责删除 Parquet。HDFS 原件由 `RealRemoteLifecycle` 按自己的登记范围删除并读回；两者各自验收。

登记和操作元数据保存在 `runtime/real/lifecycle/<lane>/hive-catalog.json` 与 `hive-last-operation.json`。回执只包含元数据、行数及哈希，不保存明细或完整聚合行。输入 request 绑定实际字节哈希和 10 分钟期限；只接受当前固定 Spark 3.5.7、`local[1]`、Hive 3.1.3 和指定私有 metastore 地址返回的结果。

权限边界是本工具的路径、字段、来源和动作白名单以及私有 VM 网络。当前共享 Hive metastore 不提供本工具专属 RPC ACL；不能把这些检查描述为 Hive 多租户访问隔离，也不能把 metastore 暴露到公开 Tunnel。

## 已验证与待实测

本地执行：

```powershell
# Windows PowerShell，Snow_Statistics 仓库根目录
uv run python -m pytest tests/test_real_hive.py tests/test_real_remote_lifecycle.py tests/test_real_publication.py -q
uv run ruff check src/snow_statistics/real_hive.py src/snow_statistics/real_hive_contract.py src/snow_statistics/real_hive_spark.py warehouse/spark/real_hive_catalog.py tools/real_hive.py tests/test_real_hive.py
& 'C:/Program Files/Git/bin/bash.exe' -n tools/spark_hive_catalog.sh
```

55 项合成及相邻回归测试通过（其中 Hive 新增 37 项），Ruff、CLI 帮助和 shell 语法检查通过。覆盖混源、代际、原截止、未知路径/字段、外表类型、分区位置、空组、非 ASCII 哈希、部分登记恢复、目录丢失、DROP 读回失败、缺失旧 Hive 适配、子进程回执伪造、超时先通知自有进程组收尾、配置禁止任意 shell 等。

下一实际离线窗口必须验证：客户端资源峰值和 Jar 锁、远端 metastore 确实使用原持久卷、四组实际外表与已验收发布物一致、重启后注册保持、受控合成到期样例的目录/数据分别清理、失败后没有遗留读进程。保存真实 engine/application ID、配置/版本和输入来源，不沿用历史 synthetic 证据替代。

## 实时存储退役后的聚合读取

生产 epoch 的物理 7 日截止先于聚合 90 日期限。`real_retired.py` 已补齐这两个期限之间的代码路径：验证原 manifest、generation、初始化和作业登记、collector/Kafka 身份及历史文件哈希记录，调用实际 `Epoch.retire()` 精确删除或再次验证原容器/卷不存在，再扫描 Docker 全部容器和卷，拒绝同名重建、同 epoch 别名、标签或挂载旁路。历史核验不依赖旧 JAR 仍在磁盘上，不使用虚假时间调用存活 writer 的 `ready()`。

三个已初始化后端保留完整原登记，以 `retired_storage_absent` 回执证明物理范围已经消失。回执明确不给 writer 恢复、Checkpoint 恢复、原始计算或在线 SQL 清理声明。缺少登记、删除失败、回执被改写或资源重现时全部失败关闭。`RealRemoteLifecycle.issue_permit()` 即使发现新预留的 raw 路径仍未到期，也拒绝为这种退役状态签发计算许可。`catalog-cleanup` 仍只处理 Hive 引用，独立调用它不能获得读取许可。

HDFS/Hive 聚合和传输后的本地副本继续分别按各自原 90 日期限清理。私有看板经 `real_aggregate_read.py` 执行实际完整清理、受管发布物校验和已登记聚合路径检查后，才获得仅供当前进程使用、最多 60 秒的聚合读取许可。其期限同时受最早远端到期时间、本地目录最早副本期限和当前聚合原期限约束；页面每秒重判当前 lane 与发布目录的登记哈希、所有者和失败 gate/journal，发生变化立即丢弃缓存。这一步只读小型元数据文件，不扫描全仓库。资源阶段不可用时显示暂停和上次已验证截止，不自动启动 VM，也不填零；公开基础统计页不依赖这些检查。

本轮 Hive、退役适配、看板准入及相邻恢复/生命周期回归共 **164 项合成测试通过**，Ruff 和两个 CLI 的帮助检查通过。实际 Docker 退役、Hive catalog 与私有看板联合运行仍待后续离线窗口验收；合成替身不构成引擎验收回执。

## 官方版本依据

[Spark 3.5.7 Hive 表与 metastore 客户端](https://spark.apache.org/docs/3.5.7/sql-data-sources-hive-tables.html)、[DESCRIBE TABLE 文档](https://spark.apache.org/docs/3.5.7/sql-ref-syntax-aux-describe-table.html)、[该版本 DescribeTableCommand 源码](https://github.com/apache/spark/blob/v3.5.7/sql/core/src/main/scala/org/apache/spark/sql/execution/command/tables.scala)、[DROP TABLE 语义](https://spark.apache.org/docs/3.5.7/sql-ref-syntax-ddl-drop-table.html)用于核对接口和外表边界，不能替代实际引擎验收。
