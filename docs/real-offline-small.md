# 小内存离线操作入口

本入口将既有 `real_lab` 的固定离线阶段串起来，使用 `real-small-1920`：control **2048 MiB**、compute **1920 MiB**、analysis **768 MiB**，三台合计 **4736 MiB**。它是显式的按阶段命令，没有 `all`，不启动实时引擎、Hive、Iceberg或治理服务，也不切换统计来源。

**验收状态：**新增入口已有本地合成单测；Windows 精确进程树终止已在本地测试。Linux `SIGHUP` 的原生进程组测试在 Windows 明确跳过，并已在源码 `46efeceb` 的 Linux CI `35438865825` 中通过。此前同资源配置的手工编排、合成 golden 和正式 ODS 落地结果属于已有独立证据，不能替代本入口的实际 VM 验收。本文件不声明新入口已实际启动 VM。

对应实现：[Windows/节点模块](../src/snow_statistics/real_offline_small.py)、[CLI](../tools/real_offline_small.py)、[合成故障测试](../tests/test_real_offline_small.py)。冻结的 `real_lab.py`、writer、恢复账本、生命周期及模型代码均继续使用原实现。

## 执行环境和前置条件

所有公开命令在 **Windows PowerShell** 的 `C:/Users/25685/Desktop/Myprojects/Snow_Statistics` 执行。配置文件为已准备的 `runtime/real/config/production.json`。它必须使用 `source=real`、`input_origin=real`、`transport_node=snow-analysis` 和实际登记的 epoch；此入口拒绝合成来源配置。

三台 VM 的固定地址、SSH 私钥、已固定的 host key、仓库、配置、`.venv`、镜像、Hadoop配置及 JAR 必须提前准备，并包含新增模块/CLI。三份配置的规范化内容哈希必须与 Windows 相同。入口不会联网安装包，不会复制数据库、原始事件或 writer registry，也不会复制凭据到新位置。

VM 的 Python 环境须已安装本仓库，而不是只安装依赖。由环境准备流程在对应 Linux checkout 使用 `.venv/bin/python -m pip install --no-deps -e .`，随后核验 import 指向当前 checkout。该命令是环境准备步骤，本入口不会隐式代执行。

开始离线前，先通过既有真实 writer 恢复流程完成 `pause-writer`，再停止整个 epoch 并软关机。必须保留已登记的外部 Checkpoint、source identity、冻结配置和 JAR。**不得手写 paused/success JSON，不能靠停止容器代替完成暂停账本。**

```mermaid
flowchart LR
    A[真实 writer 完成暂停并停止所有五个引擎] --> B[三台项目 VM 全部软关机]
    B --> C[固定 2048/1920/768 配置并仅启动 analysis]
    C --> D[核验 source/冻结 JAR/paused 账本/全部停止卷和容器身份]
    D --> E{有 capture 或 ODS 输入}
    E -->|无| F[返回暂无输入并软关 analysis]
    E -->|有| G[启动另两台 VM 和固定 NN/RM/DN/NM]
    G --> H[落地与实际生命周期检查]
    H --> I[按显式 run_id 计算、成对验证、私有发布]
    I --> J[停止项目服务并软关三台 VM]
```

## 查看计划与状态

```powershell
Set-Location 'C:/Users/25685/Desktop/Myprojects/Snow_Statistics'
& ./.venv/Scripts/python.exe tools/real_offline_small.py --help
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json start-offline --describe
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json status
```

`--describe` 只验证参数和配置，列出固定内存、门槛和路由，不连接 VM。`status` 读取 VM 状态；已停止的 VM 不会被启动。某个运行节点读取失败时，状态命令报错，不自动停止其他阶段。状态输出是资源/容器元数据，不是数据完整性回执，也不授予读取许可。

## 启动与 ODS 落地

```powershell
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json start-offline
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json land
```

`start-offline` 要求三台项目 VM **全部停止**，不接管混合运行状态。先配置三台，再启动 analysis，读取真实注册、完整 `paused` 账本与停止的五个引擎/卷身份。Checkpoint 的不可变元数据在这里校验；对 Checkpoint 文件逐字节重新哈希仍由既有恢复流程完成，这一步不宣称读取了停机卷文件。

若 analysis 没有待落地批次或 ODS 窗口，返回 `no_input`，不会启动 NameNode/YARN/DataNode，并软关刚启动的 analysis。因为输入元数据只归 analysis 持有，这个检查需要短暂启动 analysis；不把无输入说成计算得到了零指标。

`land` 仅处理已捕获的批次。没有 pending 批次时返回 `no_input`。它不自动同步、不捕获 Kafka、不确认 ACK；ACK 仍需后续按既有流程切回真实 storage 阶段后执行。不要复制或手改 ODS head/offset 文件。

## 显式计算与发布

以下例子中的 `accepted_closed_day_run` 必须替换为**已经过审核、已在 analysis 登记**的实际 run ID；示例不会创建作业。作业输入、闭合香港日期、统一截止时间、coverage 和输出 scope 都由既有真实作业校验器约束。当天尚未闭日时，不得把日期改成昨天或未来截止时间制造结果。

```powershell
$runId = 'accepted_closed_day_run'
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json prepare --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json cleanup
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json permit --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json stage-compute --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json daily --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json behavior --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json validate --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json publish-private --run-id $runId
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json stage-release --run-id $runId
```

`prepare/cleanup/permit` 仍调用实际生命周期检查。paused/stopped 检查本身不是 permit。若已登记 Hive 等后端当前不可用，这些操作会拒绝，不会把未运行的后端冒称 `not_initialized`。需要另行进入已批准的 Hive/Lake 协调窗口，不能在 analysis 768 MiB 上并发启动额外组件。

新增入口还会在 Windows 与实际执行 Linux 节点的新工作开始前，分别清理各自 `runtime/real/lake-authority` 中已登记的聚合副本，沿用原 90 日期限。发现未登记文件或清理失败时停止新读取/启动；这不是远端后端通过证明。`status`/资源探针保持只读，取消与停止不会被该清理失败拦住。

短期 permit 到期后必须重新执行实际检查和受控 metadata staging，不能改 JSON 时间。若作业已生成不可变输出、日志或 CID 后失败，保留现场，按既有恢复说明用新的显式 run ID；不要覆盖旧结果。`validate` 必须通过同输入、同截止时间的 daily/behavior 配对验证，才能执行私有发布与聚合复制。

`stage-compute` 只复制原工具允许的元数据；`stage-release` 使用既有受管理的聚合复制，保留原 90 日期限。这不是正式 Doris 发布。Doris 发布需要以后切回 storage 窗口，由 analysis 上的真实 writer publisher 执行；control 直连 Doris 仍关闭。

## 资源监测与停止

每次非状态工作在自身执行期间运行监测器：主机实占达到 **63.75 GiB** 即触发停止；硬上限仍为 **64 GiB**，主机空闲磁盘不少于 **35 GiB**、可用 RAM 不少于 **4 GiB**。每次启动额外预留 **256 MiB**；每个运行 guest 需要至少 **128 MiB MemAvailable** 与 **384 MiB 根文件系统空闲**，并检查容器 cgroup OOM 计数。

监测器通常每 5 秒发起一轮实际采样，探针时间计入间隔；SSH/主机探针都有超时，失败会关闭当前工作。它不是实时硬配额。三台 VM 的内存、容器限额和原生命周期仍是独立约束。

**命令返回后，本入口的监测线程结束。**不要把前一个 `start-offline` 成功当成后来手动脚本已经受到监测。后续计算必须继续使用受控阶段；若另跑专项合成验收，应由其独立监测器负责。工作结束立即停止，不把小配置作为长期常驻 profile。

```powershell
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json stop-offline
```

停止不受新增容量准入阻止，也不删卷、目录或历史。失败时先取消本次精确远端 worker/driver，再停固定项目服务并软关已确认归属的 VM。SSH 心跳丢失、SIGTERM/SIGHUP 会触发远端进程组收尾；Windows 用精确 PID 处理自身进程树，metadata/aggregate 传输直接使用固定 host key 的 SCP。

所有非 `status` 操作共用固定跨 lane 锁，因为三台 VM、Compose 项目和 Spark driver 名称是共享的。发现外来运行工作、同名异 owner、镜像/挂载不符或已观测容器 ID 被替换时，拒绝接管/关闭该 VM，保留失败回执。此时需要人工检查，不能以全局 `prune`、删除历史或降低门槛恢复。

## 回执与排障

本地回执：`runtime/real/offline-small/<attempt>/controller.json`。字段包括阶段、时间、采样次数、最后主机资源样本、确认归属节点、错误类型与 `cleanup_complete`。`cleanup_scope=none_admitted` 表示未获得任何资源所有权，不能读成“已停完原有服务”。

各节点保存同 namespace 的 worker 元数据和最多 1 MiB 的私有命令诊断日志。外层 `completed` 只表示冻结 CLI 正常退出；实际引擎、生命周期与发布回执仍在原受管理路径，不能用它代替后端读回。冻结 CLI 返回 `no_computable_input` 时会原样保留这一状态。数据行仍由既有 real 生命周期管理；这些诊断不作为模型/指标验收证据。不会打印 reader token，也不会读取聊天/业务数据库。

| 现象 | 处理 |
|---|---|
| `All three owned VMs must be fully stopped` | 先检查当前阶段并按其受控停止命令软关，不能混着调整内存 |
| 冷启动 SSH 255 | 入口最多等 60 秒；固定 host key、地址、VM工具需事先正确；不关闭 host key 校验 |
| 远端 venv/模块/配置拒绝 | 是环境或身份失败，不当成冷启动网络重试；先核对 checkout 和 editable 安装 |
| paused/冻结配置/存储身份不符 | 回到既有真实 writer 恢复流程，不能伪造 checkpoint/registration |
| `no_input` | 暂无可处理数据；不填零、不自动改 source、不调用模型 |
| 容量/内存/磁盘/OOM | 当前操作停止并保留进度；先检查回执，不能删除历史或放宽 64/35/4 门槛 |
| 私有 tunnel 端口仍在 TIME_WAIT | 等内核自然释放，确认不是实际 LISTEN；不杀别人的 listener，不改冻结 tunnel 逻辑 |
| `cleanup_complete=false` | 部分资源未知、未接管或收尾失败；按精确节点/attempt 检查，不继续发新计算 |

后续实际验收必须记录使用源码身份、VM 内存、主机增长、guest 最低余量、真实阶段退出结果和停止后读回；单测与 `--describe` 均不能替代这些证据。
