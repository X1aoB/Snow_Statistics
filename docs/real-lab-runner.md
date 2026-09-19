# 按需运行入口：明确阶段，不提供 all

入口为 tools/real_lab.py，配置模板为 deploy/real-run.example.json。Windows 负责虚拟机资源和受控 SSH 调用；Linux VM 负责同步、落地、生命周期及计算。配置没有任意命令、SQL、shell 或环境变量透传字段。命令不安装或修改正式服务器，只能在显式 sync 阶段建立配置过的 SSH 端口转发并读取私有事件。

本入口已有合成单元测试和 CLI 检查，**尚不代表统一入口全链路已在虚拟机验收**。已有独立工具的引擎实验回执仍按原入口与源码版本解释。

## 运行位置与前置条件

所有 Windows 命令在 C:/Users/25685/Desktop/Myprojects/Snow_Statistics 执行。Linux 命令在对应 VM 的 /home/snow/Snow_Statistics 执行，使用该目录已有的 .venv/bin/python。三台 VM 必须已有当前源码、镜像锁、生成后的 Hadoop 配置和原 SSH 主机指纹，不能用关闭 host-key 检查解决 SSH 错误。

1. 将模板复制为忽略目录中的 runtime/real/config/run.json，修改三台 VM 的真实 NAT IP。不要照抄示例 IP。
2. lane 同时绑定本次 real epoch 名称、ODS 目录和主题。主题中的 lane 将横线变成下划线，绝不截断。真实数据的 transport_node 固定为 snow-analysis；只有明确的 fixture-* 合成验收允许使用 control 的旧测试 Kafka。
3. 通过现有部署方式把相同配置文件放在三台 VM 的相同相对路径。先在 VM 创建 runtime/real/config，再使用下方 SCP 命令复制配置，不复制密钥。
4. 在 transport VM 独立保存 reader token、SSH key、known-hosts 和后端私有连接配置；文件须为常规文件且权限为 0600。令牌以纯文本单行保存，不进入命令行或 Git。SSH known-hosts 的别名为 snow-statistics-collector，须由已核验的正式服务器主机指纹建立。
5. 真实 epoch 必须已通过 tools/real_epoch.py prepare 创建并部署到期监督程序。统一入口复用已有 epoch 的启动和停止，不自动创建 Topic、Doris 账号/表、证书、Tunnel 或正式服务器路由。
6. 首次真实同步前须完成实际生产初始化及作业读回登记。writer-registration.json 绑定空 Topic/数据库/状态目录的实际检查、collector 代际、Docker 对象、冻结 JAR 和源码；writer-job.json 绑定实际运行的 Flink JobID、完整原始时间窗口及输入身份。文件由独立的生产初始化代码调用 WriterRegistry 产生，不能手写一个成功标志。synthetic_engine_test 的引擎证据不能代替此登记。

在 Windows 复制配置，替换节点占位符：

    uv run python tools/lab_remote.py --node <节点> --upload runtime/real/config/run.json --remote /home/snow/Snow_Statistics/runtime/real/config/run.json

先检查命令与配置，不启动服务：

    uv run python tools/real_lab.py --help
    uv run python tools/real_lab.py --config runtime/real/config/run.json sync --describe
    uv run python tools/real_lab.py --config runtime/real/config/run.json status

--describe 只验证 schema 并显示执行节点，不证明连接或后端通过。Windows status 调用实际 VMware 资源检查；Linux status 读取本地位点和 gap 标记，不读取真实事件。

### 低流量离线配置

默认 `start-offline` 保持历史 `scale` 的 2048/2048/1024 MiB。对于当前个位数真实事件的小批次，新增显式选项：

```powershell
# Windows PowerShell；先完成 writer 暂停和三台 VM 软关，不能修改运行中的 VM。
# 工作目录 C:/Users/25685/Desktop/Myprojects/Snow_Statistics
uv run python tools/real_lab.py --config runtime/real/config/production.json start-offline --offline-profile real-small
```

`real-small` 仅把 analysis 调整为 768 MiB，控制/计算仍各 2048 MiB；每台启动额外保留 256 MiB 作业余量，64 GiB 项目、35 GiB 宿主磁盘和 4 GiB 宿主可用 RAM 门禁不变。任一启动门禁失败，收尾本轮此前已启动的 VM。此选项不适用于实时存储阶段。

容量依据是 2026-09-11 百万条合成实验中 analysis DataNode 峰值 371.6 MiB、1 GiB VM 可用内存 428 MiB。它支持一次受监测的小配置验证，**不是已经完成 768 MiB 配置的证明，也不是承诺更大输入可用**。本轮应先以合成小样例验收同一配置，再接实际输入；每阶段记录余量、实际两副本与失败收尾。768 MiB analysis 不同时运行 Doris、Flink、Hive 客户端或治理。Hive/私有看板需要单独核验自己的内存窗口。

## 阶段表

| 阶段 | 实际执行位置 | 做什么 |
|---|---|---|
| start-offline | Windows → 三台 VM | 对已关机 VM 配置 scale 2/2/1 GiB，启动精确 NameNode/ResourceManager/DataNode/NodeManager |
| stop-offline | Windows → 三台 VM | 只 stop 上述服务；加 --power-off 后软关闭三台本项目 VM |
| start-storage | Windows → analysis（real） | 正式模式仅 analysis 4.5 GiB，启动已登记 epoch 的 Kafka/Doris 存储；只有显式合成 control 传输配置才额外启动 control 2 GiB |
| start-realtime | analysis | 启动同一 epoch 的 Flink 阶段；此前必须已验证存储阶段 |
| initialize-writer | analysis | 创建独立账号、精确四 Topic/四物理表后实际读回空库、空状态及 collector 身份，登记不可变 writer |
| submit-writer | analysis | 从冻结 JAR 实际提交首个 Flink 作业并读回 JobID/状态，登记本次受控提交参数 |
| pause-writer | analysis | 等当前同步批次完成后取操作锁，实际取消作业并冻结最后外部 Checkpoint 的文件哈希，再停止该 epoch |
| resume-writer | analysis | 同一 epoch 先启动原存储与 Flink，再从登记的精确 Checkpoint 恢复；实际读回新 JobID、RUNNING 及恢复路径后追加回执 |
| writer-status | analysis | 读取受保护 collector 身份并检查两份 writer 登记；不会修改或重建成功标记 |
| stop-epoch | analysis | 只停止该 epoch；正式模式 Windows 加 --power-off 后只软关闭 analysis，合成 control 传输配置才同时关闭 control |
| sync | transport VM | 受控回环 SSH 转发、reader token 私有读取、原始校验归档、Kafka ACK 后推进位点 |
| capture / land / ack | transport VM | 分别冻结 Kafka 范围、写入 HDFS 并回读、最后推进 Kafka 位点 |
| prepare | transport VM | 校验指定 job、ODS 和 writer 代际，登记 Kafka 四 Topic、Doris 四物理表、两个状态卷和全部预期 HDFS 输出 |
| cleanup / permit | transport VM | 实际清理 HDFS；未到期的停止 epoch 走实际存储准入，到期 epoch 走实际物理退役核验；退役后只允许有效聚合读取，禁止计算 permit |
| stage-compute | Windows | 从 transport 下载指定 job/coverage/permit/receipt/ODS state，再把经过校验的元数据复制到 control |
| stage-release | Windows | control 导出已验证聚合，Windows 校验和中转，analysis 在登记原到期时间后接收并读回成对发布物 |
| daily / behavior | control | 每次先验证短期许可，用 scale Spark 参数执行该模型；写入已登记的本地输出和日志 |
| validate | control | 读取前清理本地到期文件，验证两个模型为同一来源、快照、日期与截止时间 |
| publish-private | control | 发布经过验证的私有不可变聚合及元数据指针 |
| publish-doris | analysis | 调用 tools/real_writer.py publish，由实际 epoch/writer 检查包围每次 SQL；正式 control 直连保持禁用 |
| view | transport VM（real 为 analysis） | 必须指定 --run-id；实际完整清理及受管聚合校验后在 127.0.0.1:8502 展示，需另行通过受控隧道访问 |

start-* 对运行中的 VM 不强制改内存。切换阶段前须显式停止原服务并软关机。启动失败只尝试软停止本次已经启动的 VM；不会硬关其他 VM。单项停止失败会继续尝试其余节点，并返回失败状态供检查。

正式 start-storage 仅启动 4.5 GiB analysis，私有同步和 Kafka/Flink/Doris 均在该 VM；此阶段不额外启动 2 GiB control。离线阶段才启用三台 scale 2/2/1 GiB VM。合成验收若显式把 transport 设为 control，则存储阶段仍需 control/analysis。每次启动照常检查 64 GiB 项目、35 GiB 宿主空闲磁盘及 4 GiB 宿主可用 RAM，不因减少节点而跳过门禁。

## 传输与计算的顺序

首次必须在完整 epoch 启动后依次运行 initialize-writer、submit-writer，再运行 sync 和 capture。capture 没有推进 Kafka 位点，其原始批次留在 transport VM 的 runtime/real/ods/<lane>。正常停机使用 pause-writer，成功后再软关机并切换到离线 HDFS 阶段运行 land。最后恢复需要的 Kafka 阶段运行 ack。必须保留 transport VM 原目录，不把未确认批次当作已完成。已有 writer 不重复初始化或覆盖首次 JobID；下次同 epoch 使用 resume-writer 追加恢复登记。

### 正常暂停和同代恢复

以下是 Windows 控制入口。全部源码和 JAR 必须与首次登记时完全一致，且 epoch 原始 7 天截止尚未到达。先让当前 sync 命令正常结束，然后执行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json pause-writer
    uv run python tools/real_lab.py --config runtime/real/config/run.json stop-epoch --power-off

pause-writer 与同步整个 Kafka 批次共用非阻塞进程锁；若已有同步仍在运行，暂停立即返回锁冲突，**不会提前停止其 Kafka/Flink**。暂停首先追加 pause_requested，同步逐行校验随即关闭。它实际检查当前登记作业、同一存储对象和 JAR、外部 Checkpoint 的保留配置及完成状态，取消该作业后重新读取最终 Checkpoint。元数据只记录原 JobID、Checkpoint ID/精确路径、生成时间、文件大小和 SHA-256；不复制状态文件的内容。整个 /checkpoints 卷只允许首次及历史恢复登记的 JobID 目录，包含 shared 和 taskowned；拒绝符号链接、硬链接、未知目录、缺失文件及超过 512 文件/128 MiB 的状态。本界限是初版小数据操作上限，不是大状态恢复性能结论。

若切换到离线阶段，start-offline 会拒绝尚未完整暂停的真实 writer。完成离线操作并软关闭 VM 后，按原资源阶段重启：

    uv run python tools/real_lab.py --config runtime/real/config/run.json start-storage
    uv run python tools/real_lab.py --config runtime/real/config/run.json start-realtime
    uv run python tools/real_lab.py --config runtime/real/config/run.json resume-writer
    uv run python tools/real_lab.py --config runtime/real/config/run.json sync

resume-writer 实际重新哈希文件，校验仍是原容器/卷/collector 代际以及原到期时间，确认没有未知或正在运行的作业，随后使用 `--fromSavepoint <登记路径> --claimMode no_claim` 提交同一 JAR。它不提供 `--allowNonRestoredState`，也不接受用户填写 JobID、Checkpoint 路径或成功 JSON。只有实际新 JobID 已 RUNNING、REST 的 latest.restored ID 和路径与登记 Checkpoint 一致时，才追加 resumed 并重新开放同步。文件、恢复、重放均继承原 epoch 截止，不能续期；到期由实际 epoch 清理流程删除精确拥有的卷并回读。

首次 writer-job.json 永不覆盖。writer-recovery/ 内依次追加 pause_requested、paused、resume_requested、resume_acknowledged、resumed，每条包含前条哈希，并以原初始化及首次作业回执为链起点。记录缺失、多余临时文件、哈希不符或中断的提交都会关闭同步、离线准入和 Doris 发布。完整 paused 允许离线计算及聚合发布，不能继续同步。已收到新 JobID、但尚未完成 REST 读回时中断，当前版本会保留 ACK 证据并阻断再次提交，需要检查实际状态；不会悄悄采用未知 JobID，也不会把首次提交重做为恢复。强制 stop-epoch 是停止手段，本身不制造可恢复回执；正常操作应先 pause-writer。

Flink 1.20.3 的保留 Checkpoint 与恢复方式依据[官方 Checkpoint 文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints/)，精确 no_claim CLI 选项依据[固定版本参数定义](https://github.com/apache/flink/blob/release-1.20.3/flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontendParser.java)。既有合成引擎实验曾观察到：原 checkpoint.is_savepoint=false，而经 `-s` 加载后 REST restored.is_savepoint=true；恢复器保存该实际标志，按旧 Checkpoint 的精确路径、ID、哈希及受控提交证明来源，不把此标志单独当作来源判断。这些旧结果不代替本次新生产恢复入口的引擎验收。

每个阶段都可以从 Windows 用同一入口调用，例如：

    uv run python tools/real_lab.py --config runtime/real/config/run.json sync
    uv run python tools/real_lab.py --config runtime/real/config/run.json capture

上面只是当前存储阶段的两条命令，不能接着盲目启动所有服务。land 需要实际 HDFS 可用，ack 需要实际 Kafka 可用。空 Kafka、缺少已提交 ODS 或空数据窗口返回 no_computable_input，不创建零值统计。

## 作业、许可和输出

先参照 docs/real-lifecycle.md 创建 runtime/real/jobs/<run-id>.json 和 runtime/real/coverage/<run-id>.json，在 transport VM 保存。job 的 input 必须是已提交 ODS state 中的精确 input；warehouse_root、auxiliary_root 必须属于配置的 IP 和 lane。coverage_file 与 permit_file 必须都使用 <run-id>.json。

初版统一入口只接受 register_hive=false、auxiliary_file=null。已有辅助状态的重写和继续计算仍使用 docs/real-lifecycle.md 中的登记路径；入口不会略过辅助清理。coverage 的接收覆盖起点来自受控同步证据，不能用事件发生日期倒填。

聚合 Hive 登记使用独立的 [real_hive 入口](real-hive.md)，不打开旧作业的 inline register_hive 开关。一旦已有 Hive 登记，统一 cleanup/permit 会加入实际 Hive catalog 清理适配器；metastore 或客户端资源窗口不可用时停止，不能跳过已登记副本。

在所需后端实际可用时运行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json prepare --run-id <run-id>
    uv run python tools/real_lab.py --config runtime/real/config/run.json cleanup
    uv run python tools/real_lab.py --config runtime/real/config/run.json permit --run-id <run-id>
    uv run python tools/real_lab.py --config runtime/real/config/run.json stage-compute --run-id <run-id>

确认 compute 所需的 HDFS/YARN 已处于离线阶段后，分别运行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json daily --run-id <run-id>
    uv run python tools/real_lab.py --config runtime/real/config/run.json behavior --run-id <run-id>
    uv run python tools/real_lab.py --config runtime/real/config/run.json validate --run-id <run-id>
    uv run python tools/real_lab.py --config runtime/real/config/run.json publish-private --run-id <run-id>

每天和行为模型各自重新校验最多 15 分钟的许可。若前一步耗时导致许可过期，应通过实际 permit 和 stage-compute 更新后再执行下一模型，不能编辑过期时间。第一次计算失败后改用新 run-id，保留原失败日志与 HDFS 资源，不覆盖不可变输出。

本地候选包位于 runtime/real/runs/<run-id>/data/{daily,behavior}.json，按聚合日期保留最多 90 天；日志和容器 ID 文件按最早原始接收时间保留 7 天。写出前登记 .tmp，避免崩溃临时文件成为未登记副本。Spark 共享 eventlog 被关闭，metadata-only 真实血缘只在实际运行和验证成功时记录。

Doris 外部配置分支只接受 host/user/password/database，host 必须是 analysis IP，数据库必须为 snow_real_<lane中的横线改成下划线>，账号必须以 snow_real_ 开头。正式模式不使用这个外部配置分支，而由 initialize-writer 创建精确 sr_<lane中的横线改成下划线> 账号。正式发布器在每次 SQL 写入前调用 validate_doris_write，核验实际存储身份、原到期时间及来源。聚合以业务日期加 90 天计算自己的期限，不能用事件 accepted_at 下界否定合法的前一日迟到汇总。

正式路径使用 initialize-writer 已创建的私有 epoch 账号，不读取 control 的 Doris 密码配置。先完成 publish-private，再在 Windows 执行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json stage-release --run-id <run-id>

此时 control 和 analysis 的 SSH 需可用。转移只使用固定的 runtime/real/transfers/<lane>/<run-id>/ 路径，包含严格允许字段的 manifest 和经过验证的聚合 pair，不复制 ODS、原始事件、请求明细或数据库。control 导出、Windows 中转、analysis 接收临时文件与最终文件全部在写入前按原报表开始日登记；复制次数不会改变 date_from + 90 天。analysis 再生成自己的受管发布物，校验内容和原到期时间一致。

按原阶段规则停止离线服务、软关闭 VM，再启动该 epoch 的存储阶段后执行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json publish-doris --run-id <run-id>

命令在 analysis 通过受控 SSH 通路读取 collector 身份，调用独立 tools/real_writer.py publish --release-directory 指向该 run 的 published 目录。实际 SQL 发布依赖原 epoch 仍未到期、物理对象和冻结版本仍匹配。传输或发布失败只尝试停止该 epoch，并保留原进度；不会把传输成功当作 Doris 发布成功。

## 私有聚合看板

正式模式先完成 `stage-release`，使 analysis 上存在该 run 的受管聚合副本。仅在 HDFS 与所有已登记后端的检查窗口可用时执行：

    uv run python tools/real_lab.py --config runtime/real/config/run.json view --run-id <run-id>

这条 Windows 命令通过受控 SSH 在配置指定的 transport VM 启动 Streamlit；正式模式为 analysis，监听 `127.0.0.1:8502`。它不自动启动 VM、HDFS、Hive 或实时 epoch。第一次展示前必须完成实际远端清理、本地副本独立 90 日清理、聚合校验和已登记 HDFS 路径检查。

读回成功后只在当前 Streamlit 进程内保存最多 60 秒的聚合读取许可，同时受最早远端期限、本地目录最早副本期限和当前聚合原期限约束。页面每秒读取当前 lane 和发布目录的小型元数据，检查登记及所有者哈希、本地 gate、远端失败 journal 和到期时间；不会扫描整个仓库。到期后再次做实际清理，任何失败都会撤下数据，显示「私有看板已暂停」和上次已验证截止。不能从磁盘成功 JSON 导入许可，不用零值替代缺口。用户关闭电脑只影响这个私有看板，不影响线上基础统计。

## 已知的阶段边界

analysis 的 4.5 GiB 实时 epoch 与离线 analysis DataNode 分阶段运行。real_quiescent.py 已实现独立的停止存储准入：先由实际 epoch manager 清理到期卷并读回，再核验当前 epoch 的全部五个容器已停、五个卷仍属于同一 generation、容器 ID 和卷创建时间未变、绑定与挂载未变、初始化及实际作业回执齐全、ODS 的 collector/Kafka 身份与原始时间窗口一致。

其回执明确写 verification=stopped_storage_unexpired、sql_cleanup_verified=false 和 kafka_records_scanned=false，含义是停止副本尚未达到固定物理到期时间，绝不表示在线 SQL 删除或 Kafka 记录扫描已经执行。许可到期不得超过 epoch 的原截止时间。额外同 epoch 标签资源、未知容器挂载、缺失卷、删除失败、旧代际或过期副本都会阻止许可。

这一准入的状态机及拒绝分支已有合成 API 测试，仍须以真实生产初始化、真实作业提交及阶段切换回执完成环境验收，不能称为已实测的一键全链路。空的未初始化后端与已经停止的已初始化存储是两种状态；任何情况下都不能编辑登记来改变其含义。

prepare 的真实分支根据不可变 writer 注册自动登记全部后端副本。省略 backend_config_file 不会使这些副本消失；停止存储路径直接执行 Docker 检查，不依赖预制 passed.json。同步在新批次归档前、整个重放第一次发送前和每条发送前都检查原始接收时间，失败保持位点及已存在的原始批次，不延长其期限。

到达原 7 日截止后，`real_retired.py` 通过历史不可变登记、实际 `Epoch.retire()` 和 Docker 全量不存在检查证明原存储已物理退役。它不依赖已经清理的旧 JAR，不允许变代或同名重建，不将原登记改成未初始化。HDFS/Hive 和本地聚合仍可按各自原 90 日期限清理、校验和读取；计算 permit、同步、writer、Checkpoint 恢复以及 Doris 发布仍保持关闭。退役适配和私有看板准入已通过合成回归，实际跨阶段环境验收仍待进行。

## 失败与退出

- SSH 失败：检查私有 known-hosts、端口映射和精确账户权限；不关闭主机指纹检查。
- 410、源代际变化或 gap：同步停止。按显式新 lane 流程恢复，不自动跳过位点。
- 清理失败或停止存储身份不符：保留清理 journal，不签发许可；修复实际后端或补齐实际 writer 登记，不修改成功标志。
- Spark 失败：日志留在已登记目录。脚本用本次 --cidfile 中实际创建的容器 ID 收尾，不按泛化名称停止并行作业。
- 停用：stop-* 只停止服务。到期清理是独立操作；不会因为关机自动删除全部历史。

验收命令：uv run python -m pytest tests/test_real_lab.py tests/test_real_transfer.py tests/test_real_writer.py tests/test_real_quiescent.py tests/test_real_remote_lifecycle.py tests/test_sync_target.py tests/test_pipeline.py -q。测试覆盖严格配置、精确阶段服务、启动失败收尾、空数据、来源校验、停止副本的实际检查接口、初始化/作业回执缺失、资源替换、物理删除失败、聚合转移和不延长保留期、重放及容器 ID 收尾。测试使用合成数据/替身进程，不是引擎整链路实测。

2026-09-19 的 Hive/退役/私有读取追加验收：`uv run python -m pytest tests/test_real_retired.py tests/test_real_aggregate_read.py tests/test_real_lab.py tests/test_real_remote_lifecycle.py tests/test_real_quiescent.py tests/test_real_hive.py tests/test_real_writer_recovery.py -q --tb=short`，164 项通过。所有新入口仍须在资源允许的离线窗口保存实际引擎验收回执。
