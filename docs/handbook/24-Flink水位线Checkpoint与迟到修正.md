# 24 Flink水位线、Checkpoint与迟到修正

> **2026-10-13实测恢复闭环：** `real-prod-04`已实际同步，同一接收前缀的归档／Doris／线上SQLite整数一致。随后VM软关、读回零运行、重新启动，Flink准确恢复已登记的原Checkpoint，恢复后整数不变；最终writer已再次实际paused、保留数据和原期限。本次不是重复导入实验、成熟留存或生产P95测量。13:20香港时间最终读回确认运行VM为0，[本轮证据](../evidence/real-recovery-20261013.json)与早期01实验分开保留。

> **当前操作边界：** 当前配置是`runtime/real/config/production-real-prod-04.json`；`production.json`仍指向已退役01，下面01／storage V3命令仅解释历史流程，不能直接执行。03早期比较失败保持原状，未伪造paused。HDFS最初由独立受测范围完成；随后显式选择并核验03的兼容入口也完成三机启动、状态与停止实测，见[统一入口证据](../evidence/offline-followup-20261013.json)。默认入口仍拒绝未知失败现场，不能省略本次选择参数或复用旧状态摘要。03／04原期限在10月20日，到期先清理再读，不能恢复过期Checkpoint。当前可复制的只读status／describe入口见[第36章](36-常用命令索引与执行位置.md)。项目72GiB、宿主35GiB空闲磁盘／4GiB可用RAM、实时analysis4608MiB的阶段边界保持。

## 实时不是每条消息立即最终正确

事件发生时间可能早于到达时间，网络也可能乱序。Flink根据事件时间推进watermark，表达“当前处理进度认为早于某个时间的数据大多已到达”，不等同于物理世界再也没有更早事件。项目初始乱序容忍30秒，迟到修正窗口10分钟，更晚数据进入私有超迟到主题，留给离线校正。

```text
Kafka → v1校验 → event_id去重 → request_id去重 → Doris事实
                ├→ 冲突/非法：隔离
                ├→ 已知重放：重复诊断
                └→ 太晚新事件：超迟到旁路 → Spark离线修正
```

已知重复事件先去重，再判断迟到，否则历史重放可能被错误地当作新增迟到记录。旁路主题采用至少一次投递，恢复可能重发，因此离线再次按业务键处理；不能把诊断消息数当作端到端恰好一次证据。

## Checkpoint保存什么

Checkpoint保存算子状态及一致的源进度，恢复后延续去重和计算。固定算子UID让相同逻辑状态可定位；外部持久卷保存检查点。项目每10秒Checkpoint，最多一个并发，超时60秒。TaskManager可自动重启；JobManager不是HA，重启后要提交相同JAR并指定保存的检查点。

Checkpoint成功、Doris事务成功、业务无重复是三个不同层次，必须分别观察。两阶段提交先预提交再确认可见；响应异常不能单独说明事务未落地，要读事务状态与表内容。

## 历史合成实验与当前真实运行边界

Flink1.20.3、Java11、Kafka3.9.1、Doris3.0.6.2及固定connector组合有实际合成实验。历史回放配置为一个collector序列代际、单分区、状态TTL8天。TTL是状态保留策略，不证明长期生产已经连续运行8天，也不能延长当前真实epoch的7天原始数据截止。真实Checkpoint所在独立卷随原epoch到期而退休，恢复不重算寿命。更换lane、label前缀或重置seq可能破坏恢复语义，不得为绕过冲突随意改它们。

## 合成恢复实验的入口

```powershell
uv run --extra lab python tools/smoke_realtime.py --help
uv run --extra lab python tools/verify_realtime_transactions.py --help
```

先关闭其他VM，再按realtime配置开启analysis；同步锁定JAR、确认服务就绪。新实验使用新lane，并保证VM私有env与宿主命令一致。恢复必须复用本实验状态，不再执行会覆盖初始化边界的init。

这两个历史smoke工具用于其文档指定的合成环境，不用于暂停已经接收真实数据的writer。当前真实运行使用下面的独立入口，不手动执行任意`flink run -s`或从网页填写Checkpoint路径。

## 运行前判断：最新回执优先，保留历史故障边界

9月19日12:58 UTC宿主0xF7蓝屏中断了早期窗口，旧控制器只有started，不能补成成功或正常软停。随后精确处理无主后备和锁，未删VMDK或Checkpoint。WinDbg有限诊断完成但根因仍未知，不能把会话收尾、挂载排序或Docker错误识别补丁称作蓝屏修复。

用户之后已恢复分阶段任务。d17的1792拓扑已有实际Linux进程探针、合成Spark/YARN golden及独立cleanup恢复；6c60源码的逐台安装又有独立回执。旧时间线中的“一律禁启”仅描述当时停点，不是当前持续状态。是否进入下一阶段，由实际VM库存、原恢复链、所属对象、资源与本次scope决定。

不能用历史Checkpoint编号或旧全关回执替代当前guest检查。以下storage V3逐phase inspect／execute；未知状态保持失败关闭。完整历史见第99章。线上HTTP正常只证明线上独立服务在该时点正常，不证明本机引擎恢复。

## 历史日常暂停示例（01已退役）：先停止同步，再保存真实读回的恢复点

这里使用本机已受控部署的 `runtime/real/config/production.json`，不从示例重建或修改其来源身份。前置条件：已完成第13章的实际初始化与首次提交；仍是原source、collector代际、epoch、源码、JAR和卷；原截止时间未到；当前作业至少有一个完全完成、取消后保留的外部Checkpoint。所有命令在 **Windows PowerShell、Snow_Statistics仓库根目录** 执行，VM操作通过统一入口完成。

```text
当前同步批次正常结束
  → 取得epoch操作锁
  → pause_requested关闭后续同步
  → 实际核验当前Flink JobID和Checkpoint保留配置
  → 取消作业，读回最终完成Checkpoint与文件哈希
  → 追加paused，不覆盖首次登记
  → 停止本epoch → 软关闭analysis
```

第一步，等待正在执行的`real_lab sync`返回，不直接杀它并同时停止Kafka。一个控制窗口完成同步后，在同一仓库执行：

```powershell
# 先inspect本次暂停条件；再使用本次phase对应scope执行
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-storage-windows-v3.py pause-writer
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-storage-windows-v3.py pause-writer --execute --scope runtime/real/operator/storage-v3-scope-<本次32位ID>.json
```

pause-writer与同步整批共用操作锁。若另一个同步仍持锁，命令返回锁冲突，**不会提前停止引擎**；先让持锁操作结束，不删除锁文件。取得锁后工具读取实际Flink REST，核验外部Checkpoint取消后保留，再取消唯一已登记作业，读取取消后的最新完整Checkpoint；它在原卷中逐文件哈希，不把状态字节复制进回执或手册。

预期成功后，epoch目录的`writer-recovery/`追加完整paused记录，原`writer-job.json`保持不变，相关容器停止。记录包含链上前条哈希和原期限，不是一个可以自行填写的布尔成功标志。工具初版只接受最多512个文件、128MiB状态，且拒绝符号/硬链接、陌生Job目录、缺失文件；超限意味着此入口当前不能安全恢复这份状态，不能删除多余文件“凑到限额内”。

第二步，只有pause明确成功后才软关机：

```powershell
# stop-epoch要另生成scope；不复用pause-writer scope
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-storage-windows-v3.py stop-epoch
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-storage-windows-v3.py stop-epoch --execute --scope runtime/real/operator/storage-v3-scope-<本次32位ID>.json
```

预期analysis关闭、磁盘与未到期状态保留。正式stop-epoch只处理当前项目的analysis VM及拥有的epoch，不关闭其他人的VM。切换离线阶段前同样先完成这两步；start-offline会拒绝真实writer缺少完整paused记录的情形。

## 历史原epoch恢复机制：恢复同一epoch，而不是重新创建writer

以下条件全部成立才进入这条路线：paused完整、同代登记与文件未变、原7日截止未到，没有未知活动作业。若已经到期，直接转第30章到期处理，不运行resume。恢复时还会重新检查真实存储对象和文件哈希，手工比较文件名不能代替它。

```text
storage V3：start-storage → start-realtime → resume-writer → sync
每一个phase分别inspect，核对本次scope，再执行该phase。
```

当前入口是`runtime/real/operator/run-real-storage-windows-v3.py`，具体双步格式见第36章。`start-storage`从全VM关闭的状态开始，其他phase不替你启动缺失VM；实际资源和guest账本不符就拒绝。`writer-status`用于核验原登记及恢复链，仍不能代替实际恢复REST与数据读回。原 `real_lab` phase说明属于底层机制，不能在此改回裸Windows路由来绕过外层控制。

start-storage先处理已登记到期epoch，再启动尚可用的存储阶段；start-realtime启动原Flink session。resume内部只从登记的精确Checkpoint恢复同一JAR，使用no_claim，不开放allowNonRestoredState。Flink接受提交不算完成：工具还必须实际读回新JobID为RUNNING，并确认REST latest.restored的Checkpoint ID和路径与暂停记录完全一致，才追加resumed。

成功后再运行第13章的`sync`。预期旧原始事件即使重放，整数指标也不重复增加；仍须在相同截止时间对账，不能只看RUNNING。若电脑关机期间线上原始数据已经超过7天，Flink恢复成功也不意味着缺失历史已经找回；同步会依自己的410/代际/位点检查停止。

`writer-status`的当前实现只验证首次登记、collector身份和恢复链，并不查询当前Flink作业是否RUNNING，也不直接输出paused/resumed状态；它不能代替`resume-writer`内部的实际REST恢复读回或后续数据对账。全部VM关闭时该命令无法通过SSH运行，不应把连接失败解读为Checkpoint损坏。先在Windows用`real_lab ... status`确认VM状态，核对本次最新暂停回执，再按上面的原epoch路线逐条恢复。每条原生命令检查`$LASTEXITCODE`，失败就停止下一步；storage V3也没有自动“一键恢复全部”；每个phase都有新scope及独立回执。

### 最新真实完成事件如何进入同截止对账

线上质量日志已投递、SQLite已聚合，不表示该事件自动进入关闭中的本机数仓。18:56:54香港的质量验收当时只覆盖轻量链路；后续19:51香港已在原epoch恢复后实际再次同步，19:52:59完成新质量前缀的Doris、独立基准与生产SQLite整数对账，见[脱敏数仓对账回执](../evidence/production-quality-warehouse-20260919.json)。这是新的运行证据，不是把更早的ODS回执改个日期。以后再有新事件，仍需原epoch有效、writer实际恢复后按受控路线同步；不重新调用业务模型来制造测试流量。

当前`sync`是一次有界读取：内部默认只取一页，私有事件接口每页最多500条。因此“命令成功”不等于“追上此刻所有新事件”。检查和对账由私有受控操作完成，不把实际计数、位点或原始行复制到教材：

1. 确认collector身份、代际及原epoch未变，且没有410、未解决gap或过期批次。遇到这些状态停止，不能把缺口当作零或把游标手动跳过。
2. 在同一受控同步路线按需处理后续页，直到所选已接受前缀完整落入本地归档并获Kafka确认；记录本轮截止，而不是沿用旧验收日期。
3. 核对线上最新接收进度、线上聚合进度与本地已同步进度指向同一前缀；读取内部精确摘要前后再次核对这些进度未变化。若仍有新事件或聚合未追上，当前比较尚无共同边界，等待/重试并重新选取截止，不先宣称一致。
4. 用该前缀的原始`accepted_at`确定接受截止，同一日期范围/口径比较本地独立基准、Doris可见结果与轻量内部摘要；Doris尚未可见时保留“处理中”，不用旧结果补成零。核对的是同一已接受前缀，不是一次新的全站累计数字。

私有`/analytics/private/v1/summary.json`当前只返回内部最新精确快照，**没有按任意cutoff回读旧快照的参数**。不能给它随意加`?cutoff=...`，也不能把不断更新的最新摘要与旧Doris回执直接比较。公开v2有延迟和隐藏规则，同样不能拿来做精确整数对账。现有Windows `real_lab`没有独立的`reconcile` phase；本轮对账来自维护者的受控只读操作与回执，教材不把忽略目录中的历史脚本冒充稳定CLI。相关实现：[单批同步](../../src/snow_statistics/sync.py)、[私有接口](../../src/snow_statistics/api.py)。

### 当天尚未自然结束：什么结果才合理

| 层次 | 正确预期 | 不能推断成 |
|---|---|---|
| 轻量内部与实时Doris | 可持续更新当天已接收前缀，标明截止时间 | 一整天的最终报表已经完成 |
| 真实daily/behavior成对离线报表 | 等香港日期自然结束，并有该闭合日期的有效输入、coverage和许可 | 无报表就是PV/请求量为零 |
| 输入不存在 | 明确`no_input`或`no_computable_input`，不生成伪造零报表 | 有输入但当天未闭日也必然返回相同状态 |
| 强行提交尚未闭日的behavior日期 | 代码拒绝：`Report dates must be closed HK dates at cutoff` | 修改机器时钟、把日期写昨天或用未来cutoff即可“修复” |
| 公开统计 | 等D+3日00:15，再分别检查小样本门槛 | 内部已经有数据就应立即公开 |

例如2026-09-19香港日期最早在9月20日00:00之后才具备闭日条件；是否能计算还要看真实来源覆盖与许可，闭日也不等于D1/D7留存已经成熟。该日期的公开资格时间是9月22日00:15香港，样本不足仍会隐藏。`pending natural close`是运行回执对待办阶段的说明，不是每个CLI都会自动返回的统一状态码。已存在当天事件时，不应为了看到报表而先运行一个必然被拒绝的闭日作业。日期约束见[真实行为作业](../../warehouse/spark/real_behavior.py)。

## 恢复记录和常见失败

```text
首次writer-job
  → pause_requested → paused
  → resume_requested → resume_acknowledged → resumed
  → 下一轮pause_requested …
```

完整paused允许离线处理，禁止新同步；完整resumed才恢复同步资格。只有intent或部分ack、链哈希损坏、出现未登记JobID，都关闭同步、离线准入和Doris发布。不能因为“引擎现在停着”把它标成not_initialized，也不能拿旧success.json跳过真实读回。

| 错误/现象 | 含义 | 处理顺序 |
|---|---|---|
| 操作锁冲突 | 同步或另一次writer操作未结束 | 找到原操作等待正常收尾，不并发暂停 |
| 没有完整保留Checkpoint | 尚未完成检查点，或实际配置不保留 | 先检查当前作业和检查点状态；不关闭VM伪造paused |
| pause/resume中途失败 | 状态可能已部分变化 | 保留链和运行日志，停止受影响epoch后由既有恢复入口核对；不重写登记 |
| JAR、卷或collector代际变化 | 已不是同一可恢复实例 | 停止本路线，按新周期和观察缺口处理 |
| 文件缺失/哈希不同 | 状态不再与暂停证据一致 | 拒绝恢复，不从任意备份复制“差不多”的文件 |
| 原截止已过 | 真实原始状态不可再读取 | 精确到期清理；未到期的受管聚合另走第30章 |
| 直接关机导致JM作业丢失且无paused | 没有已证实的正常恢复交接 | 这是故障恢复，不是正常resume；保持门禁并保留现场，不能重新submit掩盖 |

初版对部分中断只允许核对已有受控提交所得JobID的路径；无法确定实际状态时保持失败关闭。可恢复性是明确边界，不承诺任何人工中断都自动修好。操作员不要将日志中的token、原始事件、Checkpoint字节复制到工单或学习文件夹。

## 本轮已验证和仍待验证的范围

恢复状态机、严格文件范围、过期/变代/未知JobID拒绝等已有合成单元测试。08轮真实Kafka/Flink/Doris进程的验收使用**合成输入**，包括整个JM/session停止后的精确Checkpoint恢复读回；这个历史fixture证据继续保留，不改写成正式流量。

2026-09-19正式real-prod-01已经增加了独立真实证据，冻结源码为 `63a0d415a938b077be0dde5c5501e386720392fe`。实际同步后，Doris、独立基准与线上SQLite在同一截止点基础整数指标一致。09:49:41 UTC暂停回执确认原存储对象未变、五个本epoch引擎全部停止、Checkpoint 24已登记；随后真实软关闭并重新启动analysis VM，09:54:34 UTC读回新作业RUNNING，REST latest.restored准确指向原Checkpoint 24，恢复模式NO_CLAIM。09:54:48 UTC再次对账，已接受前缀的整数指标不变。

后来按同一路线再次暂停并进入离线阶段，随后完成下述HDFS落地与Kafka确认闭环；不能因为曾经恢复成功就默认现在writer仍在运行。先查看Windows VM状态和本次保存的暂停/恢复回执，再按原epoch路线决定操作；VM及私有连接恢复后，writer-status可检查登记和链，但不作为当前RUNNING证明。不硬编码历史或最新Checkpoint编号、不手抄私有Checkpoint路径。手册不复制实际JobID、位点、状态文件hash或小计数。

这些事实只证明该冻结版本、该同代窗口、该有限真实前缀的正常暂停与VM重启恢复，不证明任意异常关机都能恢复，也不证明P95目标、7日到期后的恢复或完整离线治理链已通过。`real-small-1920`（2048/1920/768 MiB）现已实际完成56条合成golden，28有效、28重复，Parquet读回与基础整数一致；项目峰值63.62 GiB，三来宾最低可用内存641/669/202 MiB，无OOM。该成绩只覆盖小样本离线阶段，Hive＋Spark窗口、新Iceberg入口和正式P95仍未因此通过。

## 正式传输已完成的落地后确认闭环

2026-09-19 18:20香港，受管真实批次完成HDFS落地读回，两个DataNode的实际副本得到核验；18:29:52香港，工具回到原Kafka存储确认对应消费进度，核对broker身份和末端进度、已确认状态与落地状态一致，待确认标记移除。当时Flink的JobManager和TaskManager仍停止，确认ODS消费不是重新启动实时计算。

```text
原Kafka捕获并保存受管批次
  → 正常暂停实时writer，保留原恢复状态
  → 小内存离线阶段：HDFS写入、内容读回、实际双副本核验
  → 停止离线阶段，恢复原Kafka存储身份
  → 只确认已验证落地的输入，读回确认结果
  → writer保持paused，关闭全部实验VM，保留存储
```

18:31:57香港最终回执确认全部实验VM关闭，writer为paused、原数据没有删除。回执中analysis的配置内存为4608 MiB，是回到Kafka存储阶段后的配置；它不推翻此前离线计算使用768 MiB的事实，也不表示关闭的VM仍实际占用这些运行内存。不要将不同阶段的配置值合并成一次同时运行的预算。

在Windows PowerShell、Snow_Statistics仓库根目录，只读取已经产生的布尔验收结果：

```powershell
$closedLoop = Get-Content -LiteralPath runtime/real/operator/formal-closed-loop-20260919.json -Raw | ConvertFrom-Json
$closedLoop | Select-Object checked_at, actual_hdfs_two_replicas, kafka_committed_after_verified_land, formal_writer, all_lab_vms_off, data_deleted, closed_day_models
```

预期双副本和落地后确认均为true，writer为paused，全部VM关闭为true，data_deleted为false，闭日模型仍为`pending natural close`。命令不读取原始事件、位点或数据hash，也不会重新运行Kafka。若未来状态改变，应读取新的回执和当前状态，不能把这份历史文件当作实时健康检查。

原期限没有因暂停、落地、ACK或关闭VM而重算。本节18:32香港闭环时质量日志尚无确认样例；18:56:54香港独立完成日志与轻量聚合验收通过，见[第10章](10-两个产品接入与日志质量链路.md)。这两个时间点的证据保留各自输入范围。后续19:52:59的新质量数仓对账是第三份证据，**没有自动把该新增前缀纳入较早HDFS落地与Kafka消费确认**。

新小内存入口最初实现46efeceb通过CI后，实际启动先遇到DataNode匿名`/data`父卷身份拒绝；22ef47ae修复后又暴露节点Python标准输入后台线程在解释器退出时的fatal错误及Windows控制端断管竞态。修复版 `b1ed28df` 已通过[Linux CI 35441027661](https://github.com/X1aoB/Snow_Statistics/actions/runs/35441027661)及push CI35441025287，包含真实POSIX管道/SIGHUP测试；在19:56这一历史停点尚未安装；之后修复被de406cb包含，并于20:22–20:27香港逐节点安装、验证stdin保持打开时正常零退出。这是20:27时点的记录；之后1792 Linux probes和合成golden的独立结果见第12章及1792回执，原失败不改写。1920组合的小样本成绩不自动证明新入口可运行，2048/1792/768 MiB的1792组合在该历史时点仍是候选，后续小样本实测不能倒填到这里。冻结`real_lab --offline-profile`不接受这两个新名字，不向旧CLI替换参数绕过准入。

## 19:51–19:56香港：新增质量前缀的实时对账与最新暂停

实际流程是：原epoch恢复→受控同步→Kafka/Flink/Doris→独立基准→同前缀生产SQLite对账。11:51:19 UTC读回Flink RUNNING及Doris基础整数与独立基准一致；11:52:59 UTC在生产SQLite的一个只读事务中确认同collector身份、相同接收和聚合前缀、相同日期口径的精确整数一致。核验不导出指标行、原始事件、标识或数据指纹，不读取聊天正文，也不修改生产。

11:54:53 UTC保存Checkpoint84并正常暂停，实际读回原存储未变及五个所属引擎全停；11:56:09 UTC三台VM全关、数据未删、原期限未延长。84是这次回执中的恢复点编号，不是下次命令可以硬编码的固定值。当前状态是paused，并非持续运行。

截至这份19:56历史回执，新增质量前缀尚未capture/land到HDFS；当时真实闭日报表仍待香港日期自然结束，D1/D7成熟、Hive＋Spark窗口、新Iceberg入口和P95各有独立门槛。回放积压的耗时不能用来宣称正常线上新鲜度。原ODS回执只证明原先已落地的前缀。

教材只引用[脱敏数仓对账回执](../evidence/production-quality-warehouse-20260919.json)；私有暂停和安全停机回执仅提取上述时间与状态，不复制Checkpoint路径、JobID、位点、真实计数或数据hash。

## 20:22–20:32香港：安装、正常退出与原期限诊断纳管

`de406cbfe0eff069f56a3b04420d280175966d95`通过CI后实际安装到三节点。逐台测试保持父进程stdin打开，节点worker仍返回0，未重现旧解释器退出崩溃；12个冻结writer文件原字节保持不变。analysis supervisor已重载新源码，检查既存同步registry、保留未到期数据及原登记/期限，当时容器停止。详见[三节点退出与启动清理证据](../evidence/small-worker-exit-20260919.json)。这里的“逐节点”不能省略：该窗口没有证明三节点同时运行、1792 probes/golden或完整wrapper计算通过。

12:30:54 UTC，analysis的一份已精确关联旧worker诊断完成原子且不覆盖的同文件系统移动，登记进该节点已有sync数据生命周期。随后实际清理读回确认同inode、原路径缺失、gate开放，期限取原epoch最早接收时间及原7日截止，绝不从文件mtime、安装时间或移动时间重算。没有读取、散列或复制转储正文，也没有删除未到期数据。首次预检曾因元数据hash口径不一致正确拒绝，修正审阅scope后才继续；原manifest/registration保持不变。

诊断可能含进程上下文，不能因为文件叫crash就当作普通无隐私日志。这个纳管结果仅针对analysis精确对象；control/compute没有相同sync归属，不复制analysis registry或套用其scope。两节点的实际inspect和删除均仍待后续独立步骤，本章不把授权或工具准备记作已经执行。

12:32:52 UTC安装收尾回执记录当时三VM全关。该结果是历史安全停点，不表示今后维护窗口不会再启动VM；新的profile/引擎结果须独立记录。原到期清理已接入共同启动/监督入口，详见[同步副本启动清理说明](../real-sync-retention.md)，但尚未等待真实七日期限自然经过，不能宣称长期到期演练或物理介质擦除已经完成。

## 练习与答案

题：Checkpoint显示成功，Doris里结果却没变，能马上重发所有原始业务请求吗？答案：不能。先查sink事务是预提交、可见、已中止还是确认未知，读取实际表中业务键。重发模型请求可能产生新的付费业务副作用；实验消息重放也应走既定lane和业务去重，不混同“重新调用业务”。

历史实测成绩（合成输入）：10万条按事件时间排序的历史回放得到9万有效，与轻量/YARN日指标和明细校验一致；杀TaskManager恢复后追加1000条旧事件不增事实。这不等于百万条持续公网实时SLA。

再问：既然暂停已经保存Checkpoint，为什么恢复仍检查epoch截止？答案：Checkpoint含原始级状态，是一份真实数据副本；恢复只是重新使用同一副本，不能获得新的7天保留期。再问：为什么paused能离线计算却不能继续sync？答案：离线读取依赖已冻结输入及清理许可，sync还需要当前已登记的实时writer实际运行。

## 对照源码与项目证据

- [docs/realtime.md](../realtime.md)
- [docs/realtime-scale.md](../realtime-scale.md)
- [warehouse/flink/pom.xml](../../warehouse/flink/pom.xml)
- [真实暂停与恢复实现](../../src/snow_statistics/real_writer_recovery.py)
- [恢复链与原时间门禁](../../src/snow_statistics/real_quiescent.py)
- [第30章：到期、计算发布与聚合读取](30-日常运维备份精简与移除.md)

[返回学习目录](00-阅读地图与当前状态.md)
