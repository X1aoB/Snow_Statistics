# 17 Spark、YARN、Hive与离线分层

> PowerShell 示例从克隆后的仓库根目录运行。标注为历史的命令和 `runtime/real/operator/` 记录依赖当时的私有环境。新的合成复现按 [README](../../README.md) 与 [分阶段运行手册](../runbook.md) 操作。

> **2026-10-14真实Hive／Iceberg与最终停止已通过：** 原BCE闭日模型包保持原来源，F8／483在`real-small-1792`（control2048／compute1792／analysis1024 MiB）完成真实Hive登记与独立验证。Spark 3.5.7 `local[1]`应用分别为`local-1791973277761`和`local-1791973714447`，daily／session_daily／retention／funnel四组存在性、计数比较及私下行摘要比较均通过。随后真实Iceberg由两个不同YARN应用`application_1791974201828_0001`、`application_1791974201828_0002`完成执行和独立验证，四组当前／适用历史快照及新增`model_revision`列的校验有效，analysis权威确认通过，见[F8真实Hive与Iceberg技术证据](../evidence/real-hive-lake-f8-r2-20261014.json)。公开只给比较布尔与引擎身份，不发布业务计数、行摘要或可枚举的请求／结果／metadata内容哈希。原聚合2026-10-13香港日期对应的90日到期2027-01-11保持；DIM原始输入仍按其独立七日到期。这不表示Lake表已登记到Hive，也不声称分区演进、MERGE、全图或列级血缘、生产P95。

> **2026-10-14真实DIM已实际通过：** F8／483来源在显式`real-dim-2560`条件下完成r2，Spark 3.5.7、Python 3.8.10、`local[1]`，实际应用为`local-1791971531318`。`at_event`与`current`两种解析模式的全部字段比较均为true，非空输入验收通过；driver及外层SSH实际退出0，后续独立检查确认本次精确所属容器已不存在，见[F8／r2真实DIM技术证据](../evidence/real-dim-f8-r2-20261014.json)。业务计数和可能被小范围枚举重构的私有结果文件哈希只在私下校验；公开只保留“私有绑定与验证已通过”的布尔结论。这证明本轮真实DIM解析与Join的引擎执行，不会把空历史覆盖补齐：`history_coverage`仍为unknown，unknown也是需正确比较的结果。原输入2026-10-13 04:06:35.627 UTC对应的七日到期2026-10-20 04:06:35.627 UTC保持；没有改写BCE指标、正式产品或公开统计。监控仅有10次宿主采样，54.359秒是SSH窗口时长，不是精确Spark耗时、来宾连续监控或峰值，也不声明VM已关闭。真实Hive／Iceberg和其他结果分别验收。

> **2026-10-14有限真实血缘已读回：** 原闭日daily、behavior、publish三任务的六条START/COMPLETE元数据单独投递并获ACK；旧八条fixture元数据排除，后端三任务均为COMPLETED、输入输出身份匹配，有限图非空，见[实际闭日Marquez证据](../evidence/real-closedday-marquez-20261014.json)。六条ACK只属于该子集，06:28:14 UTC（香港14:28:14）的旧后端证据仍保留ACK尚未导回的历史false；随后06:45:27 UTC（香港14:45:27），C0F来源已将六条闭日ACK导回control原journal，原14条元数据身份保持，八条fixture仍未ACK，见[原journal ACK导回证据](../evidence/real-closedday-marquez-control-ack-20261014.json)。四节点三边是本次观察，不宣称整图完整、全部历史边或列级血缘；没有读取业务行。原BCE模型与c0f投递工具来源分别记录。

> **C0F历史源码截点（F8之前）：** c0f签名、双CI与三节点安装已经完成；analysis、control、compute分别完整读回477文件，每台相对BCE改变27文件，12冻结writer、配置和原恢复元数据/期限保持，见[c0f三节点安装证据](../evidence/signed-source-install-c0f542c-20261014.json)。旧V18使用fa057fe、V19及原闭日模型使用BCE，安装新源码不改写原成绩。本轮私有UI已有四页签DOM与有限进程收尾证据；DIM2304实际CLI在Spark启动前的资源门禁失败，新的2560仍待实测，见[实际私有看板证据](../evidence/real-private-view-20261014.json)、[2304实际前置失败](../evidence/real-dim-preflight-2304-20261014.json)。旧Hive/Lake条件不随DIM新profile扩大。

## 当前实际结果与新来源

**BCE历史校准（香港2026-10-14，后续c0f安装见上方）：** bce1289已完成签名、双CI全部4作业54步骤及三节点463文件实际安装读回，每节点相对fa057fe变化15文件；12冻结writer、依赖、配置及受保护恢复元数据不变，见[BCE签名与CI](../evidence/signed-source-bce1289-20261013.json)、[BCE三节点安装](../evidence/signed-source-install-bce1289-20261013.json)。PID512修复和公共DIM模块已部署；V19实际结果与未完成的真实报表在下文分别列明。

V18在实际fa057fe来源下，由两个不同Spark 3.5.7 local[1]应用完成Hive登记与独立读回，四组6/6/2/3行及摘要一致，见[V18合成Hive验收](../evidence/hive-catalog-v18-20261013.json)。旧`lake20261013`的native-thread失败仍保留，不能唯一归因于PID。随后新`lake20261014r1`/V19已在BCE来源下实际通过两个不同Spark 3.5.7 YARN应用的Iceberg执行、独立验证与analysis权威确认，四组当前/历史快照读回一致，见[V19合成Iceberg实际验收](../evidence/iceberg-catalog-v19-20261014.json)。这是合成聚合夹具，未读取生产事件；组级行摘要未独立导出，证据为固定输入/回执哈希链与实际等值校验。原2026-12-14聚合到期不变，旧partial不覆盖。

V19整轮保存56份完整资源样本，采样OOM计数均为0；另两次精确容器PID观察仅留下10/11条样本，原计划180条均未完成并保留FileNotFoundError。采到的最高PID为295（上限512）；它不是连续峰值，未采到的时段不能推断没有OOM或其他资源事件。实际引擎完成与不完整监控分别判断，不用新成功改写旧失败原因。

公共DIM现已在本机由正式产品公开目录生成3个版本/87条目录项的独立metadata，未读取真实事实；连续覆盖为空、小吉实际切流时间未知，观察时刻不冒充业务生效时间，见目录输入与边界（历史本机记录）。此前25项新测试/36项相关回归及4版本/4覆盖段/12维度行合成产物保留，真实DIM与事实Join仍待独立验收。香港午夜已过，但真实10月13日闭日仍须新鲜源、同前缀ACK、原permit、计算对账和发布回执；不填伪造成熟留存或生产P95。

**以下导言保留V18开始前的历史状态：** 本章先解释分工，再提供不启动虚拟机的可手算练习，以及低内存 Spark/Hive 的分阶段机制。当前节点来源为签名 `fa057fe1dca87f11c6edf10895820c55b6ae0d74`，双轮CI全部作业／步骤及三节点452文件实际安装读回均已完成；本章Python练习是在此前50706a8版本下实际运行的教学结果，不改写其来源。**V11退出、V12登记、V13诊断入口和V14 Snappy原生库加载失败分别保留。固定镜像的Snappy旧／新实际对照已通过；V15范围核验和hive-enter通过，登记随后因analysis可用内存127 MiB低于128 MiB门槛被监控器中断；自动收尾已完成。随后离线analysis已从768配置为1024 MiB，来宾可用内存385 MiB已读回；这只验证容量调整；V16随后enter成功、register在宿主前置检查失败且未进入登记driver，见[V16失败证据](../evidence/hive-host-prerequisite-20261013.json)。V16原触发样本缺失，不能指定某个宿主约束为已证实原因。之后V17在hive-enter前的analysis guard因旧20秒等待上限中断，未切换引擎；单独65秒外层窗口中的实际guard在26.706秒成功。仅analysis guard改70秒的修复通过193项测试、4项跳过及Ruff，现已随fa057fe签名／CI／三节点安装完成部署；V18登记／独立查询／Iceberg仍待实际验收，见[V17核验](../evidence/hive-guard-timeout-20261013.json)。**最新阶段结果以[项目状态](../status.md)为准。

2026-10-13 后续已将 control 系统盘从18扩至20 GiB，根分区与 ext4 已实际读回增长，检查时空闲约2.52 GiB；临时维护预算82 GiB已经恢复为项目72 GiB，宿主35 GiB空闲磁盘和4 GiB可用RAM门槛不变。这次扩盘完成不等于Hive或Iceberg验收完成，见[控制盘扩展证据](../evidence/control-disk-growth-20261013.json)。

## V19实际复盘：成功读回与不完整资源观察分开判断

执行应用 `application_1791908609747_0001` 与验证应用 `application_1791908609747_0002` 是两个实际YARN应用；analysis于UTC 16:27:13.927668完成权威确认，根操作人记录整个lake进程退出0。四组daily/session_daily/retention/funnel为6/6/2/3行，当前及原始快照的实际等值检查通过；增加`model_revision`属于字段演进，本次未宣称分区演进、MERGE、物理HA或列级血缘。组级行摘要没有另行导出，不把输入hash当作每组独立行hash。

56份全阶段资源样本中项目最高69,243,850,379 bytes，宿主空闲最低210,132,193,280 bytes、可用RAM最低13,393 MiB；guest可用RAM最低control521/compute409/analysis334 MiB。都是采样值。精确容器PID两次观察原计划180条，但实际只有10/11条；其退出1和FileNotFoundError原样保留，最高PID295不能当整个进程峰值。

练习：执行/验证成功而两次PID观察失败，能否说Iceberg失败？不能混同，实际双应用和权威确认支持本次合成Iceberg通过，PID监控完整性则未通过。能否说调整512已经证明旧256必然是唯一原因？不能；新条件下运行成功不补齐旧失败瞬间缺失的PID/内存证据。

## V18历史排障复盘：成功的Hive与失败的Lake分开判断

登记应用`local-1791904234704`与读回应用`local-1791904937393`不同，两次实际退出0；四组摘要一致，登记41份、读回39份资源样本完整。采样OOM计数0不表示严格连续峰值或绝无未采样错误。

Lake后续有39份完整样本，失败后精确收尾、独立VM0和数据保留只属于当时停点。日志证明线程创建失败，不证明唯一原因是PID256或堆内存耗尽；诊断只保留固定attempt执行标记与有界合成日志，不复制真实事件或整盘。日志保留前1 MiB，所以截取它的尾部也可能缺少进程最终输出。

BCE仅将独立driver的PID上限256→512；driver内存1280MiB、CPU2、Java堆640MiB、executor/VM和精确清理均不变。新attempt必须独立目录和新鲜基线，保留旧partial及原期限，后续采集精确容器的pids/memory元数据并分别验证执行、读回和最终确认。

练习：Hive通过能否替Iceberg打勾？不能，目录读取与湖表写入是独立过程。增加PID上限是否已经证明修复？不能，合成参数检查与源码安装只证明修改落实，仍需新的实际引擎结果。

## 历史时点香港22:48–22:54：70秒guard已部署、当时V18结果待验

最新[签名来源](../evidence/signed-source-fa057fe-20261013.json)为`fa057fe1dca87f11c6edf10895820c55b6ae0d74`；push `38415757345`与PR `38415767291`两轮CI的4个作业、54个步骤全部通过。完整归档包含452文件，逐字匹配签名Git树；[三节点实际安装](../evidence/signed-source-install-fa057fe-20261013.json)每台均取得prepare、install和独立readback，以及下载的原安装回执。相对2434每节点变化10文件，12个冻结writer、依赖、原配置和analysis恢复元数据保持，原保留期没有延长。

因此本章现在可以说“analysis guard70秒修复已经安装”，普通probe／其他节点仍20秒。安装没有运行数仓引擎；后续V18仍须按新来源和新基线分别验收Hive登记、独立查询、Iceberg及最终收尾。下文22:36候选阶段的失败、测试与待签名描述保留为历史，不反写原记录。

## 22:06–22:11香港：容量调整已读回，引擎结果另验

当前[2434来源](../evidence/signed-source-2434ab6-20261013.json)与[三节点安装](../evidence/signed-source-install-2434ab6-20261013.json)均已独立核对：每节点447文件完整回读，12份冻结writer与原配置／恢复状态保持。随后在全VM关闭时只修改analysis VMX内存768→1024 MiB；control2048、compute1792 MiB不变，其余VMX配置与虚拟盘均未改变，见配置回执（历史本机记录）。

22:11的来宾采样（历史本机记录）记录MemTotal **983516 KiB**、MemAvailable **385 MiB**，符合新896–1024 MiB内核可见区间且高于原128 MiB可用余量门槛。这是一次真实读数，不是持续最低值、严格峰值或“整轮稳定”的证明。三机内存后备预算更新为4864 MiB，作业写入预算128 MiB、单次启动预留256 MiB以及72／71.75／35／4边界均保留。

下文V15案例保留原768 MiB条件与中断事实；原聚合、03／04期限不变。此容量截点之后的V16在宿主前置检查失败，V17又在analysis guard等待超时；两者不是相同已证实原因。后续签名修复安装后仍须分别取得登记、独立读回、Iceberg及收尾的实际回执，不因为385大于128或guard单次成功就提前宣布全部通过。

## V17排障实例：检查完成太慢，不等于检查不通过

`guard`是进入下一阶段前对暂停writer、实际引擎对象、存储与Checkpoint元数据的检查。它比只读内存和磁盘数字的普通`probe`工作更多。V17这次在`hive-enter`的前置guard等待超时，父阶段退出1、`complete=false`、资源样本数0；尚未调用引擎切换。这里不能推断Hive SQL失败，更不能把旧Snappy错误搬到本次失败上。

协调者随后在同一已签名2434节点，用65秒的外层上限单独测量公开guard。香港22:36的实际结果为**26.706秒、退出0**，返回writer已暂停、实际引擎对象停止、Checkpoint元数据合法、`read_permission=false`。这支持“原20秒等待窗口不足”的修复，不是Hive登记成功，也不是所有未来guard都会在26.706秒完成的保证。该guard仍会执行既有登记副本到期检查／必要清理；没有读取事件或Checkpoint行载荷，没有新建读取许可，没有绕过原期限。证据见[失败与实际计时](../evidence/hive-guard-timeout-20261013.json)。

最小修复仅把**analysis节点的guard等待上限改成70秒**；普通probe、其他节点的检查和input仍20秒。原SSH失败分类、超时终止自有进程、暂停状态和资源门槛检查保持。相关合成回归**193项通过、4项跳过**，其中12个新case覆盖节点／操作组合、超时收尾和错误分流；Ruff及独立审阅通过。本节22:36编写截点还是等待GPG签名、CI和三节点实际安装的候选。之后部署事实见本章页首fa057fe的独立安装证据；该后续证据与本机源码修改、旧失败是不同环节。

V17在取得阶段资源所有权之前失败，控制器没有提供自动关机成功证明。协调者另行运行公共`stop-offline`退出0，再独立读回运行VM为0；保留数据。这与V16的自动收尾是不同路径，排障报告应分别说明。下一轮使用新的来源与基线后，仍要分别验收enter、登记、独立查询、Iceberg及收尾。

练习：如果某检查在26.706秒完成，而调用方20秒就报超时，应该直接跳过检查吗？答案：先保留原失败，再确认检查在有限更长窗口内返回的实际身份与状态，调整这个具体调用的有限等待预算，同时保留原失败门槛和收尾；不要改变其他探针的等待时间，也不要把局部检查成功写成整个流程成功。

## 组件分工

Spark执行数据转换和SQL计算；YARN给应用分配容器资源；HDFS存文件；Hive metastore提供表和分区元数据。使用Spark SQL查Hive表，不意味着每一步由Hive执行引擎计算。Parquet是列式文件格式，Hive表只是把路径、字段和分区等组织成可查询元数据。

```text
ODS原始/规范事件
  → DWD：校验、event_id去重、请求赢家、业务日期
  → DIM：角色/内容/渠道等维度及历史版本
  → DWS：会话日汇总、cohort、工单汇总
  → ADS：面向看板的固定指标结果
```

分层的价值是统一语义、复用计算和缩小排错范围，而不是目录名字越多越专业。DWD的一行代表一个有效事件，日ADS的一行代表某source/app/date；不能对已经聚合的结果再按事件方法去重。

## client模式的一次作业

Windows通过SSH调用control脚本，driver位于提交端，YARN的ApplicationMaster协调资源，executor在compute节点执行任务。driver被kill不等于executor已干净结束，所以停止脚本还要检查对应容器和VM。YARN应用ID与项目run-id要一并记录。

首次学习先查看脚本和帮助，不需要立即百万扩样：

```powershell
Set-Location 'Snow_Statistics 仓库根目录'
Get-Content tools/spark_yarn.sh
Get-Content warehouse/spark/batch.py | Select-Object -First 80
```

`tools/smoke_yarn.sh` 是历史批处理环境的入口，它使用另一套 Spark 容器参数并带 `--register-hive`，**不能在当前低内存配置直接复制执行**。当前阶段拆开 YARN 计算和 Hive 目录查询，操作方法见下文。每次新计算使用独立输出目录，不能用相同run-id覆盖旧证据。

目录查询阶段另用 Spark `local[1]`：control 上一个受限临时 driver 查 Hive metastore、读取小聚合，RM/NM 暂停。它与 YARN 批计算承担不同任务，不把 `local[1]` 查询结果描述为分布式性能成绩。

## 惰性计算与执行计划

Spark的select/filter/join通常构建计划，count/write等action触发实际执行。`explain`看到逻辑或物理算子不等于任务已跑过；要结合Spark事件日志、输入输出行数和结果验证。一个应用可有多个job，每个job有stage/task；累计task输入可能包含多次扫描，不能当源数据量。

Hive注册与实际Parquet读回分别验收。表存在但location错会查错历史；目录里有文件但未注册分区会让SQL看不到。对照来源清单、表路径、业务日期和cutoff逐层排查，不先猜是Spark算错。

## 练习与答案

题：百万原始事件经过DWD得到90万有效、10万重复；某任务日志累计读取200万行，是否证明输入其实有200万？

答案：不能。任务计数可能包含重扫和多阶段读入；源输入由固定清单和原始条数证明。应分别报告原始规模、质量账本、应用时长、task累计指标。当前百万验收没有同时运行Hive/Airflow，不能据此声称整套模型DAG在2/2/1GiB上全部完成。

## 对照源码与项目证据

- [docs/offline-pipeline.md](../offline-pipeline.md)
- [warehouse/spark/batch.py](../../warehouse/spark/batch.py)
- [docs/scale.md](../scale.md)

## 不开 VM 的完整练习：从56条输入得到6行日指标

### 环境、工作目录与前置条件

以下均在 **Windows PowerShell** 执行，工作目录为 `Snow_Statistics 仓库根目录`。只需要本仓库 `.venv` 和已安装的项目 Python 包，不需要 VMware、Java、Kafka、Hive 或网络服务。输入全由合成生成器产生，不访问正式采集器。

```powershell
Set-Location 'Snow_Statistics 仓库根目录'
& ./.venv/Scripts/python.exe -c "import snow_statistics; print(snow_statistics.__file__)"
```

预期打印本仓库 `src/snow_statistics` 下的路径。如果解释器或模块缺失，先修复本仓库的 venv；不要临时给系统 Python 全局安装一批库。

### 第一步：运行教学脚本

已保存独立脚本 [example.py](spark-oracle-exercise.py)。它每次在 ignored 教学目录中创建一个新输出子目录，不覆盖上一轮。

```powershell
& ./.venv/Scripts/python.exe docs/handbook/spark-oracle-exercise.py
if ($LASTEXITCODE -ne 0) { throw '基准失败，不继续解释结果为成功' }
```

为保证手册自成体系，完整脚本如下。ignored 脚本丢失时可以将以下内容保存回上述路径，再运行；不保存到正式配置或业务项目中。

```python
import json
from pathlib import Path
from uuid import uuid4

from snow_statistics.model import build, deduplicate
from snow_statistics.simulator import generate

folder = Path("runtime/handbook/ch17-20261013/runs") / uuid4().hex
folder.mkdir(parents=True, exist_ok=False)
fixture = generate(seed=42, users=2)
raw = fixture["events"] * 2
valid, quality, quarantine = deduplicate(raw)
actual = build({"events": raw, "changes": fixture["changes"]})
reference = build(fixture)
assert quality == {"raw": 56, "valid": 28, "duplicates": 28, "quarantined": 0}
assert not quarantine and actual["daily"] == reference["daily"]
assert len(actual["daily"]) == 6
for name, rows in (("ods.synthetic.jsonl", raw), ("dwd.synthetic.jsonl", valid)):
    (folder / name).write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
summary = {"engine": "Python correctness oracle; Spark not executed", "source": "synthetic",
           "seed": 42, "quality": quality, "daily": actual["daily"], "daily_matches_single_copy": True}
(folder / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps({"output": str(folder), **summary}, indent=2))
```

固定种子决定业务数据；UUID 只改变输出目录。生成器中编号0的合成用户活动三天，编号1活动一天；每次活动产生7个事件，所以单份有 `(3+1)×7=28` 条。复制输入整份一次成为56条，但有效事件和日指标不应增加。

### 第二步：手算与逐项核对

预期打印 `raw=56, valid=28, duplicates=28, quarantined=0`，以及 `daily_matches_single_copy=true`。实际日结果如下，全部是合成答案：

| 日期 | 应用 | PV | UV | 请求量 | 成功数 |
|---|---|---:|---:|---:|---:|
| 2026-01-01 | mywebsite | 2 | 2 | 0 | 0 |
| 2026-01-01 | project_snow | 2 | 2 | 2 | 2 |
| 2026-01-02 | mywebsite | 1 | 1 | 0 | 0 |
| 2026-01-02 | project_snow | 1 | 1 | 1 | 1 |
| 2026-01-03 | mywebsite | 1 | 1 | 0 | 0 |
| 2026-01-03 | project_snow | 1 | 1 | 1 | 1 |

1月4日没有结果行，不是额外一行已证明完整的零日报。正式系统首先还需判断该日覆盖和输入完整性。

将终端刚打印的 `output` 路径粘贴到下面的提示，只使用这次合成教学目录：

```powershell
$lessonOutput = Read-Host '粘贴刚生成的 runtime/handbook/ch17-20261013/runs/... 路径'
Get-Content -LiteralPath (Join-Path $lessonOutput 'summary.json')
(Get-Content -LiteralPath (Join-Path $lessonOutput 'ods.synthetic.jsonl') | Measure-Object -Line).Lines
(Get-Content -LiteralPath (Join-Path $lessonOutput 'dwd.synthetic.jsonl') | Measure-Object -Line).Lines
```

两份 JSONL 预期分别56行和28行。2026-10-13 的独立教学结果（运行练习后生成）实际符合上述答案。这只证明 Python 基准，不声称本次运行了 Spark 或 Hive。

### 第三步：对应实际 Spark 实现

在同一 **Windows PowerShell、仓库根目录**阅读源码，不会启动服务：

```powershell
Get-Content warehouse/spark/event_input.py
Get-Content warehouse/spark/batch.py
Get-Content tools/spark_yarn_scale.sh
```

按下面顺序理解：

1. **确定输入：**source、输入清单、日期、cutoff、run-id 一起定义本次计算范围。
2. **校验和两次去重：**`event_input.py` 先校验信封与来源、排除截止之后的事件，再按 `source/app/event_id` 去重；请求完成事件再按 `source/app/request_id` 选赢家。
3. **确定业务日期：**`occurred_at` 用于香港日期。例如 UTC 1月1日16:00属于香港1月2日。`accepted_at <= cutoff` 决定此次是否能看见该条；迟到记录不能混淆这两种时间。
4. **写入结果：**`batch.py` 按source/app/date计算PV、UV、请求数、成功数，将DWD和ADS写入不可变run目录。`errorifexists`用于阻止误覆盖。
5. **读回校验：**重新读取已经落盘的ADS Parquet，与`--expected`基准核对；不是只比较内存里的DataFrame。
6. **验收后发布：**质量和读回通过后才写publication/accepted；有输出文件不等于结果已经验收。

真实 Spark 账本包含 `after_cutoff`，应有 `raw = valid + duplicates + quarantined + after_cutoff`。本章Python基准没有完整cutoff筛选，样例全部早于固定截止，因此`after_cutoff=0`；不能把教学四字段账本原样代替生产账本。

以下 SQL 仅帮助理解已经清洗好的数据如何聚合，省略了截止、去重和日期窗口，不能替代完整作业：

```sql
SELECT source, app, business_date,
       SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS pv,
       COUNT(DISTINCT anonymous_id) AS uv,
       SUM(CASE WHEN event_type = 'request_complete' THEN 1 ELSE 0 END) AS requests,
       SUM(CASE WHEN event_type = 'request_complete' AND success = 'true'
                THEN 1 ELSE 0 END) AS successes
FROM validated_and_deduplicated_events
GROUP BY source, app, business_date;
```

当前daily的UV是去重后这个应用、这个日内的有效匿名标识数，不限于page_view行；不带匿名标识的服务端完成事件仍能计请求量。`request_observed`不再次增加请求量。DIM/SCD2是运营模型的单独职责，并不是每个daily作业都关联全部维表。

[1792历史小样本证据](../evidence/offline-1792-recovery-20260919.json)记录Spark3.5.7 on YARN的56→28条实际验收和整数指标一致；原控制器收尾失败，后来独立恢复成功，两件事都要保留。更早的[YARN/Hive证据](../evidence/yarn.json)使用不同资源条件，不能代替当前低内存Hive验收。

## 当前真实 Spark 作业：从观察到停止的顺序

这一段只在真实作业已经合法登记后执行，不自动创建闭日job、补源位点或恢复过期数据。

| 前置条件 | 实际要求 |
|---|---|
| 配置 | 明确 `runtime/real/config/production-real-prod-04.json`；旧 `production.json`可能仍指向退役窗口，不直接照抄 |
| 状态 | 03保留原失败停止状态；04已有完整暂停链、全部真实引擎停止；不能手写paused |
| 输入 | 有实际登记run-id、已闭合香港日期、固定截止、合法coverage、已确认ODS及有效原期限 |
| 源码 | Windows及节点均已核验兼容模块、镜像/JAR、venv；安装成功不是计算成功 |
| 配置和容量 | 显式1792为control2048/compute1792/analysis768 MiB；当前72 GiB项目、71.75 GiB停线，宿主35 GiB空闲磁盘/4 GiB RAM，来宾128 MiB RAM/384 MiB磁盘 |
| 启动状态 | 上次精确收尾已完成；启动离线时三台VM全关，不接管不明工作 |

03/04兼容只适用于满足原登记的这对窗口。到期或正常退役后重新核验，不重建03、复制旧登记或修改期限来延长教程有效期。

先在**按受控流程单机启动的 analysis Linux VM**，工作目录 `/home/snow/Snow_Statistics`，做只读观察：

```bash
cd /home/snow/Snow_Statistics
.venv/bin/python tools/real_offline_preserved.py \
  --config runtime/real/config/production-real-prod-04.json inspect
```

预期有实际`state_sha256`、`read_permission=false`。取得摘要后按原流程软关analysis，再在**Windows PowerShell、仓库根目录**准备参数：

```powershell
$lessonPython = './.venv/Scripts/python.exe'
$stateSha = Read-Host '输入刚才实际 inspect 返回的 state_sha256'
if ($stateSha -notmatch '^[a-f0-9]{64}$') { throw '观察摘要格式不正确' }
$offlineArgs = @('--config', 'runtime/real/config/production-real-prod-04.json',
                 '--profile', 'real-small-1792',
                 '--preserve-stopped-epoch', 'real-prod-03',
                 '--preserved-state-sha256', $stateSha)
& $lessonPython tools/real_offline_small.py @offlineArgs start-offline --describe
```

正则只检查形式，实际范围由节点验证。不能制造64位字符串当作观察值；`--describe`的`executes=false`也不是实际准入通过。

以下是**逐条执行**的阶段表。每条检查退出码和实际状态再判断下一条；不要把整列串成遇错仍继续的脚本。先将`$runId`设为实际已登记的job ID，不能照抄教学日期猜一个名字。

| 阶段 | Windows命令，或沿用同前缀的子命令 | 预期和检查 |
|---|---|---|
| 启动 | `& $lessonPython tools/real_offline_small.py @offlineArgs start-offline` | 实际03/04范围及停止对象通过，启动固定HDFS/YARN；不启动Hive/Flink/Doris |
| 落地 | `& $lessonPython tools/real_offline_small.py @offlineArgs land` | 只处理已有pending；无pending可正常无输入，不能冒充新同步或ACK |
| 准备 | `& $lessonPython tools/real_offline_small.py @offlineArgs prepare --run-id $runId` | 校验实际job与登记范围 |
| 清理与许可 | 同前缀分别执行`cleanup`、`permit --run-id $runId` | 实际后端生命周期通过；观察摘要不能代替短期permit |
| 元数据传递 | 同前缀`stage-compute --run-id $runId` | 只传允许的job/coverage/permit等元数据 |
| 实际计算 | 同前缀分别执行`daily --run-id $runId`、`behavior --run-id $runId` | 有真实应用、输出和回执；短许可不足时先重新实际校验与staging |
| 验证和发布 | 同前缀依次`validate --run-id $runId`、`publish-private --run-id $runId`、`stage-release --run-id $runId` | 同来源/输入/cutoff成对通过后，才私有发布与受管聚合复制 |
| 停止 | `& $lessonPython tools/real_offline_small.py @offlineArgs stop-offline` | 精确停止与三台VM软关，不删数据 |

若已登记Hive后端而当前资源窗口不能完成Hive实际读回，生命周期检查会拒绝。这时进入独立Hive协调阶段；不能在analysis768 MiB上随手启动额外引擎，也不能把后端改成“未初始化”。详见[小内存入口](../real-offline-small.md)及[03/04兼容说明](../real-offline-preserved.md)。

## Hive阶段说明与V11／V12／V13失败记录：暂不执行历史候选

本节保留历史路径和阶段含义，供理解、排错和面试复盘。实际 V11 `hive-enter` 已通过；`hive-cleanup` 虽收到 `complete` 消息，但 authority 进程随后非零退出，因此没有判为成功，也未继续登记、查询和Iceberg。当轮精确收尾后已读回三台VM关闭。之后退出修复完成实际Linux合成验证、双轮Linux CI和新签名来源dc48ff1的三节点安装；V12 final包绑定新来源并通过52项合成检查后，实际`hive-enter`和`hive-cleanup`均通过，清理同时满足合法完成帧与退出码0。随后`hive-register`在control RPC报错，未取得登记成功、独立读回或Iceberg结果。见[V12实际阶段证据](../evidence/hive-catalog-followup-20261013.json)。

V12外层报错是“Actual control Hive process failed”，当时没有保留内部Spark标准错误输出，不能据此判断是字段、目录、引擎还是收尾失败。资源采样未观察到OOM，仍不能排除未被该采样覆盖的异常。V13随后增加私有诊断，但SSH默认工作目录与包装器的`Path.cwd()`假设不一致：worker和cancel没有通过固定节点／目录门禁，诊断下载也失败。它没有到达V12原始Spark故障点，**修正V13的工作目录不等于修复V12的Spark问题**。V12和V13登记失败回执（历史本机记录）均记录自动收尾后三VM全关、`data_deleted=false`，原失败不改写。上述记录止于V13失败时。随后V14完成实际入口检查，但登记取得Snappy原生库映射错误；下节保留其诊断、修复对照和V15准备进度。不能只改历史命令版本号自行执行。

V12前置检查已实际确认：合成聚合manifest一致，原期限仍为**2026-12-14 00:00（香港）**，本地登记和HDFS中尚未使用本次Lake attempt；03／04保留范围也已重新检查，仍没有授予真实读取许可。较早一次附加元数据检查被维护者提前软关中断，未记为通过；之后的完整前置回执才是本次依据。这里不把观察摘要写成可重复使用的口令，下一次须重新观察。[签名CI证据](../evidence/signed-source-dc48ff1-20261013.json)与[安装读回证据](../evidence/signed-source-install-dc48ff1-20261013.json)只证明来源准备，不证明引擎。

当前`fixture-lake-01`后续Hive实验只验证原来已经产生的合成聚合。其固定run是`fixture-lake-01-r1`，聚合窗口2026-09-15～17，原聚合期限是**2026-09-15 00:00至2026-12-14 00:00（香港时间）**。本机manifest与incoming-manifest一致，pair副本登记同期限。原始信封7天期限已过，不能重新initialize同一lane、改时钟、重导旧raw。

```text
analysis: runtime/real/transfers/fixture-lake-01/fixture-lake-01-r1/
  manifest.json       固定来源、日期、哈希、原期限
  data/pair.json      受管聚合副本；读前仍需生命周期检查
  published/         已验收聚合发布目录；Hive从此读取
       ↓ 原90日期限、已登记的HDFS aggregate位置
analysis: Hive登记意图与实际清理
       ↓ 元数据RPC，经Windows协调
control: Spark local[1] → Hive metastore → ADS Parquet读回
       ↓ 行数与聚合哈希核对
analysis: 再次清理与校验，确认本次结果
```

本章只读核对本机transfer元数据与源码路径，没有读取pair载荷。远端published、Hive表和HDFS真正一致，仍须实际`register/verify`证明。

### 后续验收需要的前置与历史阶段含义

后续重新验收仍要求受控离线服务健康、双DN均存活、无其他YARN作业，原真实writer继续停止；当次协调器、worker及完整源码经过审核、签名绑定与节点安装，profile为`real-small-1792`。**这些条件要在新候选实际具备时重新核验；目前不执行下面的历史V11阶段。**候选文件在ignored目录，文件存在不等于已经安装或通过验收，也不能沿用冷重启前绑定容器启动时间的旧基线。

在**Windows PowerShell、仓库根目录**可以只查看历史协调器源码；下面不会运行它：

```powershell
$fixtureController = 'runtime/real/operator/run-lake-windows-monitored-v11.py'
Get-Content -LiteralPath $fixtureController -TotalCount 45
```

下表说明各阶段原本负责的工作，不是本轮可复制执行清单。若回到V11当轮停点，只有第一步实际通过、第二步失败；之后V12清理通过，登记仍失败。当前进度见页首及下节，不能混用不同时点。

| 顺序与阶段 | 动作 | 预期检查 |
|---|---|---|
| 1 `hive-enter` | 实际03/04门禁、固定源码校验；清理原登记过期fixture local raw；停NM/RM，保留NN和双DN，启动原Hive | 实际容器身份、阶段状态与内存；SSH返回0本身不够 |
| 2 `hive-cleanup` | analysis权威清单执行HDFS、Hive及已初始化后端清理 | 原期限、物理删除/不存在读回、ODS位点头保留；local raw清理不是HDFS清理证明 |
| 3 `hive-register` | 对固定aggregate登记外部表并真实读回 | location、来源范围、原90日期限、行数及聚合哈希一致 |
| 4 `hive-verify` | 另一次真实Spark查询已登记表 | 不复用旧成功JSON；缺表不补成0 |
| 可选 `lake` | 后续Iceberg及受控YARN阶段切换 | 需独立执行/验证/确认回执，不能由Hive通过推断 |
| 收尾 `hive-exit` | 精确停止当前Hive | 仍需离线控制器`stop-offline`；此时并不等于VM全关 |

例如，第三步和第四步有先后关系，但目前都未获得本轮通过回执：

```text
历史阶段含义：hive-register（登记并读回）→ hive-verify（另一次实际查询）
V11历史停点：hive-cleanup未验收，当轮未进入上述两步，也未执行lake。
```

### 768 MiB 余量与 VM 配额不是同一件事

control分配2048 MiB，仍须在停RM后、启动Hive前及其readiness循环实际有`MemAvailable >= 768 MiB`。每个catalog Spark driver启动前再次检查；临时driver限768 MiB、Java heap512 MiB，运行中另查来宾128 MiB余量及OOM。Lake恢复Hive后也重新检查768 MiB。

若不足，保留失败回执并按精确范围收尾。冷态仍不足时可以准备明确的新容量候选，例如仅增加control内存；但必须同步资源清单、profile、MemTotal允许范围、源码绑定和测试，记录新的条件。单改VMX、降低门槛、停止第二DN都不能让原候选变成成功。本章没有实施或证明新的内存扩容。

## 一次完整排障案例：从Hive登记失败到Snappy临时目录修复

这次问题发生在读取合成聚合的Hive目录阶段。**已经证明的是错误类型和局部修复有效；截至本次校准，尚未证明修复后的完整Hive登记、独立查询或Iceberg通过。**这一区分能防止把“测试小程序成功”写成“整个数仓成功”。下面所有字节、日期及路径都是技术元数据或合成实验范围，不包含真实事件。

### 第一步：把现象拆成层次，先问失败来自哪里

V12外层只说“Actual control Hive process failed”，内部stderr又没有留存。这句话只能证明调用失败，无法区分SSH入口、Python协议、Spark启动、SQL、HDFS读写或原生库加载。资源采样没有看到OOM，也不能凭此宣布排除了所有内存问题。不要立即扩大内存、改SQL或删除输出目录。

```text
Windows阶段协调器
  → SSH是否进入正确主机和固定源码目录
  → authority是否通过门禁、发出合法协议帧并正常退出
  → control临时Spark driver是否实际启动
  → Java异常是否来自SQL／文件系统／原生库
  → 最后再检查登记结果与另一次独立查询
```

V13为了取诊断增加包装器，却把SSH默认工作目录误当成项目根目录，worker和cancel在节点门禁前失败。这个失败必须单独记：它没有到达原Spark故障点。V14先用实际默认SSH工作目录完成4项入口检查，再保留有界的私有诊断，才取得实际异常：`Snappy UnsatisfiedLinkError: failed to map segment from shared object`，driver退出码为1。控制器自动精确收尾，保留数据和失败回执。公开证据只发布错误类别及技术身份，不把原始stderr全文放进仓库。

### 第二步：看懂为什么Java还要使用一个临时的本地库

Snappy负责压缩和解压缩。应用通过Java调用，但所用实现还会加载Linux原生共享库，通常是`.so`文件。把库解压到临时目录之后，Linux仍须允许把其中需要执行的代码映射进进程。文件存在、Java包存在、甚至普通读写都成功，并不等于原生库能够加载。

本项目容器普通`/tmp`是带`noexec`的tmpfs。`noexec`是挂载层的限制，不是文件的普通读写权限；仅做`chmod +x`不能取消这个挂载选项。实际旧配置对照恰好复现原生库映射失败。因此这次修复针对原生库加载路径，不能解释为“JAR缺失”“HDFS坏了”或“需要把Java heap无限加大”。

tmpfs是内存支持的临时文件系统，限额内实际写入仍占容器／宿主资源；它不是额外免费磁盘。512 MiB Java heap、容器768 MiB与两个tmpfs限额也不能机械相加当成已测峰值，须继续观察实际用量及容器总限制。

### 第三步：只给必要目录建立有界例外

实际实现位于[tools/spark_hive_catalog.sh](../../tools/spark_hive_catalog.sh)。以下是阅读源码的参数摘录，不是一条可以直接执行的Docker命令：

```text
普通临时目录：/tmp             rw,noexec,nosuid,nodev,size=192m
专用原生库目录：/opt/snow-native rw,exec,nosuid,nodev,size=16m,mode=0700
Spark driver选项：-Dorg.xerial.snappy.tempdir=/opt/snow-native
保留：只读根文件系统、固定镜像与JAR、原来源／容器身份检查及精确收尾
```

`exec`只在16 MiB专用目录生效，`nosuid`和`nodev`继续保留，普通`/tmp`仍是`noexec`。0700限制目录访问；实际容器使用的用户和挂载参数仍以锁定脚本为准。没有全局关闭宿主防护，没有把所有临时目录改成可执行，没有升级一整套依赖来碰运气，也没有为此改变原始数据期限。

### 第四步：做相同条件的旧／新实际对照

这一步在实际Linux容器内运行固定合成字节的压缩／解压缩，不访问生产事件，容器网络关闭。两组都用同一个锁定Spark 3.5.7镜像摘要、同一组已核验JAR、`local[1]`、1 CPU、768 MiB容器上限、512 MiB driver和128个进程上限。差异是专用原生库目录及对应Java选项。

| 检查项 | 旧配置实际结果 | 新配置实际结果 |
|---|---|---|
| 普通`/tmp` | 192 MiB tmpfs，`noexec` | 保持192 MiB和`noexec` |
| 专用native临时目录 | 无 | 16 MiB `/opt/snow-native`，可执行、0700 |
| Snappy加载 | 出现预期映射错误 | 无该原生库错误 |
| 固定296字节合成内容roundtrip | 未通过 | 压缩后解压与原输入一致 |
| 收尾 | 本次自有容器已移除 | 本次自有容器已移除 |

实际对照发生在香港21:09—21:11，证据整理于21:14。它证明在这些条件下旧配置失败、新配置能完成原生压缩库调用；不证明Hive metastore、HDFS、表定义或完整聚合结果。探针使用`network=none`；实际目录作业要访问既有Hive/HDFS，网络条件并不完全相同，不能把探针当作真实登记任务。

### 第五步：把修复作为完整来源发布，再继续引擎验收

探针证据中的`candidate_source_signed=false`描述21:14当时尚未签名的状态，应保留历史。随后修复进入签名`7173a0011ae60d654eae1e0210917029c31788d8`，两轮Linux CI全部成功，三节点444文件安装与独立读回完成；12份冻结writer、依赖和既有配置／恢复元数据保持。不能为了看起来一致而改写旧探针回执。

V15使用新候选及新的冷启动基线命名空间。21:32后的前置检查已重新核验03保留、04暂停、合成聚合manifest及原到期时间；当次检查时本次Lake attempt未使用，`read_permission=false`。这不是允许任意读取真实数据的许可，也不能在下一次启动直接重用观察摘要。当时维护者开始`hive-enter`，随后阶段切换实际通过；登记阶段发生了下节说明的资源中断。下列顺序仍是后续验收要求，不能把清理子步骤通过当成整轮登记通过：

1. 阶段切换和真实资源／服务检查；冷重启后不用旧容器启动时间基线。
2. 生命周期清理同时通过业务完成帧与正常退出码0。
3. 固定合成聚合的Hive登记、位置与哈希检查。
4. 另一次实际Hive查询确认，不复用登记成功JSON。
5. Iceberg另按实际输入、原期限、阶段资源及读回结果验收。

本段列出验收顺序，**不提供已中断V15或下一未验收配置的执行命令**。原合成聚合仍在2026-12-14 00:00香港到期；已过期的7日raw不能重新导入来支持这次重跑。真实业务日2026-10-13需到10月14日00:00后，再按新鲜源状态、同步前缀、实际存储ACK和覆盖条件登记，不能借这个9月合成聚合实验交付真实闭日。

### 第六步：局部修复之后，继续发现并处理资源门槛

Snappy旧新对照通过后，V15使用7173完整安装来源实际开始验收，`hive-enter`已通过。但是登记期间，监控器采样到analysis的可用内存为**127 MiB**，低于维持的**128 MiB**门槛，于是向主流程发送中断。最外层记录`KeyboardInterrupt`，这里不是用户手动按键，也不是新的Snappy映射异常；实际触发来源是`monitor → guest-gates → snow-analysis`。不应只读异常类名就误判原因。

该轮共有17份有界资源采样，analysis可用内存采样范围127～175 MiB、磁盘最低2143 MiB；control为602～1043 MiB、compute为1176～1204 MiB。它们是采到的范围，不是每一毫秒的严格峰值。已采容器OOM／oom_kill计数没有非零值，只能这样表述，不能扩大成整个宿主或所有未采时段绝无OOM。

三个已经执行的Spark请求都属于`cleanup`，各自返回0并有完成回执。这说明这些子请求正常收尾；**整轮`hive-register`仍是`complete=false`，没有登记成功证据**。自动收尾进一步确认三台VM实际关闭、待处理阶段切换为空、`data_deleted=false`。这和前面“业务完成消息与进程正常收尾是两项检查”一致；对子任务和整个父任务也要分别判断。

```text
原生库局部对照通过
  → V15实际hive-enter通过
  → 登记流程的清理子请求退出0
  → analysis采样127 MiB < 原门槛128 MiB
  → 监控器中断父任务：登记未验收
  → 精确收尾：VM全关、数据与原期限保留
  → 准备analysis离线768→1024 MiB，再以新来源和新基线复验
```

用户已经允许适当扩容并记录。本轮候选只为离线analysis增加256 MiB，从768到1024 MiB，control2048和compute1792 MiB保持，项目72 GiB、宿主35 GiB空闲磁盘／4 GiB可用RAM和来宾128 MiB门槛都保持。**在这份证据截点，实际VMX尚未改变，更大配置未经过引擎验收。**源码改好、测试通过和实际更改VM配置也应分别留下记录；不能通过把128门槛降低到127来“解决”失败。后续即使采样余量提高，也要重新完成登记和独立查询。

协调者随后独立核对运行VM为0，冷态项目约59.52 GiB，宿主空闲约190.83 GiB。这是停止后的时点，不是运行高峰，也不是本章作者再次启动测得。原合成聚合2026-12-14香港到期、03／04原期限、12份冻结writer和用户隐私选择不随资源调整改变。详细元数据见[V15资源中断证据](../evidence/hive-analysis-reserve-20261013.json)；其中不公开诊断tail或行数据。

**补充练习：三个子请求退出0、父任务被监控器中断，是否可以先发布登记成功？**不可以。子请求执行的是清理，父任务还需完成登记、结果检查和自己的正常收尾；正确记录是“清理子请求成功、整轮登记中断且已收尾”。下一轮需要新基线和新实际回执，不能复用旧成功片段拼成一轮不存在的通过。

### 在Windows本机核对证据，不启动任何服务

工作目录为仓库根；前置条件是checkout中已有下列公开、脱敏JSON。这组PowerShell只读取文件并显示选定字段，不启动VM、不发SSH请求、不提交作业。路径不存在应先核对版本，不用手造同名成功文件。

```powershell
Set-Location 'Snow_Statistics 仓库根目录'
$nativeEvidence = Get-Content -Raw docs/evidence/hive-snappy-native-20261013.json | ConvertFrom-Json
$nativeEvidence.failed_actual_register | Select-Object controller, accepted, exception, driver_returncode
$nativeEvidence.actual_container_probe | Select-Object engine, network, old_expected_failure_and_new_roundtrip_verified
$nativeEvidence.actual_container_probe.cases | Select-Object case, roundtrip, expected_native_error, owned_container_removed
$sourceEvidence = Get-Content -Raw docs/evidence/signed-source-7173a00-20261013.json | ConvertFrom-Json
$sourceEvidence | Select-Object source_sha, archive_sha256, archive_files
```

预期V14登记仍为`accepted=false`和退出码1；old为`roundtrip=false`、`expected_native_error=true`，new为`roundtrip=true`、`expected_native_error=false`。这些失败布尔值是保留下来的真实对照结果，不需要“修成true”。这段历史对照的来源输出应为7173完整提交、对应归档哈希和444个文件；2434的447文件和当前fa057fe的452文件安装分别另有证据，不改写旧探针条件；完整来源与安装检查另见[签名证据](../evidence/signed-source-7173a00-20261013.json)、[三节点安装证据](../evidence/signed-source-install-7173a00-20261013.json)及[Snappy对照证据](../evidence/hive-snappy-native-20261013.json)。

### 练习与逐项答案

1. **文件可读写，为什么还加载不了？**读写权限只决定能否操作文件；原生代码映射还受挂载选项限制。本例实际对照支持`noexec`路径限制，不能用“文件在”证明“库可用”。
2. **能否直接把宿主`/tmp`全改成exec？**本项目无需扩大到全局：16 MiB专用tmpfs和明确的Snappy加载路径已通过局部对照。通用临时目录、宿主和其他项目不需要随之改变。
3. **新探针通过是否代表Hive表已正确登记？**否。它没有连接metastore或核对表位置、分区、聚合哈希；完整登记和独立查询仍是下一层证据。
4. **临时目录16 MiB是否意味着多占16 MiB磁盘？**这是tmpfs上限，实际占用随写入变化并进入资源约束，不能当成额外永久磁盘，也不能直接据限额推算峰值。
5. **面试如何描述修复？**可以说“保留实际失败，用分层诊断定位原生库映射问题；在固定镜像和资源下复现旧配置，采用专用有界临时目录，完成真实旧新roundtrip对照，并继续进行完整Hive验收”。最后一句应随最终回执更新，不能提前说已经通过。

## 回执检查、常见失败与恢复

### “工作做完”与“正常退出”要分别检查

可以把 `complete` 理解为工作人员说“这一段处理结束了”，退出码则是他交还资源、结束程序后的最后检查。程序可能已经发出完成消息，却仍有后台线程占着输入流，最后在退出时崩溃。因此，**业务完成消息与进程正常收尾是两项检查**：当前协调器必须同时收到同一会话的合法完成帧，并确认 authority 进程退出码为0；只看到 `complete`、有输出文件或SSH曾经能连上都不够。

本次独立合成子进程使用原版本输入线程，在父进程保持管道打开时，确实复现了“先发complete，随后解释器退出崩溃”；半帧输入也能复现。V11实际失败的 stderr 当时未留存，所以不能把合成堆栈冒充那次实际进程的崩溃栈。修复让Linux读线程可以被取消，并在退出前等待输入线程和断连监控结束；实际Linux合成管道验证、双轮Linux CI与三节点来源安装现已通过，V12清理也通过了两项检查。V12后续登记失败和V13诊断包装器失败是另外的故障，不能因退出修复已通过就忽略它们。合成退出诊断（历史本机记录）只证明合成进程边界。

结果分四层核查：

1. **传输层：**SSH退出码、控制器完整结束；仅说明命令及进程结果。
2. **引擎层：**Spark版本、master、应用ID、YARN最终状态、原临时driver退出；driver结束不证明所有executor都结束。
3. **数据层：**固定输入、source、cutoff、质量恒等式、ADS落盘读回和同聚合哈希；回答“是否正确算了这批数据”。
4. **发布与收尾层：**accepted何时生成、旧发布是否保留、cleanup_complete、实际VM状态；独立恢复不改写原失败。

不必打印真实原始行排错，先检查允许的元数据和回执。逐行查看只在本章合成教学目录进行。

| 现象 | 含义与处理 |
|---|---|
| `no_input/no_computable_input` | 无可处理输入或不满足计算条件；核对位点、闭日和coverage，不当作0人0请求 |
| 源码不符或保留范围摘要失配 | 代码或03/04状态变了；核对部署与实际观察，不复用旧成功回执 |
| `errorifexists`或既有CID/输出 | run已使用；先确认旧工作精确停止，再登记新run，不删目录强行重跑 |
| Parquet与golden不一致 | 检查输入hash、去重、香港日期、source、cutoff；保留失败输出 |
| YARN长期ACCEPTED | 可能资源不足或NM未就绪；查阶段和资源分配，不盲目增加executor |
| Hive有表但结果不符 | 核对metastore、location、分区、来源与聚合版本，不首先猜Spark算错 |
| 7日raw过期而90日aggregate有效 | 清理完成后可走受管聚合读取，不能给旧raw重发permit或延长原期限 |
| 短permit过期 | 重做实际准入与metadata staging，不能修改JSON时间 |
| 内存/磁盘/OOM失败 | 收尾并记录；准备明确资源调整，不全局prune、不删除业务资料 |
| `cleanup_complete=false`或取消未知 | 不发新计算；按本次attempt查精确worker/driver与VM，不全局kill或强制断电 |
| 收到`complete`但进程退出非零 | 本次仍失败；保留回执与实际退出码，检查线程、管道和精确收尾，不能跳过退出检查进入下阶段 |

离线控制器回执在`runtime/real/offline-small/<attempt>/controller.json`。V11回执在`runtime/real/operator/fixture-lake-01-<phase>-<唯一值>-windows-v11-resources.json`。保留phase、错误类型、资源样本、pending transitions和cleanup。

历史V11的`lake-recover`只适用于它固定`lake`阶段执行或取消不确定的情况：先确认原attempt精确取消，再恢复原阶段；取消未确认不能因“没看到容器”而关闭VM。V11当轮失败发生在`hive-cleanup`，没有进入`lake`，当轮不执行这条恢复命令。它也不适用任意旧run，普通Hive过渡失败应先核对pending attempt。

正常收尾仍在**Windows PowerShell、仓库根目录**，沿用同一`$offlineArgs`：

```powershell
& $lessonPython tools/real_offline_small.py @offlineArgs stop-offline
if ($LASTEXITCODE -ne 0) { throw '停止未完成：保留回执检查，不宣称VM已全关' }
& $lessonPython tools/vmware_lab.py status
```

预期三台VM均停止，数据保留。停止不用`down -v`、NameNode格式化或全局清理；关功能和到期清理是不同操作。

## 追加练习、答案与面试追问

**练习1：28条输入重复三份应得到什么？**答案：raw84、valid28、duplicates56、quarantined0，daily不变。可复制教学脚本到新教学文件，把`* 2`及断言改为`* 3`后验证；不修改正式数据或事件ID来制造正确结果。

**练习2：本例每日UV为2、1、1，三日去重UV是多少？**答案：2，不是4；后两天是第一个匿名标识再次活跃。没有跨日辅助状态时，不能单从日表推算跨日人数。

**练习3：Hive表存在，另一台机器查不到，先查什么？**答案：是否连同一metastore，location是否属于当前lane/run，分区是否登记，底层文件是否在有效期，查询来源/日期/cutoff是否一致。不通过手填0证明修复。

**练习4：10月1日接收的raw已于10月8日到期，10月10日能否读未到期聚合？**答案：只有已登记副本实际清理完成、聚合自身有效并通过读取门禁后可以；不能重读raw、重发旧计算许可，复制也不重新计时。

**面试追问：为什么不可变run并且验证后发布？**回答要点：失败输出与正在服务的历史结果分离；新run先查输入、质量和落盘结果，再更新发布状态。重复导入用业务键去重，重跑以固定输入/cutoff和版本控制验证。只说“用了事务所以exactly-once”不能解释这些边界。

**面试追问：实际证明了什么？**可以展示合成56条的YARN结果、原资源条件、真实失败与独立恢复、本地手算基准。当前真实闭日、Hive/Iceberg、生产P95分别需要对应回执；配置文件、成功安装或其他历史输入不能替代。单宿主上的HDFS双副本实验不是物理机容灾。

继续对照[Hive协调入口](../real-hive-dispatch.md)、[聚合复制源码](../../src/snow_statistics/real_transfer.py)、[Python基准](../../src/snow_statistics/model.py)。

[返回学习目录](00-阅读地图与当前状态.md)
