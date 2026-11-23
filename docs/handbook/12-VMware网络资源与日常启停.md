# 12 VMware、网络、资源与日常启停

> PowerShell 示例从克隆后的仓库根目录运行。标注为历史的命令和 `runtime/real/operator/` 记录依赖当时的私有环境。新的合成复现按 [README](../../README.md) 与 [分阶段运行手册](../runbook.md) 操作。

> **2026-10-14真实Hive／Iceberg与最终停止已通过：** 原BCE闭日模型包保持原来源，F8／483在`real-small-1792`（control2048／compute1792／analysis1024 MiB）完成真实Hive登记与独立验证。Spark 3.5.7 `local[1]`应用分别为`local-1791973277761`和`local-1791973714447`，daily／session_daily／retention／funnel四组存在性、计数比较及私下行摘要比较均通过。随后真实Iceberg由两个不同YARN应用`application_1791974201828_0001`、`application_1791974201828_0002`完成执行和独立验证，四组当前／适用历史快照及新增`model_revision`列的校验有效，analysis权威确认通过，见[F8真实Hive与Iceberg技术证据](../evidence/real-hive-lake-f8-r2-20261014.json)。公开只给比较布尔与引擎身份，不发布业务计数、行摘要或可枚举的请求／结果／metadata内容哈希。原聚合2026-10-13香港日期对应的90日到期2027-01-11保持；DIM原始输入仍按其独立七日到期。这不表示Lake表已登记到Hive，也不声称分区演进、MERGE、全图或列级血缘、生产P95。

> **本轮独立收尾与公网回归：** Lake完成后，三个节点分别读回原容器身份、HDFS保持、Hive阶段恢复、阶段租约消失；control精确driver／CID／worker不存在。然后`hive-exit`（9316be）与公共`stop-offline`（d58aa2）分别退出0、数据未删除；独立VM状态（4cbe36）在该观察时点运行数为0，项目约61.99GiB、宿主空闲约194.52GiB，正常72GiB／35GiB边界保持。这是当时的观察，不承诺后续永远关机。UTC10:50:48公网只读回归中，两站和统计页HTTP200，测试的私有路由404，v1／v2契约、旧v1隐藏旁路、no-store及26小时快照检查通过；没有POST、付费模型调用或业务数值记录，也没有新增浏览器交互验收。

> **2026-10-14真实DIM已实际通过：** F8／483来源在显式`real-dim-2560`条件下完成r2，Spark 3.5.7、Python 3.8.10、`local[1]`，实际应用为`local-1791971531318`。`at_event`与`current`两种解析模式的全部字段比较均为true，非空输入验收通过；driver及外层SSH实际退出0，后续独立检查确认本次精确所属容器已不存在，见[F8／r2真实DIM技术证据](../evidence/real-dim-f8-r2-20261014.json)。业务计数和可能被小范围枚举重构的私有结果文件哈希只在私下校验；公开只保留“私有绑定与验证已通过”的布尔结论。这证明本轮真实DIM解析与Join的引擎执行，不会把空历史覆盖补齐：`history_coverage`仍为unknown，unknown也是需正确比较的结果。原输入2026-10-13 04:06:35.627 UTC对应的七日到期2026-10-20 04:06:35.627 UTC保持；没有改写BCE指标、正式产品或公开统计。监控仅有10次宿主采样，54.359秒是SSH窗口时长，不是精确Spark耗时、来宾连续监控或峰值，也不声明VM已关闭。真实Hive／Iceberg和其他结果分别验收。

> **DIM阶段关闭历史及现在沿用的顺序：** 如果此前切入了Hive专属阶段，先用本轮绑定控制器执行`hive-exit`，检查实际退出和仍有工作项；然后才用与启动一致的profile及本轮保护摘要运行公共`real_offline_small.py stop-offline`。Hive服务不属于公共停止入口的默认服务集合。本次直接stop-offline2560首次以`UnknownRunningWork`拒绝（工具179335，退出1），未取得或停止VM、未删除数据；随后精确`hive-exit`实际退出0（a4e371），数据保留。该早期截点没有提前宣称公共停止完成；本轮最终停止结果见页首独立观察，不能把Hive退出本身等同于全VM关闭；不清空服务白名单、不绕过公共门禁，也不全局清理Docker。

> **2026-10-14最新源码截点：F8三节点安装已完成。** `f8e3ca8c7823b8736150362ab57316f6a1bca75b`已完成GPG签名、独立验签和推送；push／PR两轮Linux CI共4个作业、54个步骤全部成功，483文件归档与该提交的Git树一致，见[F8签名与CI证据](../evidence/signed-source-f8e3ca8-20261014.json)。analysis、control、compute各自完成prepare、install及独立483文件readback，九个阶段均退出0；每台相对C0F改变16文件，12份冻结writer及依赖保持。配置、恢复元数据和原期限按同一节点的本次fresh prepare与install比较保持，不要求跨节点或与旧历史摘要相等，见[F8三节点安装证据](../evidence/signed-source-install-f8e3ca8-20261014.json)。安装没有启动容器，也不代表引擎验收。原BCE闭日计算、C0F看板／血缘及旧Hive过程继续保留各自实际来源；在源码安装这个时点，DIM引擎尚待实际验收；其后F8／r2已通过，结果见本章最新DIM截点。安装后的全VM关闭是当时观察，后续已进入新的启动窗口，不能作为当前停机声明。

> **2026-10-14私有UI实际完成：** c0f来源在有限loopback窗口显示四个页签及对应表格，日期、cutoff、分母、留存解释、会话30分钟和漏斗30分钟文案通过实际DOM检查，未读取或导出私有业务数值，见[实际私有看板证据](../evidence/real-private-view-20261014.json)。配置权限从0664改为0600，字节与owner保持。UI外层退出0、900秒timeout子进程退出124，实际清理后无活成员且8502监听消失；转发外层退出0、SSH子进程退出1，两类退出原样保留。60秒租约跨界后的浏览器刷新及个别D1/D7状态没有独立观察，仍为未验证。这里展示原BCE闭日包，不证明新的DIM Join或独立Doris SQL。

> **历史容量截点：2304前置失败，当时2560待实测。** 已完成的2304 VMX配置与单机无引擎MemTotal2269148KiB／MemAvailable1814740KiB是较早的有限采样。后来的实际CLI在`resource_preflight`报`Analysis guest is too small`，Spark尚未启动，见[2304实际前置失败](../evidence/real-dim-preflight-2304-20261014.json)。不能由旧采样推断该点最低余量或唯一失败原因。新显式`real-dim-2560`为control2048／compute1792／analysis2560MiB、冷态后备6400MiB，另保留128MiB写入预算和每台256MiB启动预留；新VMX、同一次CLI资源准入、DIM引擎和精确收尾仍待实测。2048MiB可见总内存、1536MiB实际可用内存及72／71.75／35／4边界没有降低；原2304、默认和旧Hive/Lake条件保留。

> **2026-10-14 analysis容量实际更新：** 本机analysis虚拟盘已经从29扩到34GiB，分区身份和其他分区保持；第一次来宾启动已自动扩大根分区和ext4，后续growpart/resize2fs没有继续增长，见[analysis扩盘证据](../evidence/analysis-disk-growth-20261014.json)。此前“analysis29GiB、本次未调整”的记录是10月13日历史截点，不能再当当前容量。宿主当前源码的新建analysis默认值已同步为34GiB；虚拟盘逻辑容量不等于宿主实际占用，也不等于全部可用根分区空间。

> 这次全部VM关闭时使用的临时冷维护预算已恢复72GiB。运行停止线71.75GiB、35GiB宿主空闲磁盘和4GiB可用RAM不变，不把临时预算带到正常启动。容量验收只证明磁盘维护；analysis、control、compute均已分别安装并完整读回c0f的477文件，见[c0f三节点安装证据](../evidence/signed-source-install-c0f542c-20261014.json)，真实DIM仍需独立验收；本轮私有UI的实际DOM与有限收尾已通过，见[实际私有看板证据](../evidence/real-private-view-20261014.json)。不要为复现同一结果再次扩盘、重复增长或重跑已经完成的管理员校时。

> **2026-10-13 恢复说明：** 用户已授权继续任务，并允许在记录操作的前提下适当扩容。当前项目文件预算从 64 GiB 调整为 72 GiB；宿主至少 35 GiB 空闲磁盘、4 GiB 可用 RAM 的要求不变。实时存储阶段使用仅 analysis 4608 MiB，离线服务另分阶段启动。旧 real-prod-01／02 已过期，不能恢复旧 Checkpoint、改期限或继续用旧 scope。先清理已登记到期副本，再按真实接收时间选择新窗口并显式记录缺口。今天已确认存在真实输入，但这不等于自然闭日报表、D1/D7 留存或本轮完整引擎验收已经完成。历史实验仍按当时预算与源码解释，当前路线见[第13章](13-真实数据按需同步与观察完整性.md)和[第36章](36-常用命令索引与执行位置.md)。

## 三台VM的职责

control主要负责控制面和作业提交，compute承载计算executor与DataNode，analysis按阶段运行第二DataNode、Doris、实时或治理组件。它们使用NAT，不需要给每个实验服务购买公网服务器。FinalShell可用于查看终端；可重复操作优先使用仓库SSH脚本。

VMware程序：`C:\Program Files (x86)\VMware\VMware Workstation\vmware.exe`。FinalShell：`%LOCALAPPDATA%\finalshell\finalshell.exe`。VMX、磁盘、seed、私钥、known_hosts在忽略的runtime中，不放GitHub。

**历史磁盘容量记录（2026-10-13 15:57 香港时间）：** 当前这台电脑的 control 已从18GiB实际扩到20GiB并核验ext4；compute此前登记18GiB、analysis此前登记29GiB，本次没有调整后两台磁盘。这里是现有虚拟盘的逻辑容量，不是已占用宿主空间，也不是所有新建环境的默认值。下文“control扩盘实操实例”记录操作与结果；不要为达到同一容量重复扩盘。

| profile/场景 | 内存预算 | 已验证范围 |
|---|---|---|
| scale | control2GiB / compute2GiB / analysis1GiB | HDFS/YARN日指标10万和百万合成扩样 |
| real-small | control2048MiB / compute2048MiB / analysis768MiB | 2026-09-19新增小流量离线选项；此处校准时仅源码检查/测试，实际阶段验收待回执 |
| real-small-1920 | control2048MiB / compute1920MiB / analysis768MiB | 已有56条合成golden与正式ODS落地的独立阶段证据；新增`real_offline_small.py`实现/CI已通过，实际VM入口验收进行中，两类证据分开 |
| real-small-1792 | control2048MiB / compute1792MiB / analysis768MiB | 9月19日15:37 UTC完成56条合成Spark/YARN golden；原控制器cleanup失败另保留，15:49独立精确收尾通过。不是全部真实闭日/Hive/Iceberg验收 |
| realtime | 仅analysis4.5GiB | 单VM Kafka/Flink/Doris实时及恢复 |
| ha | 仅analysis3GiB | Kafka与ZooKeeper分开做协议故障实验 |
| 完整模型/治理 | 按各阶段已有文档检查 | 不能把scale成功外推成全部服务同时运行 |

```text
先查VM状态 → 关机后改profile → 检查预算 → 启动所需VM
→ 查实际IP/时钟 → 按阶段启服务 → 作业 → 保存证据 → 停服务/VM
```

## 新环境从零搭建：当前电脑已有 VM 时跳过本节

本节只建立独立实验基础设施，不修改正式服务器，也不把真实 reader 凭据复制进新环境。当前电脑的 `runtime/vmware` 已有三台 VM，应从后面的“日常使用”开始。若这个目录已有磁盘、VMX、密钥或历史实验，先辨认原环境；不能为了重试初始化删除整个目录。

### 第一步：核对宿主工具和可用资源

先完成[第11章](11-PowerShellPythonNodeJava基础环境.md)。在 Windows 仓库根目录执行：

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
Test-Path -LiteralPath 'C:\Program Files (x86)\VMware\VMware Workstation\vmrun.exe'
Test-Path -LiteralPath 'C:\Program Files (x86)\VMware\VMware Workstation\vmware-vdiskmanager.exe'
Get-Command ssh, scp
uv run python tools/vmware_lab.py status
```

两个 Test-Path 应为 True。新电脑未安装 VMware 时先安装 Workstation，并确认自己的安装目录与 `tools/vmware_lab.py` 的 `VMWARE` 常量一致；本工具不会替你下载安装 VMware。status 的当前容量报告要求项目最多72GiB、宿主至少35GiB空闲。还需为预计启动的 VM 内存额外保留4GiB宿主可用内存。72GiB是2026-10-13用户授权并记录的预算调整；门禁失败仍要停止或重新核验资源，不能默默改数，也不清理其他项目。

### 第二步：生成本项目 VM、启动一台并安装基础软件

以下 prepare 会下载固定 Ubuntu 24.04 云镜像、核验 SHA-256、创建独立薄置备盘、NAT VMX、cloud-init seed 和 SSH 信任材料。`pycdlib` 只供制作 seed ISO 使用，普通 `uv sync --extra lab` 不包含它。

```powershell
# 仅新环境；会下载镜像并写入 runtime/vmware
uv run --with pycdlib==1.14.0 python tools/vmware_lab.py prepare
# prepare 的初始内存不是日常小配置；先改关机VM配置，再逐台初始化
uv run python tools/vmware_lab.py configure --node snow-control --profile scale
uv run python tools/vmware_lab.py start --node snow-control
uv run python tools/vmware_lab.py ip --node snow-control
uv run python tools/lab_remote.py --node snow-control --script tools/bootstrap_guest.sh
```

预期 prepare 生成三台 VM 的文件；首次启动等待 cloud-init 建立 snow 用户及 SSH，ip 返回实际 NAT 地址。bootstrap 只接受三个项目主机名，安装 Docker Engine、Compose v2、Python venv 等基础包，并输出 Docker 服务端版本。它会访问 Ubuntu 软件源，网络失败应保留日志 `/tmp/snow-bootstrap-packages.log` 检查；不能把半完成安装当成可运行集群。

此时只启动 control。后续对 compute 按同样次序逐台完成；analysis 首次基础安装可用 `batch` 的2GiB，安装结束关机后再切回所需阶段。更换命令里的 `--node` 只允许 snow-control、snow-compute、snow-analysis，不填正式服务器。先完成一台下述源码/环境步骤、正常关机，再处理下一台，避免同时堆叠安装内存。

### 第三步：分发源码与锁定的 Python 依赖

先在 Windows 检查 `git status --short` 和 `git rev-parse HEAD`。从干净的、已选定版本打包。`tools/bundle.py` 打包 Git 已跟踪文件的当前内容，所以未提交修改也会改变内容；单有提交 SHA 不能代替压缩包哈希。它不会把 runtime、VM磁盘或私钥打入包。

```powershell
# Windows仓库根目录；只用于这个新环境，勿覆盖正在登记的真实epoch源码
uv run python tools/bundle.py
uv export --frozen --extra lab --no-emit-project --format requirements.txt --output-file runtime/handbook-lab-requirements.txt
Get-FileHash -LiteralPath runtime/source.tar.gz -Algorithm SHA256
uv run python tools/lab_remote.py --node snow-control --upload runtime/source.tar.gz --remote /home/snow/handbook-source.tar.gz
uv run python tools/lab_remote.py --node snow-control --upload runtime/handbook-lab-requirements.txt --remote /home/snow/handbook-lab-requirements.txt
```

在 FinalShell 中建立到刚才 NAT 地址的 SSH 连接：用户 snow，端口22，使用 `runtime/vmware/id_ed25519`，核验该节点在 `runtime/vmware/known_hosts` 的主机公钥。FinalShell 程序路径见本章开头。不要导入正式服务器私钥，也不要关闭主机校验。命令行替代方式是将下段保存为 Windows 的 `runtime/handbook-install-source.sh`，使用 `lab_remote.py --node snow-control --script runtime/handbook-install-source.sh` 运行；工具会处理 CRLF。

```bash
# 新建Linux VM，用户snow，起点/home/snow；已有源码目录立即停止
set -euo pipefail
cd /home/snow
sha256sum handbook-source.tar.gz
# 先与Windows输出逐字核对；一致后才执行下面部分
test ! -e /home/snow/Snow_Statistics
mkdir /home/snow/Snow_Statistics
tar -xzf /home/snow/handbook-source.tar.gz -C /home/snow/Snow_Statistics
cd /home/snow/Snow_Statistics
python3 --version
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r /home/snow/handbook-lab-requirements.txt
.venv/bin/python -m pip install --no-deps -e .
.venv/bin/python -c 'import snow_statistics; print(snow_statistics.__file__)'
.venv/bin/python tools/real_lab.py --help
```

使用脚本远程执行时，把哈希检查和后续安装分成两次调用：第一次只执行 `sha256sum`，核对后再执行安装段，不能指望脚本中的中文注释自动暂停。预期 Python3.12、模块路径为 `/home/snow/Snow_Statistics/src/snow_statistics/...`，help 能正常导入。依赖安装成功但模块找不到时，检查是否遗漏 editable 安装；2026-09-19 的实际环境准备已验证这个修复。安装器若因源码要求的 Python 版本不符而拒绝，先解决解释器，不跳过版本限制。

### 第四步：配置私有实验地址与 Hadoop

三台 VM 都完成上述初始化后，记录各自实际 NAT IP。在新环境建立 `lab/.env`：地址字段为 CONTROL_IP、COMPUTE_IP、ANALYSIS_IP；模拟器/治理后续需要 LAB_MYSQL_PASSWORD、LAB_MYSQL_ROOT_PASSWORD、LAB_CDC_PASSWORD、LAB_GOVERNANCE_PASSWORD。密码只用于合成实验，不复用生产密码。没有现成的 `lab/.env.example`，下面的教学脚本会生成独立随机值，并拒绝覆盖已有文件。

将以下内容保存为 **Windows 仓库** `runtime/handbook-create-lab-env.py`，用 `uv run python runtime/handbook-create-lab-env.py` 执行。该脚本只输出完成消息，不显示密码。

```python
from pathlib import Path
from ipaddress import IPv4Address
from secrets import token_hex

target = Path('lab/.env')
if target.exists():
    raise SystemExit('已有私有配置，请核对并保留，不覆盖')
values = {key: str(IPv4Address(input(key + ' 的实际 NAT IP: ').strip()))
          for key in ('CONTROL_IP', 'COMPUTE_IP', 'ANALYSIS_IP')}
for key in ('LAB_MYSQL_PASSWORD', 'LAB_MYSQL_ROOT_PASSWORD', 'LAB_CDC_PASSWORD', 'LAB_GOVERNANCE_PASSWORD'):
    values[key] = token_hex(24)
with target.open('x', encoding='utf-8', newline='\n') as stream:
    stream.write(''.join(key + '=' + value + '\n' for key, value in values.items()))
print('已建立本机私有 lab/.env；不要展示内容或加入 Git')
```

将同一份私有配置逐台上传到新建仓库的 `lab/.env`，例如：

```powershell
# Windows；每次目标必须是已初始化并确认目录存在的项目VM
uv run python tools/lab_remote.py --node snow-control --upload lab/.env --remote /home/snow/Snow_Statistics/lab/.env
```

在各 VM 的 `/home/snow/Snow_Statistics` 执行下面步骤。只 source 刚才自己生成的私有配置；`.env`在这里是 shell 输入，不能执行来源不明的文件。

```bash
cd /home/snow/Snow_Statistics
chmod 600 lab/.env
set -a
. lab/.env
set +a
.venv/bin/python tools/render_hadoop.py --control "$CONTROL_IP" --compute "$COMPUTE_IP" --scale-small
test -f lab/generated/hadoop/core-site.xml
test -f lab/generated/hadoop/yarn-site.xml
```

预期各 VM 的配置引用同一 control/compute IP，副本数2，scale 的 YARN 内存上限已缩小。不要手改已有真实环境的地址或 XML 来“试一下”；IP变化时应停止对应阶段，核对三端一致后再部署。锁定镜像摘要在 `lab/locks/images.env`，不能把 `@sha256:...` 改成 latest。首次阶段启动可能下载缺少的锁定镜像；这会显著增加 VMDK，先检查剩余空间，不提前拉取所有 profile。

### 第五步：检查时钟并结束基础搭建

在 Windows 先看 `Get-Date -AsUTC`，再对已启动节点执行只读时钟检查；实际 IP 用前面 ip 的结果填写：

```powershell
$guestIp = Read-Host '输入已核对的 snow-control NAT IPv4'
uv run python tools/guest_clock.py --node snow-control --ip $guestIp
```

偏差达到工具门槛时停止实时实验。仅当这台新 VM 没有运行任何容器、已核对宿主时间时，才使用同一命令加 `--set` 修正 guest UTC/RTC；不要在运行中的真实作业里调钟。依次检查其他节点。首次到此仅证明 VM、SSH、Docker、Python和配置可用，不证明 HDFS、Spark、Flink或Doris已经跑通。结束后正常软关机，后续按具体阶段启动。

## 日常使用：每次运行前做什么

```powershell
uv run python tools/vmware_lab.py status
uv run python tools/vmware_lab.py ip --node snow-analysis
```

资源门禁从deploy/resources.json读取：当前项目文件72GiB、宿主至少35GiB空闲磁盘、至少4GiB可用RAM。`real_offline_small.py`保持256MiB额外启动/作业预留，当前停止线为71.75GiB；历史`run_scale_guarded.py`的额外预留要按对应源码读取，两者不能混写为同一个参数。VM的.vmem后备文件也计入预算，磁盘薄置备逻辑容量不等于当前物理占用。2026-10-13把预算提高到72GiB的操作本身不扩展guest磁盘；同日随后完成的是另一次control18→20GiB维护，两项结果分别记录。历史60／64GiB实验及63.75GiB停止线保留旧值，不以新预算重新宣称历史验收合格。

仅在三VM关机且决定运行scale阶段后：

```powershell
uv run python tools/vmware_lab.py configure --node snow-control --profile scale
uv run python tools/vmware_lab.py configure --node snow-compute --profile scale
uv run python tools/vmware_lab.py configure --node snow-analysis --profile scale
uv run python tools/vmware_lab.py start --node snow-control
```

依次检查每台启动结果，再决定继续，不一次性打开所有profile。IP可能变化，以工具观测为准，手册里的192.168.216.*只是历史例子。

## control扩盘实操实例：容量预算、虚拟盘、分区和文件系统是四件事

### 通俗解释与实际结果

可以把宿主项目预算理解为“允许这批文件共用多少空间”，VMDK逻辑容量是“交给虚拟机的硬盘有多大”，分区是“这块盘怎样分区”，ext4则是“根目录真正可以使用的文件系统”。只修改72GiB门禁，不能让Linux的`df`多出空间；只扩VMDK，也必须检查分区和ext4是否一起增长。

这次离线窗口里control根目录曾只剩约371MiB，低于384MiB门槛，因此先停整个窗口。2026-10-13完成的具体变化如下；单位GiB按1024³字节计算。

| 项目 | 扩盘前/维护期间 | 完成后 |
|---|---|---|
| control虚拟盘 | 18GiB | 20GiB |
| 根分区实际字节 | 18,252,545,536 | 20,400,029,184，增加2GiB |
| 根文件系统总字节 | 17,602,314,240 | 19,682,557,952 |
| 根文件系统可用字节 | 冷态检查614,338,560；运行低点另计 | 2,702,868,480，约2.52GiB |
| 项目文件预算 | 日常72GiB；全VM关闭维护时临时82GiB | 已恢复原72GiB配置 |
| 项目文件大小合计 | 维护过程采样峰值75.2358GiB | 工具结束约59.41GiB |

根分区保留原起点和身份，其他分区逐项比对不变。首次开机检查时，根分区与ext4已经自动长大；之后执行`growpart`得到`NOCHANGE`，`resize2fs`报告`Nothing to do`。因此正确表述是“扩展虚拟盘后，开机自动完成guest增长，再实测核验”，不能写成后两条命令才使文件系统增加空间。这里没有测定自动增长的每个内部调用。

### 当时的操作顺序与为什么这样做

```text
离线窗口磁盘低于门槛
  → 停止窗口并软关全部VM
  → 核对精确control磁盘、无快照、原分区结构
  → 登记仅冷维护82GiB，并预留21GiB工作空间
  → VMware固定18→20GiB扩盘，监测临时文件
  → 回读20GiB、VMX未变、未知残余0、项目回到72GiB内
  → 按字节恢复原72GiB配置
  → 仅开control，核验自动增长后的分区与ext4
  → 保存回执并软关，再进入新的离线窗口
```

`monolithicSparse`扩盘期间可能建立接近原盘大小的临时副本。[官方说明](https://knowledge.broadcom.com/external/article?legacyId=2041556)要求为原虚拟盘容量保留工作空间，本次按18GiB工作空间＋2GiB增长＋1GiB余量，共21GiB预留，并一直保留宿主35GiB空闲磁盘及4GiB RAM。工具实际创建过临时盘和锁文件，结束后未知残余为0；没有人工全局删除缓存，也没有添加保留真实数据的VM快照。

最终薄盘文件只增加131,072字节，不能据此推断“扩盘只需128KiB”。项目采样峰值75.2358GiB表明原72GiB日常门槛不够覆盖这次维护；采样可能错过瞬时峰值，因此只称采样峰值。扩完仍恢复72GiB日常预算，不把临时82GiB变成新的引擎运行额度。

### 现在如何检查（不重复执行扩盘）

**Windows PowerShell，仓库根目录；不启动VM：**

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
# 当前项目容量/VM状态；额外256MiB使status同时检查RAM余量。
.venv/Scripts/python.exe tools/vmware_lab.py status --reserve-mib 256
# 公开的是容量元数据与私有回执摘要，不含真实事件或磁盘UUID。
Get-Content -LiteralPath docs/evidence/control-disk-growth-20261013.json
```

预期公开证据状态为`host_expanded_and_guest_capacity_verified`、虚拟盘`after`为21,474,836,480字节，临时预算已恢复。`status`显示的是当前运行情况；如果另一阶段正运行，不应为了获得“全关”的显示去中断它。源配置中的新建磁盘默认值与现有VMDK可能不同，检查实际磁盘不能只看代码常量。

**Linux control，已经按当前阶段启动且允许只读检查时：**

```bash
cd /home/snow/Snow_Statistics
lsblk -b -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS
findmnt -no SOURCE,FSTYPE /
df -B1 /
```

预期`sda`为21,474,836,480字节，根挂载仍为`/dev/sda1 ext4`。`df`当前空闲会随日志/作业变化，不要求永远等于维护回执中的2,702,868,480字节。若未启动，不为查看这些数字另外开启虚拟机；先读保存的容量证据即可。

### 常见失败与处理

- 宿主有空间但项目门禁不够：先按工作空间估算登记维护范围和预算，不能绕过检查。82GiB只属于这次已完成的冷维护。
- 工具报错或留有未知临时文件：保留日志和现场，先核对原盘与新盘状态，不直接重试、全局清理或缩回原盘。
- guest分区或文件系统未增长：先核对设备、GPT、根分区位置和文件系统类型，再选择相应工具。本例的`growpart /dev/sda 1`及`resize2fs /dev/sda1`只适合已确认的这块ext4根盘，不能套到LVM、加密盘或其他设备。
- `growpart NOCHANGE`：可能已经在开机时完成增长；对照`lsblk`、`findmnt`、`df`和原始回执。不能仅凭这个单词认定失败或成功。
- 根分区已变大而ext4检查失败：保留分区表备份与实际状态，不格式化、不缩盘，也不对已挂载根分区强行运行修复型`fsck`。

### 练习与面试答案

1. **VMDK最终只多128KiB，为什么还临时提高预算？** 因为最终文件差值不是操作期间的临时副本空间；本次项目采样峰值实际超过72GiB。
2. **为什么`df`总量比20GiB小？** 20GiB是整块盘；还包含其他分区及文件系统自身结构，不全部属于根目录可用块。
3. **`NOCHANGE`后能说手动命令完成了扩容吗？** 不能。本次第一次guest检查已看到分区与ext4增长，后续命令起核验作用。
4. **扩盘成功是否证明Hive/Iceberg跑通？** 不能。它只恢复容量条件，后续作业及对账需要各自的实际回执。

实现和证据：[公开扩盘容量记录](../evidence/control-disk-growth-20261013.json)、[资源说明](../resources.md)。私有维护脚本及原回执保存在忽略的`runtime/real/operator`，只绑定本次机器/磁盘/时间，不能作为通用从零安装命令复制运行。此次不变更冻结writer、03/04登记或数据到期时间。

## SSH与时钟

主机公钥必须匹配本项目known_hosts；不要关闭全局StrictHostKeyChecking。实时新鲜度前检查宿主与guestUTC偏差，本项目曾发现VM快8小时，说明“看起来显示本地时间正常”不代表时钟正确。时钟偏差会破坏延迟、水位线和跨日判断。

## 2026-10-14：时钟定位、有限校准与复查

### 1．通俗解释：一致的钟仍可能一起走错

把Windows主机、实验VM和生产采集端理解为三只钟。VM跟随主机时，两者可能只差几毫秒，但如果主机没有同步到外部时间，它们仍会一起领先。UTC只是时刻的表示方式，不能自动消除误差。机器走快时，实验端可能提前认为某个UTC截止已经到达，`clock_proof`因此需要同时核验源端与请求端时间。业务`event time`、原`accepted_at`和mono秒表各自用途见[第04章](04-事件契约时间与来源隔离.md)。

### 2．实际实现与证据位置

校时前的历史事实记录在[host-clock-precheck-20261014.json](../evidence/host-clock-precheck-20261014.json)。它汇总当时已实际执行的探测，文件自身没有再次运行探测：Windows Time虽然Running/Automatic，仍为Leap Indicator=3、Stratum=0、Local CMOS Clock，并无最后成功同步时间；普通权限resync返回`0x80070005`；旧`time.windows.com`探测均`0x800705B4`超时；可达`ntp.aliyun.com`显示本机领先约21.64秒。这些历史状态不反写成已同步。

同次GitHub无缓存HTTPS `Date`显示粗略23–24秒差，但只有整秒分辨率和请求区间，不能把它当精确NTP测量。guest在host请求区间内也没有证明绝对UTC准确。历史预检里的`host_time_corrected=false`、`closed_day_registered=false`、`compute_permit_granted=false`只代表当时停点。

当前[实际校时公开证据](../evidence/host-clock-alignment-20261014.json)记录用户批准、全部VM关闭前提、管理员原参数备份、peer更新及一次resync退出0。首次读回Leap0、Stratum3、LastSyncError0、有限NTP偏移约1.8–2.6毫秒；后续导出读到LastSyncError2（stale-time）。原成功与后续错误分别保留，没有继续resync，也不承诺持续同步或毫秒绝对UTC准确。这份证据及相关验收文档随签名`1b910a2`完成push/PR双轮CI；引擎实际安装来源仍为BCE463，文档提交不证明新引擎通过。

### 3．流程：先定位，再有限校准，最后复查

```text
读必要时钟元数据和有限NTP样本 → 保存差异与失败代码
→ 正常暂停writer并停止本项目引擎/三VM，保存数据与恢复链
→ 审阅并批准本机管理员的单一peer变更和一次同步
→ 备份旧必要参数 → 更新指定peer → 同步一次 → 读回状态/偏移
→ 原资源门禁下分阶段启动VM → 新的host/guest/源端时钟及clock_proof验收
```

本次校时之前已核验原Kafka和三VM全部关闭、保留数据未删除，管理员校准已按上面流程完成。这个关闭状态属于校时前提，不代表后续窗口仍全关；后续重新读回实际资源和时钟，不能凭一次校时结果跳过每次复机及闭日检查。

### 4．分步操作：执行位置与管理员前提

普通**Windows PowerShell、本项目仓库根目录**可先看本机UTC、服务是否运行以及本机帮助；这些命令不会校时或启动VM：

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
Get-Date -AsUTC
Get-Service -Name W32Time | Select-Object Name, Status, StartType
w32tm /?
```

`w32tm /?`中有resync帮助。本机`w32tm /resync /?`会报参数错误，不作为帮助入口。运行时`/query /configuration`需要管理员权限，本次普通终端已被拒绝；不能把权限失败解释成“没有NTP配置”。只记录Type、peer、NtpClient及必要状态，完整设备配置不放仓库或教程。

以下保留**本次已批准且已执行的历史步骤**。校准只在这台Windows主机的管理员终端完成，当时所有项目VM和引擎正常停止；没有在Linux/容器、FinalShell或生产服务器执行。批准前的限定范围保留于历史管理员审阅r2（历史本机记录）：备份Type=`NTP`、旧peer=`time.windows.com,0x9`及必要NtpClient值，仅换为`ntp.aliyun.com,0x9`并`/update`，确认生效后resync一次。固定命令如下，**当前不重新执行**；未来再次校时需新的现场诊断和明确范围，阅读本书不构成另一次授权：

```powershell
# 历史已执行命令，仅供复盘；原备份和结果已存在，勿重跑或覆盖。
& '.\runtime\real\operator\host-time-align-peer-admin-r2-20261014.ps1'
```

脚本为本机忽略runtime中的受审文件，并非公开安装包；缺文件或版本不符时停止，不照文件名新写替代品。它限制查询/更新各10秒、resync45秒、有限stripchart25秒；备份与结果各最多16KiB，采用CreateNew，现有原备份/结果不覆盖、不自动重试。实际只经w32tm更新指定peer，Type、时区、必要NtpClient设置、生产主机及历史数据保持；没有直接编辑注册表或重启服务。

### 5．预期与实际结果：一次成功不保证持续同步

正常验收应核对新peer、最后成功同步时间、Leap Indicator、Stratum、Source、LastSyncError及有限offset。本次这些首次读回与resync退出0一致，说明当次Windows Time同步实际完成；不再把它写成仅stripchart源可达。随后stale-time错误2同时记录，尚未认证持续同步。不能仅用首次offset小、服务Running或Source名字不变承诺每个后续时刻的精度；Windows状态需要在新窗口重新核对。

### 6．检查方式：保存必要字段，复机后重新比较

管理员原操作记录已写到固定`runtime/real/operator/host-time-alignment-backup-r2-20261014.json`和`host-time-alignment-result-r2-20261014.json`并保留原字节。首次Source因尾部空格被旧精确匹配过滤器脱敏；后续独立必要字段读回确认批准源，原回执没有被回写成新内容。核对成功退出与未超时、原备份、保持的Type/时区和必要设置；不能以后续错误2否认那次实际成功，也不能以那次成功隐藏后续错误。

校时后的HTTP-only探测中，源正文generated_at仍分别比该较窄响应终点超前约0.102/0.103秒；原`protected_status`测量的是含tunnel/client建立和关闭的完整操作区间，实际原`clock_proof`已通过。它们是两个真实区间，不添加sleep、不移动边界，也不把约0.10秒当精确绝对校正量。随后元数据诊断显示源末端已推进，本地cursor与最新末端不一致；诊断过程退出0只表示问题定位完成，没有替用户同步、登记闭日或授予计算许可。

后续仍按72GiB项目上限、71.75GiB停止线、至少35GiB空闲磁盘、4GiB可用RAM检查实际资源并分阶段启动VM，每个窗口重新读host/guest UTC和新鲜源状态。guest只读检查可在正常阶段启动后执行；`--set`是另一次guest UTC/RTC变更，不能因本次host校时成功而在运行作业中顺手使用。模型对账、发布、实际DIM2304及持续时间精度都不是这份校时证据的验收范围。

### 7．失败处理：不把错误转换成通过

Access denied先核对管理员UAC。Type=NoSync、策略接管、旧peer已变化或必要参数不符时，脚本应停止并另行诊断。无时间数据/UDP超时则检查已指定源的DNS和UDP123请求/回复，不关闭防火墙或连续更换源试错。stripchart使用临时UDP源端口，Windows内置客户端使用UDP123源端口，两者成功条件不同。[Microsoft Learn的工具与端口说明](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings)

命令超时只终止该子进程，已经提交的更新或校时请求可能继续，需要读回实际状态。不能说“超时就没有改钟”。即使校准失败，也保持原事件UTC、`accepted_at`、七日保留起点与过期截止，不放宽时钟/新鲜度门禁，不回拨机器到旧错误时间来延长旧epoch。

本次后续读到错误2时，只保存状态，没有自动再resync。未来需先核对同步年龄与来源可用性，确定需要的最小操作范围；不为消除一个错误码反复更新peer或在活动VM/数据库事务中调钟。

### 8．练习与面试追问

**历史合成题：** guest与host看起来很接近，host对可达NTP源的offset却约-21.64秒，是否可以开始跨机器延迟测试或闭日计算？

**答案：** 不能。前者只说明两只本地钟接近，后者显示本机相对该源领先。先按批准范围校准并取得新证据，再重验跨节点UTC及闭日门禁；mono可测本机等待时长，不能证明某个UTC日已经结束。历史引擎回执保留原时间和原结论，这次预检没有重新认证其绝对UTC准确性。

**本次复盘题：** 一次resync退出0且有限offset很小，后来LastSyncError又为2，能否只引用成功那一刻并取消今后的时钟检查？答案仍是不能；已完成操作、有限测量和后续持续状态是三种证据。原clock_proof通过也只证明当次完整操作符合原门禁，不证明前缀已经追平或模型/DIM完成。

阿里云官方资料将`ntp.aliyun.com`列为公网NTP域名，说明非阿里云实例可使用；这支持选源依据，实际当次与后续状态分别看本机证据。[ECS NTP说明](https://help.aliyun.com/zh/ecs/user-guide/alibaba-cloud-ntp-server)，[ECI NTP地址示例](https://help.aliyun.com/zh/eci/user-guide/configure-the-ntp-service)。命令索引见[第36章](36-常用命令索引与执行位置.md#时钟命令只读定位与单次校时记录)。

## 停止与故障

下面的单VM stop只适用于新机初始化收尾、普通合成实验已停止自己的容器之后，或明确的故障处理。**已有正式真实 writer 时，日常关机先由第24章的 storage V3 执行 pause-writer，再以新的对应 scope 执行 stop-epoch；受控入口负责实际停止读回和软关机**。保留磁盘不等于已登记一个能安全恢复的 Checkpoint；不要用直接软关机代替正常暂停。

```powershell
uv run python tools/vmware_lab.py stop --node snow-analysis
uv run python tools/vmware_lab.py status
```

停止不删除磁盘和检查点。容量不足就停扩样，不通过全局Docker prune腾空间。仅删除guest内文件不一定让VMDK缩小，压缩还可能需要整盘临时副本；这是独立维护动作，不能把“压缩一下”当作零成本解决方案。

练习答案：同一宿主上的两份HDFS副本可以帮助验证DataNode进程/VM故障后读回，但宿主磁盘或整机故障可能同时影响两份，不能写成两地容灾。

## 对照源码与项目证据

- [tools/vmware_lab.py](../../tools/vmware_lab.py)
- [deploy/resources.json](../../deploy/resources.json)
- [docs/runbook.md](../runbook.md)

[返回学习目录](00-阅读地图与当前状态.md)

## 当前正式恢复与离线阶段：三个入口各管一段

本机已经有正式 epoch、专属读取通路、原 Checkpoint 和版本登记，不再运行上面的从零安装流程。特别不要重新 initialize、submit 或 prepare 一个同名 epoch。

| 历史阶段（2026-09-19） | 当时入口；旧epoch现已过期 | 资源与边界 |
|---|---|---|
| 原存储／实时恢复 | `runtime/real/operator/run-real-storage-windows-v3.py` | 仅 analysis4608MiB；control/compute关闭；每个phase独立inspect和execute，原epoch/截止不变 |
| 小样本离线 | `tools/real_offline_small.py --profile real-small-1792` | 2048/1792/768MiB；先完整paused、存储全停和软关；逐阶段监测、HDFS双副本 |
| 固定首闭日登记／Doris发布 | `runtime/real/operator/run-real-closed-day-windows-v1.py` | 不负责开VM；只接已核验的analysis4608存储窗口，固定首日/固定run/固定cutoff |

私有脚本是本机忽略目录中的受审操作工具，并非公开安装包的一部分。换电脑、源码或epoch时不能仅复制文件名继续执行；缺文件/哈希或实际安装回执时停止核对。当前源的逐台安装必须按独立窗口核验，不能把CI成功当三节点已经装好。

先做不启动的参数检查：

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production.json --profile real-small-1792 start-offline --describe
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-storage-windows-v3.py --help
& ./.venv/Scripts/python.exe runtime/real/operator/run-real-closed-day-windows-v1.py --help
```

这里 `--help`／`--describe`不等于实际guest检查。storage V3／closed-day V1不带`--execute`的具体phase称为inspect：在线阶段会连接原SSH并读取guest元数据，不是纯离线help；它生成本次scope供核对。scope最长15分钟且绑定phase、宿主启动身份、源码、VM和实际原登记，换阶段必须重新生成。示例见第36章。

冷态进入storage时由受控入口处理analysis的4608MiB配置与启动；不要先用底层vmrun手动把VM开起来来绕过cold门禁。结束必须先暂停正常writer，再实际stop-epoch、确认五引擎已停后软关。进入1792离线时由公开small入口检查原paused链并按阶段配置/启动；analysis768不能同时运行Doris、Flink、治理或本机Hive客户端。

`real_lab.py`是冻结Linux业务阶段及旧Windows路由的机制参考，旧Windows `--offline-profile`只接受scale/real-small，**不接受real-small-1792**。不要给旧CLI换一个参数名，也不要把scale或1920的历史资源成绩算作1792完整验收。

### 已有1792证据，以及还不能宣称什么

9月19日15:37 UTC，d17源码的1792拓扑实际运行Spark3.5.7/YARN，固定56条合成输入得28有效、28重复，独立整数基准一致；此前Linux EOF/孤儿会话合成进程探针实际通过。该轮控制器在计算后的清理阶段因Docker缺失错误大小写识别失败而返回失败，原回执保留。15:49独立精确cleanup确认原driver已消失、原会话无活动成员，未重跑计算，15:50全VM软关。

[1792合成计算与独立清理回执](../evidence/offline-1792-recovery-20260919.json)只证明这些具体步骤。它不证明真实闭日报表、Hive、Iceberg、P95或宿主0xF7根因已修复。更早1920/scale成绩及失败过程保留在第24、99章和仓库证据中。新6c60源码修复公共Docker缺失识别及Lake挂载列表指纹，实际后续阶段仍各自验收。
