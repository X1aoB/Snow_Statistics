# 完成范围与验收清单

## 当前交付（香港2026-10-14，F8 真实链路验收）

- [x] F8 16文件GPG提交、独立验签、推送；push/PR两轮4作业54步骤成功，483文件归档匹配并验签，见[来源证据](evidence/signed-source-f8e3ca8-20261014.json)。
- [x] F8 三节点prepare/install/readback均实际退出0，同节点本轮保护状态不变，见[安装证据](evidence/signed-source-install-f8e3ca8-20261014.json)。历史BCE/C0F引擎结果不重标来源。
- [x] analysis 2560MiB实际配置、单机启动及guest内存检查满足原DIM门槛；检查时零容器、r2未用。
- [x] 原Marquez六条ACK实际导回control，原事件身份保持、八条历史夹具未确认，见[后续ACK证据](evidence/real-closedday-marquez-control-ack-20261014.json)。
- [x] 新鲜实际Hive清理与原job短期许可、[真实DIM r2](evidence/real-dim-f8-r2-20261014.json) Spark执行及独立元数据/精确容器退出验收；历史coverage仍unknown，不改原指标。
- [x] 2560DIM窗口独立Hive-exit后通用stop-offline及全VM为0核验完成、数据保留；首次未退出Hive即通用stop的拒绝保留。后续1792窗口结果见下项。
- [x] [真实Hive登记及独立verify、真实Iceberg执行／独立验证／权威确认](evidence/real-hive-lake-f8-r2-20261014.json)；两个Hive和两个YARN应用分别核验，原模型及期限保持，保留旧成功和失败记录。
- [x] 三节点独立Lake技术读回、阶段恢复／租约释放／精确driver退出；Hive-exit后公开stop-offline成功，独立全VM为0、数据保留。
- [x] 最终公网只读复核：两站及统计页200、v1/v2保护／no-store／26小时新鲜度、所检查私有路径404；无测试事件和付费模型调用。
- [x] 新结果已校准到独立40篇学习手册，[最终静态校验](evidence/handbook-final-20261014.json)的589本地链接、186围栏及95段PowerShell／12段Python／14段Bash无错误；不将语法检查称作全部教程命令实跑。最终文档提交与CI以本轮Git及Actions回执为准，文档提交不冒充节点安装。
- [x] [有界自然流量观察与收尾](evidence/engineering-closeout-20261014.json)：原Checkpoint恢复、900／180秒跟随、失败诊断、60秒独立诊断和全部VM关闭已实测；原ReadTimeout／OSError失败保留。
- [ ] 生产P95≤60秒：自然新可见样本条件未满足，尚不能给出真实P95；属于待满足流量条件的观测指标，不以合成成绩代替，也不为凑样本保持VM常开。
- [x] 宣传范围已按用户10月14日要求分离：产品视频不宣传统计功能，素材／制作／发布不再作为Snow Statistics验收依赖，见[更新移交说明](promotion-handoff.md)。原角色互动宣传任务独立继续。

下面未勾选条目属于各自历史截点，最新进度以上表和页首证据为准；不将历史未完成状态误作当前重复待办。

## 历史截点（香港2026-10-14，增量落地及闭日登记后）

- [x] 八份文档GPG签名、推送及push/PR全部CI步骤通过；实际VM来源继续为BCE463。
- [x] 用户批准的一次管理员校时完成；原clock_proof在实际完整操作区间通过。后续Windows Time stale-time错误保留，有限NTP样本不冒充连续绝对精度，见[实际校时](evidence/host-clock-alignment-20261014.json)。
- [x] 原04 Checkpoint恢复、10分钟自然跟随及收尾、最终公共同步实际通过；被动观察退出0且没有足够的新自然样本，生产ACK P95仍未验证。
- [x] capture/暂停、增量HDFS落地及独立同前缀对照、新鲜ACK与闭日登记，见[实际证据](evidence/real04-increment-and-closedday-registration-20261014.json)。不宣称整日覆盖或登记即授读取许可。
- [x] 原权威prepare/cleanup/permit/stage-compute，daily/behavior及同口径质量校验、私有成对发布/stage-release均实际退出0；原期限、截止和输入不变。
- [x] R3只读血缘实际退出0，三项journal COMPLETE及原模型/发布边验证；R2字节表示差异导致的原失败保留。
- [x] [本次公网只读复核](evidence/production-readonly-20261014.json)：两站/统计页HTTP200，v1/v2保护契约与新鲜快照、no-store及私有路径404；未写事件或调用模型。
- [x] 两节点前后完整来源核验与26份允许元数据捕获，独立投影检查及[真实闭日技术证据](evidence/real-closedday-20261013-bce1289.json)公开写入完成；没有发布业务数值。
- [x] 原真实Doris日结publisher退出0及内部SQL整数读回一致，独立元数据读取通过，见[技术证据](evidence/real-closedday-doris-20261014.json)；新独立SQL及行为表不在本次范围。
- [x] [私有看板](evidence/real-private-view-20261014.json)实际浏览器可读：日指标、会话、留存、归因，截止与口径说明可见；有限UI及host转发收尾，未发布小样本数值。
- [x] [Marquez](evidence/real-closedday-marquez-20261014.json)实际投递六条原闭日元数据并独立后端读回三个COMPLETED及输入输出身份；精确治理停服务和VM正常关闭。
- [ ] Marquez确认回执导回原control journal；八条历史fixture保持未投递，不追求整个历史pending归零。
- [x] 新DIM只读入口及独立2304配置的本机1159项回归、Ruff、契约检查和独立P2修复复审通过。
- [x] 新增21文件c0f542c提交GPG签名、推送及push/PR两轮全部CI通过，完整477文件归档独立验签。
- [x] analysis磁盘29→34GiB实际扩容及guest核验，临时冷维护98GiB预算已按原字节恢复72GiB，见[容量证据](evidence/analysis-disk-growth-20261014.json)。
- [x] c0f477三节点实际prepare/install/readback，见[安装证据](evidence/signed-source-install-c0f542c-20261014.json)；旧BCE作业来源不重标。
- [x] 2304实际DIM preflight拒绝及独立读回：Spark未启动，原失败和期限保留。
- [ ] 独立2560候选已通过293项相关测试、4项跳过；仍待签名安装、VMX/guest读回和真实DIM引擎。
- [ ] 真实DIM受控桥接和事实Join、真实Hive/Iceberg；准备材料与既有合成结果继续分开。

## 历史停点（香港2026-10-14，日结时钟门禁）

- 原04 Kafka启动/ACK实际通过；日结登记在时钟证明处拒绝，未开始计算。Windows未同步，备用NTP只读测得宿主约快21.64秒；[诊断证据](evidence/host-clock-precheck-20261014.json)不等于校时完成。
- 原Kafka及所有项目VM已正常停止、数据保留。需完成宿主时间同步并重查源/guest时钟；不放宽门禁、不改截止或历史时间。之后继续原04日结、对账、成对发布、看板、实际血缘和被动新鲜度。
- DIM合成Spark已返回退出0/success，完整结果元数据仍待control读回；真实公开目录覆盖为空，真实历史归属及事实Join继续明确未知。

## 当前继续入口（香港2026-10-14，V19合成Iceberg已验收）

- [x] BCE463安装来源下的独立合成Iceberg：两次不同YARN应用执行/验证、analysis确认及lake退出0；四组6/6/2/3当前/历史快照等值，见[实际证据](evidence/iceberg-catalog-v19-20261014.json)。旧FA Hive和Lake失败分别保留，不延长期限。
- [x] 资源记录准确区分56份完整阶段样本与10/11条部分PID观察；最高采样PID295，未观测OOM不表示全时段保证。两次PID观察失败不改成成功，也不取消独立引擎通过。
- [x] 正式产品公开目录元数据在本机生成并回读：3版本/87目录项，coverage为空，未采集真实事实。
- [ ] 真实自然闭日计算、同口径对账、成对发布/私有看板、真实发布血缘与生产P95；保持04原期限、来源隔离及未成熟/缺覆盖说明。
- [ ] 真实DIM与事实Join；现有公开metadata及独立合成SQL候选不替代实际引擎结果。以下未签名/未安装/待V19文字保留其历史截点。

## 历史安装截点（香港2026-10-14，V19结果取得前）

- bce1289签名、双CI4作业54步骤、三节点463文件逐字安装读回实际完成；每机15变更，12writer/依赖/配置/恢复状态不变，见[来源](evidence/signed-source-bce1289-20261013.json)及[安装](evidence/signed-source-install-bce1289-20261013.json)。
- V18合成Hive登记/独立读回在fa057fe来源下通过；其成绩不改写为新BCE实测。旧Iceberg attempt失败和线程诊断保留，不复用partial，不延长原聚合期限。
- PID512和公共DIM源码已部署；新attempt lake20261014r1/V19、真实DIM Join和真实闭日报表均未提前勾选。DIM当前完成25新测试/36相关测试及合成产物，并未接入原daily/behavior指标。
- 香港午夜已过；按新鲜源/同前缀ACK/permit继续真实闭日、发布、看板、血缘和自然样本观察，未成熟或覆盖不足如实说明。旧标题中的待签名/未安装为历史状态。

## 当前继续入口（香港2026-10-13深夜）

- V18 Hive登记/独立读回实际通过，4组行数和摘要一致，隔离合成夹具与真实用户报表分别声明，见[Hive验收](evidence/hive-catalog-v18-20261013.json)。
- Iceberg执行遇到native-thread错误，已正常全VM收尾、数据保留；有限PID上限256→512候选以及下一精确容器采样已通过合成与独立复核。旧partial attempt不可复用，原到期不续，见[失败及候选](evidence/iceberg-native-thread-followup-20261013.json)。
- 独立公共DIM候选具备版本覆盖、SCD2、unknown和墓碑解析，不改既有指标；25新测试/36相关测试及合成实跑通过，真实DIM/Join仍需实测。
- 统一工作区1096 passed / 23 skipped，Ruff/契约/diff通过；下一统一签名/CI/安装后继续新attempt与真实闭日、发布、看板、血缘及自然样本观察，不由配置或候选推断完成。

## 当前继续入口（香港2026-10-13 23时后）

- `fa057fe` 已签名、两轮CI全部步骤通过、三节点452文件实际安装和完整读回通过；70秒暂停核验修复已部署，见[签名与CI](evidence/signed-source-fa057fe-20261013.json)及[安装](evidence/signed-source-install-fa057fe-20261013.json)。
- V18/r9已通过准入与合成复核，本轮实际预检和三机私有工具读回完成，开始Hive/Iceberg实测。未预填登记、读回、湖仓或真实报表通过；旧失败和未安装描述为历史。
- 独立手册40篇、457本地链接、181围栏无静态错误。后续真实闭日、成对发布/看板、血缘及被动新鲜度仍需实际回执；覆盖不足、未成熟留存和自然样本不足如实记录。

## 当前继续入口（香港2026-10-13 22:40前后）

- V17在进入Hive切换之前遇到公共暂停核验20秒超时；另一次65秒有界检查实际26.706秒通过，原状态/身份/保留条件保持。根操作人已另行正常停止自己启动的离线窗口，独立确认VM全关、数据保留，见[实际核验与失败](evidence/hive-guard-timeout-20261013.json)。
- 新补丁仅将analysis guard设70秒，其他probe仍20秒；相关193项通过、4跳过、Ruff与独立复核通过，仍须签名/CI/安装及实际引擎重跑。下方2434安装与容量操作继续有效，但不包含新超时修复。

## 当前继续入口（香港2026-10-13 22时后）

- `2434ab6` 已签名并推送，两轮 CI 全部步骤通过，三节点各447文件完整安装及独立读回通过，见[来源](evidence/signed-source-2434ab6-20261013.json)及[安装](evidence/signed-source-install-2434ab6-20261013.json)。
- analysis 768→1024 MiB 已在全 VM 关闭时实际配置，VMX 与 Linux 读回通过，当次可用内存385 MiB；资源保留线不变，见[容量操作](evidence/analysis-memory-1024-20261013.json)。
- V16阶段切换实际通过；登记前宿主检查失败，尚未进入登记驱动。失败后实际三VM全关、数据保留，不将旧成功或空采样视为本轮登记结果。后续继续Hive登记/读回、Iceberg、自然闭日后的真实模型与成对发布、真实发布血缘及被动延迟观察。
- 学习手册40篇、445本地链接、181围栏静态检查无错误；宣传制作仍由指定任务接收用户真实录屏。以下pending描述保留其历史时点。

## 当前继续入口（2026-10-13晚间 V15 资源中断后）

- V15 登记被 analysis 实际可用 127 MiB 触及原 128 MiB 保留线中断；三次 cleanup 子请求通过不等于登记通过。自动收尾已实际全 VM 停止、数据保留，见[资源中断](evidence/hive-analysis-reserve-20261013.json)。
- 用户授权范围内的最小调整已准备：`real-small-1792` 仅 analysis 768→1024 MiB，增加 256 MiB 冷启动预算，不降低原监控门槛。完整 Windows 1055 passed / 23 skipped，相关 208 项检查及独立审查通过；新配置仍待签名/CI/安装及实际 VMX configure/readback，之后重跑引擎。不覆盖旧 768 MiB 结果，不修改数据或到期时间。

## V15 实测开始时点（历史）

- `7173a00` 修复已签名/推送，两轮 CI 全部步骤通过；三节点各 444 文件完整安装及独立回读通过，原配置、12 冻结 writer、依赖与恢复元数据保持，见[来源与 CI](evidence/signed-source-7173a00-20261013.json)和[三节点安装](evidence/signed-source-install-7173a00-20261013.json)。候选未签名/未安装描述为历史。
- V15 现场原状态、聚合期限及未使用 Lake 范围检查通过，新 r6 worker 和诊断程序已精确上传回读。实际 Hive 阶段切换通过，登记运行中；后续登记、读回与 Iceberg 仍须实际结果，不能以安装或小 Snappy 往返代替。

## V14 诊断后入口（2026-10-13晚间，历史）

- V13 诊断包装器的默认 SSH 工作目录缺陷已保留为失败；V14 路径修正通过实际四项入口预检，阶段切换及四次 Spark 清理通过。实际登记仍失败，但已取得 Snappy 本地解压库在 `/tmp` 无法映射的具体异常；失败后实际三 VM 全关、数据保留。锁定镜像 V2 小合成对照已实际通过：旧配置复现错误，新配置完成 native 压缩/解压往返，普通 `/tmp` 保留 noexec；容器清理与 VM 全停实际回读通过，见[原生库对照](evidence/hive-snappy-native-20261013.json)。
- Hive 专用 16 MiB 原生库目录及固定 control 发布血缘候选已合并工作区。完整 Windows Python **1042 passed / 23 skipped**、浏览器 23 项、契约/Ruff/diff 检查通过；12 冻结 writer 不变，独立审查无 P1/P2。候选待签名、CI、完整安装与实际引擎重跑，见[发布血缘候选](evidence/real-publish-lineage-candidate-20261013.json)。
- 当前仍需完成 Hive 登记/独立读回、Iceberg、自然闭日后的真实模型与成对发布、实际血缘投递及生产 P95。当前 Doris 发布器只发布 daily，私有 pair/看板包含行为模型；不把未观察的真实请求、未成熟留存或合成性能写成生产成功。

## V12 继续入口（2026-10-13晚间 V12 实测后）

- 新签名来源`dc48ff1`已推送，push及PR两轮Linux CI全部作业/步骤成功；三节点完整437文件安装及独立回读通过，冻结writer、依赖、原配置及analysis恢复元数据保持。见[源码与CI](evidence/signed-source-dc48ff1-20261013.json)和[三节点安装](evidence/signed-source-install-dc48ff1-20261013.json)。下方等待GPG、未绑定V12等描述保留其历史时点。
- V12实际`hive-enter`与`hive-cleanup`通过。后续`hive-register`在Control RPC阶段以RuntimeError失败，具体原因待诊断；不能将登记、独立读回或Iceberg勾为完成。32次完整资源采样没有监控报警或观测到OOM，失败收尾已实际确认三VM全关、数据未删除，见[本轮失败与资源证据](evidence/hive-catalog-followup-20261013.json)。下一步继续诊断并保留失败现场，不以清理成功替代注册成功。
- 本轮输入为独立合成夹具，未测试Kafka链路，不代表真实用户数据全链验收。自然闭日最早仍须香港10月14日00:00后；真实离线模型、发布、Hive/Iceberg、血缘、P95与宣传素材继续分别验收。

## 历史继续入口（2026-10-13 Linux退出验证后）

- Hive输入线程候选在768 MiB analysis独立目录完成10项实际Linux合成管道/子进程检查，旧安装模块不变、无引擎或真实数据读取；测试后全部VM已关闭，见[独立验证](evidence/hive-reader-linux-20261013.json)。完整Windows1022项、浏览器23项、契约及Ruff已通过；Linux CI仍待新提交。
- 新增GPG提交因解锁超时未生成，改动保留，等待用户回应后重新触发；随后签名归档、CI、逐台安装，再完成Hive清理/登记/读回和Iceberg。V12/r3保持未绑定和禁止执行，不复用已用的r2冷启动基线。
- 安装准备工具43项测试和独立复核通过；旧版本、03失败与04暂停及原到期时间保留。真实闭日/模型/发布/血缘/P95与宣传素材继续是独立待办，不因退出修复而提前勾选。

## 2026-10-13 16时后：容量完成，Hive退出修复待重跑

- 控制磁盘18→20 GiB及根文件系统实际读回通过，临时82 GiB维护预算已恢复72 GiB；宿主35 GiB/4 GiB保留。新建默认同步，既有盘不重建，见[容量证据](evidence/control-disk-growth-20261013.json)。
- V11实际Hive阶段切换通过；清理在complete后进程非零退出，自动关闭全部VM。原stdin线程退出缺陷已在合成子进程复现并修复，完整Windows回归1022 passed / 23 skipped，浏览器23项、契约和Ruff通过；新增Linux测试待CI。此时不将目录清理、注册、Iceberg判为已完成。

## 2026-10-13 后续：安装与教程操作

- 三节点已安装完整签名 `50706a8`，每台430文件回读、原配置与恢复状态不变通过；这项已完成，不再等待源码安装，见[三节点证据](evidence/signed-source-install-20261013.json)。
- 手册Kafka、CDC、Spark/Hive实操已补齐并运行本地合成练习；40篇、392链接、167代码围栏静态检查无错误，见[操作教程证据](evidence/handbook-operations-20261013.json)。
- 合成Hive第一轮在计算前因旧探针heredoc换行失败，实际收尾全VM关闭。Linux只读复现及LF修复已验证，新候选50项检查通过；真正的Hive/Iceberg与后续真实闭日仍按实际引擎回执分别记录。

## 2026-10-13 15时后：已签名发布的新增完成项

- 个人站 `e4d9242` 已通过GPG验证、main推送及Pages生产部署；公网脚本身份、两卡/完整日报、桌面/手机/中英文、缓存/CORS和私有路径隔离验收通过。数值显示问题的候选已正式上线，后续仍按D+3和样本门槛发布，见[发布证据](evidence/production-public-cards-20261013.json)。
- 数仓 `50706a8` 已签名推送原草稿PR，两轮Linux CI的全部作业成功；430文件完整归档及detached签名已准备。新归档尚未安装VM，不能据此声明后续闭日、Hive/Iceberg完成，见[源码与CI证据](evidence/signed-source-50706a8-20261013.json)。
- 用户本轮已允许后续GPG提交，下方“等待用户再次发出签名指令”和未发布条目为此前快照。自然闭日、真实Hive/Iceberg/血缘、生产P95及宣传用户素材仍需分别完成。

## 2026-10-13 恢复与当前待办

- 两站与轻量服务今日只读可达，公开快照生成时间和保护投影有效。紧凑卡只选今天导致历史已公开数值被忽略，已在 MyWebsite 独立候选修复；浏览器恢复后补齐桌面及390px手机、中英文、完整页与键盘重试验收，并修复英文状态截断。最终77项测试、类型检查和35页构建通过，五个文件尚未签名或发布，见[候选证据](evidence/public-card-candidate-20261013.json)。签名仅在用户再次明确要求时触发，发布后仍需公网回归。
- 用户授权项目预算扩展为 72 GiB，运行停止线 71.75 GiB，宿主磁盘/RAM 保留 35 GiB/4 GiB。analysis 4608 MiB 已实际完成本轮 Kafka/Flink/Doris 同截止对账及整机重启恢复；HDFS 阶段三 VM 实测项目 67.74 GiB、宿主空闲磁盘 182.83 GiB。
- 旧 real-prod-01/02 已到期并核验物理 epoch 退役，原 real ODS 的一个过期批次及 HDFS 快照引用已清理，位点头保留。恢复中的清理锁竞争已修复并在 analysis 安装单文件工作区补丁；冻结 writer 文件保持。完整 Python 960 passed / 16 skipped，Ruff 通过。
- 新真实同步窗口按实际源身份、可读位点和原 accepted_at 建立，不能恢复旧 Checkpoint 或重置保留期。当前近期输入的接收时间均为今天；业务日期须另核 occurred_at，不将接收时间描述成已有完整自然日、成熟留存或生产 P95。
- real-prod-04 已完成真实同步、线上 SQLite/源归档/Doris 同截止整数对账、实际 Checkpoint 与 VM 重启恢复；HDFS 落地及源归档/ODS 精确内容、两个实际副本读回通过，Kafka 最终 ACK 后集群/Topic 身份和 committed offset 独立回读通过。03 的提前比较失败保留，不能冒充 paused 或恢复成功；HDFS 使用核验实际停止对象的专用窗口，未改原通用门禁或冻结 writer。最终04正式暂停、三台 VM 全停，数据保留，见[本轮证据](evidence/real-recovery-20261013.json)。
- 下一实测交付是自然闭日后的真实离线日报及行为模型、Doris/私有看板成对发布，再补真实 Hive/Iceberg/血缘。今天输入的实际业务日期为 10 月 13 日，不能提前计算完整自然日或伪造成熟留存。手册已修正并实跑初学者示例，最新为 40 个 Markdown、365 个本地链接、145 个代码围栏，无静态错误。
- 新显式离线兼容入口已完成1016项Python回归，并逐台安装三节点；实际三机启动、状态和停止通过，原03失败与04暂停、数据及期限保留。该结果只证明入口启停，不等于闭日模型或Hive/Iceberg验收，见[独立证据](evidence/offline-followup-20261013.json)。
- 宣传任务最新安排已读取并校准移交包：先由用户录制 20–30 秒测试素材，制作及发布继续在“规划小吉终端宣传推广”任务完成；旧120秒/12段清单不再是当前要求。

纠正旧末端检查措辞：`accepted == aggregate` 以及从 accepted 末端读取为空，只证明轻量聚合追平且该位点之后没有新行，不能证明本地数仓没有待同步数据。

## 2026-09-27 公开汇总与诊断准备复查

- 线上 v2 汇总、统计页和个人站首页只读请求均为 HTTP 200，响应保持 no-store；project_snow 的 2026-09-24 服务质量组已公开请求 43、成功 21、成功率约 48.84%，其余组继续受 D+3 和样本门槛保护。
- collector source=real 且 accepted 与 aggregate 末端一致，未读取原始事件；storage V4 诊断候选 84 项合成边界测试通过，仍保持 SOURCE_PENDING，未启动 VM。
- 当前容量复查为项目约 59.23 GiB、宿主可用磁盘约 187.6 GiB、vmrun 运行数为 0；这只是启动前门禁证据，不构成真实闭日、Hive、Doris 或 P95 通过。

## 2026-09-19 正式接入进展

两站已发布统计告知、同意/撤销和公开统计页；独立统计入口与轻量采集常驻。小吉最新正式`65fa0d0`已将入口移到“设置 → 隐私”并简化说明，具体人工晋级与公网检查见[设置调整版本证据](evidence/production-settings-release-20260919.json)。首次接入、浏览器入库与撤销回归见[正式发布记录](production-rollout.md)及[原接入证据](evidence/production-release-20260919.json)。

本地真实私有同步→Kafka→Flink→Doris、同截止 SQLite 对账、整台 VM 关机后 Checkpoint 恢复，以及真实 ODS 两副本落地→原 Kafka 确认闭环均已实际通过，见[真实闭环证据](evidence/real-production-loop-20260919.md)。真实完成日志也在用户正常对话后通过转发、入库及聚合验证。自然闭日离线、Hive、Iceberg 与生产新鲜度仍逐项验收，不能由下表的合成成绩代替。宣传录制已按[移交包](promotion-handoff.md)转入原任务，新正式设置版本已同步。

以下表格保留此前阶段快照；当前完成项与待办以页首最新更新为准。

| 正式接入交付 | 当时结果 | 当时尚需满足的条件 |
|---|---|---|
| 两站告知、同意、拒绝、撤销 | 已发布并通过实际公网浏览器回归 | 后续产品变化按各自发布门禁处理 |
| 小吉设置位置和简短说明 | blue `65fa0d0` 已人工晋级；入口为“设置 → 隐私” | 原 green 保留回滚 |
| 独立轻量统计和公开页 | 已启用，VM 全关时继续运行 | D+3 和样本门槛按规则等待，不能补零冒充公开数据 |
| 完成日志读取与转发 | 用户正常对话后，真实完成日志已 ACK、持久化、去重与聚合，日整数独立 SQL 对账通过 | 单次实际样例不代表全部请求覆盖；验证器不调用模型 |
| 真实同步、实时计算与恢复 | 已实际完成相同截止点对账及整台 VM 关停恢复；用户正常对话的新质量事件也已再次同步并与线上 SQLite 同前缀对账 | 新质量前缀尚未落入 HDFS；生产 P95 需要无历史积压的单独实际测量 |
| 真实 HDFS ODS | 实际校验哈希、两台 DataNode 副本及 Kafka 提交 | 后续增量继续遵循原代际、位点和期限 |
| 小内存离线配置 | 2048/1920/768 MiB 与后续 2048/1792/768 MiB 均已实际计算56条golden；后者Linux断管/孤儿会话探针通过 | 后者原控制器收尾失败、独立精确恢复成功；公共兼容修复待新版本安装，见[本轮证据](evidence/offline-1792-recovery-20260919.json) |
| 新 Hive/Iceberg 连续运行入口 | 实现和 Linux CI 通过 | 独立新合成夹具的三节点实际联调仍在执行准备中 |
| 真实日指标、会话、留存、归因 | 模型与合成测试已具备；2026-09-26 固定首闭日候选已完成 storage/截止观察/实时恢复，但同步阶段以 `NodeCommandFailure` 失败并已安全收尾，未发布真实报表；窗口关闭后的只读检查显示 accepted 与 aggregate 一致、从 accepted 末端读取为空，不能证明本地数仓追平 | 需先建立新的 writer 恢复窗口并修复/复核同步阻断；不得复用失败 scope、补写覆盖或绕过 paused 门禁；留存另等成熟期 |
| 下一真实窗口诊断准备 | ignored 私有 storage V4 已加入受限阶段失败投影，84 项边界测试通过；候选保持 `SOURCE_PENDING`，不允许复用已过期 epoch | 需要新的签名来源归档、新 real epoch、fresh scope 和合成假后端失败投影验收 |
| 私有看板、真实表级血缘 | 已有实现与历史合成实验 | 跟随真实闭日报表补充当前版本的引擎证据 |
| 学习手册 | 独立目录已有 38 个专题及总索引、校验记录 | 随最后实际验收更新版本与操作路线 |
| 宣传制作 | 最新正式版本已移交指定任务，可录制 | 用户录制真实操作和对白；成品审阅与具体发布在原任务完成 |

这里的“等待”不会阻断线上网站、聊天或轻量统计，也不表示相关数仓能力已经完成真实验收。自然日期、用户素材和未发生的请求都不能用合成结果代替。

## 2026-09-12 求职实验交付（历史）

2026-09-12，本机可独立完成的项目主线已具备可执行实现和实际验收。下表区分求职实验交付与未来生产推广；配置或文档的存在不作为引擎运行成功证据。

| 主线 | 实际交付与验收 | 证据 |
|---|---|---|
| 产品解耦 / 轻量统计 | 独立仓库；两产品默认关闭的可回退草稿；可信完成日志、公开字段白名单、WAL 去重与事务聚合；满盘拒收 / 重启 / 万条小配置 | [接入](integrations.md)、[资源](resources.md) |
| 离线数仓 / 运营建模 | MySQL / Debezium / Kafka、带校验和 ODS、HDFS 双副本、YARN/Spark/Hive、Doris 发布、Airflow；SCD2、工单轮次与日快照、会话/留存/归因 | [离线](offline-pipeline.md)、[模型](behavior-models.md) |
| 扩样 / 实时恢复 | 百万条 YARN；10 万条 Flink/Doris 与轻量/YARN 对账；Checkpoint / 2PC / 迟到校正；300 条本地 HTTP P95 目标 | [扩样](scale.md)、[实时扩样](realtime-scale.md)、[新鲜度](freshness.md) |
| Spark 优化报告 | 固定 90 万有效行，分区/Join/热点/小文件共七组查询各三轮；数据精确一致；保留加盐更慢的结果 | [优化](optimization.md) |
| HDFS / Iceberg | 实际 9 万 DWD 迁移、快照/字段/分区演进、MERGE/DELETE/重写；NameNode 停止后重启，新 YARN 会话读回 | [湖仓](lake.md) |
| Kafka / ZooKeeper | 3 GiB 单 VM 分阶段实测选主、ISR、丢多数拒写/无会话、持久状态、Watch、CAS、临时节点过期 | [HA](ha.md) |
| 治理 / 资源中断 | 24 条核心自动表级血缘实际 Marquez 读回；真实 kill 探针不误报成功；监测器停止运行 driver 和三台 VM | [血缘](lineage.md)、[资源中断](lake.md) |
| 精简 / 关闭 / 移除 | 同一轻量库 full→lite→off；私有导出逐行一致；两产品移除适配后的构建、存储及聊天回归；清理范围可审阅，历史保留 | [退出](retirement.md) |
| 演示 / 可复现交付 | 固定锁、独立合成 demo、离线归档、学习和面试说明；94 Python / 7 浏览器测试及新增生命周期 CI 探针 | [演示](demo.md)、[工作流](../.github/workflows/ci.yml) |

该历史交付轮次完成优化、湖仓、HA、中断和退出的实际验收；当时所有实验 VM 已停止，历史输入、数据库、Checkpoint、快照和失败目录保留。该轮宿主冷态观察约 55.87 GiB 项目文件 / 52.07 GiB 可用磁盘；它不是当前资源读数，启动仍须动态检查当前 72 GiB 项目、35 GiB 磁盘、4 GiB 宿主可用 RAM 门禁。

Git 提交及远端 CI 结果以本次交付提交和 Actions 为准；Java 作业本轮未修改，工作流继续运行 8 项 Java 测试与依赖锁检查。远端成功后才完成交付确认。

## 当前推广状态与研究边界

“MyWebsite #2 / Project_Snow #59 尚为默认关闭草稿”仅是 2026-09-12 的历史状态。2026-09-19 两产品已按各自流程完成正式发布；小吉在用户批准精确候选后人工晋级，个人站已合并并发布启用配置，独立 collector 已切换 lite。源码默认关闭的约束继续保留。真实宿主的独立配额、代理/CSP、凭据边界与实际公网、同意/撤销验收见[9月19日发布进展](production-rollout.md#2026-09-19-发布进展)。这些已完成事项无需再按旧草稿状态重复发布；后续变更仍遵循产品自身门禁，不扩大到无关生产操作。

已通过本地合成输入和故障演练，不代表生产公网持续 P95、真实用户规模、长期 7/30/90 天容量、百万条实时、多分区、8 天 TTL 实际经过或完整 RSS 峰值验收。没有宣称物理容灾、HDFS 自动 HA/fencing、Iceberg 多写者、全链路自动血缘或列级血缘。这些属于选择性后续研究，不为补满技术名词同时启用所有服务。

关闭不自动删除历史数据库。移除演练已实际反向移除产品适配与停止服务；历史数据销毁仍按真实保留需求单独处理，当前只生成可审阅的项目资源清单。
