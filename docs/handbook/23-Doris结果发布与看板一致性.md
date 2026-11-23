# 23 Doris结果发布与看板一致性

> **2026-10-14真实Hive／Iceberg与最终停止已通过：** 原BCE闭日模型包保持原来源，F8／483在`real-small-1792`（control2048／compute1792／analysis1024 MiB）完成真实Hive登记与独立验证。Spark 3.5.7 `local[1]`应用分别为`local-1791973277761`和`local-1791973714447`，daily／session_daily／retention／funnel四组存在性、计数比较及私下行摘要比较均通过。随后真实Iceberg由两个不同YARN应用`application_1791974201828_0001`、`application_1791974201828_0002`完成执行和独立验证，四组当前／适用历史快照及新增`model_revision`列的校验有效，analysis权威确认通过，见[F8真实Hive与Iceberg技术证据](../evidence/real-hive-lake-f8-r2-20261014.json)。公开只给比较布尔与引擎身份，不发布业务计数、行摘要或可枚举的请求／结果／metadata内容哈希。原聚合2026-10-13香港日期对应的90日到期2027-01-11保持；DIM原始输入仍按其独立七日到期。这不表示Lake表已登记到Hive，也不声称分区演进、MERGE、全图或列级血缘、生产P95。

> **2026-10-14真实DIM已实际通过：** F8／483来源在显式`real-dim-2560`条件下完成r2，Spark 3.5.7、Python 3.8.10、`local[1]`，实际应用为`local-1791971531318`。`at_event`与`current`两种解析模式的全部字段比较均为true，非空输入验收通过；driver及外层SSH实际退出0，后续独立检查确认本次精确所属容器已不存在，见[F8／r2真实DIM技术证据](../evidence/real-dim-f8-r2-20261014.json)。业务计数和可能被小范围枚举重构的私有结果文件哈希只在私下校验；公开只保留“私有绑定与验证已通过”的布尔结论。这证明本轮真实DIM解析与Join的引擎执行，不会把空历史覆盖补齐：`history_coverage`仍为unknown，unknown也是需正确比较的结果。原输入2026-10-13 04:06:35.627 UTC对应的七日到期2026-10-20 04:06:35.627 UTC保持；没有改写BCE指标、正式产品或公开统计。监控仅有10次宿主采样，54.359秒是SSH窗口时长，不是精确Spark耗时、来宾连续监控或峰值，也不声明VM已关闭。真实Hive／Iceberg和其他结果分别验收。

## 2026-10-14实际Doris与私有UI：八项操作判断

1. **解释：** 原BCE闭日日结已经成功发布到独立Doris；publisher进程退出0，内部SQL精确整数读回一致。结果元数据的另一次读取成功，但这次读取没有发起新的独立SQL查询，不能把两类检查合成同一证明，见[Doris技术证据](../evidence/real-closedday-doris-20261014.json)。
2. **实现位置：** Doris发布由统计仓库`tools/real_lab.py publish-doris`及对应发布适配模块处理；私有看板由同一入口的`view`显式传入配置和run ID，启动`dashboard/real_app.py`，渲染和聚合准入分别位于`src/snow_statistics/real_view.py`与`real_aggregate_read.py`。个人站公开统计页保持独立。新源码签名/CI与VM安装分别看[项目当前状态](../status.md)。
3. **流程：** 有效闭日包与原期限 → 显式Doris发布 → publisher内部SQL比对 → 保存技术回执 → 独立检查元数据 → 有限私有浏览器DOM及所属进程收尾已完成，独立SQL和行为表继续另行检查。任何一步缺证据，只能说明该步待验收。
4. **操作：** 下列命令只在Windows PowerShell、统计仓库根目录读取已脱敏的公开JSON；不需要管理员，不启动VM或连接Doris，也不读取业务行。真正发布/引擎检查另需当前有效配置、原读取凭据、所需阶段资源及授权；源码替换前停止项目容器，容量调整在全VM关闭时完成。

```powershell
Set-Location 'Snow_Statistics 仓库根目录'
$dorisProof = Get-Content -Raw -LiteralPath 'docs/evidence/real-closedday-doris-20261014.json' | ConvertFrom-Json
$dorisProof | Select-Object source_commit, publisher_process_exit_code, publisher_internal_doris_integer_readback_equal, new_independent_sql_query_performed, doris_behavior_tables_verified
```

5. **预期：** 可确认publisher退出0和内部读回一致；新独立SQL及行为表的标志仍为false。analysis的Streamlit等lab依赖已按未修改的uv锁补齐，依赖检查通过；后续四页签、对应表格及日期/cutoff/口径文案已有实际DOM检查，见[实际私有看板证据](../evidence/real-private-view-20261014.json)；没有读取私有业务数值，也没有独立观察60秒租约跨界刷新或个别D1/D7值。
6. **检查：** 对照来源、业务日期、截止时间、原到期和步骤的实际退出。BCE的闭日包仍记BCE来源，c0f看板工具不重写旧计算回执；签名、源码回读、Docker镜像拉取和浏览器是不同证据。
7. **失败处理：** 看板首次缺Streamlit的失败保留；补依赖按锁和hash核验，不改指标或降低门禁。本轮配置0664权限被拒后只修为0600，字节/owner保持；UI外层0与timeout子进程124、转发外层0与SSH子进程1均保留。任何新发布、读回或原期限失败都停止并保留原包，不能填零或延长期限。
8. **练习：** 分别列出本次已经验证与仍待验证的步骤。参考判断是“内部SQL整数一致、技术元数据读回成功”已验证；私有浏览器四页签DOM和有限收尾也已验证；F8真实DIM双模式解析与Join已另行实测通过；新独立SQL、Doris行为表、成熟留存状态和生产P95仍待对应回执。DIM通过不表示结果已经写入Doris或生产页面。界面启动、DOM通过和模型/引擎成绩各有范围。

## Doris负责什么

Doris是实验分析查询层，用于查询已经验证的离线或实时结果。个人站公开页不直接连接Doris，也不持有数据库凭据；Doris停机只影响本机深度分析，不应影响轻量公开统计。

离线结果不是“写入表就算完成发布”。项目先加载不可变结果快照，全量读回校验日指标，再用一次指针更新让整个日期范围切到新版本。旧报表在新结果未通过时保持可读。

```text
Spark验收包 → 校验来源/粒度/口径/质量 → 写daily_snapshots
→ Doris读回逐行一致 → 原子更新offline_releases → daily_published/report_published
```

## cutoff为何参与版本控制

较旧cutoff不允许覆盖新版本；相同cutoff生成不同内容则拒绝，需要调查输入或代码是否不一致。新run-id只是执行身份，不足以说明它应该覆盖旧报表。内容寻址snapshot_id使同样数据重发仍指向同一不可变结果。

空日期也要有发布指针。如果原来1月1日有数据，新校正认为该日结果为空，而发布器只插入非空行，旧数据就会继续显示。为完整日期范围提交空修正状态才能让旧行退出可见结果。

## 实时与离线分开

Flink实时事实表和离线校正表独立，稳定业务主键及版本控制分别验证。不能用一个无条件覆盖语句让离线老批次覆盖更新的实时数据。项目不同实验lane还有独立数据库和表，避免恢复状态串线。

## 操作入口

先选学习对象，再开服务。下表对应 `dashboard/app.py` 的实际菜单；名称相似不表示数据源相同。

| 看板菜单 | 读什么 | 能证明什么 / 前置条件 |
|---|---|---|
| Python 正确性基准 | 你刚生成的合成 `model.json` | 无VM教学；不是Spark或Doris实际执行 |
| Spark 已发布运营与行为模型 | 本机已验证的合成发布归档 | 解释该归档的历史cutoff；不自动表示今天已更新 |
| Marquez 血缘归档 | 已保存的表级血缘证据 | 只能说明归档内已有输入/输出关系 |
| Doris 已发布数仓结果 | 专用Doris中的synthetic发布表 | 需要本阶段Doris及独立配置；这个菜单不是real查询入口 |
| 真实来源私有汇总 | 已登记配置、已发布批次及受生命周期保护的真实聚合 | 先清理/验证来源、期限、发布与读取回执；缺失时明确暂停 |

**Windows PowerShell，工作目录为统计仓库根目录；前置为第11章已安装dev/lab依赖。** 第一个命令只显示发布器帮助，不连接Doris。第二个命令启动本机私有看板，只监听回环地址；终端按Ctrl+C关闭。

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
& .\.venv\Scripts\python.exe tools/publish_daily.py --help
& .\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

初次学习按第38章先生成小样本，打开终端提示的localhost地址，切到“Python 正确性基准”，把“实验结果文件”填为自己的 `model.json` 路径。预期能看到quality账本、每日指标、维度历史和工单轮次；看板默认选择Doris时显示暂不可用只是该源没有启动，不要把这条提示当成Python样例失败。

真实入口需填写本次有效配置和已发布run ID，不复制书中历史epoch或旧run ID。`real_view.py` 会在读取前核验清理状态及短时读取凭据，未通过时显示暂停，不自动启动引擎，也不把缺失画成零。**本章代码和合成测试可以单独验证；本次真实自然闭日报表、Doris发布与看板读回仍以新的实际回执为准。**

下面是无需Doris的发布契约练习，仍在上述PowerShell目录执行：

```powershell
& .\.venv\Scripts\python.exe -m pytest -q tests/test_publication.py tests/test_real_publication.py
```

预期测试通过。它检查包、粒度、来源和本地受控发布规则，不替代Doris加载/事务的引擎验收。真正运行发布命令前必须已有合格包、正确独立数据库和本阶段资源；这里不给初学者一条缺少这些前置的“直接写生产”命令。

## 验收思路

验证三种失败：加载前失败不改变旧结果；加载后指针前失败可能留下不可见快照但旧结果不变；指针提交成功后响应丢失，重试须得到同一可见结果。用一条查询读取指标与其cutoff，避免先读指标、再读元数据时跨过一次发布形成混合版本。

练习：本次结果14行写入成功，为什么还要全量读回？答案：输入类型、主键、字符集、重复版本或加载返回语义都可能导致实际查询结果与期望不一致。返回成功只证明某个请求被处理，最终可见内容才是发布验收对象。

## 对照源码与项目证据

- [src/snow_statistics/publication.py](../../src/snow_statistics/publication.py)
- [warehouse/doris/schema.sql](../../warehouse/doris/schema.sql)
- [docs/offline-pipeline.md](../offline-pipeline.md)

[返回学习目录](00-阅读地图与当前状态.md)
