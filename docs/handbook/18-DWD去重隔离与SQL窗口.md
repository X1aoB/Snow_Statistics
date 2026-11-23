# 18 DWD去重、隔离与SQL窗口

## 先理解这一层到底做什么

假设邮递员连续送来两份相同回执，你不能说发生了两笔业务；如果两份回执使用同一个编号，却写着不同结果，也不能随意选一份并丢掉矛盾。DWD 的重要工作就是把“收到多少条消息”和“实际发生多少次业务”分开。它输出粒度稳定、来源清楚、可以解释的事实，供后续指标使用。

本章只在 Windows 的 Python 内存 SQLite 中练习。所有名字、时间和编号均为人工合成，没有真实用户、原始日志、聊天正文或付费模型调用。练习不连接采集接口，不启动 Kafka、Spark、Hive、Doris 或虚拟机。完成它能够证明你理解 SQL 与口径，不能证明分布式引擎已经验收。

本章源码解读基线为本次扩写前已安装的 `dc48ff1a9786100bb79088c3c0f162a24311deb1`。新版本的签名、CI、逐节点安装与引擎验收会分别推进，当前状态请以总目录及实际回执为准；独立 Snappy old/new 探针通过也不等于 Hive register 通过。不要把本章练习结果写成当前正式数仓的引擎运行成绩。

## 在本项目中找到对应位置

| 文件 | 负责的事 | 阅读时注意 |
|---|---|---|
| [model.py](../../src/snow_statistics/model.py) 的 `deduplicate` | 小数据 Python 正确性基准，验证事件、先事件去重再请求去重 | 明确比较同事件 ID 的内容，不同内容进 `event_id_conflict` |
| [event_input.py](../../warehouse/spark/event_input.py) | Spark 日指标与行为模型共用的输入校验、截止时间和两层窗口去重 | 按 `accepted_at, seq` 排序；当前代码没有单独比较同 ID 内容的冲突分支 |
| [batch.py](../../warehouse/spark/batch.py) | 在有效事实基础上做日聚合、质量检查与运行输出 | 输入处理位点和输出日期是两件事 |
| [test_pipeline.py](../../tests/test_pipeline.py) | 固定种子、重放、隔离及其他小样本断言 | 测试范围以断言为准，不自动覆盖所有异常 |
| [test_acceptance.py](../../tests/test_acceptance.py) | 轻量与离线整数指标、跨日及应用边界 | 对账必须同输入、同截止时间、同口径 |

上表有意保留一个重要区别：Python 基准能够指出内容冲突，不代表当前 Spark 的 `ROW_NUMBER` 已做相同检查。若新增一个绕过正规入口的冲突样例，必须补齐上游证明或引擎检查后再声称冲突隔离全链路一致。本章 SQL 为了讲清正确语义显式实现了冲突分类；它是教学例子，不是偷偷替换生产 DWD。

## 一条消息依次经过哪些判断

```text
固定输入清单、来源及接收截止时间
  → 契约与来源检查
  → 截止时间以后的消息单独计数
  → (source, app, event_id) 选首次接收记录并比较内容
  → 对 request_complete 再按 (source, app, request_id) 去重
  → 有效事实按 occurred_at 转成香港业务日期
  → PV、请求数、成功数等指标
```

`event_id` 回答“是不是同一条事件”，`request_id` 回答“是不是同一次已记录的生成请求”。两个不同的完成事件可以描述同一次请求，因此只做第一层会重复计数。页面事件没有请求身份，不能把所有空 `request_id` 放在同一组，否则一天的访问可能只剩一条。

`source` 区分真实与合成来源，`app` 区分个人站和小吉终端。即使两个应用碰巧生成相同 ID，也不应互相吞掉。本练习全部用 `synthetic`，仍保留这两个分组字段，是为了让你养成完整写业务键的习惯。

`occurred_at` 是业务发生时间，决定香港业务日期；`accepted_at` 是采集器确认接收时间，用于重放范围和稳定首次接受语义；`seq` 是同一受控来源内的确定顺序。不能用本地文件读取顺序、Spark 分区返回顺序或“最后一次跑 SQL 时碰巧排在前面”决定赢家。跨多个数据源代际时还需要先证明输入属于同一代际，不能把两个独立递增序列当成一个全局序列。

## 先手算，再执行：十条消息的账本

下面时间均为 UTC。`2026-01-01 16:00:00Z` 对应香港 `2026-01-02 00:00:00`。接收截止时间设为 `2026-01-02 00:00:00Z`，即香港 1 月 2 日 08:00。它只是合成练习的固定观察时点，不是线上闭日调度建议。

| seq | 应用 / 事件 | 业务内容 | 本次应归类 | 原因 |
|---:|---|---|---|---|
| 1 | mywebsite / p1 | 15:59:50 页面访问 | valid | 首次事件 |
| 2 | mywebsite / p1 | 内容和 1 完全相同，稍后重送 | duplicates | 同事件重放 |
| 3 | project_snow / q1 | 15:59:40，r1 完成，失败 | valid | r1 首次可信完成 |
| 4 | project_snow / q2 | 16:00:10，r1 完成，成功 | duplicates | 事件新，但请求 r1 已有赢家 |
| 5 | project_snow / q3 | 16:00:20，r2 完成，成功 | valid | 新请求 |
| 6 | project_snow / q3 | 同 q3，却改成失败 | quarantined | 同事件 ID 不同内容 |
| 7 | mywebsite / p2 | 16:00:00 页面访问 | valid | 香港新一天 |
| 8 | mywebsite / bad | 人工标记契约不合格 | quarantined | 不进入去重候选 |
| 9 | mywebsite / late | 事件发生较早，接收在截止后 1 秒 | after_cutoff | 本次输入截止外 |
| 10 | project_snow / p1 | 16:00:05 页面访问 | valid | 与个人站 p1 不同应用 |

逐步相减：原始 10 条，先隔离 8、排除本次截止外的 9；同 ID 阶段将 2 判重复、6 判冲突；请求阶段将 4 判重复；最后保留 `1,3,5,7,10`。因此：

```text
10 = 5 valid + 2 duplicates + 2 quarantined + 1 after_cutoff
```

对于小吉生成请求，香港 1 月 1 日有 1 请求、0 成功；1 月 2 日有 1 请求、1 成功。两天合计 2 个请求、1 个成功，但这里不把每日 UV 相加冒充跨日期人数。PV 只数 `page_view`，不能把每个 DWD 事件都当浏览量。

“先失败、后成功”为什么不改结果？本项目完成日志按不可变事实建模，第二次完成属于重复报告，而非经过定义的业务状态修订。若未来业务需要纠正结果，应设计显式修订事件、业务版本及冲正规则，不能在 `ORDER BY` 改一个 DESC 就悄悄改变统计口径。

## 分步操作：只用 Python 标准库和内存 SQLite

### 第一步：准备单独的练习目录

执行环境：**Windows PowerShell**。工作目录：仓库根目录。前置条件：已存在本项目 `.venv/Scripts/python.exe`；不安装任何依赖，不连接 Linux VM。下方随机目录写在 Windows 临时目录，和真实 `runtime/real` 数据完全分开。

```powershell
Set-Location 'Snow_Statistics 仓库根目录'
if (-not (Test-Path './.venv/Scripts/python.exe')) { throw '先按搭建章准备 Python 环境' }
$lesson18 = Join-Path $env:TEMP ('SnowStatistics-Learning-18-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $lesson18 | Out-Null
$exercise18 = Join-Path $lesson18 'ch18_dwd.py'
$exercise18
```

把下一段 Python **完整复制**到显示出的文件路径，保存成 UTF-8 文本。不要把外面的三反引号复制进去。若不使用仓库虚拟环境，也可用已有 Python 3.10+；其 SQLite 至少要 3.25，脚本会先检查窗口函数版本。本次实际验证使用的是本项目 Python 与 SQLite 3.53.1。

### 第二步：保存并阅读完整程序

程序只调用 `sqlite3.connect(":memory:")`，退出后没有数据库文件。`contract_ok` 是教学用开关，短 ID 也只是便于手算，不符合完整生产 UUID 契约。生产入口必须使用已有事件 schema 和服务端校验，不能把本例当采集器。

```python
"""Synthetic, in-memory SQLite lesson; no project/production imports or network."""
import hashlib
import json
import sqlite3

db = sqlite3.connect(":memory:")
db.row_factory = sqlite3.Row
assert sqlite3.sqlite_version_info >= (3, 25, 0), "SQLite window functions need 3.25+"
db.executescript("""
CREATE TABLE raw(
 seq INTEGER PRIMARY KEY, source TEXT, app TEXT, event_id TEXT, kind TEXT,
 request_id TEXT, success INTEGER, occurred_at TEXT, accepted_at TEXT,
 contract_ok INTEGER, payload TEXT
);
CREATE TABLE settings(cutoff TEXT);
INSERT INTO settings VALUES ('2026-01-02T00:00:00Z');
""")


def add(seq, app, event_id, kind, occurred, accepted, request=None, success=None, ok=1):
    event = dict(app=app, event_id=event_id, kind=kind, request_id=request,
                 success=success, occurred_at=occurred)
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
    db.execute("INSERT INTO raw VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (seq, "synthetic", app, event_id, kind, request, success,
                occurred, accepted, ok, payload))


add(1, "mywebsite", "p1", "page_view", "2026-01-01T15:59:50Z", "2026-01-01T15:59:51Z")
add(2, "mywebsite", "p1", "page_view", "2026-01-01T15:59:50Z", "2026-01-01T15:59:52Z")
add(3, "project_snow", "q1", "request_complete", "2026-01-01T15:59:40Z", "2026-01-01T15:59:41Z", "r1", 0)
add(4, "project_snow", "q2", "request_complete", "2026-01-01T16:00:10Z", "2026-01-01T16:00:11Z", "r1", 1)
add(5, "project_snow", "q3", "request_complete", "2026-01-01T16:00:20Z", "2026-01-01T16:00:21Z", "r2", 1)
add(6, "project_snow", "q3", "request_complete", "2026-01-01T16:00:20Z", "2026-01-01T16:00:22Z", "r2", 0)
add(7, "mywebsite", "p2", "page_view", "2026-01-01T16:00:00Z", "2026-01-01T16:00:01Z")
add(8, "mywebsite", "bad", "page_view", "2026-01-01T16:00:00Z", "2026-01-01T16:00:02Z", ok=0)
add(9, "mywebsite", "late", "page_view", "2026-01-01T16:00:00Z", "2026-01-02T00:00:01Z")
add(10, "project_snow", "p1", "page_view", "2026-01-01T16:00:05Z", "2026-01-01T16:00:06Z")

db.executescript("""
CREATE VIEW eligible AS
 SELECT * FROM raw
 WHERE source='synthetic' AND contract_ok=1
   AND accepted_at <= (SELECT cutoff FROM settings);
CREATE VIEW event_ranked AS
 SELECT *,
   ROW_NUMBER() OVER (
     PARTITION BY source,app,event_id ORDER BY accepted_at,seq) AS event_rn,
   FIRST_VALUE(payload) OVER (
     PARTITION BY source,app,event_id ORDER BY accepted_at,seq) AS first_payload
 FROM eligible;
CREATE VIEW event_winners AS
 SELECT *, CASE WHEN kind='request_complete'
   THEN 'request:'||request_id ELSE 'event:'||event_id END AS business_key
 FROM event_ranked WHERE event_rn=1;
CREATE VIEW business_ranked AS
 SELECT *, ROW_NUMBER() OVER (
   PARTITION BY source,app,business_key ORDER BY accepted_at,seq) AS business_rn
 FROM event_winners;
CREATE VIEW ledger AS
 SELECT r.seq,
   CASE
     WHEN r.source!='synthetic' OR r.contract_ok!=1 THEN 'quarantined'
     WHEN r.accepted_at > (SELECT cutoff FROM settings) THEN 'after_cutoff'
     WHEN e.payload!=e.first_payload THEN 'quarantined'
     WHEN e.event_rn>1 THEN 'duplicates'
     WHEN b.business_rn>1 THEN 'duplicates'
     ELSE 'valid'
   END AS disposition,
   CASE
     WHEN r.source!='synthetic' OR r.contract_ok!=1 THEN 'invalid_contract'
     WHEN r.accepted_at > (SELECT cutoff FROM settings) THEN 'after_cutoff'
     WHEN e.payload!=e.first_payload THEN 'event_id_conflict'
     WHEN e.event_rn>1 THEN 'same_event'
     WHEN b.business_rn>1 THEN 'same_request'
     ELSE 'accepted_winner'
   END AS reason
 FROM raw r LEFT JOIN event_ranked e ON e.seq=r.seq
 LEFT JOIN business_ranked b ON b.seq=r.seq;
CREATE VIEW dwd AS
 SELECT r.*, date(r.occurred_at,'+8 hours') AS business_date
 FROM raw r JOIN ledger l USING(seq) WHERE l.disposition='valid';
""")


def rows(sql):
    return [dict(row) for row in db.execute(sql)]


quality = dict.fromkeys(("valid", "duplicates", "quarantined", "after_cutoff"), 0)
quality.update(dict(db.execute("SELECT disposition,COUNT(*) FROM ledger GROUP BY disposition")))
quality["raw"] = db.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
assert quality == dict(raw=10, valid=5, duplicates=2, quarantined=2, after_cutoff=1)
assert quality["raw"] == sum(quality[k] for k in ("valid", "duplicates", "quarantined", "after_cutoff"))
winners = [r[0] for r in db.execute("SELECT seq FROM dwd ORDER BY seq")]
assert winners == [1, 3, 5, 7, 10]
requests = rows("""SELECT business_date, COUNT(*) AS requests, SUM(success) AS successes
                   FROM dwd WHERE kind='request_complete'
                   GROUP BY business_date ORDER BY business_date""")
assert requests == [dict(business_date="2026-01-01", requests=1, successes=0),
                    dict(business_date="2026-01-02", requests=1, successes=1)]
# Wrong on purpose: discard yesterday's winner BEFORE resolving request identity.
wrong_day_two = db.execute("""WITH incorrectly_trimmed AS (
 SELECT *,ROW_NUMBER() OVER(PARTITION BY source,app,request_id
                           ORDER BY accepted_at,seq) AS rn
 FROM event_winners WHERE kind='request_complete'
   AND date(occurred_at,'+8 hours')='2026-01-02'
) SELECT COUNT(*) FROM incorrectly_trimmed WHERE rn=1""").fetchone()[0]
assert wrong_day_two == 2
digest_before = hashlib.sha256(json.dumps(rows("SELECT * FROM ledger ORDER BY seq"), sort_keys=True).encode()).hexdigest()
digest_after = hashlib.sha256(json.dumps(rows("SELECT * FROM ledger ORDER BY seq"), sort_keys=True).encode()).hexdigest()
assert digest_before == digest_after
print(json.dumps(dict(input_origin="synthetic fixtures", sqlite_version=sqlite3.sqlite_version,
                     quality=quality, winners=winners, requests=requests,
                     ledger=rows("SELECT * FROM ledger ORDER BY seq"),
                     wrong_day_two_requests=wrong_day_two, repeat_query_equal=True),
                 ensure_ascii=False, indent=2))
db.close()
```

### 第三步：执行并保留自己的结果

仍在同一个 **Windows PowerShell** 窗口，工作目录不变：

```powershell
& ./.venv/Scripts/python.exe -B $exercise18 | Tee-Object -FilePath (Join-Path $lesson18 'result.json')
if ($LASTEXITCODE -ne 0) { throw '练习断言失败，请按下面的排错表检查' }
```

输出应含 `input_origin: synthetic fixtures`、`raw: 10`、`valid: 5`、`duplicates: 2`、`quarantined: 2`、`after_cutoff: 1`，以及赢家 `[1,3,5,7,10]`。`wrong_day_two_requests: 2` 是特意写错查询的反例，不是正确指标。程序中的 `assert` 是手算结果的自动校验；不要删除断言只为让程序退出码变成 0。

## 把窗口函数一层一层拆开

第一层 `ROW_NUMBER() OVER(PARTITION BY source,app,event_id ORDER BY accepted_at,seq)` 不会直接减少行，只给每组编号。真正减少行的是后面的 `WHERE event_rn=1`。`FIRST_VALUE(payload)` 将首条规范内容带到组内每行，后来的记录与它逐字比较；如果 ID 相同而内容不同，归为冲突，不能仅因为编号大于 1 就叫普通重复。

内容比较不包含接收时间和投递位点，否则同一事件重送时仅因接收时间改变就会被误判冲突。例子使用排序后的规范 JSON 字符串，便于看清比较对象；生产可保存规范化内容指纹，但必须先明确字段、类型、空值和序列化约定。哈希不同可以证明内容不同，哈希相同不是任意输入下的数学绝对证明。

第二层把请求事件的业务键设为 `request:<request_id>`，其他事件设为 `event:<event_id>`，再在来源与应用内排序。前缀可以避免“一个 event_id 恰好等于某个 request_id”造成混组。这里 `request_id` 不为空由练习数据保证，真实链路必须由契约保证。

把下面的调试语句插入程序末尾 `db.close()` **之前**即可观察中间结果；它不是连接线上数据库的命令：

```python
print(rows("SELECT seq,event_rn,payload=first_payload AS same_content FROM event_ranked ORDER BY seq"))
print(rows("SELECT seq,business_key,business_rn FROM business_ranked ORDER BY seq"))
print(rows("SELECT seq,business_date FROM dwd ORDER BY seq"))
```

预期事件层 `seq=2` 的编号是 2 且内容相同；`seq=6` 编号是 2 但内容不同。请求层 `seq=4` 的 `business_rn=2`。最终 `seq=7` 的业务日期是 `2026-01-02`。这三个观察比只看最后一个 COUNT 更容易定位错误。

## 为什么不能先按报表日期过滤

如果计算 1 月 2 日时先删掉 1 月 1 日记录，r1 的原始失败赢家消失，q2 就成为当前输入里“第一次出现的 r1”。错误 SQL 会得到新一天 2 请求且 2 成功，正确结果应为 1 请求且 1 成功。脚本已经把这两种查询都执行并用断言展示差异。

应先在输入实际覆盖的去重范围中确定赢家，再按事件业务日期投影报表。受限保留期意味着你不可能永远记住所有过去 ID；生产要结合受控去重辅助状态、输入覆盖证明和缺口标记，不能用“我查不到旧事件”推导“它一定是首次”。七天原始保留和最多三十天辅助状态也是明确边界，而非无限历史去重保证。

## 更新、删除与重放分别处理什么

| 变化 | 不应做的事 | 正确区分 |
|---|---|---|
| 完成事件再次投递 | 再加一次请求数 | 同事件或同请求去重 |
| 同事件 ID 内容改变 | 随机选最新 | 内容冲突隔离，说明谁被接受 |
| CDC 业务行版本从 1 改到 2 | 把版本 2 当重复删掉 | 这是有效状态更新，按版本或 SCD2 建模 |
| CDC `op=d` | 丢掉删除消息，令实体永远存活 | 留下删除事实或墓碑，快照按删除语义处理 |
| Kafka 空 tombstone | 当普通业务事件解析 | 与 Debezium 的有内容删除事件分开计账 |
| 隐私到期清理 | 伪造一条业务删除事件 | 数据生命周期清理，不代表当时发生业务删除 |

本章处理的是不可变访问/完成事件，CDC 的 `before/after` 与事务位置见第 15 章，内容历史见第 19 章，工单状态见第 20 章。不能把“最新版本赢”的 CDC 规则套在“首次完成赢”的请求指标上。

## 检查方法与常见失败

先检查账本等式，再检查具体赢家，最后检查报表。这能区分“少了一行”“选错一行”和“时区归属错了”三种问题。只比较最终总数可能让一次漏数和一次重复互相抵消。

| 现象 | 优先检查 | 处理 |
|---|---|---|
| 报错 `near OVER` | SQLite 版本低于 3.25 | 使用搭建章已有 Python 环境；不要为了练习操作业务数据库 |
| `raw=10` 却无法对账 | 一行同时计入两个原因，或 JOIN 放大 | 每行只能有一个最终 disposition，先查 seq 明细 |
| r1 被记成功 | 排序使用 DESC，或先按日期过滤 | 恢复接收时间、位点升序，先去重后投影日期 |
| 两站 p1 只剩一个 | 分组漏掉 app | 用完整来源、应用、事件键 |
| 所有页面只剩一条 | 用空 request_id 给所有事件分组 | 非请求事件使用 event 键 |
| 截止外消息混入 | 比较了 occurred_at 而非接收窗口 | 分开接收范围与业务日期；边界是否含等号按契约 |
| 字符串时间顺序奇怪 | 混用不同 offset、精度或无时区文本 | 先规范成同一 UTC 表示或时间类型，再比较 |
| 重跑结果随机变化 | 排序键相同且没有稳定决胜条件 | 核对源代际和位置，冲突拒绝，不能任意加随机排序 |

这里固定 `Z` 且固定秒精度，所以 SQLite 文本比较可靠；不能把同样写法直接复制到混合 `+08:00`、`Z`、不同小数精度的生产文本中。本例把香港时间写作 `+8 hours`，只用于当前香港无夏令时的固定样本；生产仍按 `Asia/Hong_Kong` 统一处理。

## 练习、答案与面试追问

1. **把 seq=4 的 `event_id` 改成 q1，会发生什么？** 它和 seq=3 ID 相同但成功值、发生时间不同，变成冲突隔离。赢家仍是 seq=3；质量变成 valid=5、duplicates=1、quarantined=3、after_cutoff=1。请求数仍是 2。修改后同步修改对应预期断言，再解释为什么分母没变。
2. **把 seq=9 的接收时间改成恰好 cutoff 呢？** 本练习接收窗口含截止点，9 会进入候选并成为新页面赢家；valid=6、after_cutoff=0。发生日期仍由原 occurred_at 决定。生产自然闭日和新鲜 ACK 的证明不能由这个等号练习替代。
3. **把全部查询改成 `ORDER BY seq DESC` 能修正最新结果吗？** 不能。这改变事件口径，不是性能优化。若需要纠错，定义新修订模型并单独验收。
4. **把 source 从 synthetic 改成未知值会怎样？** 当前练习的来源检查会隔离，不默默合并。真实入口的 source 由受控服务决定，不从浏览器任意字符串信任而来。
5. **重复导入十次，raw 是否必须仍是 10？** 取决于 raw 的定义和输入清单。若确实读了十次投递，raw 可以是 100，有效事实仍需稳定；若先在 Kafka 位点层去重，进入 DWD 的 raw 已是另一层输入量。面试时先说明分层，不混用分母。
6. **窗口去重为什么比 `SELECT DISTINCT` 更合适？** DISTINCT 比较的是整行；接收时间改变就可能保留两行，也不能表达同请求不同事件只接受首条。窗口能明确业务键、排序和赢家，但本身不自动验证内容冲突。

可用两分钟讲解：“我先固定输入来源与截止时间，把原始投递量分成有效、重复、隔离和本次截止外四类；事件与业务请求分别去重，按接收次序确定赢家，再按香港业务日期汇总。内容冲突与普通重放分别记账，失败记录不携带敏感正文。教学 SQL 已实际运行，生产能力则以各引擎回执为准。”

[返回学习目录](00-阅读地图与当前状态.md)
