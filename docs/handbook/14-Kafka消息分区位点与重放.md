# 14 Kafka消息、分区、位点与重放

## Kafka在这里的职责

Kafka是可重读的事件日志与处理缓冲。producer写入topic，topic由一个或多个partition组成，partition里的offset给出顺序位置。consumer group记录各分区进度。它让离线落地、实时作业和恢复演练可以各自消费；不会自动理解event_id或帮你决定一次聊天是否成功。

```text
Topic
  partition0: offset0 → 1 → 2 → ...
  partition1: offset0 → 1 → 2 → ...
组A已消费位置：[p0:100,p1:80]
组B已消费位置：[p0:40,p1:20]
```

offset只在分区内部有顺序，不存在全topic统一offset。当前实时严格验收使用单分区和一个collector序列代际，便于保持首次接受赢家；多分区是后续需要重新验证的场景，不是把partition数改大就自动保持所有语义。

## 四个概念的区别

| 概念 | 含义 | 故障时的作用 |
|---|---|---|
| event_id | 业务事件身份 | 跨offset重放仍可去重 |
| topic/partition/offset | Kafka传输位置 | 确定从何处恢复读取 |
| consumer group | 一组消费者的进度 | 独立作业不互相抢同一进度 |
| lane | 项目定义的隔离实验批次 | 新实验不覆盖旧主题、状态和收据 |

本地ODS工具记录cluster ID和topic ID。如果主题删掉重建，即使名字相同，也不是原来那条日志；工具应拒绝继续而不是误接新历史。分区拓扑变化、保留造成的缺口、外部修改group位点也需要显式处理。

## 学习操作

先完成下面的本地闭环，再接真实 broker。它执行本项目真正的 `capture → land → acknowledge` 代码，但用内存中的消息源和目标对象代替 Kafka/HDFS。这样能在不开 VM 时亲眼检查确认顺序、固定输入与故障重试；不能把结果写成 Kafka 性能或 HDFS 双副本验收。

### 一次完整的本地合成练习

**执行位置：Windows PowerShell，工作目录为 Snow_Statistics 仓库。** 前置条件是第11章已安装 Python 3.12、本项目与开发依赖；不需要正式配置、reader token、Java或VM。输入全部由 `generate(seed=42, users=1)` 生成。

1. 先在文件管理器中创建仓库内的 `runtime/learning` 文件夹，已有文件夹则保留；将本节后面的完整 Python 代码保存为其中的 `kafka-ledger-lesson.py`。文件名可自选，但下面命令要一致；`runtime` 是本项目忽略的实验目录，不放到两个业务产品仓库。
2. 在 PowerShell 创建一个新的结果目录名并执行。目录必须不存在；程序会主动拒绝覆盖已有实验。

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) { throw '先完成第11章环境' }
$kafkaLesson = 'runtime/learning/kafka-ledger-' + [guid]::NewGuid().ToString('N')
& ./.venv/Scripts/python.exe runtime/learning/kafka-ledger-lesson.py --output $kafkaLesson
if ($LASTEXITCODE -ne 0) { throw '练习失败；保留当前目录，先读错误' }
Get-Content -LiteralPath (Join-Path $kafkaLesson 'result.json')
Get-ChildItem -LiteralPath (Join-Path $kafkaLesson 'journal') | Select-Object Name
```

3. 预期第一批3条、第二批1条，事件主题的下一条offset为4，四条原始记录只有3个不同 `event_id`。第二批是第一条事件的重放；归档保留它，业务去重是后续DWD的职责，不把一个新的Kafka位置当作一个新用户。
4. 预期 `frozen_pending_reused / post_commit_recovered / no_new_input` 都为 `true`。目录最终有 `batches/`、`landed.json`、`state.json`；`pending.json` 已按成功确认流程移除。每个批次有原始/规范化文件和校验清单。
5. 程序结束即已退出，没有服务需要关闭。保留输出用于复盘；下一次改输入时换新目录。

本练习故意制造两次失败：目标批次已写、快照尚未发布时失败；Kafka进度已确认、本地 `state.json` 尚未写时失败。程序只捕获预期注入异常，再调用原操作恢复。代码末尾的 `kafka_engine_tested=false`、`hdfs_engine_tested=false` 必须保留；内存中的 `LessonSink.root` 只是接口需要的合成命名空间，不会连接这个地址。

```python
"""Synthetic in-memory Kafka/HDFS substitutes: exercise the actual landing code."""
import argparse
import base64
import copy
import json
from pathlib import Path

from snow_statistics.landing import TOPICS, acknowledge, capture, land, pending
from snow_statistics.publication import canonical
from snow_statistics.simulator import generate

parser = argparse.ArgumentParser()
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)


class LessonBroker:
    def __init__(self):
        self.offsets = {}
        self.events = generate(seed=42, users=1)['events'][:3]

    def identity(self):
        return dict(cluster_id='synthetic-memory-only', group='snow-ods-lesson',
                    topic_ids={t: 'synthetic-' + str(i) for i, t in enumerate(TOPICS)})

    def bounds(self):
        return {t + ':0': (0, len(self.events) if i == 0 else 0) for i, t in enumerate(TOPICS)}

    def committed(self, key):
        return self.offsets.get(key)

    def read(self, key, start, end):
        for offset in range(start, end):
            yield dict(topic=TOPICS[0], partition=0, offset=offset, timestamp_ms=0,
                       key_b64=None, value_b64=base64.b64encode(canonical(self.events[offset])).decode(), headers=[])

    def commit(self, ends):
        self.offsets.update(ends)


class LessonSink:
    # This URI is only the interface's expected namespace. No HDFS connection.
    root = 'hdfs://synthetic-memory-only/snow/ods/synthetic/kafka/lesson'

    def __init__(self):
        self.directories = {}

    def put_directory(self, name, files):
        if name in self.directories and self.directories[name] != files:
            raise ValueError('Synthetic immutable destination conflict')
        self.directories[name] = copy.deepcopy(files)


broker, sink = LessonBroker(), LessonSink()
journal = args.output/'journal'
first = capture(journal, broker)
assert broker.offsets == {}
try:
    acknowledge(journal, broker)
    raise AssertionError('ACK must reject input that was not landed')
except ValueError:
    pass
try:
    land(journal, sink, fail_after_batch=True)
    raise AssertionError('Expected synthetic post-land interruption')
except RuntimeError:
    pass
broker.events.append(copy.deepcopy(broker.events[0]))
assert capture(journal, broker) == first
first_counts = pending(journal)[1]['counts']
land(journal, sink)
try:
    acknowledge(journal, broker, fail_after_commit=True)
    raise AssertionError('Expected synthetic post-ACK interruption')
except RuntimeError:
    pass
assert broker.offsets[TOPICS[0] + ':0'] == 3 and not (journal/'state.json').exists()
acknowledge(journal, broker)
assert not (journal/'pending.json').exists()
second = capture(journal, broker)
assert second and second != first
second_counts = pending(journal)[1]['counts']
land(journal, sink)
acknowledge(journal, broker)
assert capture(journal, broker) is None
result = dict(input_origin='synthetic fixtures', kafka_engine_tested=False, hdfs_engine_tested=False,
              first_batch_records=first_counts['raw'], second_batch_records=second_counts['raw'],
              event_topic_next_offset=broker.offsets[TOPICS[0] + ':0'],
              unique_event_ids=len({row['event']['event_id'] for row in broker.events}),
              frozen_pending_reused=True, post_commit_recovered=True, no_new_input=True)
(args.output/'result.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result))
```

### 把同一原则用于已准备好的合成 Kafka

下面是**真实引擎捕获阶段**的操作卡，和刚才已本地执行的替身实验分别记录。本次手册补写没有运行这些VM命令。历史真实 Kafka/CDC 的测量见[摄取证据](../evidence/ingest-receipt.json)和[增量ODS证据](../evidence/incremental-ods.json)，不能把历史28条或81条作为今后每次运行的固定期待值。

前置条件：按第12章进入 **snow-control Linux VM**，工作目录 `/home/snow/Snow_Statistics`；本次阶段已获准运行控制机Kafka，已核验资源、现有卷和锁定镜像，Hadoop、Doris、Flink等其他阶段停止。`lab/.env`、`lab/locks/images.env` 已准备，不打印其内容。四个允许输入的 `snow.synthetic.*` 主题必须存在且具有当前可读取的连续历史；不能拿真实主题补足示例。

```bash
# Linux VM；只在上述前置条件已满足时运行
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/.env
set +a
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.control.yaml up -d kafka
timeout 60 sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.control.yaml exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$CONTROL_IP:9092"
lessonLane="learn-kafka-$(date -u +%Y%m%d%H%M%S)"
lessonDir="runtime/learning/$lessonLane"
test ! -e "$lessonDir"
.venv/bin/python tools/land_kafka.py capture --source synthetic \
  --lane "$lessonLane" --directory "$lessonDir" --max-records 100
```

该命令使用独立group `snow-ods-<lessonLane>`和独立本地目录，读取的是已有四个固定合成主题；**新lane不会新建一套主题**。首次要求offset 0仍可读；保留期截断、主题重建、外部提交group或未知分区变化都会拒绝。应保留失败信息并重新设计合成输入范围，不能reset位点、删除旧主题或将缺口当零。

成功且有输入时返回 `batch_id` 和 `kafka_acked=false`。检查 `pending.json` 与该批次 `manifest.json`：原始数应等于行为数＋CDC变更数＋tombstone数＋隔离数，内容文件的SHA256应匹配清单；此时group尚未因capture推进。无新数据返回 `batch_id=null`，不能据此宣称业务量为零。

本阶段到capture即可停；`pending.json`是保留给第16章HDFS落地的恢复状态。随后用相同lane、相同目录执行land，核验完成后才ack；不为“结束得干净”删pending。若本阶段独自启动了Kafka且没有其他任务使用它，可用同一Compose命令的 `stop kafka` 停止容器，保留卷；控制机正常软关按第12章办理。若服务本来已由其他任务运行，则交接其生命周期，不擅自停掉它。

### 参数说明和实现对应

```powershell
uv run --extra lab python tools/land_kafka.py --help
```

先读capture、land、ack三个子命令说明。capture只固定本批分区起止位置并持久化，不提交位点；land确认落到HDFS；ack才提交下一条offset。这种阶段拆分让你可以在小内存下先运行Kafka捕获、再运行HDFS落地、最后回到Kafka确认。

日常单broker降低资源，故障专题另开RF3主题。副本数与分区数不是同一个参数：副本数描述同一分区的多份拷贝，分区数影响并行和顺序范围。Kafka3.9.1使用KRaft，本项目另学ZooKeeper协议，不把ZooKeeper说成日常Kafka必需组件。

## 排错与练习

本地练习出现 `FileExistsError` 时换新输出目录，不删除之前结果；`ModuleNotFoundError` 时先检查本项目 `.venv` 及安装路径。断言失败要保存报错、源码SHA和现有journal，不把断言删除后重跑。`ACK must reject`对应的检查证明未落地不能确认；如果先执行ack的失败被吞掉后继续当成功，整个证据就失真了。

能连bootstrap却消费失败，检查broker向客户端公布的advertised.listeners是否是客户端可达地址；启动状态正常但元数据暂时没有分区，先有界等待服务就绪。不能无限等待，也不能把容器Started当作完整健康证明。

练习：同一个event_id在offset10与offset20各出现一次，应该存几条原始消息、几条有效业务事件？答案：原始消息可为2，DWD有效事件应为1，重复诊断为1。若内容不同，则是冲突隔离，不是普通重放。把两个offset都算成两位用户是粒度错误。

追加练习：为什么第四条消息已经进入消息源，第一次capture重试仍返回原批次？答案：pending已固定原来的 `[0,3)` 范围；先完成这批，下一批才读取 `[3,4)`。为什么下一条offset是4而不是3？因为确认保存的是下一条待读位置，不是最后已读记录的位置。两次故障恢复通过仍不能证明broker副本选主、网络分区或分布式事务已经验收。

## 对照源码与项目证据

- [docs/incremental-ods.md](../incremental-ods.md)
- [src/snow_statistics/sync.py](../../src/snow_statistics/sync.py)
- [docs/realtime.md](../realtime.md)

[返回学习目录](00-阅读地图与当前状态.md)
