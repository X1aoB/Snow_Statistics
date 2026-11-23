# 06 轻量服务、SQLite与WAL

## 为什么这个项目用SQLite

真实流量小，基础统计需要的是可确认写入、去重和稳定的日聚合，而不是先搭一套分布式数据库。SQLite是嵌入应用进程的数据库；不需要额外数据库服务器。WAL把变更先写入预写日志，配合事务和检查点，可以让读写协调得更好，但不会让SQLite变成无限并发写入系统。

线上采用单进程服务，应用锁与SQLite事务共同序列化关键修改。预算包含主数据库、WAL及辅助文件。只看statistics.db大小而忽略WAL，会低估实际占用。

```text
HTTP校验 → 事务写events/seen → 提交成功 → 返回202
后台聚合 → 读取seq之后的事件 → 更新daily/辅助去重 → 同事务推进seq
公开投影 → 固定日截止 → 冻结低计数保护结果 → JSON
```

主要表可按职责理解：events是短期明细；seen记录事件去重；requests_seen处理完成请求去重；visitors支持每日UV；daily和popularity保存精确基础聚合；meta保存位点与维护信息。公开投影另外拥有候选、独立访问者辅助表和冻结快照，不能直接把内部精确daily原样暴露。

## 在本地观察数据库

不要用 `sqlite3.connect('随手写的路径')` 判断服务是否正常：它可能创建一份全新的空库，得到的 `delete` 日志模式与实际服务无关。下面使用项目自己的 Store 初始化一份明确标为 synthetic 的新库，只在本机执行，不开 HTTP 服务，不连接生产，也不需要令牌。

前置条件：已完成第11章，仓库 `.venv` 可导入 `snow_statistics`。先在 **Windows PowerShell、仓库根目录**运行：

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
New-Item -ItemType Directory -Path 'runtime/learning' -Force | Out-Null
& .\.venv\Scripts\python.exe -c "import snow_statistics; print(snow_statistics.__file__)"
```

将下面完整 Python 脚本保存为仓库内 `runtime/learning/sqlite-wal-demo.py`，使用 UTF-8。固定时钟和标识都是合成值；每次运行用新目录，旧实验不覆盖。

```python
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from snow_statistics.config import Settings
from snow_statistics.contracts import Event
from snow_statistics.store import Store

root = Path("runtime/learning")
root.mkdir(parents=True, exist_ok=True)
directory = Path(tempfile.mkdtemp(prefix="wal-demo-", dir=root))
now = datetime(2026, 1, 1, 12, tzinfo=UTC)
settings = Settings(mode="lite", source="synthetic", db=directory / "stats.db",
                    allowed_characters=frozenset({"sample_character"}))

def page(number, visitor):
    return Event(event_id=UUID(int=number), app="mywebsite",
                 event_type="page_view", occurred_at=now, path="/",
                 anonymous_id=UUID(int=visitor))

def completed(number, request, success):
    return Event(event_id=UUID(int=number), app="project_snow",
                 event_type="request_complete", occurred_at=now,
                 request_id=request, character_id="sample_character",
                 elapsed_ms=120, success=success)

events = [page(1, 100), page(2, 100), page(3, 200), page(2, 100),
          completed(4, "R1", True), completed(5, "R1", True),
          completed(6, "R2", False)]
query = "SELECT app,pv,uv,requests,successes FROM daily ORDER BY app"
store = Store(settings, clock=lambda: now)
try:
    print("directory:", directory)
    print("journal_mode:", store.db.execute("PRAGMA journal_mode").fetchone()[0])
    print("first:", store.ingest(events))
    print("replay:", store.ingest(events))
    store.aggregate()
    store.aggregate()  # 重复聚合不重复加总
    before = [tuple(row) for row in store.db.execute(query)]
    print("daily:", before)
    with sqlite3.connect(directory / "backup.db") as destination:
        store.db.backup(destination)
        print("backup_integrity:", destination.execute("PRAGMA integrity_check").fetchone()[0])
finally:
    store.close()

reopened = Store(settings, clock=lambda: now)
try:
    reopened.aggregate()
    after = [tuple(row) for row in reopened.db.execute(query)]
    assert before == after
    print("reopen_equal:", before == after)
finally:
    reopened.close()
```

运行位置仍为 **Windows PowerShell、仓库根目录**：

```powershell
& .\.venv\Scripts\python.exe -X utf8 runtime/learning/sqlite-wal-demo.py
```

预期看到 `journal_mode: wal`、首次 `accepted=6 / duplicates=1`、整批重放 `accepted=0 / duplicates=7`；daily 为 `('mywebsite', 3, 2, 0, 0)` 与 `('project_snow', 0, 0, 2, 1)`；备份完整性为 `ok`、重新打开后相等为 `True`。这同时验证第03章手算、WAL初始化、两层去重、幂等聚合和一致备份读回，但不是进程掉电测试或生产压测。库和备份留在输出的新目录中，停止脚本不会删除它们。

若导入失败，先核对当前目录和 `.venv` 路径；若字段校验失败，比较脚本与当前事件契约；若输出不是预期，保留该合成目录和报错，先运行 `tests/test_lite.py`，不要改生产配置或复制正式库来“找答案”。服务退出时会尝试 WAL checkpoint，因此退出后看不到较大的 `-wal` 文件不代表此前没有使用 WAL。

## 两个常见误区

第一，“commit完成就一定永不丢数据”是过强结论。真实持久性还依赖文件系统、磁盘和fsync等条件；项目证明的是既定环境下的确认与恢复行为。第二，复制正在写入的.db文件不等于一致备份，WAL里的已提交事务可能尚未归并。使用SQLite backup API取得一致副本，再对副本执行完整性检查。

## 排错顺序

收到503先分清是off、配额满还是后台聚合异常。查看healthz只能从私有回环或SSH渠道访问；公网不开放健康详情。存储达到预算时拒收新事件，而不是偷偷提前删除仍在保留期内、已确认的历史。解决容量问题前先停新流量并审阅本项目资源，不执行共享宿主全局清理。

练习：写events成功后返回202，但聚合进程崩溃，是否必须让客户端重发？

答案：不需要以客户端重发修复聚合。事件已持久化，后台按最后成功聚合位点恢复。客户端即使因为网络未见202而重发，也由event_id去重，不能重复累加。

## 对照源码与项目证据

- [src/snow_statistics/store.py](../../src/snow_statistics/store.py)
- [src/snow_statistics/api.py](../../src/snow_statistics/api.py)
- [docs/resources.md](../resources.md)

[返回学习目录](00-阅读地图与当前状态.md)
