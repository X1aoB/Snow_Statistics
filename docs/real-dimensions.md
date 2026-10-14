# 独立真实 DIM 只读验证

`tools/real_dimensions.py` 是新增的独立操作入口。它只在原生产 transport
authority `snow-analysis` 执行，使用原 real04 作业、不可变 ODS snapshot、
cutoff、计算 permit 和实际 lifecycle registry。它不修改原配置或作业字段，
不执行原 daily/behavior 命令，不写 HDFS 模型或公开指标，也不调用 SSH、启动
VM、调整资源 profile、创建事件或注册 Hive 表。

本模块已随已签名的c0f542c完整477文件来源安装到三节点，并各自实际读回。
原BCE闭日日结仍保持其发生时的来源；安装不代表真实Spark通过。2304配置的
实际尝试被资源门禁拒绝，新的2560候选仍须单独签名安装及受控引擎验收。

## 权限与数据路径

1. 原 `real_lab` 流程完成真实输入、闭日作业、prepare/cleanup/permit。新入口
   校验原 `validate_job`、gateway guard、permit 的 input/outputs/hash/15分钟
   有效期；gateway 只构造原命令作校验，不执行或覆盖原模型。
2. 新入口读取 **同一 analysis authority 的原文件**。原 owner 的 source/generation、
   HDFS roots、registry digest、last-cleanup 和 permit lifecycle receipt 必须相等，
   无 pending capture/cleanup；retired-storage 不能授予 raw compute 读取。它不把
   拷贝到另一个节点的 JSON 当作新的权限。原 HDFS snapshot 的实际字节再次核验。
3. `--prepare` 先在
   `runtime/real/dim-verification/<lane>/<original_run_id>/<fresh_attempt>/data/`
   建立 `RealLifecycle`，注册 descriptor、log、CID、spill、私有 validation/
   acceptance/failure 及其临时文件，再写 descriptor。全部采用最早的原
   `original_min_accepted_at + 7天`，不因技术输出只有聚合而延长到90天。
   prepare 不授予 permit、不读取事件 payload、不启动引擎。
4. `--execute` 重核原 live permit、实际 HDFS snapshot 和每个已注册本机 scope。
   持有原 authority metadata 的锁，锁等待、最后一次权威/HDFS复核、日志打开和
   引擎启动均扣除同一原 UTC 截止时间及 monotonic 预算。最后复核后重新计算
   driver 截止，不重置相对计时器。原 reader 的每次独立 count 及后续 action
   前后重核原文件/hash/deadline；同一截止 watchdog 取消仍在运行的 Spark job。
   所有 mounts 除本次已拥有
   data 目录外均只读；不挂整个 checkout、credential、Docker socket 或 business
   目录。Hadoop XML 同时只读挂在 `/etc/hadoop` 及 hash guard 实际读取的
   `/opt/snow/lab/generated/hadoop`；所有 engine 文件的容器路径均检查。HDFS读取
   使用原 lab 网络和三个固定节点别名。
5. Driver 调用原 `event_input.read_events`，保留原 validation/dedup/cutoff。
   在 bounded iterator 前只选择 source/app/event_type/occurred_at/accepted_at/
   path/character_id。匿名、event、request、session、jump ID 和 seq 不进入 DIM
   projection。临时 ordinal 由本次序号生成，不能代替持久身份。
6. 固定 typed DIM views 接入 pinned SQL。每行所有结果字段与相同公开元数据
   语义比较，之后只保存 **本机私有的无行聚合校验**，不保存投影或 join 明细，
   不写 HDFS/原 publication 路径。原 reader 的缓存可能落盘，所以 Spark spill
   也放在已注册的原7天 scope。完成后 unpersist/drop temp views/stop Spark，
   只核验并移除本次精确 label/name/CID 的容器。

全量原 snapshot 最多10000条事件；允许晚到事实按发生时间解析，同时 accepted_at
必须在原 cutoff 内。未知路径/角色、来源错配、重复 ordinal、空或过期 permit、
不完整 cleanup、非原 snapshot、source 文件变化、输出数/任何字段不等均不能
得到成功。没有可映射事件时不凭零行 comparison 声称 DIM 已验收。

## 当前锁定的公开 DIM

产物 ID 为
`87f5dbfa297a04e7ac699877354077c1db1ef093aae6a019934634361b97b563`，
`dimensions.json` SHA256 为
`fc9f7a4945c2a9e1c281c07fcb72377e3944e0e6e69509cbd1a5ab4b3f8b9616`。
这是已验证的公开目录材料：source real、3个版本、87条目录、`coverage=[]`。
root 要把这份 **公开元数据** 精确拷到 analysis 的同一 ignored artifact 路径，
核验 hash；它不是事件 payload，也不改变原 job/config。

本模块保留空 coverage。历史 Join 结果全部为 `unknown`，release/dimension ID
为空；不得用最新目录、连续推测或观察时间填补历史。当前 Join 显式使用原 job
cutoff，仅命中闭合观察区间才已知。现有观察是点观察，大多数 cutoff 不命中是
正确结果。知识截止仍为原产物值，不随执行时间延长。更新 DIM 必须提供新的真实
证据并重新审阅固定绑定，不能改历史时间或覆盖旧产物。

## 资源与命令

Driver 使用原锁定 Spark 3.5.7 镜像/JAR、Python3.8、local[1]、1280MiB/1CPU/
PID512、512MiB heap、只读 root、`/tmp` noexec 和独立16MiB Snappy exec tmpfs。
日志合并限1MiB；进程上限240秒并缩短到原 permit/raw scope 截止之前。最后复核后
至少剩余190秒才启动：至少30秒 driver 预算，加160秒固定清理预留。预留覆盖最终
HDFS复核的3次30秒请求、精确容器 inspect/stop/inspect/rm/list 的55秒上限、进程
TERM3秒/KILL2秒与管道 drain1秒，并留出余量。各次检查或启动耗时不能延长原截止。
真实读取需要 HDFS
lab 网络，不能沿用前一个合成 probe 的 network-none 声明。

POSIX driver 客户端先建立等待放行的 marker session，记录 leader 的 PID/start/
session 身份，再放行命令。收尾总是检查此已拥有 session 的精确 PGID，包含 parent
已经退出但子进程仍持 stdout 的分支；有界 TERM/KILL 后确认没有活成员，再用可取消
同步管道读取完成有界 drain。需要 sudo 的信号也只作用于重新核验的精确 PGID。
容器另按本次 label/name/CID 单独清理。Windows 合成子进程测试使用自己的 Job
handle；它不授予真实 Windows authority，也不代替后续 Linux/Spark 实测。

**原 analysis 1024MiB offline profile 不能运行此 driver 加原 DataNode。**
CLI要求实际 guest usable MemTotal至少2048MiB、MemAvailable至少1536MiB及
384MiB空闲磁盘；配置为2048MiB的VM可能因内核保留而不满足 usable 阈值。
2026-10-14 的 `real-dim-2304` 实际CLI尝试在 `resource_preflight` 报
`Analysis guest is too small`，尚未启动Spark；较早的minimal Python余量采样
不能替代同一次CLI完成导入和权威检查之后的准入。保留该失败，不降低门槛。
新增显式 `real-dim-2560` 候选为control2048／compute1792／analysis2560MiB，
三VM冷态内存后备6400MiB，另留原128MiB作业写入预算；每台启动还保留256MiB
余量。2304配置与默认 `real-small-1920` 保持，历史Hive/Lake内存范围不扩大。
root须先结束窗口并确认全部VM关闭，在新签名来源安装后配置、读回并实际测量，
新的2560 VMX、guest准入与DIM引擎/精确收尾仍待验收。CLI不会偷偷切回4608
或改VM。宿主72GiB项目/71.75GiB停止线/35GiB空闲
磁盘/4GiB可用RAM仍须 root 外部复核；本机 guest preflight 不是宿主峰值证据。

在 **analysis 项目目录**，完成新增签名来源安装和原 fresh permit 后才可执行：

```bash
.venv/bin/python tools/real_dimensions.py
.venv/bin/python tools/real_dimensions.py --prepare --config runtime/real/config/production-real-prod-04.json --run-id ORIGINAL_CLOSED_RUN --attempt r1
.venv/bin/python tools/real_dimensions.py --check --config runtime/real/config/production-real-prod-04.json --run-id ORIGINAL_CLOSED_RUN --attempt r1
.venv/bin/python tools/real_dimensions.py --execute --config runtime/real/config/production-real-prod-04.json --run-id ORIGINAL_CLOSED_RUN --attempt r1
```

`ORIGINAL_CLOSED_RUN` 必须换成原已注册作业，不能生成替代输入。默认命令不读取
配置或输入。prepare/check不会重发permit，原permit过期就通过原真实流程重新获得，
然后使用新的 attempt；旧 descriptor/evidence 保留，不覆盖旧结果、不重置 expiry。

CLI stdout只报告成功标志和receipt hash，不公开真实样本/status/business counts、
身份、目录键、IP或诊断。`validation.json` 中实际引擎身份和比较计数仅留在原期限的
私有scope。root须单独观察进程退出、容器收尾、原权限/source/hash和资源条件后审阅
acceptance。这里没有公开 evidence exporter，也没有现有生产指标发布动作。

## 本机合成检查

```powershell
& .venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/test_real_dimensions.py
& .venv/Scripts/python.exe -m ruff check src/snow_statistics/real_dimensions.py tools/real_dimensions.py warehouse/spark/real_dimensions.py tests/test_real_dimensions.py
```

测试只使用合成 metadata/events、SQLite SQL 对照和本机有界子进程；不会执行 HDFS、
Docker、Spark、SSH或VM。Driver有 Python3.8 AST 检查，容器内不导入需要
`datetime.UTC` 的 host builder。事件v1/summary v1与原模型 writer 均未修改。
本次没有 browser/Java行为变化；真实引擎、资源、来源安装与独立验收仍为后续事项。
