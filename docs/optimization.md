# 固定输入的优化对比

2026-09-12 在实际 Spark 3.5.7 / YARN / HDFS 运行 `optimize-01`，复用百万条原始事件产生的 **900,000 行已验收 DWD**。三 VM 为 2/2/1 GiB，只有 compute 上一个 512 MiB、1 core executor。目标是解释执行机制和成本，不是模拟真实网站百万用户。

先验证来源及行数，再生成两份投影数据布局，通过双向 `exceptAll` 检查与原数据逐行等价；原始 DWD 的 28 个文件、长度和 HDFS 校验值在实验前后不变。7 种查询各运行三轮，中间一轮反转顺序，清除 Spark cache，但不清除 OS/HDFS 缓存。所有查询结果均匹配固定基准。

## 结果与取舍

| 实验 | 基线中位秒 | 调整后中位秒 | 可观察的机制 |
|---|---:|---:|---|
| 日期分区裁剪 | 1.165 | 0.338 | 读取 7 个日期/28 文件变为 1 个日期/4 文件，任务输入 40,321,191 → 5,166,759 bytes |
| 两行角色维表广播 | 1.321 | 1.138 | SortMergeJoin → BroadcastHashJoin，Shuffle 写出 64,376 → 937 bytes |
| 热点键加 16 个盐值后 Merge Join | 1.321 | 1.872 | 最大分区记录 285,000 → 125,641，但增加哈希、额外列读取和维表复制，当前更慢 |
| 合并小文件 | 1.586 | 0.605 | 64 文件 → 4 文件，查询任务 12 → 4，实际输入量接近不变 |

分区基线从事件时间重新计算业务日期，调整后直接过滤已有 `business_date`，两者都按 Asia/Hong_Kong 口径。除裁剪外也减少日期表达式计算。SQL `filesSize` 是选中文件的完整大小，任务 `Input Metrics.Bytes Read` 才是本次列裁剪后实际读取的计数，不能混为一谈。

角色输入 300,000 行，其中 285,000 是热门角色。按角色键分区时两个非空分区分别 285,000/15,000；加盐后四分区为 125,641/23,324/75,959/75,076，并没有均匀到每个分区相同。执行计划显示加盐方案额外读取 `event_id`，本次输入从约 0.24 MB 增至 31.87 MB、Shuffle 写出约 1.13 MB。在单 core 上无法获得并行收益，本项目保留广播小维表的做法，不默认采用加盐。

小文件复制为相同六列、900,000 行，64 文件合计 38,047,453 bytes，4 文件为 37,924,642 bytes；逻辑数据逐行相同。这里的 4 只是受限样例参数，不能把生产分区都固定合成 4 文件。

为使机制可见，本实验显式关闭 AQE、设置自动广播阈值 -1、Shuffle 分区 4、文件分片上限 32 MiB，并使用明确 Join hint。它们属于对照实验配置，不改主作业默认行为。Spark 对 hints、文件分片和 AQE 的定义见[固定 3.5.7 官方文档](https://spark.apache.org/docs/3.5.7/sql-performance-tuning.html)；实际采用的算子由保存的执行计划证明。

## 证据与重现

完整应用 `application_1789144357700_0001` 时长 128.452 秒，包含布局准备和逐行对比；包装器 167.547 秒。404 个 task、50 个 job 全部成功，CPU 合计 46.737 秒、GC 2.490 秒、磁盘 spill 0。总任务读取 29,314,392 条是多次扫描累计，不能写成源输入条数。

23 次宿主采样：项目最大 60.25 GiB、宿主 RAM 最少 12,991 MiB、空闲磁盘最少 48.20 GiB，门禁未触发。三节点内存快照中无 OOM；driver 只在运行中采样，未声明完整进程峰值。原始输入、复制布局和事件日志全部保留。

- [公开聚合证据](evidence/optimization.json)：三轮时间、扫描算子计数、每组 task 成本、输入与代码校验值。
- [七份代表执行计划](evidence/optimization-plans.json)：分区过滤、Join 和 Exchange 的实际物理计划。
- 忽略路径 `runtime/optimization/optimize-01-complete`：完整结果 JSON、Spark 事件日志和提交日志；不公开原始事件或环境属性。

复现需先完成[百万条 YARN 基线](scale.md)，使用新 attempt，依照资源手册启动 2/2/1 GiB HDFS/YARN 阶段并同步源码：

```powershell
uv run python tools/run_scale_guarded.py --events 1000000 --workload optimization --attempt optimize-02
```

工具先核对原 DWD 双副本健康，输入只读，新输出位于 `/snow/warehouse/optimization/<attempt>`，收据位于 control 的 `runtime/optimization/<attempt>`。事件日志由 root 运行的实验 driver 创建，导出时仅对明确文件使用 sudo 读取，不修改共享目录权限。回收实验必须按资源清单另行审阅，重跑不覆盖旧数据。
