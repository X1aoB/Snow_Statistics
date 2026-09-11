# 实验、取证和学习路线

每周投入 25–35 小时。程序实现可集中推进；故障恢复、延迟、容量和优化结果必须来自实际运行，不能由配置推导为成功。

| 阶段 | 内容 | 必须保存的证据 |
|---|---|---|
| 1–2 周 | 解耦、SQLite、公开统计页 | 单元及业务回归、停服务/满盘/默认关闭、公开字段白名单 |
| 3–4 周 | VM、可靠同步、ODS/DWD/ADS | 镜像/JAR SHA、消费位点、断点重放、Spark accepted manifest、整数对账 |
| 5–7 周 | 三运营业务、CDC、维度建模 | 固定状态流、事务位点、分类前后归属、工单重开/删除/跨天样例 |
| 8–10 周 | Flink、迟到与恢复 | checkpoint/label/业务键分别记录；无积压时 accepted_at→Doris 可查的逐条时间与 P95 |
| 11–12 周 | 质量、血缘、性能 | 人工样例、表级血缘查询、分区/Join/倾斜/小文件前后执行计划 |
| 13–16 周 | Iceberg、HA、退出 | 快照/演进查询、恢复记录、同宿主虚拟副本边界说明、保留 lite 和完整移除演练 |

## 性能方法

先运行 `snow-stats benchmark --events 100000`，通过后运行 1000000；这是 Python 正确性基准。Spark 分布式实验单独生成 JSONL，保存输入 SHA、分区数、executor/driver 参数、EXPLAIN FORMATTED、Spark event log、CPU/RAM/磁盘证据。至少比较分区裁剪、广播小维表和压缩小文件三项。

实时新鲜度只在无历史积压且资源满足时评价 P95≤60秒。开始/结束标记、源接收时间、Kafka offset、Doris查询轮询间隔都写进实验记录；历史回放不可混入当前实时成绩。近似分位数不能与整数指标一样声称完全一致。

## 高可用专题

Kafka 日常单节点 KRaft。单独运行三 broker/controller 配置，复制因子 3，min.insync.replicas=2，acks=all；停止一个 broker 验证继续写入，停止第二个验证明确拒绝写入，恢复后检查 ISR 和消息完整性。学习 ZooKeeper 使用独立集群及 HDFS ZKFC，避免把 Kafka KRaft 错写为依赖 ZooKeeper。

HDFS 先验证两个 DataNode 副本；HA 专题使用双 NameNode、三 JournalNode、三 ZooKeeper 和 ZKFC。仅在启用 fencing 且有明确恢复步骤后进行自动故障切换。不可把单宿主虚拟节点的恢复写成跨物理机容灾。

## 求职材料

README 架构、可复现命令、固定样例和测试结果可直接展示。简历只写实测吞吐和已验证链路；真实用户量与合成事件量分开标注。每周保留 SQL/Java 笔记，以及一个能解释取舍和故障恢复的实验复盘。
