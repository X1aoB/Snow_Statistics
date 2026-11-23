# 16 ODS归档、HDFS与不可变清单

## ODS保存什么

ODS是最接近来源的数仓层。它保留原始事件/CDC、来源位置和接收元数据，方便重新解析、对账和重放。ODS不是随便复制一份CSV；必须知道文件来自哪批消息、是否完整、内容是否被改过，以及模型到底读取了哪些文件。

HDFS中NameNode管理文件元数据和块位置，DataNode存数据块。两副本是同一个块有两份可用副本，不是两个目录各自复制一份同名文件。fsck健康和实际读回校验都需要观察，不能只看replication配置值。

```text
原始bytes/headers/key/offset → 本地fsync + SHA256 manifest
→ HDFS staging上传 → 读回校验 → rename成正式批次
→ 非空块双副本检查 → 不可变_snapshot.json → 下游计算
```

## 清单为什么重要

如果Spark简单读取 `/snow/ods/*`，作业运行中可能又来了新文件，重跑就得到不同输入。不可变清单固定本次所有批次及各分区结束位点；Spark只读清单列出的文件，排除_staging、后来的批次与清单本身。

应用级 `_snapshot.json` 是项目的输入清单，不等于HDFS原生快照。两者名称都叫snapshot，但机制不同。清单使用内容寻址，改变输入应产生新ID；旧归档不覆盖成不同内容。

## 分阶段操作

在控制VM仓库目录，根据已经审查的合成lane执行：

```bash
.venv/bin/python tools/land_kafka.py capture --lane learning01 --directory runtime/ods/learning01
.venv/bin/python tools/land_kafka.py land --lane learning01 --directory runtime/ods/learning01
.venv/bin/python tools/land_kafka.py ack --lane learning01 --directory runtime/ods/learning01
```

这不是让你在所有服务未启动时连续粘贴三行。capture需Kafka；land需HDFS；ack需已落地收据和Kafka。每一步检查输出与门禁，再切换资源阶段。只有新lane可以创建新实验，已有pending时必须恢复原批次，不能另起一批掩盖未完成状态。

当前基础合成ODS工具有受控Topic白名单、消息上限和连续offset假设，不能直接拿来消费任意压缩或事务topic。真实链路使用独立source/路径策略，是否支持某输入要看当前工具明确验证范围。

## 异常恢复

HDFS rename已成功但客户端没收到响应：先读回正式目录校验，不能重新覆盖。Kafka ACK成功但本地checkpoint没保存：按pending和已落地收据核对，允许幂等恢复。发现少了offset：停止并记录缺口，不静默earliest reset。

练习：原始100条，其中80行为、15CDC、2tombstone、3隔离，模型能发布吗？答案：原始数账本可以核对100，但存在3隔离，本项目模型发布质量门禁不通过。应保留原批次、修复解析或输入处理版本，再以可追溯新结果恢复，不把3条直接丢掉后报告全部有效。

## 对照源码与项目证据

- [docs/incremental-ods.md](../incremental-ods.md)
- [tools/land_kafka.py](../../tools/land_kafka.py)
- [warehouse/spark/event_input.py](../../warehouse/spark/event_input.py)

[返回学习目录](00-阅读地图与当前状态.md)
