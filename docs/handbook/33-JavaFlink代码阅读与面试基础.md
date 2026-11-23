# 33 Java、Flink代码阅读与面试基础

## 为什么项目同时使用Python和Java

Python适合模拟器、轻量服务、编排脚本和小样本正确性基准。Java负责实际Flink流处理，实现事件校验、状态去重、迟到处理和Doris sink。语言不是为了凑数量，而是围绕执行环境和可维护性选择。

本项目Java目标版本11，Flink1.20.3。Maven负责依赖解析、测试和打包；`provided`依赖由Flink运行时提供，不能把整套Flink运行时随应用重复塞入JAR。Shade打包把应用需要的connector和依赖整合，版本冲突仍要通过锁定和实际作业验收发现。

```mermaid
flowchart LR
  P[pom.xml 版本与作用域] --> C[编译和JUnit]
  C --> J[应用JAR]
  J --> V[SHA256和运行时锁]
  V --> F[Flink提交 相同UID与状态]
```

## 从入口逐层阅读代码

先读`EventContract.java`，识别允许事件、必填字段、来源和白名单，理解非法输入为什么被隔离。再读`RealtimeJob.java`的main和source配置，画出每个map/filter/keyBy/process/sink的作用。最后找状态描述符、TTL、timer、Checkpoint配置和Doris写入设置。不要从头背每行代码，要能解释一条消息经过哪些决策。

Java泛型如`Map<String,Long>`约束键值类型；不可变事件对象可减少误修改；异常处理应区分非法数据与基础设施失败。不要把所有异常catch后返回成功，否则失败输入会永久丢失且无诊断。

## 状态和普通变量的区别

普通成员变量在任务进程重启后丢失；Flink managed state参与Checkpoint和恢复。keyBy后，状态按键分区；同一键到同一算子分区，便于去重和窗口更新。将海量用户所有记录放在一个全局HashMap里既难恢复也容易撑爆内存。

状态TTL用于限制长期存储，不代表事件本身的业务保留期。去重状态过期后旧消息可能再次被视为未知，因此历史回放应有独立lane或更高层业务键保护。项目TTL8天必须与原始保留、迟到与回放策略一起解释。

## 线程、背压和资源

背压表示下游处理能力不足，导致上游发送变慢，可能来自Doris写入、网络、序列化、热点或CPU。提高source并行度不会解决慢sink，反而可能增加积压。Checkpoint变慢时，先观察背压、状态大小、外部提交和存储耗时，不能一律把间隔改得更短。

JVM堆内存与容器内存不同，直接内存、线程栈、元空间和运行时开销也消耗内存。容器被OOM kill不一定会留下Java OutOfMemoryError堆栈。资源比较必须同时记录容器limit、JVM参数、TaskManager slots和实际并行度。

## 编译和验证步骤

Windows仓库目录先检查Java与脚本配置；`tools/maven_build.ps1`使用本机配置的JAVA_HOME路径，该路径不是通用安装标准。确认安装目录后运行，脚本校验Maven下载摘要并完成package：

```powershell
java -version
Get-Content -LiteralPath tools/maven_build.ps1 -First 35
powershell -NoProfile -File tools/maven_build.ps1
```

预期JUnit通过并生成应用JAR。测试可以在无Kafka/Flink集群情况下验证纯契约和逻辑；真正Checkpoint、Doris两阶段提交和进程恢复仍需第24章集成实验。构建成功只是允许进入集成，不是生产可用结论。

## 常见错误与练习

ClassNotFoundException先查依赖是否应打进应用JAR、是否provided错误；NoSuchMethodError常是运行时版本不兼容；恢复报UID/serializer不匹配先对比作业拓扑与版本，不要直接删除Checkpoint使报错消失。不要把换lane当作“修复恢复”，它实际上可能开启全新状态。

题1：用普通HashSet保存event_id能否实现故障后的幂等？答案：仅进程存活期间有效，重启丢失；要用受管理状态并验证恢复，同时sink/业务键仍需独立约束。

题2：Checkpoint每10秒，所以数据最多丢10秒吗？答案：不能直接推导。可重放source、sink事务、确认语义和状态持久化共同决定恢复行为；本项目需要实际对账而不是仅看间隔参数。

题3：为什么不把所有服务统一到最新JDK？答案：各组件支持范围和连接器兼容性不同；锁定已验收组合并隔离JDK，升级先在独立候选做集成验证。

## 对照源码与项目证据

- [warehouse/flink/src/main/java/dev/xiaob/snow/RealtimeJob.java](../../warehouse/flink/src/main/java/dev/xiaob/snow/RealtimeJob.java)
- [warehouse/flink/src/main/java/dev/xiaob/snow/EventContract.java](../../warehouse/flink/src/main/java/dev/xiaob/snow/EventContract.java)
- [warehouse/flink/src/test/java/dev/xiaob/snow/RealtimeJobTest.java](../../warehouse/flink/src/test/java/dev/xiaob/snow/RealtimeJobTest.java)
- [warehouse/flink/pom.xml](../../warehouse/flink/pom.xml)
- [tools/maven_build.ps1](../../tools/maven_build.ps1)

[返回学习目录](00-阅读地图与当前状态.md)
