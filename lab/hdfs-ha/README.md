# HDFS HA acceptance gate

日常配置是单 NameNode + 两 DataNode。HA 不允许使用 `shell(/bin/true)` 伪装 fencing。

启用自动切换前需要：双 NameNode、三个 JournalNode、三个 ZooKeeper、两个 ZKFC；独立 nameservice/元数据目录；可以验证旧 Active 已失去写入能力的 fencing（例如专用实验主机上受限 SSH 命令停止已知的 Active 容器）。

当前交付不启用自动 HA，以免在未配置 fencing 时宣称完成双活防护。实施时先验证共享 edits、standby bootstrap 和手动切换，再接入 ZKFC，分别注入进程故障和网络隔离；记录 txid、租约、读写结果与恢复时间。不得对业务服务器执行关机或 fencing。
