# Doris 3.0.6.2 小资源 BE 启动配置审计

本记录是源码核查与候选参数建议，不是新配置的引擎验收回执。没有为本次核查启动、停止或修改虚拟机。实际配置与验收由独立 real epoch 流程管理。

## 版本和证据绑定

- 锁定镜像：`apache/doris:be-3.0.6.2`，摘要 `sha256:4d3bb3b70568b044f36035b9f0b6337fafa0c06ffae89bfd39387bcfdc302517`，见 `lab/locks/images.env`。
- 官方 tag `3.0.6.2/bin/start_be.sh` 的 SHA-256 为 `d901a0cb6f13b3016d65fe3ccfdb3832a0dd88352f96cea99516bfc1076d906b`，与 `deploy/real-epoch/prepare-be-start.sh` 中经过镜像核验的原脚本摘要一致。
- 官方 `3.0.6.2/conf/be.conf` 的 SHA-256 为 `ad3b9f4efe1a73152ab72b584a6878384492367fdacbd08395a363be18c9186f`。
- 本机只读参考副本在忽略目录 `runtime/doris-tuning-research/`，文件名用双下划线替换源码路径中的 `/`。它们是公开源码，不包含真实事件或凭据。

## 为什么普通 JAVA_OPTS 没有生效

[锁定版本启动脚本](https://github.com/apache/doris/blob/3.0.6.2/bin/start_be.sh)先从 be.conf 导出大写配置，随后检查实际 Java 版本。JDK 17 分支选择 `JAVA_OPTS_FOR_JDK_17`，最终将这个值赋给 `LIBHDFS_OPTS` 和 `JAVA_OPTS`。因此仅在追加配置中设置 `JAVA_OPTS=-Xmx256m`，不能改变该分支采用的参数。

[同版本默认配置](https://github.com/apache/doris/blob/3.0.6.2/conf/be.conf)中的 `JAVA_OPTS_FOR_JDK_17` 含 `-Xmx2048m` 及一组兼容性参数。候选修正应保留这组原始参数，仅把堆配置替换为 `-Xms64m -Xmx256m`；不要只写一个短 `-Xmx` 行而丢失模块开放参数。是否使用 Serial GC 或修改 JVM 活跃处理器数，应另行实测，不能把 C++ 线程池问题全部归因于 JVM。

验证时应读取实际 `doris_be` 进程的环境并只投影 `JAVA_OPTS`/`LIBHDFS_OPTS`，确认两者都采用 256 MiB 堆。仅查看 Compose 环境或宿主 shell 的变量不够。不得打印整个进程环境。

## 为什么 scanner 处触及 pids_limit

[同版本 scanner 初始化](https://github.com/apache/doris/blob/3.0.6.2/be/src/vec/exec/scan/scanner_scheduler.cpp)使用两个固定大小的本地/受限扫描池；默认扫描线程数为 `max(48, 2 × CPU 核数)`，另外还有远端扫描池。[线程池实现](https://github.com/apache/doris/blob/3.0.6.2/be/src/util/threadpool.cpp)会在初始化时立即创建最小线程数。设置 `cpus=2` 并不会把默认扫描线程数降为 2。

扫描器之前还会创建 SendBatch、预取、上传、关闭等池。[执行环境初始化顺序](https://github.com/apache/doris/blob/3.0.6.2/be/src/runtime/exec_env_init.cpp)能够解释为什么尚未进行业务查询就可能发生线程创建失败。扫描器之后仍有服务线程：[主程序](https://github.com/apache/doris/blob/3.0.6.2/be/src/service/doris_main.cpp)继续启动 Thrift、BRPC、HTTP 及心跳服务。因此修复 scanner 的第一处错误，不等于整个 BE 已有足够线程余量。

以下都是[锁定 tag 实际定义的参数](https://github.com/apache/doris/blob/3.0.6.2/be/src/common/config.cpp)。建议用于单 BE、小样本、分阶段实验；降低并发能力是明确的取舍，不能称为一般生产推荐。

| 参数 | 原默认值 | 候选值 |
|---|---:|---:|
| brpc_num_threads | 256 | 8 |
| be_service_threads | 64 | 8 |
| webserver_num_workers | 128 | 4 |
| doris_scanner_thread_pool_thread_num | 自动，至少 48 | 4 |
| doris_scanner_min_thread_pool_thread_num | 8 | 1 |
| doris_max_remote_scanner_thread_pool_thread_num | 自动，至少 512 | 4 |
| send_batch_thread_pool_thread_num | 64 | 4 |
| fragment_mgr_asynic_work_pool_thread_num_min | 16 | 2 |
| fragment_mgr_asynic_work_pool_thread_num_max | 512 | 8 |
| pipeline_executor_size | CPU 核数 | 2 |
| spill_io_thread_pool_thread_num | 自动，至少 48 | 2 |
| num_buffered_reader_prefetch_thread_pool_min_thread | 16，另按 CPU 计算 | 1 |
| num_buffered_reader_prefetch_thread_pool_max_thread | 64，另按 CPU 计算 | 4 |
| num_s3_file_upload_thread_pool_min_thread | 16，另按 CPU 计算 | 1 |
| num_s3_file_upload_thread_pool_max_thread | 64，另按 CPU 计算 | 4 |
| min_nonblock_close_thread_num | 12 | 1 |
| max_nonblock_close_thread_num | 64 | 4 |
| min_s3_file_system_thread_num | 16 | 1 |
| max_s3_file_system_thread_num | 64 | 4 |

上述最小值满足线程池构造的非负最小值、正数最大值及最小值不大于最大值约束。扫描数显式为 4 时，不再执行 `-1` 才触发的自动 48 下界；远端最大线程数会与本地扫描数取较大值，所以远端也设为 4、最小值为 1。参数合法只代表代码能够接受，是否足以完成实际导入、查询和 Checkpoint 恢复仍需实验。

还存在事务发布、删除位图、刷盘、压缩、快照等后台池。本次不根据名字随意把所有池降为 1；若有限样本验收仍触及线程限制，应保留失败位置与实际线程计数，再核查具体初始化路径。

## 下一轮必须记录的实际结果

1. 使用全新、明确声明 synthetic fixtures 的 epoch；保留原失败实验身份，不修改旧回执来冒充新配置。
2. 除原来的 JVM 堆和内存证据外，记录本 BE 的实际 `pids.max`、`pids.current`、`pids.events`、`doris_be` 进程 `Threads` 及容器 PIDs，分别在启动、导入、查询和恢复后检查。
3. 实际查询 `SHOW BACKENDS`，核验本次 BE 的 Alive、启动时间和错误状态；保留内存峰值、无 OOM、没有线程创建错误的证据。
4. 对每个改动的 C++ 配置使用精确名称的 `SHOW BACKEND CONFIG LIKE '<参数>' FROM <实际 backend ID>`，结果再检查 Key 与期望完全一致。该语句需要管理员权限，不能把管理凭据给浏览器。[语法说明](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/cluster-management/instance-management/SHOW-BACKEND-CONFIG/)
5. 重启同一个候选 BE 后再次核验实际参数，防止启动包装脚本的重复追加或镜像默认值覆盖。之后再进入原有 Flink/Doris 合成恢复与对账验收。

本次核查不宣称 256 的 pids 限额一定足够，也不自动提高它。若实际数据表明仍需增加，必须作为独立、有限的本项目配置调整记录，继续受 VM 内存和磁盘边界约束。
