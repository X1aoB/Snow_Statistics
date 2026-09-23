# 本地运行与资源门禁

三 VM `scale`（2/2/1 GiB）已实际完成 Spark/YARN 10 万及百万条合成事件计算；计算阶段只启 HDFS/YARN/Spark，Hive、Airflow、Kafka、Doris 和 Marquez 停止。三台 VM 当前均已关闭并保存该内存配置。该成绩不代表完整模型 DAG 或全部组件能在这个配置同时运行。单分析 VM `realtime`（4.5 GiB）仍是独立阶段，启动前须先切换关机 VM 的配置。详见[扩样手册](scale.md)、[低流量资源配置](resources.md)和[实时手册](realtime.md)。

最近冷态文件约 54.79 GiB；`scale` 内存后备增加 5 GiB，再加 1 GiB 作业余量约 60.79 GiB。根据用户允许按实际需要调整资源的要求，当前项目文件预算从初始 60 适度调整为 **64 GiB**，统一读取 `deploy/resources.json`；宿主磁盘余量仍 35 GiB，RAM 余量仍 4 GiB。百万条实测发生在旧 60 GiB 上限内，最大 59.67 GiB。旧 `ods-compact` 加作业余量约 63.29 GiB，仍须检查数据增长和 RAM，不能直接当作可运行承诺。冷态三 VM 启动约需 10 GiB 可用 RAM。仅删除 guest 文件不保证 VMDK 缩小，磁盘压缩仍需完整临时副本，不能绕过维护门禁。

## 已知工具

VMware：`C:\Program Files (x86)\VMware\VMware Workstation\vmware.exe`；同目录 `vmrun.exe`、`vmware-vdiskmanager.exe`。

FinalShell：`C:\Users\25685\AppData\Local\finalshell\finalshell.exe`。可导入本地实验 SSH 连接；脚本使用 SSH，便于重复验证。不要导出生产密码或私钥到仓库。

## 虚拟机

```powershell
uv run --with pycdlib==1.14.0 python tools/vmware_lab.py prepare
uv run python tools/vmware_lab.py start --node snow-control
uv run python tools/vmware_lab.py ip --node snow-control
uv run python tools/vmware_lab.py status
```

初始生成三个仅属于本项目的 NAT Linux 节点：control 6 GiB/2 vCPU、compute 6 GiB/4 vCPU、analysis 最高 10 GiB/4 vCPU；实际启动使用经过验证的分阶段小配置。薄置备系统盘分别 18/18/28 GiB。Doris 与实时镜像使分析节点逻辑盘从初始 18 GiB 分阶段扩到 24、28 GiB；当前项目文件预算为 64 GiB。Ubuntu 镜像固定 release-20260826 并验证 SHA256。seed、SSH 凭据、VMX、磁盘都在忽略的 `runtime/vmware`。从 VMware 界面打开各节点的 VMX 即可手动管理。

启动会检查宿主空闲磁盘至少 35 GiB、整个项目文件预算 64 GiB（含本次启动可能新增的 .vmem），并保留 4 GiB 当前可用内存。文件长度是保守预算近似，不等于底层精确分配量；外部共享 uv/Maven 缓存不纳入本目录计数，扩样前还需核查依赖缓存和宿主空闲空间。薄置备逻辑容量与物理实占不同，不能只看输入文件大小。

analysis 的真实 storage 窗口使用 4608 MiB VM 加 256 MiB 启动余量；`tools/vmware_lab.py status --reserve-mib 4864` 是该阶段的宿主预检，不能被旧的 4096 MiB CLI 上限拦截。预检余量仍须同时满足项目 64 GiB、磁盘 35 GiB 和宿主可用内存门禁。

首次 SSH 主机公钥在本地 seed 中生成，以 `HostKeyAlias=snow-control` 等名称记录在 `runtime/vmware/known_hosts`，使用 `StrictHostKeyChecking=yes`。IP 可从 VMware NAT DHCP 租约按本节点 MAC 查询；不要关闭全局 SSH 校验。初始化脚本 `tools/bootstrap_guest.sh` 只接受三个指定实验主机名。

首次初始化后，在没有运行容器的节点执行 `uv run python tools/guest_clock.py --node snow-control --ip <节点IP> --set`，其余节点同理。校正前检查宿主 UTC 与 HTTPS Date 大致一致，再同步系统时间和 UTC RTC；本地 NAT 中未收到 NTP 包，因此该实验配置使用 VMware Tools 跟随宿主。去掉 `--set` 是只读检查，主机与虚拟机偏差达到 2 秒会拒绝通过。每次开始实时新鲜度实验、恢复快照或重新启动后检查；HTTPS Date 只用于排除大幅错误，不提供精密 NTP 证明。

首轮曾发现 guest UTC 快 8 小时，不能仅凭 timedatectl 的 synchronized 标志判断时钟正确。Linux 时间源与 RTC 原则参考 [Broadcom Linux timekeeping](https://knowledge.broadcom.com/external/article?legacyId=1006427)，周期同步及 RTC 偏移行为参考 [VMware time synchronization](https://knowledge.broadcom.com/external/article?legacyId=1189)。若之后改用可达的 NTP/Chrony，应关闭 VMware Tools 周期同步，避免同时由两个服务校时。

每台复制本仓库到 `/home/snow/Snow_Statistics`，镜像摘要在 `lab/locks/images.env`。`lab/.env`（忽略）设置三节点 IP 与 LAB_MYSQL_PASSWORD、LAB_MYSQL_ROOT_PASSWORD、LAB_CDC_PASSWORD、LAB_GOVERNANCE_PASSWORD，不复用生产密码。渲染并分发 Hadoop 配置：

```sh
uv run python tools/render_hadoop.py --control CONTROL_IP --compute COMPUTE_IP
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f lab/compose.control.yaml --profile ingest up -d
```

控制节点：ingest（Kafka/MySQL/Connect）、batch（NameNode/RM/Hive）；计算节点：batch（DN/NM）、realtime（Flink）；分析节点：batch（第二 DN）、olap（Doris）、governance（Marquez）。按阶段启动，避免在同一预算内同时运行重算、实时、治理和 HA。实验端口只用于 NAT 网络和 SSH 转发，不能发布到公网。

历史离线验收使用 `batch` 4/4/2 GiB 和 `olap` 4/关闭/6 GiB；通过 `vmware_lab.py configure --profile ...` 在关机时切换。依赖锁、真实 YARN 作业、Doris 发布与 Airflow 操作见[离线闭环手册](offline-pipeline.md)。这些历史配置与 standard 容量上限都不能当作当前同时启动承诺，先执行最新容量检查。

## 模拟、CDC、离线及实时

1. `uv run snow-stats demo` 生成固定样例；运行 `tools/mysql_simulator.py --host CONTROL_IP` 向独立 snow_ops 写入事务。
2. `tools/register_cdc.py` 创建专用 CDC 用户、注册 Debezium。Connect 配置含凭据，只可通过私网访问；不把 REST 返回的配置写日志。
3. `tools/consume_cdc.py --bootstrap CONTROL_IP:9092` 保存 CDC 原始批次；每批持久化后才提交 Kafka offset。
4. `snow-stats sync` 读取 collector 私有 API，先归档后发 Kafka；`tools/archive_to_jsonl.py` 校验原始归档并转换为 Spark JSONL。
5. 将 JSONL 放入独立 HDFS ODS 路径。Spark `batch.py` 必须显式指定日期、共享截止时间和新 run-id；相同路径不会覆盖既有结果。`ops.py` 用归档 CDC 建立内容及工单模型。
6. 在 Doris 执行 `warehouse/doris/schema.sql`。`tools/maven_build.ps1` 用下载并校验的 Maven 构建 Java 11 字节码；在 Flink 1.20.3 Java 11 运行。配置 KAFKA_BOOTSTRAP、SNOW_SOURCE、DORIS_FE、DORIS_USER、DORIS_PASSWORD 和可选 SNOW_REPLAY_LANE。
7. 启用 Airflow DAG 前配置 `/opt/snow` 挂载及 Spark/Hadoop 路径。核心模型任务已自动记录 START/COMPLETE/FAIL；按[血缘手册](lineage.md)启用独立本地日志、导出及补发，一次尝试使用同一 run UUID。

组件镜像 digest 已固定不代表组合已通过集成测试。进入下一阶段前必须保存实测结果，检查 `docs/status.md`。

## 线上轻量部署

独立目录 `/var/lib/snow-statistics`，使用 `deploy/prepare-state.sh` 默认创建 512 MiB 文件系统，可为新卷显式选择 1 或 2 GiB；检测到已有数据、挂载或符号链接会拒绝格式化，已有 2 GiB 卷保持原容量和显式配置。新部署默认 256 MiB 内存、0.25 CPU，已通过实际小配置测试，见[资源手册](resources.md)。持久化挂载及重启顺序需要在部署候选中配置，确认 mountpoint 后才启动 Compose，避免写入挂载点底层磁盘。

`deploy/start-lite.sh` 拒绝在未挂载目录启动。部署候选将代码放在 `/opt/snow-statistics` 后，可安装 `deploy/snow-statistics-lite.service`；该服务依赖专用持久化挂载。容器使用 `on-failure:5`，不会随 Docker 重启绕过挂载门禁。先配置并验证该文件系统的持久化 mount unit/fstab，再启用本项目 systemd 服务；此仓库不会修改共享 Docker 服务的启动条件。

以两个不同随机令牌配置服务端完成日志和私有读取；`SNOW_STATE_DIR` 必须是已挂载的专用目录。镜像用 `deploy/lite.Dockerfile` 构建，只开放本机 8100。代理仅加入两条公开路由，私有读取经 SSH 隧道。访问统计请求和代理不记录 IP/URL，生产代理需设置有界限流。

共享线上主机仍存在宿主故障共同边界。先运行本地合成验收，生成候选资源/路由/CSP/隐私配置及回退清单，再按产品发布流程推广。


## 本轮补齐的按需实验

固定输入优化和 Iceberg DWD 恢复继续使用 scale 2/2/1 GiB，见 [优化](optimization.md)与[湖仓](lake.md)；HA 新增单分析 VM 3 GiB 的 `ha` profile，Kafka 与 ZooKeeper 分开运行，见[故障专题](ha.md)。本机配置不要求同时开启这些服务。最新冷态文件约 55.87 GiB，源于保留历史和退出 worktree；每次仍需动态检查 64 GiB / 35 GiB / 4 GiB 门禁。

停止注入已实际验证：`tools/run_scale_guarded.py --inject-stop-after-seconds 25` 会令当前实验失败并停止 driver、指定容器和三台 VM，保留状态。只在新的合成 attempt 演练，不用于无关任务或生产。

全部运行停止后可按[离线演示](demo.md)检查结果；[退出手册](retirement.md)包含实际移除候选和私有导出方式。HA 辅助工具只停止容器，阶段完成仍须显式停止 VM。
