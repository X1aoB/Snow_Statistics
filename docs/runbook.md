# 本地运行与资源门禁

当前已安装完整离线和治理镜像，小样本计算优先选择 `ods-compact`（control/compute/analysis：3.5/3/1 GiB），仍执行 `--reserve-mib 1024` 作业前检查。Marquez 使用独立 `governance` 阶段（分析机 2 GiB，计算服务停止）。这些是小样本验收配置，扩样须再次测量。详见[血缘与资源切换](lineage.md)。

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

生成三个仅属于本项目的 NAT Linux 节点：control 6 GiB/2 vCPU、compute 6 GiB/4 vCPU、analysis 最高 10 GiB/4 vCPU；薄置备系统盘分别 18/18/24 GiB。Doris 镜像实际解压使分析节点 18 GiB 初始盘只剩约 2.9 GiB，因此为该节点增加逻辑磁盘容量；整体物理文件预算仍为 60 GiB。Ubuntu 镜像固定 release-20260826 并验证 SHA256。seed、SSH 凭据、VMX、磁盘都在忽略的 `runtime/vmware`。从 VMware 界面打开各节点的 VMX 即可手动管理。

启动会检查宿主空闲磁盘至少 35 GiB、整个项目文件预算 60 GiB（含本次启动可能新增的 .vmem），并保留 4 GiB 当前可用内存。文件长度是保守预算近似，不等于底层精确分配量；外部共享 uv/Maven 缓存不纳入本目录计数，扩样前还需核查依赖缓存和宿主空闲空间。薄置备逻辑容量与物理实占不同，不能只看输入文件大小。

首次 SSH 主机公钥在本地 seed 中生成，以 `HostKeyAlias=snow-control` 等名称记录在 `runtime/vmware/known_hosts`，使用 `StrictHostKeyChecking=yes`。IP 可从 VMware NAT DHCP 租约按本节点 MAC 查询；不要关闭全局 SSH 校验。初始化脚本 `tools/bootstrap_guest.sh` 只接受三个指定实验主机名。

首次初始化后，在没有运行容器的节点执行 `uv run python tools/guest_clock.py --node snow-control --ip <节点IP> --set`，其余节点同理。校正前检查宿主 UTC 与 HTTPS Date 大致一致，再同步系统时间和 UTC RTC；本地 NAT 中未收到 NTP 包，因此该实验配置使用 VMware Tools 跟随宿主。去掉 `--set` 是只读检查，主机与虚拟机偏差达到 2 秒会拒绝通过。每次开始实时新鲜度实验、恢复快照或重新启动后检查；HTTPS Date 只用于排除大幅错误，不提供精密 NTP 证明。

首轮曾发现 guest UTC 快 8 小时，不能仅凭 timedatectl 的 synchronized 标志判断时钟正确。Linux 时间源与 RTC 原则参考 [Broadcom Linux timekeeping](https://knowledge.broadcom.com/external/article?legacyId=1006427)，周期同步及 RTC 偏移行为参考 [VMware time synchronization](https://knowledge.broadcom.com/external/article?legacyId=1189)。若之后改用可达的 NTP/Chrony，应关闭 VMware Tools 周期同步，避免同时由两个服务校时。

每台复制本仓库到 `/home/snow/Snow_Statistics`，镜像摘要在 `lab/locks/images.env`。`lab/.env`（忽略）设置三节点 IP 与 LAB_MYSQL_PASSWORD、LAB_MYSQL_ROOT_PASSWORD、LAB_CDC_PASSWORD、LAB_GOVERNANCE_PASSWORD，不复用生产密码。渲染并分发 Hadoop 配置：

```sh
uv run python tools/render_hadoop.py --control CONTROL_IP --compute COMPUTE_IP
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f lab/compose.control.yaml --profile ingest up -d
```

控制节点：ingest（Kafka/MySQL/Connect）、batch（NameNode/RM/Hive）；计算节点：batch（DN/NM）、realtime（Flink）；分析节点：batch（第二 DN）、olap（Doris）、governance（Marquez）。按阶段启动，避免在同一预算内同时运行重算、实时、治理和 HA。实验端口只用于 NAT 网络和 SSH 转发，不能发布到公网。

离线验收现使用 `batch` 4/4/2 GiB 和 `olap` 4/关闭/6 GiB；通过 `vmware_lab.py configure --profile ...` 在关机时切换。小配置、依赖锁、真实 YARN 作业、Doris 发布与 Airflow 操作以[离线闭环手册](offline-pipeline.md)为准。原始 standard 是容量上限，不能把它当作同时启动承诺。

## 模拟、CDC、离线及实时

1. `uv run snow-stats demo` 生成固定样例；运行 `tools/mysql_simulator.py --host CONTROL_IP` 向独立 snow_ops 写入事务。
2. `tools/register_cdc.py` 创建专用 CDC 用户、注册 Debezium。Connect 配置含凭据，只可通过私网访问；不把 REST 返回的配置写日志。
3. `tools/consume_cdc.py --bootstrap CONTROL_IP:9092` 保存 CDC 原始批次；每批持久化后才提交 Kafka offset。
4. `snow-stats sync` 读取 collector 私有 API，先归档后发 Kafka；`tools/archive_to_jsonl.py` 校验原始归档并转换为 Spark JSONL。
5. 将 JSONL 放入独立 HDFS ODS 路径。Spark `batch.py` 必须显式指定日期、共享截止时间和新 run-id；相同路径不会覆盖既有结果。`ops.py` 用归档 CDC 建立内容及工单模型。
6. 在 Doris 执行 `warehouse/doris/schema.sql`。`tools/maven_build.ps1` 用下载并校验的 Maven 构建 Java 11 字节码；在 Flink 1.20.3 Java 11 运行。配置 KAFKA_BOOTSTRAP、SNOW_SOURCE、DORIS_FE、DORIS_USER、DORIS_PASSWORD 和可选 SNOW_REPLAY_LANE。
7. 启用 Airflow DAG 前配置 `/opt/snow` 挂载及 Spark/Hadoop 路径。按实际任务 START/COMPLETE/FAIL 调用 `governance/lineage.py`；一条任务生命周期使用同一 run UUID。

组件镜像 digest 已固定不代表组合已通过集成测试。进入下一阶段前必须保存实测结果，检查 `docs/status.md`。

## 线上轻量部署

独立目录 `/var/lib/snow-statistics`，使用 `deploy/prepare-state.sh` 创建 2 GiB 文件系统；检测到已有数据会拒绝格式化。持久化挂载及重启顺序需要在部署候选中配置，确认 mountpoint 后才启动 Compose，避免写入挂载点底层磁盘。

`deploy/start-lite.sh` 拒绝在未挂载目录启动。部署候选将代码放在 `/opt/snow-statistics` 后，可安装 `deploy/snow-statistics-lite.service`；该服务依赖专用持久化挂载。容器使用 `on-failure:5`，不会随 Docker 重启绕过挂载门禁。先配置并验证该文件系统的持久化 mount unit/fstab，再启用本项目 systemd 服务；此仓库不会修改共享 Docker 服务的启动条件。

以两个不同随机令牌配置服务端完成日志和私有读取；`SNOW_STATE_DIR` 必须是已挂载的专用目录。镜像用 `deploy/lite.Dockerfile` 构建，只开放本机 8100。代理仅加入两条公开路由，私有读取经 SSH 隧道。访问统计请求和代理不记录 IP/URL，生产代理需设置有界限流。

共享线上主机仍存在宿主故障共同边界。先运行本地合成验收，生成候选资源/路由/CSP/隐私配置及回退清单，再按产品发布流程推广。
