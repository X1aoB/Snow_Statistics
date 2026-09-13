# 正式接入实施与验收记录

此文记录 2026-09-13 正式接入工作。历史数仓报告继续表示合成实验；`source=real` 的代码分支测试仍明确使用隔离合成样例，不能写成真实用户规模。

## 当前上线边界

统计服务已在现有服务器独立安装，保持 `SNOW_MODE=off`。独立 512 MiB ext4 状态挂载已启用；collector 与网关运行，网关只监听宿主 `127.0.0.1:8110`。日志读取、转发、正式采集和独立 Tunnel 尚未启用，业务 Tunnel 未改变。

已验证安装镜像：`sha256:cd5e3c2f3aa6ebe35c7e765de0339e1581d25d7f857cad288c56d0aa8f91dd50`。冻结 collector 源包：`8089cf1b59f22fbdd44b15ceb543b0b8994661f44c4134a55eae8cfe3173d7f8`。这是实际安装身份，不代表此后所有本地实验代码已经发布。

实际检查：off 时公开 v1/v2 为归档，采集返回 503；网关拒绝私有事件、健康、文档及额外路径；精确 CORS 预检通过、不允许的来源被拒绝；在线备份回读完整；缺失持久挂载拒绝启动。独立 synthetic HTTP collector 验证入库去重、Authorization 剥离、私有路由封闭与故障，不向正式库灌入测试数据。正式库验收时事件数为 0。

私有技术回执保存在忽略目录 `runtime/production-candidate/`，包括 `off-acceptance.json`、`synthetic-acceptance.json`、`final-runtime.json`、`installed-source-manifest.json`。这些文件不进入公开仓库。部署步骤与具体资源见 [production README](../deploy/production/README.md)。

## 用户可见功能

- 两站访问统计需主动同意；独立命名空间保存选择，支持到期、GPC/DNT、存储不可用、跨标签页撤销、清空队列及取消在途请求。关闭不修改业务存储。
- 非阻断提示在 320px 和 1440px 实际 Chromium 中检查；个人站配合现有中英文。公开路径/角色列表与实际候选目录一致。
- 小吉完成日志使用专用白名单 INFO logger，不启用全局详细日志。质量统计分母为“已记录的生成请求”，不表示所有 HTTP 或所有聊天尝试。
- summary v2 按访问、质量、热度表达 `published/suppressed/pending/empty`，隐藏值为 null；旧 v1 整行保护，私有精确汇总与位点状态需要 reader token。
- 香港日期 D 在 D+3 00:15 起按固定截止时间冻结公开。访问/热度按有效匿名标识门槛 10；质量按请求量及成功、失败小计数门槛整组判断。公开状态 26 小时过期，长时间开页重新判断；失败不伪造刷新或零值。

产品分支：[MyWebsite #2](https://github.com/X1aoB/MyWebsite/pull/2)、[Project_Snow #59](https://github.com/X1aoB/Project_Snow/pull/59)。个人站 main 合并会生产发布；小吉需要精确候选回执和人工晋级，候选 CI 不能代签。

## 实際引擎验收：隔离合成输入、真实代码分支

VM 使用 scale 2048/2048/1024 MiB，control / compute / analysis；Kafka 捕获阶段与 Spark 阶段切换运行。保留 64 GiB 项目、35 GiB 宿主空闲、4 GiB 可用 RAM 门禁。

| 项目 | 实际结果 |
|---|---|
| 隔离收集与同步 | 140 条合成样例，经 loopback collector → 校验归档 → `snow.real.fixture_prod01.events.v1`；正式请求 0 |
| Kafka / HDFS | 一个输入分区，offset 140；HDFS 校验后才 ACK。v2 清单将位点头与有效批次窗口分开 |
| Spark 日指标 | YARN `application_1789302517038_0001`；140 有效、0 重复/隔离，6 行日指标与独立基准一致 |
| Spark 行为 | YARN `application_1789302517038_0002`；44 会话、14 转化，5 组结果逐行与基准一致 |
| 留存 | 2 个分群的 D1 因覆盖不足为 null；D7 尚未成熟。没有倒填覆盖时间或称为自然新增用户 |
| Iceberg | YARN `application_1789302517038_0003`；daily/session_daily/retention/funnel 共 6/6/2/3 行，当前和历史快照全量读回一致；新增字段验证通过 |
| ODS 到期 | 仅该 fixture 模拟 7 日到期，实际删除 HDFS 旧批次与旧快照，读回旧快照不可达；offset 140 及 head 保留，新窗口 0 批次 |
| 私有看板 | AppTest 用同组 fixture 检查真实分支 4 个表，覆盖不足显示原因；读取前清理受管聚合副本 |

这轮 Spark 没有开启 Hive 注册；Iceberg 没有声明 Hive 或列级血缘。新真实 Kafka/Doris/Flink epoch 的独立配置与生命周期需单独实测，不能用上述离线成绩替代实时恢复和性能验收。真实生产 P95 尚无证据。

本轮私有回执：`runtime/production-acceptance/{daily,behavior,lake-receipt,ack,lake-cleanup,ods-expiry-acceptance}.json`。用于测试的合成标识留在忽略目录，不进入学习手册或公开 API。

## 生命周期与运行方式

见 [真实生命周期操作](real-lifecycle.md)。原始 7 天、辅助 30 天、聚合 90 天均依据原始时间。私有同步先绑定实例代际和位点；HTTP 410、源重建、未聚合输入丢失或保留期缺口会停止并生成元数据缺口回执。

显式新起点恢复使用 `snow-stats rebase-sync --help`，必须填写新目录、新 lane、当前合法位点与简短原因。旧日志保留，新通道不能伪装成连续历史；不会自动从 earliest 跳过缺失。

私有发布保存受 `RealLifecycle` 管理的不可变汇总文件，`real-latest.json` 仅保存位置、校验和、到期时间；看板每次读取先清理再校验。停止并不全删历史；关机期间不能即时物理删除，开机先清理再开放读取。

## 后续正式门禁

独立 Tunnel 与 DNS 配置 → 两产品精确候选测试/发布 → 人工小吉晋级 → 启用采集/质量转发 → 公网实际版本、同意行为、私有隔离与数据流回归。失败关闭相应统计开关或回滚独立发布。生产 real 数据、真实实时恢复、新鲜度和运行样本不足项目必须单列。

公网回归通过后才移交“规划小吉终端宣传推广”。素材使用已验证版本及真实操作，公开样本不足就录制真实等待状态，不伪造统计。

## 学习手册

独立目录：`C:/Users/25685/Desktop/简历及相关材料/Snow_Statistics数据开发学习手册/`。38 个正文专题、总目录与校验记录；SQL 练习和链接已检查。目录、部署、真实同步、发布章节最终仍须按正式回执校准。其他简历材料未覆盖。
