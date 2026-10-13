# 保留停止失败窗口的离线兼容候选

2026-10-13 的候选专门处理这一已知组合：`real-prod-03` 在首次正式同步前失败，仍保留原存储但没有可恢复的完整暂停记录；`real-prod-04` 已经通过实际暂停，需要正常进入离线阶段。原 `real_lab.py` 会要求所有未退役 writer 都处于 paused，因此默认仍拒绝这个组合。

本候选没有改动 12 个冻结 writer 文件。新增 [real_offline_preserved.py](../src/snow_statistics/real_offline_preserved.py) 只允许这一对明确的 epoch，不能传入任意未暂停窗口。它不把 03 写成 paused，不创建退休或恢复记录，不读取或删除 03 的 Kafka 数据，不修改原始到期时间，也不签发生命周期许可。

## 验证边界

已完成[合成测试](../tests/test_real_offline_preserved.py)及完整回归。2026-10-13 05:40—05:44 UTC将受测工作区补丁逐台安装三节点，旧文件备份、摘要、CLI导入及12个冻结writer不变读回通过。05:45:26—05:50:48 UTC实际执行 `start-offline → status → stop-offline`，全部预期服务身份、资源和OOM检查通过，最终三台VM均已软关。详见[新入口实际证据](evidence/offline-followup-20261013.json)。这轮没有执行闭日计算、Hive/Iceberg或重新读取HDFS内容；此前HDFS落地有独立证据。它仍是未新签名的工作区版本，不是新的签名发布。

部署候选前，Windows 和三台 Linux checkout 必须拥有相同版本的 `real_offline_small.py`、新增 `real_offline_preserved.py` 及两个对应 CLI。节点 CLI 会导入新选择器，不能只更新 analysis，也不能把此前安装的 epoch 清理补丁或 prefix helper 视为本候选已经安装。部署需记录实际文件摘要，并确认 frozen writer 摘要仍与现有登记一致。

## 读取实际范围摘要

先按既有流程完成 04 实际暂停并确认全部引擎容器停止。此只读观察只需要 analysis VM；不需要为了获得摘要启动另两台。命令在 **analysis Linux VM** 执行，工作目录为 `/home/snow/Snow_Statistics`：

```bash
.venv/bin/python tools/real_offline_preserved.py \
  --config runtime/real/config/production-real-prod-04.json inspect
```

输出的 `state_sha256` 来自实际观察，不是手填成功标志。观察会核验：

- 03、04 的原 manifest、JAR、writer 源文件摘要、原始登记、期限以及全部实际容器/卷身份。每个未退役 scope 的引擎都必须停止。
- 03 没有恢复链，source 与初始登记一致、cursor 仍等于原 rebase 起点，且没有 data、pending、gap 或 ODS 输入目录。这里只证明本地输入链缺失；输出明确声明 `kafka_empty_claimed=false`。
- 04 及其他任何未退役 writer 都完成真实 paused 链。其他退役窗口需要原 manifest 对应的退休记录，且 Docker 对象实际不存在；未知或不完整 scope 不能跳过。

观察不创建缺失目录、不写入事件或准入文件。03 元数据、存储或期限变化会使摘要失配；到期时不得人为延长期限继续沿用。04 在同一原始登记下完成新的合法暂停仍需重新实际校验，但不因新的暂停账本末端改变这一原始范围摘要。

## 分阶段使用

取得实际摘要并按原流程软关 analysis，确认三台 VM 均已关闭后，在 **Windows PowerShell** 的项目目录中执行。以下 `<实际观察摘要>` 必须替换成上述实际输出的 64 位十六进制值；没有该值时不要执行：

```powershell
& ./.venv/Scripts/python.exe tools/real_offline_small.py --config runtime/real/config/production-real-prod-04.json --profile real-small-1792 --preserve-stopped-epoch real-prod-03 --preserved-state-sha256 <实际观察摘要> start-offline
```

后续 `prepare`、`cleanup`、`permit`、`stage-compute`、`daily`、`behavior` 等继续逐阶段调用同一个入口并携带这两个选项。需要 `run-id` 的阶段仍必须提供已经实际登记的公共 job；合法闭日、coverage、源位点、已确认 ODS、质量校验和计算许可一个都不能省略。`land` 继续走原公共函数，已完成 ACK 的输入不会重新冒充首次 capture。`--describe` 只描述选择，不产生实际观察摘要或许可。

所有资源门禁继续生效：项目预算目前 72 GiB、运行停止线 71.75 GiB、宿主至少 35 GiB 空闲磁盘及 4 GiB 可用 RAM，另加既有阶段预留。1792 是显式选项，默认仍为1920。用户后续授权的实际容量调整需要修改共同配置并记录，不能只修改一个入口的数值。

## 实际启动与精确收尾

只有 analysis 的 `node-start-offline` 会选择新子进程。它再次验证实际保留范围和全部停止状态，只能启动已经存在、归属正确、镜像与挂载符合原配置的 DataNode 完整 Docker ID；不会创建新容器或卷。Hadoop 声明的 `/data` 匿名父卷仍须实际核验镜像声明、唯一使用者及创建身份。其他节点和业务计算继续调用原冻结入口。

新子进程必须属于实际存活的监督父进程。启动之前，它在已有 attempt 目录原子持久化 `preserved-service.json`，将原 DataNode ID 和身份绑定到 worker 摘要。心跳丢失、子进程错误、取消和超时仍走原进程组收尾；失败 attempt 再依据该记录停止精确原 ID 并实际读回。取消不依赖原 epoch 仍在有效期内；期限到达不能阻止停止已登记服务。连接失败、身份替换或停止失败会保留失败记录，不能记为收尾成功。

正常的 `start-offline` 保留已启动的离线服务供后续阶段使用。取消已经完成的启动 attempt 不会误停后续工作；正常结束仍执行 `stop-offline`。失败后的精确停止回执使重复取消幂等，避免误停后来重新准入的同一个 DataNode。停止不删除存储。

03 到达原期限并完成正常物理退役后，应先重新核验范围，撤去这两个兼容选项，恢复默认入口。不要为继续沿用该候选而重建03、复制旧登记或修改期限。
