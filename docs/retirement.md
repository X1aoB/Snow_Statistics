# 精简与移除手册

## 保留基础统计

先检查最近一次已确认同步位点，再停止本地消费者和实验 Compose/VM。轻量服务继续运行，同一数据库和公开接口维持基础指标。网站不改数据源，不需要导出 Doris 再迁移。

`uv run python tools/retirement_review.py lite` 生成审阅范围；不会删除任何资源。

## 完全关闭

1. 两产品将采集 enabled 改为 false，分别发布；撤销按钮可主动清除当前浏览器统计标识，已离线浏览器的本地标识不能由服务器即时清除。
2. 将已接收事件聚合到当前接受位点，导出 summary JSON 并记录生成时间/截止范围。
3. 切换 SNOW_MODE=off 拒收事件，确认 summary 标记 archived，或者移除公开统计页和导航入口。
4. 停止日志适配器、同步任务和 collector，保留数据库。停用不自动执行保留期清理或删除。

## 删除

先导出公开汇总、合成实验结果、依赖锁和必要运行证据。根据 `deploy/resources.json` 审核明确的容器、卷、路由、目录和 VMX 清单，只删除所属本项目且已经停用的资源。

禁止共享宿主全局 prune；禁止删除业务数据库、发布包或回退版本；禁止使用未检查绝对路径的递归删除。VMware 运行状态必须在删除 VM 文件前确认已停止。卷数据删除、专用镜像文件卸载及删除都必须作为独立明确操作。

移除业务适配器时删除其脚本入口和复制目录，回退 request_id 可选回调；统计页选择移除或明确归档。原有聊天 API、签名状态、Cookie、IndexedDB 及业务存储兼容保持不变。

`uv run python tools/retirement_review.py delete` 仅生成范围，故意不提供自动删除指令。保留 GitHub 仓库可继续用合成数据复现实验。

## 已执行的退出演练

2026-09-12 在三台 VM 全部关闭时，实际启动私有回环 HTTP 进程，依次 full → lite → off。full 接收页面事件及外部日志转发的可信完成事件；重复日志没有重复计数。切到 lite 后同一页面新事件使 PV 从 1 增至 2、UV 保持 1；off 返回 503 拒收，公开汇总标为 archived，保留请求 1 / 成功 1 和原统计日期。

关闭前通过 SQLite backup API 导出一致副本，并转换为独立的 DELETE journal 文件，公开汇总另外导出 JSON。停止进程后，数据库 integrity_check 为 ok，3 条事件与导出逐项一致。数据库和导出均保留；SQLite 包含明细，只保存在私有目录，不能与 summary JSON 一起公开。

实际日志跟随使用独立 subprocess + stdin：丢弃畸形与 20 万字符长行、正确读取其后的完成日志，白名单排除合成的聊天/IP 附加字段；缺省关闭不发送，采集端不可用时有限重试后读取器仍能退出。该实验没有连接业务 Docker 日志或启用生产日志转发。

复现：`uv run python tools/smoke_retirement.py --output runtime/retirement/lifecycle04`，选择新目录。普通退出通过私有 supervisor 管道让 ASGI lifespan 收尾；没有开放远程停机 API。第一轮生命周期断言通过但误把临时 WAL/SHM 纳入导出哈希；第二轮 Windows 硬终止后重开出现磁盘 I/O 错误。失败目录保留，最终第三轮改为正常停机、显式关闭连接及独立快照后全部通过。容器硬故障恢复与满盘另见 [轻量资源验收](resources.md)。

两个产品各建立了专用 detached worktree，逐个反向应用统计适配提交，完整移除入口、适配脚本、隐私文案和统计页。运行后 tracked tree 分别精确等于接入前 d612e44 / f3f7397，业务 API、签名、数据库及浏览器业务存储代码均恢复为原内容。原草稿候选 710ca3f / dbe0c5f 没有改动或推广。

| 移除候选 | 实际检查 |
|---|---|
| MyWebsite | 自己的 npm lock 独立安装；59 测试通过；78 Astro 文件 0 错误/警告；构建 31 页面 |
| Project_Snow | 自己的 npm lock 独立安装；类型检查、构建、6 前端测试；4 个真实 Chrome 聊天/存储/恢复回归；7 发布包检查通过，2 个浏览器包测试未启用 |

浏览器使用本地假后端，无付费模型请求；进程没有统计凭据或数仓依赖。产品原有 CI 只检出各自仓库；本次移除 worktree 的检查没有导入 Snow_Statistics。这里没有把本机仍存在统计仓库描述为从磁盘物理移除了它。

具体基线 SHA、移除 patch 哈希、结果及导出哈希见 [retirement.json](evidence/retirement.json)。宿主 `runtime/retirement` 保存可审阅 patch、独立构建和私有导出；`deploy/resources.json` 登记这些资源。实际完成了业务适配移除及服务停止，没有演练删除历史 VM/数据库。彻底删除仍须依据真实保留需求明确执行，停用不会触发删除。
