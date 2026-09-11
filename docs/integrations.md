# 业务仓库接入

业务适配位于各自 `codex/snow-statistics` 分支，修改集中且可单独回退。

当前审查入口：[MyWebsite #2](https://github.com/X1aoB/MyWebsite/pull/2)、[Project_Snow #59](https://github.com/X1aoB/Project_Snow/pull/59)。两者都是默认关闭的草稿候选；创建 PR 不启用生产采集。

MyWebsite：复制 `analytics.mjs`、`bootstrap.mjs`、`public-summary.mjs` 到自己的静态目录；BaseLayout 添加单个可删除模块入口；统计页运行时读汇总。页脚入口由 `PUBLIC_STATISTICS_PAGE_ENABLED=true` 控制，构建不请求统计 API。

Project_Snow：自己的静态 statistics 目录 + 单个初始化入口；聊天提交点通过可选回调只通知 request_id，回调仅在启用并同意后注册，关闭时不创建统计事件。不读取请求正文，不修改 wire API、签名、IndexedDB 或业务表。角色点击从已有 data-character 监听。后端完成事件通过外部日志适配，不加入生成路径。

静态 config.mjs 默认 enabled=false 且 endpoint 为空。真实开启前：

1. 生成网站公开页面路径/角色允许列表，同时配置 collector；不得导出草稿、非公开角色或私有反馈。
2. 配置独立 HTTPS 采集地址、MyWebsite summaryEndpoint，并将**该具体 origin**加入各产品 CSP connect-src；禁止使用通配符。
3. 检查隐私文案；用户可选择允许访问统计并随时撤销，服务质量使用既有脱敏日志。
4. 测试统计超时、503、满盘时页面/聊天仍可用，开启/关闭不改业务快照。
5. 两个业务项目独立构建、测试、提交、发布；运行和 CI 不需要 Snow_Statistics checkout。

仓库内适配器为源版本，各产品持有副本且没有运行时跨仓库引用。更新时比对文件 SHA256 与事件契约版本，不用子模块强制产品依赖实验仓库。

生产推广遵循 Project_Snow/AGENTS.md：在具体候选验收后手动推广；本任务的实现与提交不代表生产采集已经开启。
