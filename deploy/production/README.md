# 独立线上部署候选

本目录只拥有 Snow Statistics 的服务。业务蓝绿、业务 Caddy、现有 Tunnel 和业务数据库不属于安装范围。
`production_install.py plan` 生成带 SHA256 的安装清单；`install --expected-plan-sha256 ...` 仅安装清单中的 unit、默认关闭的日志配置并重读 systemd，**不创建文件系统、不启用 unit、不启动或切流**。程序拒绝非 `/opt/snow-statistics`、非 root 控制的代码、符号链接目标及已有不同内容。新版本升级应重新审查具体差异，不覆盖未知文件。

## 安装顺序

1. 重新读取宿主容量、现有业务颜色及构建身份。部署操作保留业务 root runner、蓝绿容器、挂载、密钥和所有历史。复制可核验的代码到 root 拥有且普通用户不可写的 `/opt/snow-statistics`；无需宿主 Python 包安装，日志工具仅依赖 Python 3.11 标准库。
2. 用现有 `deploy/prepare-state.sh` 在**新目录**创建 512 MiB 文件系统。先安装 `state.mount.in` 为 `systemd-escape --path --suffix=mount /var/lib/snow-statistics/state` 得到的名称，再校验 mount unit 与实际 mount 一致。已有挂载或数据会拒绝格式化。
3. 单独审查 collector 配置，默认 `SNOW_MODE=off`；server token、reader token 各自随机生成，root 私有文件，禁止写入文档或命令行。镜像使用已有 Python digest 和 frozen lock 构建，collector 的原 Compose 保持独立。
4. 安装并核对 systemd units 后，先启动挂载与 off collector。验证数据库位于配额挂载、端口仅 `127.0.0.1:8100`、新事件拒收、公开 v1/v2 为归档/不可用。私有读取从 SSH 本地转发访问 collector，不走公网入口。
5. 启动 edge 的 **gateway** 候选即可通过 `127.0.0.1:8110` 验证路由；请求 Host 为 `stats.xiaob.dev`。网关只向 collector 转发 `POST/OPTIONS /analytics/v1/events`、`GET/OPTIONS /analytics/public/v1/summary.json`、`GET/OPTIONS /analytics/public/v2/summary.json`。错误方法、health/private/docs 和任意其他路径均为 404。网关去除 Authorization、Cookie、IP 和 Referer 上游头，可信服务端完成仅走宿主回环端口。
6. 独立创建 `stats.xiaob.dev` Tunnel。`configure-tunnel --tunnel-id ... --credential-file ...` 只安装匹配的新私有凭据及固定 ingress；拒绝覆盖既有配置。它不修改 DNS。Tunnel 仅连接 edge 网络的 gateway，不连接 collector/业务网络；最终 fallback 为 404。必须另行验证 DNS、TLS、CORS 预检、无交互挑战及两站精确 CSP。
7. 两产品数据说明完成并发布前，日志转发保持 `SNOW_LOG_FORWARD_ENABLED=false`。启用时 `/etc/snow-statistics/server-token` 由 systemd `LoadCredential` 交给独立低权限用户；不用环境变量保存 token，不授予 Docker socket/group 或 sudo 权限。

新配置的 collector 上限 256 MiB/0.25 CPU；gateway 64 MiB/0.10 CPU；Tunnel 128 MiB/0.10 CPU；两个日志进程各 64 MiB/0.10 CPU。总上限约 576 MiB/0.65 CPU。collector quota 512 MiB，日志 spool 最多 4096 条且数据库 16 MiB（DELETE journal 短暂额外占用，最多约数据库大小）；各 Docker 日志另有轮转限制。不得把这些配置值当成真实峰值测量。

## 私有读取通路

`tools/private_access.py plan --public-key-file <独立公钥文件>` 输出精确计划；核对后在正式服务器 root 控制的 `/opt/snow-statistics` 下执行 `install --public-key-file <同一公钥文件> --expected-plan-sha256 <计划哈希>`。只接受全新的 `snow_stats_reader` 系统账户及本项目专属路径，已有配置不覆盖。不要把业务 root 私钥复制进虚拟机。

账户只能使用专属公钥进行本地 TCP 转发，目的地仅 `127.0.0.1:8100`；Shell、SFTP、远程转发、其他端口、代理转发和终端均关闭。`Match User` 仅作用于此账户；安装前后比较 root/deploy 的有效配置，语法验证成功才 reload SSH，保留现有连接。读取接口仍要求独立 reader token，公钥本身不能替代接口鉴权。

2026-09-13 实际验证：私有 status 携 token 为 200、无 token 为 401；Shell、SFTP、8110 端口和远程转发均被拒绝。collector 位点仍为 0，没有注入正式事件。Windows 私钥/已验证 host key 位于忽略的 `runtime/real/ssh/`，reader token 位于 `runtime/real/secrets/reader.token`。Linux 副本使用 0600，只在按需运行节点保存；日常停机保留，移除账号/配置属于明确的退出操作。

生产 SSH 账户的部署不改变业务容器；私有转发也不经过 Cloudflare 公网入口。实际验收回执位于 `runtime/production-candidate/private-access-acceptance.json`，不要把其中的主机或代际信息当作公开业务统计。

## 日志跟随及恢复

root reader 每轮仅执行固定 Docker `ps/inspect/logs`，只接受 `project-snow-public` 的 `public-api-blue` / `public-api-green` 标签，不读取容器环境或业务表。两色停止/排空中的容器也纳入最近 10 分钟重叠回读；单轮最多 4 个 API 容器、每容器 2000 行/2 MiB，命令及全轮均有截止时间。达到边界会增加 `incomplete_polls`，停止过久增加 `unobserved_seconds`，不能称为完整采集。

原始 Docker 行只在受限内存中短暂处理。进入独立 SQLite 的只有事件 v1 白名单投影，事件 ID 与原日志适配器一致；不保存错误文本、provider、模型、IP、聊天或工具输入。记录按原日志发生时间 24 小时到期，重放不延长。SQLite 使用 `secure_delete`，没有 WAL。容量退休数、到期退休数为全 spool 的实际记录数，**不等于未投递损失数**；源窗口与截断无法确定的缺口独立记录。

转发器通过组权限 UNIX socket 读取最多 50 条，不接触 Docker。仅 HTTP 202 才计为已确认；超时/连接错误/401/429/503 保留发送游标供下一轮重放。409/413/415/422 计入拒绝数后推进，避免坏批次无限阻塞；202 返回后崩溃允许重放，collector 按事件 ID 去重。reader 超出 24 小时/容量后的退休可能形成数据缺口，游标状态同时保留 reader 的覆盖和退休指标。日志只代表产生完成日志的生成请求，不包括生成之前的认证或排队拒绝。

reader 的 `/var/lib/snow-statistics-reader/spool.db` 和 forwarder 的 `/var/lib/snow-statistics-forwarder/cursor.json` 都属于本项目；状态目录独立、受限。停止服务不删除它们，转发器禁用时不会发网络请求。运行时安全投影 spool 的到期/容量退休属于既定保留期维护，不得清理业务日志、容器或其他项目数据。

## 验证与发布门禁

- Python 合成测试覆盖白名单、事件 v1 相等、重启重放、重复、24 小时到期、容量边界、401/503、确认丢失、蓝绿标签限制及明确缺口。
- 2026-09-13 已完成独立服务器候选安装：collector 保持 `SNOW_MODE=off`，gateway 仅绑定回环端口；正式库事件数为 0。网关实测 v1/v2 汇总为归档状态、写入为 503、私有及非白名单路径和方法为 404、许可 Origin 预检为 200、其他 Origin 预检为 400。公网 DNS/TLS/Tunnel 尚未验收，不能把回环测试视为公网发布完成。
- 同日使用独立网络、临时内存数据库和合成 collector 完成 HTTP 入库、重复事件去重、网关剥离 Authorization、可信服务端完成直接入库、私有读取封闭及 collector 停止后 gateway 返回 502 的验证。公开汇总排除合成事件。测试容器已停止，未将合成数据打入正式 real 数据库。
- 已验证持久 mount unit 安装及启用、collector 停止时缺 mount 拒绝启动、恢复挂载后启动，以及 off SQLite 在线备份回读完整性。备份为初始空库；真实数据备份仍必须按保留期管理。新环境安装应重做这些门禁，不能全局重启 Docker。
- 实测回执和源文件 SHA256 清单位于忽略的 `runtime/production-candidate/`；远端对应私有回执位于 `/root/snow-statistics-candidates/`。collector 镜像保留冻结源包的标识，运行时配置另有逐文件清单。Caddy 镜像内二进制具有文件 capability，实际容器需要 `NET_BIND_SERVICE` 才能执行；其余 capability 仍移除。`/data`、`/config` 使用独立 tmpfs，避免新建匿名持久卷。初次失败尝试的容器挂载清单单独留证，不做全局清理。
- 更改数据同意前的缓存、撤销行为、公开统计页、保留期跨 real 数仓副本及生产晋级仍由各自候选验收；此安装器不会绕过产品回执。

配置语义依据 [Caddy handle](https://caddyserver.com/docs/caddyfile/directives/handle)、[reverse_proxy](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy) 和 [Cloudflare Tunnel 本地配置](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/local-management/configuration-file/)。
