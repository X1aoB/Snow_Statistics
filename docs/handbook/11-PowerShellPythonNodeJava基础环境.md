# 11 PowerShell、Python、Node与Java基础环境

## 四种环境先分清

Windows PowerShell是你控制本机的shell；Linux bash是VM或线上主机内的shell；Python负责轻量服务、控制脚本和模拟器；Java负责Flink作业及大数据引擎。把Linux的export原样粘到PowerShell不会设置环境变量；把Windows路径直接交给Linux容器也不会自动映射。

```text
Windows PowerShell → uv/Python控制工具 → SSH → Linux bash → Docker/Java进程
                 → Node测试/个人站构建
```

项目Python要求3.12，uv管理锁定依赖；Node要求22以上用于浏览器模块测试与个人站构建。Flink目标字节码为Java11；不同服务自己的JDK随镜像隔离，不能用“我电脑上装了Java”证明容器JDK一致。

## 从零准备与已有环境的分界

当前电脑、三台 VM 和正式服务器已经有各自的环境。日常使用从下一节的版本检查开始，不重新克隆、不重新生成密钥、不重建 VM。下面的新环境步骤只适用于尚未安装工具的电脑；下载和安装会占用网络、磁盘，有的 Windows 安装器会请求管理员权限。

1. 安装 PowerShell 7，打开它而不是把所有示例都粘到旧版 Windows PowerShell 5.1。可从 Microsoft 官方安装页选择 MSI 或 WinGet；本书的 `$PSNativeCommandUseErrorActionPreference` 行为以 PowerShell 7 为准。[官方安装说明](https://learn.microsoft.com/en-us/powershell/scripting/install/install-powershell-on-windows)
2. 安装 Git for Windows，在安装器中保留允许命令行使用 Git 的选项；安装结束后重开终端。Git 获取源码，SSH/SCP 用来连接 VM；它们不负责启动 Kafka。[Git 官方下载页](https://git-scm.com/install/windows)
3. 安装 uv。已有 WinGet 时可在 PowerShell 运行下面的命令；否则从官方安装页取得安装器。本次命令参数核验使用本机 uv 0.11.28，项目依赖仍以 `uv.lock` 为准。[uv 官方安装说明](https://docs.astral.sh/uv/getting-started/installation/)
4. 从 Node.js 官方下载页选 Windows x64 的 Node 22 系列或项目已验版本并安装。先满足本项目 `22+` 的要求，再按实际产品构建结果确认兼容，不能把随意升级到最新大版本当成验证。[Node.js 官方下载页](https://nodejs.org/en/download)

```powershell
# Windows PowerShell；只在尚未安装 uv 的新环境执行
winget install --id=astral-sh.uv -e
```

安装完成后重开 PowerShell，运行 `Get-Command git, uv, node, ssh, scp`。预期每项都有程序路径；缺失时先解决安装或 PATH，再继续。这里不修改全局执行策略、不关闭 SSH 校验，也不在正式服务器安装开发工具。

新电脑获取源码时，在你选定的父目录运行以下命令。已有 `Snow_Statistics` 目录时停止，先检查它是否就是当前项目，不覆盖或重新初始化它。

```powershell
# 新环境示例，从打算存放仓库的父目录执行
if (Test-Path -LiteralPath 'Snow_Statistics') { throw '目录已存在，请转到已有环境检查' }
git clone https://github.com/X1aoB/Snow_Statistics.git
if ($LASTEXITCODE -ne 0) { throw 'Git 下载失败，停止后续步骤' }
Set-Location -LiteralPath 'Snow_Statistics'
git rev-parse HEAD
uv python install 3.12
uv sync --frozen --extra dev --extra lab
```

`uv python install 3.12`可能下载解释器；`uv sync`建立本仓库 `.venv` 并按锁安装包。记下实际提交 SHA、Python 补丁版本和 uv 版本。它们是复现条件，不是正式发布收据。若 clone 需要你登录 GitHub，使用正常 Git 凭据流程，不把访问 token 写进 URL 或手册。

## 第一次安装后检查

```powershell
Set-Location -LiteralPath (git rev-parse --show-toplevel)
uv --version
node --version
git --version
uv sync --frozen --extra dev --extra lab
uv run python --version
uv run python -c "import snow_statistics; print(snow_statistics.__file__)"
uv run pytest -q
node --test adapters/browser/*.test.mjs
```

`--frozen`要求遵循已有uv.lock，避免临时升级改变实验条件。`--extra dev`安装pytest/ruff；`--extra lab`安装Kafka/MySQL/Streamlit等实验客户端，不会自动启动对应服务。Python包安装与集群启动是两件事。

预期 Python 为 3.12.x，包路径在本仓库 `src/snow_statistics`，测试没有失败。测试数随源码版本变化，不把旧文档中的固定数量当验收条件。测试失败时停止使用该版本推进生产操作，保存失败名称及提交 SHA；不要删掉失败测试以获取绿色结果。

## Java 编译器与容器 JDK 分开准备

本项目的 Windows 构建脚本 `tools/maven_build.ps1` 会下载固定 Maven 3.9.9 并核验 SHA-512，再把 Flink 作业编译为 Java 11 字节码。可设置 `JAVA_HOME` 指向所安装的 JDK 11 或更新版本。

```powershell
# Windows仓库根目录；只读检查，不下载或构建
javac -version
```

预期路径存在并输出编译器版本。新电脑不存在该路径时，先准备能够支持 `--release 11` 的 JDK，明确安装位置，再经代码检查调整构建脚本的编译器路径；不能把一个仅有 JRE 的目录填进去，也不要为适配脚本覆盖其他项目的 Java 安装。本书没有把当前机器的绝对路径当成所有电脑必然存在的路径。需要构建时才运行：

```powershell
# 会下载Maven、解析锁定pom并执行Java测试/打包；此时不启动VM
& .\tools\maven_build.ps1
```

构建失败须读退出码和 Maven 错误。成功后核对 `warehouse/flink/target/` 中本版本 JAR 及哈希；真实 writer 首次登记后冻结该 JAR，同一个 epoch 不能边运行边替换它。容器里的 Java 11 与宿主编译器是两个执行环境，分别验证。

## Linux VM 为什么还要安装本项目自身

宿主 `.venv` 是 Windows 可执行文件，不能复制到 Linux 使用。新 VM 的 Python 环境、源码上传与 Docker 安装在[第12章](12-VMware网络资源与日常启停.md)逐步完成。VM 中只装 Kafka/MySQL 等依赖还不够：统一入口 `tools/real_lab.py` 会导入 `snow_statistics`，项目自身也必须安装到该 VM 的 `.venv`。

2026-09-19 的实际准备发现过 `ModuleNotFoundError: snow_statistics`；在已装好锁定依赖的 VM 仓库目录执行 `.venv/bin/python -m pip install --no-deps -e .`，并读回模块路径后解决。这是环境修复证据，不代表真实数仓整链路已通过。已有真实 epoch 冻结后不随意改变源码或运行环境；升级需要遵守新的版本登记流程。

PowerShell环境变量示例：

```powershell
$env:SNOW_MODE='off'
uv run snow-stats serve --host 127.0.0.1 --port 8101
```

另一个新shell不会自动继承你在这个shell之后设置的值。`.env`文件也不会因为存在就自动进入所有命令的环境；Compose读取它的方式与直接CLI不同。命令结束后按Ctrl+C正常关进程，必要时删除当前shell的测试变量，不修改全局系统变量。

## 脚本可靠性

在组合PowerShell操作前设置 `$PSNativeCommandUseErrorActionPreference=$true; $ErrorActionPreference='Stop'`，避免前一条原生命令失败，后面的打包或上传却继续。脚本中不使用全局HOME等常见变量名做任务变量。文件路径有空格时始终带引号；不要用拼接字符串让用户输入进入shell代码。

Windows文本可能带CRLF，bash here-document或shell脚本可能因此解析失败。上传执行使用项目lab_remote脚本的换行处理；不要随意批量改整个仓库换行。看到bad interpreter或带^M错误先核对编码和换行，而非重装Linux。

## 练习与答案

练习：`uv sync --extra lab`通过后，Kafka连接拒绝是否说明依赖没装好？

答案：不一定。客户端库安装成功并不意味着broker已启动，也不保证端口、NAT地址、advertised.listeners和防火墙正确。先确认本阶段应运行哪个服务，再读实际容器健康与监听地址。

排错顺序：找不到命令先查 `Get-Command`；Python 版本不符查 `uv run python --version`；包导入错误查模块实际路径；Java 错误先区分宿主构建与容器运行；网络失败再查下载端点或实验服务，不直接清空缓存和数据库。面试追问：为什么有 `requirements` 还要安装项目？答案是第三方依赖和自己的 Python 包是两个对象，解释器需要同时找到它们。

## 对照源码与项目证据

- [pyproject.toml](../../pyproject.toml)
- [uv.lock](../../uv.lock)
- [tools/maven_build.ps1](../../tools/maven_build.ps1)
- [docs/runbook.md](../runbook.md)

[返回学习目录](00-阅读地图与当前状态.md)
