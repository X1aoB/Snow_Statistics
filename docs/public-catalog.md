# 公开目录快照与采集允许列表

这是一项由统计项目单独执行的发布操作。两个产品的构建、启动和 CI 不调用本工具，也不依赖此仓库。

快照只保存公开页面路径、可选角色 ID、产品域名、完整发布 SHA、发布时间与内容校验和。它不复制角色注册表中的其他字段，不保存用户标识，不把合成运营分类当成真实维度。每个产品的路径仍归属自己的应用；当前 collector 配置使用两应用路径的精确并集，比较结果明确记录这一范围，不宣称 collector 已具有应用级路径授权。

## 候选版本

在 Windows PowerShell、`Snow_Statistics` 仓库根目录执行以下命令。先把 `<...>` 换成自己的实际路径或完整 40 位提交 SHA；不要直接执行带占位符的命令。collector 环境文件由统计部署单独管理，工具只读取 `SNOW_ORIGINS`、`SNOW_ALLOWED_PATHS`、`SNOW_ALLOWED_CHARACTERS` 三个字段，不输出令牌等其他字段。

```powershell
uv run python tools/public_catalog.py `
  --output runtime/real/catalog `
  --collector-env '<统计服务当前或候选的私有env文件>' `
  capture --stage candidate `
  --mywebsite-repo '../MyWebsite-snow-statistics' `
  --mywebsite-sha '<个人站完整候选SHA>' `
  --project-snow-repo '../Project_Snow-snow-statistics' `
  --project-snow-sha '<小吉完整候选SHA>'
```

工具使用 `git show <SHA>:<固定公开目录文件>`，不执行产品 JavaScript，也不读取工作目录中未提交的改动。当前支持个人站 `public/statistics/paths.mjs` 的 JSON 字符串数组，以及小吉统计配置中的 `paths`、`characters` 字符串数组。小吉的角色列表还必须与同一提交中 `mvp_character_registry.json` 的 `selector_enabled != false` 目录一致。如果产品修改了导出格式，命令明确失败，需要更新解析器及测试，不能退回执行任意 JavaScript。

成功时先输出 `exact_match: true` 的比较回执，再输出 `candidate/<snapshot_id>.json`。候选的 `release_at` 为 `null`。如果存在缺少或多余路径/角色，工具输出精确差异且不写快照；应由各自仓库的独立修改和发布流程修正配置，不要自动扩大允许列表。

## 已发布版本

只有实际发布并完成公网版本核验后，才用相同命令将 `--stage` 改为 `published`，并追加：

```powershell
  --mywebsite-release-at '<个人站实际发布时间，带时区>' `
  --project-snow-release-at '<小吉实际发布时间，带时区>'
```

例如时间格式为 `2026-09-15T12:00:00+08:00`，不是预计发布时间。候选不能填写生产发布时间，已发布记录不能填写未来时间。工具验证提交和字段，但不会代替人工晋级或公网回归；快照的 `evidence: explicit_release_declaration` 表示执行者提供的版本声明，应与生产验收回执一起保存。

已发布记录写入独立的 `published/<snapshot_id>.json`，不会修改或删除候选。数据内容校验和与发布快照标识分别计算：公开目录不变时，产品版本或发布时间变化仍能形成独立快照。重复执行相同声明保留原 `recorded_at`。

## 其他显式清单来源

如果公开列表来自经过核验的独立清单，可以使用 `from-json --input <清单.json>` 代替 `capture ...`。仍必须提供 `--output` 和 `--collector-env`。输入只接受以下字段；例子全部为合成路径、角色和 SHA：

```json
{
  "schema_version": 1,
  "stage": "candidate",
  "apps": {
    "mywebsite": {
      "origin": "https://xiaob.dev",
      "release_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "release_at": null,
      "paths": ["/", "/statistics/"],
      "characters": []
    },
    "project_snow": {
      "origin": "https://snow.xiaob.dev",
      "release_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "release_at": null,
      "paths": ["/"],
      "characters": ["fixture_character"]
    }
  }
}
```

`from-json` 不宣称已检查 Git 提交或页面在线状态。`capture` 是当前两个产品发布时的首选方式。

## 检查与边界

运行 `uv run python -m pytest tests/test_public_catalog.py -q` 验证候选/发布隔离、内容校验、精确配置比较、提交绑定、隐藏角色排除与字段保护。此测试不等于生产发布验收。

公开目录元数据可以按实验资料规则保留，不包含真实行为行。行为事实仍使用原应用、发生时间和目录版本解释；当前工具交付的是可追溯快照，尚未自动生成 Hive SCD2 或宣称列级血缘。版本回滚需要明确记录回滚后真正在线的产品 SHA 与时间，不能把旧候选自动标成已发布。
