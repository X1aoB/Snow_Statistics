# 真实同步副本的启动清理

本入口只处理已有真实 epoch 对应的本地同步 registry，不导入或移动系统崩溃报告，也不创建新 registry。新增代码不修改冻结 writer 文件。

`real_epoch.expire_due()` 是现有启动、显式 cleanup 和 supervisor watch 的共同入口。它读取并验证实际 epoch manifest 后，仅对 `mode=engines`、`input_origin=real` 调用同步副本清理；synthetic fixture 和其他目录不处理。

清理范围严格为该 epoch 同级的 `sync/<epoch_id>/data`。缺失目录正常无操作；已有目录必须无链接、具备既有 owner/registry，且 `target.json` 的来源和事件 lane、`source.json` 的 collector 标识与原 writer registration 一致。epoch generation 与 collector generation 分别核对，二者不是同一种标识。验证只读取有界归属元数据，不读取事件、凭据或转储正文。

锁顺序与同步一致：先取得同步目录的 `publication_lock`，再由 `RealLifecycle.cleanup()` 取得 data 锁。全部释放后，调用者才可能进入 epoch 退休锁。锁竞争沿用监督入口的有界重试；其他 I/O、归属失配和未知物理文件继续拒绝启动，不伪装清理成功。

到期判断保持原 registry 的时间，不以本次启动、注册或崩溃时间续期。对于未来经独立核验、明确批准接管的精确 worker 转储，可以先在这个已有 registry 中按 `raw` 登记，再受控移入；起始时间应取原 epoch 最早接收时间。该操作不由本钩子完成，外部 `/var/crash` 文件仍不在清理范围。

在 analysis VM 中，既有命令自动经过同一入口，无需增加新服务：

```sh
cd /home/snow/Snow_Statistics
.venv/bin/python tools/real_epoch.py --root "$PWD/runtime/real/epochs" cleanup
.venv/bin/python tools/real_epoch.py --root "$PWD/runtime/real/epochs" start --epoch real-prod-01 --stage storage
```

第二条命令仍须满足原启动授权、资源和到期门禁；这里只说明接线，不建议为测试清理而启动引擎。watch 正常轮询的等待上限为 30 秒，不包含锁重试或实际清理耗时；关机期间不能执行删除，下次启动先处理到期数据。停止不删除未过期数据，删除也不代表 VMDK/SSD 取证级擦除。

测试使用临时目录中的合成文件，覆盖实际文件到期删除、未到期保留、共享启动/watch 路径、同步锁竞争、collector 与 epoch 代际区分、链接与归属失配、未知文件及 synthetic 保留。这些测试不代表转储已迁移或 VM 已安装该版本。
