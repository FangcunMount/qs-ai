# QS 评测资源基线

本目录保留六版用例集、两版语义评测模板原 Markdown、语义输出规范、两份策略规范，以及从 QS Go 函数直接导出的执行/门槛策略。固定来源提交与各文件校验和见 manifest.json。来源是本任务独立且干净的 QS worktree，不读取其他任务未提交改动。

`policies.json` 的 definition_json 保留 Go 序列化字节和原 fingerprint；当前执行策略 release-evaluation-bounded-recovery/v2、门槛策略 release-gates/v2。策略 Schema 的 v1 与策略实例的 v2 是不同版本维度，不能混淆。两份实例通过原策略 Schema 验证。

可在相同源提交的干净 QS checkout 重放 `uv run python scripts/export_qs_evaluation.py <checkout> --check`。导出脚本临时 Go 程序只调用策略构造及校验/指纹函数，不发起模型调用、不修改源业务代码，结束后移除临时目录。

边界：六版用例集仍为 planned，不是已运行通过；语义模板 Markdown 尚未证明与运行时 system/task 消息完全一致；语义模型路由实际生产配置尚未盘点。未提供 Python 评测执行器、评分/门槛实现、人工复核或发布审批，不将资源导入视为 M3 验收。资源随 wheel/镜像分发，尚未进入数据库资产表或自动触发评测。


## Python 执行策略投影

`load_execution_policy` 校验资源 checksum、原 Go fingerprint 与原 JSON Schema，显式只接受当前已迁入 v2，然后构造冻结 ExecutionPolicy。类型保留样本目标、单目标/整轮预算、自动与人工恢复名单及三个恢复开关。

`selects_automatic_retry` 只是 policy whitelist 判断，不包含 ClassifiedFailure 的完整合法性/处置判断，也不代表可以直接重试。`within_budget` 是预留前检查，计数由后续持久执行聚合提供，失败及结果未知调用不能从预算中扣除。尚未实现并发原子预留、完整 NextAction、人工确认或评测发布门槛；未连接生产生成 worker。
