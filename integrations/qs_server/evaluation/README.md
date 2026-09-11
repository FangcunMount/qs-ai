# QS 评测资源基线

本目录保留六版用例集、两版语义评测模板原 Markdown、语义输出规范、两份策略规范，以及从 QS Go 函数直接导出的执行/门槛策略。固定来源提交与各文件校验和见 manifest.json。来源是本任务独立且干净的 QS worktree，不读取其他任务未提交改动。

`policies.json` 的 definition_json 保留 Go 序列化字节和原 fingerprint；当前执行策略 release-evaluation-bounded-recovery/v2、门槛策略 release-gates/v2。策略 Schema 的 v1 与策略实例的 v2 是不同版本维度，不能混淆。两份实例通过原策略 Schema 验证。

可在相同源提交的干净 QS checkout 重放 `uv run python scripts/export_qs_evaluation.py <checkout> --check`。导出脚本临时 Go 程序只调用策略构造及校验/指纹函数，不发起模型调用、不修改源业务代码，结束后移除临时目录。

边界：六版用例集仍为 planned，不是已运行通过；语义模板 Markdown 尚未证明与运行时 system/task 消息完全一致；语义模型路由实际生产配置尚未盘点。未提供 Python 评测执行器、评分/门槛实现、人工复核或发布审批，不将资源导入视为 M3 验收。资源暂作为迁移开发基线，未进入运行镜像或数据库资产表。
