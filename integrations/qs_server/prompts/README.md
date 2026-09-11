# QS 可执行 Prompt 迁移资产

从固定 QS 提交的 Go Catalog 导出 v1–v6，包含 SystemMessage、TaskTemplate、DataPreamble、白名单占位符与原始 PromptRef。来源与文件校验和见 manifest.json；原指纹语义保持不变，文件 SHA256 另行记录。

这是迁移资产，不代表运行时已经接入。不得因为存在 v6 就选择 v6；首个生产案例应由实际发布 Profile 决定版本，同时迁移输入组装、模型路线和输出校验。

使用 `uv run python scripts/export_qs_prompts.py /path/to/clean/qs-server --check` 重新执行固定版本 Catalog 并逐字比较。导出器只在指定干净工作区的 scripts 下创建临时 Go 程序，退出后移除，不修改 QS 业务代码。CI 固定相同提交执行此验证。
