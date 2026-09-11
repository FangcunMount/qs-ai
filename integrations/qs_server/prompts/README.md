# QS 可执行 Prompt 迁移资产

从固定 QS 提交的 Go Catalog 导出 v1–v6，包含 SystemMessage、TaskTemplate、DataPreamble、白名单占位符与原始 PromptRef。来源与文件校验和见 manifest.json；原指纹语义保持不变，文件 SHA256 另行记录。

这些资产已随 wheel 和镜像打包，Python 加载器按明确的模板与版本读取，并验证 manifest 文件校验和；不自动选取最新版本。生产观测到的发布 Profile 见 published-profile-baseline.json，使用 v6。

应用层 render_prompt 接收已验证 Profile 的 RenderPolicy 投影与独立 provider payload，保持 system、task、data preamble 和 data JSON 分离。占位符只能来自白名单；locale 和 focus areas 经过格式及允许范围检查，报告字符串不进入指令。对重复 JSON 字段、非 JSON 数字常量、空 context/facts 和残缺占位符额外拒绝。

当前完成的是加载与渲染组件，尚未绑定执行 worker。完整 Profile 校验/解析、真实报告输入组装、模型路线和输出校验仍待迁移，不能据此宣称已具备真实生成能力。

使用 `uv run python scripts/export_qs_prompts.py /path/to/clean/qs-server --check` 重新执行固定版本 Catalog 并逐字比较。导出器只在指定干净工作区的 scripts 下创建临时 Go 程序，退出后移除，不修改 QS 业务代码。CI 固定相同提交执行此验证。

CI 同时设置 QS_AI_PROMPT_SOURCE 指向固定 QS 工作区，运行 tests/test_prompts.py 的跨语言用例：使用发布 Profile、无关注点/有关注点两组输入，对照原 Go Render 的 v1–v6 三段指令逐字结果与数据 JSON 语义。该验证只证明渲染兼容，不证明报告输入、模型质量或生产授权验收。
