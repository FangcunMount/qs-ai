# QS 可执行 Prompt 迁移资产

从固定 QS 提交的 Go Catalog 导出 v1–v6，包含 SystemMessage、TaskTemplate、DataPreamble、白名单占位符与原始 PromptRef。来源与文件校验和见 manifest.json；原指纹语义保持不变，文件 SHA256 另行记录。

这些资产已随 wheel 和镜像打包，Python 加载器按明确的模板与版本读取，并验证 manifest 文件校验和；不自动选取最新版本。生产观测到的发布 Profile 见 published-profile-baseline.json，使用 v6。

应用层 render_prompt 接收已验证 Profile 的 RenderPolicy 投影与独立 provider payload，保持 system、task、data preamble 和 data JSON 分离。占位符只能来自白名单；locale 和 focus areas 经过格式及允许范围检查，报告字符串不进入指令。对重复 JSON 字段、非 JSON 数字常量、空 context/facts 和残缺占位符额外拒绝。

Profile 迁移解码器已校验 published 状态、全部策略字段及内容指纹，并按显式 profile_id/version 加载基线。它拒绝重复版本、缺失版本、未知字段、类型强制转换、无效数量范围、弱化安全规则和篡改内容；不自动选 latest。输出是不可变的应用层策略对象，保留规范化定义用于审计。该静态加载器用于首个迁移案例，不等于 M3 的可管理发布体系。

prepare_explanation 已串联证据绑定、报告输入组装与 Prompt 渲染，返回输入摘要、Prompt 指纹、发布定义及 provider_route。尚未绑定执行 worker，真实模型路线配置、输出校验、持久成果仍待接通，不能据此宣称已具备真实生成能力。

旧源码导出器已随 M5 退役。原始资产和来源提交保持不变；字节摘要由资产测试验证，渲染、输入、输出、身份与候选断言的原 Go 结果保存在 `tests/fixtures/legacy_go_contracts.json`，记录固定来源提交并校验整份文件 SHA-256。九组离线基准测试不再编译旧引擎；Go→Python 新业务互操作仍在 CI 独立执行。固定基准仅证明迁移一致性，不代替生产验收。
