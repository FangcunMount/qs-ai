# 标准报告事实快照

现行接入材料；源码基线 `c977cb9`。外层 StartCommand.evidence / MQ 原业务正文保持显式证据身份，`facts[ref=standard_report].value` 为版本化 JSON。以下首先描述量表 `qs-report-snapshot/v1`；人格版本见文末。旧无版本的领域对象 JSON 不具备完整维度事实，不能作为模型输入降级使用。

| 字段 | 内容与边界 |
|---|---|
| schema_version | 固定 `qs-report-snapshot/v1` |
| source | report_id、outcome_id、report_type、report_template_version、content_schema_version、builder_identity、generated_at；ID 为字符串，时间为 UTC |
| model | 提交 Outcome 的 kind、algorithm、code、version、title |
| runtime | Outcome 的 decision_kind |
| primary_score / level | 标准报告主分数与等级；缺失为 null，不补零或默认等级 |
| conclusion | 原标准结论 |
| dimensions | QS 按报告冻结的参与者可见范围过滤后的维度，保留原顺序；AI 再应用 Profile eligibility |
| suggestions | 标准建议；保留原 source_index，过滤不可见维度所属建议，避免过滤后重编号改变引用 |
| model_extra | 冻结模型扩展，缺失为 null；AI 仍须按 Profile 决定是否向模型提供 |

维度字段：code、kind、name、raw_score、max_score、derived_scores、level、norm_reference、description、suggestion、role、parent_code、hierarchy_level、sort_order。缺失 max_score、level、norm_reference 保持 null；derived_scores 为空数组。parent_code 保留原关联，不代表该父维度一定可见，AI 只能引用实际选入的维度。

Score：kind、value、label、max。Level：code、label、severity。NormReference：score_kind、benchmark、table_version、form_variant、min_age_months、max_age_months、gender；这是使用的常模组信息，不是参与者身份资料。

QS 检查 Report 与 Outcome 的 ID、组织、测评及 Testee 关联。需要因子可见性但缺少冻结 PresentationProfile 时拒绝发起，不退回全量维度。原报告文本不执行、不改写，仍是待解释数据。

这是 QS→AI 的事实传输格式，不是供应商 Prompt 的数据格式。Python application/interpretation/input.py 已实现接收经验证 Profile 的 InputPolicy 投影，执行模型匹配、维度筛选/数量检查/排序、父 ref 映射、稳定 ref、标准建议去重、context/facts 投影和输入摘要。执行从原 publication 加载已校验冻结资产，并核对外层证据关联、输入与输出版本；不能把整个快照直接发给模型。

输入组装对照：tests/fixtures/report_snapshot.json 是合成报告，不含真实测评者信息。Go fixture 重新构造原领域报告与已发布 Profile，调用原 Assemble；Python 使用同一报告。两组用例覆盖父维度保留/排除、常模开关、中文与特殊字符引用、建议原始下标和去重、缺失值；比较完整输入文档的 JSON 语义。没有建议引用的维度保留 QS 当前的 null。输入字节摘要由 qs-ai 对自身冻结 JSON 计算，不声称与旧 Go 编码字节摘要相同，也不覆盖既有 Generation 摘要。

组装与 Prompt 渲染对照验证报告原文留在 data 中；静态策略/合成报告测试只证明格式与投影，不证明真实模型质量或生产任务验收。

preparation.py 的 prepare_report_input 进一步绑定 Session 与 EvidenceSet：会话/证据集 ID、摘要、Testee/测评关联必须一致，仅接受单报告 qs-snapshot-v1 或 qs-published-snapshot-v1 的 standard_report 事实。组装后校验快照内部 report_id 和 content_schema_version:outcome_id 与外层证据一致。摘要正确不代表授权有效，执行用例仍须调用 QS 进行当前权限复核。发布绑定版本通过冻结的 Profile/Prompt/route/Schema 构造工作流，空建议引用的版本差异见 [输入规范](schemas/README.md)。

验证：QS 单元测试覆盖私有字段导出、null、派生分数/常模/层级、隐藏维度及建议、建议原始下标、Outcome 缺失或关联不符、可见性缺失。未以该测试替代真实报告案例验收。

## 人格快照与三主题输入

人格输入采用 `qs-report-snapshot/v2`，只允许精确 `MBTI_OEJTS / v64-report-202608-v1` 或 `MBTI_FC_93 / v55-report-202608-v1` 模型；类型、方向和强度由 QS 评分/报告产生，AI 不重算。人格事实使用明确定义字段，不把量表 norm、suggestion 或其他模型的扩展字段强行映射进人格输入。

报告解读场景 `mbti-single-assessment/v1` 使用 AI input v2，三主题 `mbti-single-assessment/v2` 在基础版使用 input v3，在探索版使用 input v4，输出均为 output v2。三主题额外引用原冻结 Profile 中与本次类型相符的参考条目；通用参考不是本用户实测事实。原量表和旧 MBTI 已冻结请求按原版本继续读取，不能自动升级或 fallback。

精确字段和组合校验由 [输入/输出规范](schemas/README.md)、[MBTI 输入](../../src/qs_ai/application/interpretation/mbti_input.py)、[三主题输入](../../src/qs_ai/application/interpretation/mbti_themes_input.py)及各初始化 manifest 持有。真实版本核对与参与者验收见 [MBTI 管理与验收](../../docs/04-接口与运维/08-MBTI管理与验收.md)。
