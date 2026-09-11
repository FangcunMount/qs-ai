# 标准报告事实快照 v1

源实现：QS 专属分支提交 `1a947bfd` 的 application/aibridge/snapshot.go。尚未发布生产。外层 StartCommand.evidence 协议不变，`facts[ref=standard_report].value` 为下述 JSON；旧的无版本直接领域对象 JSON 不具备完整维度事实，不能作为模型输入降级使用。

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

这是 QS→AI 的事实传输格式，不是供应商 Prompt 的数据格式。Python 后续须完成发布 Profile 匹配、维度数量与层级校验、稳定 ref、标准建议去重、context/facts 投影、输入摘要及输出约束，不能把整个快照直接发给模型。

验证：QS 单元测试覆盖私有字段导出、null、派生分数/常模/层级、隐藏维度及建议、建议原始下标、Outcome 缺失或关联不符、可见性缺失。未以该测试替代真实报告案例验收。
