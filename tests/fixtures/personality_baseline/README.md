# 人格扩展前置测试资源

来源、生产核验时间和隐私边界见 [基线报告](../../../docs/mbti-contract-baseline.md)。

- `mbti-model.json`：真实不可变 MBTI 发布定义的字段投影。
- `mbti-report.anonymized.json`：真实测试报告与关联 Outcome 的脱敏事实。
- `scale_replay.json`：替换来源 ID/时间后重新生成的输入及 Prompt 期望值。
- `scale_profile.json`：与生产指纹相同的已有公开 v6 Profile 资源。
- `scale_publication.json`：生产发布/评测资产引用与原回执只读回放的摘要证据。

均为测试资源，不用于初始化生产配置或自动发布。缺失人格结构化字段保持缺失，不补造已实现的 v2 输入。SHA256SUMS 只校验这些文件的实际字节，不能代表生产原始完整导出包。
