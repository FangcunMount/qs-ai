# MBTI 接入前的真实数据与量表回放基线

状态：三项前置基线已固定；MBTI AI 功能尚未实现、评测或发布。记录日期：2026-09-22。

## 1. 核验范围与版本

本批仅固定真实 MBTI 模型版本、脱敏报告样本和既有量表回放基线。生产数据库只读；没有模型调用、数据库写入、服务重启或发布变更。

| 项目 | 本次核验值 |
| --- | --- |
| qs-ai 基线及运行镜像 | `c6a9d1eb3317ec983d6ff855204264000646223f` |
| QS 运行镜像与源码 | `5573735ae2e7443ecd70e42703aadc4378fbe94a` |
| qs-ai 数据库版本 | `0036_evaluation_slot_claims` |
| 就绪检查 | HTTP ready，数据库 connected |
| 最终量表回放时间 | `2026-09-22T05:33:00.595236+00:00` |

## 2. 固定的 MBTI 身份

| 字段 | 固定值 |
| --- | --- |
| code / release_version | `MBTI_OEJTS / v64-report-202608-v1` |
| kind / algorithm / decision_kind | `typology / personality_typology / pole_composition` |
| 发布状态 | `published / active` |
| 问卷 code / version | `MBTI_OEJTS / 8.0.1` |
| 定义 Schema | `2` |
| 类型合成顺序 | `EI → SN → TF → JP` |
| 报告内容 / 模板 / 构建器 | `report-content/v1 / 2026-08-v1 / typology` |

归档的 `v64` 和没有 release_version 的编辑头均不作为本轮来源。首版 MBTI 场景必须精确匹配上述 code 和 release_version，不使用 typology 通配或当前最新模型。

固定规则的两极为 EI：I/E，SN：S/N，TF：F/T，JP：J/P。按该版本题目贡献及当前 Go 实现，每轴原始分范围为 8–40、阈值 24、最大偏移 16。范围是由规则和代码推导的，不是原始定义中直接存储的范围字段，也不是人群常模。

当前规则 `raw <= 24` 取左极，`raw > 24` 取右极；恰好 24 时强度为 0。强度为 `min(100, abs(raw - 24) / 16 * 100)`；match_percent 是四轴强度均值，不是准确率、概率或置信度。后续 AI 扩展沿用这些事实，不重新计算或修改评分。

代码依据：QS 的 `domain/calculation/classification/selector.go`、`pole_deviation.go` 及 `application/evaluation/registry/mechanisms/typology/runtime/configured/graph.go`（均在 `internal/apiserver/` 下）。

## 3. 脱敏样本与已证实的缺口

样本来自先前获准使用的专用测试账号，输出只保留解读所需事实；当前报告目录与报告的 source/outcome 引用已比对一致。没有导出账号、受试者、测评、报告、Outcome 原始 ID、姓名、凭据或原始答案。

- [固定模型投影](../tests/fixtures/personality_baseline/mbti-model.json)：保留发布身份、问卷、轴及类型规则。
- [真实报告脱敏投影](../tests/fixtures/personality_baseline/mbti-report.anonymized.json)：保留报告版本、类型、维度及关联 Outcome 的必要结构化事实。

已确认两个实现缺口：

1. 标准报告没有结构化保存两极、方向及精确强度，方向和强度仅进入 description，且强度经过取整。例如 Outcome 的 56.25、6.25 被展示为 56%、6%。不能反解析展示文字作为 AI 输入。
2. Outcome 的 Preference、Strength 有值，但 LeftPole、RightPole、Name 为空。后续不能仅复制 Outcome；QS 必须从**该报告绑定的不可变模型版本**补齐轴定义，再随版本化标准报告冻结，不读取最新模型。

QS 仍是类型、方向和强度的唯一事实生产者；qs-ai 负责契约与来源校验。旧报告支持策略、接口、MBTI 根评测资产及实际业务验收仍属于后续实施。

## 4. 量表发布与原回执回放

[完整发布元数据和回放结果](../tests/fixtures/personality_baseline/scale_publication.json) 固定了当前发布指针、Profile、Prompt、生成/裁判路线、输入输出 Schema、评测套件、执行策略和门槛策略的版本及摘要。

| 项目 | 固定值 |
| --- | --- |
| 发布 | `a9f7f8a7-e0aa-4d5b-ab23-c4af584317e4`，指针版本 1 |
| Profile | `participant-scale-score-range-default/v6` |
| Prompt | `cross-dimension-participant-scale/v6` |
| 生成路线 | `balanced_text_v1/v8` |
| 裁判路线 | `semantic_judge_v1/v5` |
| 发布清单指纹 | `sha256:617a7b766fc69681efb5cabc6cdc03f313cb0c12665dbbb06a4a15486c2bc53a` |

生产回放使用只读 REPEATABLE READ 一致快照事务，复用已完成调用的原 FrozenGeneration、ModelResponse、绑定及发布证据。重新编译配置、组装输入、验证契约并构建成果，未实例化供应商调用客户端。

- 原输入重建相等：`prepared_equals_original = true`。
- 原完整成果重建相等：`artifact_equals_original = true`。
- 原/重建成果 SHA256 均为 `034975d3db918742f35663b11215a1270f50423f506dbd8d81385334ac7a6286`。
- 再次锁定同一 request SHA 回放，结果及脱敏测试输入一致。

这证明本次固定版本能够重建该已保存回执，不能代替崩溃恢复、重新调用模型或 MBTI 业务验收。

## 5. 可重复的本地保护与验证

[契约测试](../tests/test_personality_extension_baseline.py) 新增三项：脱敏量表输入字节与指纹不变；固定 Prompt 消息字节不变；真实 MBTI 身份不能误入量表 v1 输入通道。第三项明确使用混合的负例，不把它当作新 MBTI 输入格式。

本次共 64 项不同测试通过：既有 input assembly、evaluation input version、publication 测试 61 项；新增测试最终 3 项。新测试首次暴露测试代码属性名错误，修正后仅重跑该文件通过，没有重复运行已通过的其他检查。Ruff 检查与格式检查通过。

本地测试无需数据库、网络或模型。生产原回执仍保留在原库，未复制到测试资源；因此本地三项测试不冒充完整生产成果回放。

## 6. 摘要与脱敏约定

这些 JSON 是明确保留字段的投影，不是完整原始导出包。MBTI definition_sha256 来自生产 `EJSON.stringify(definition_v2)` 的观测摘要，对字段顺序敏感，不冒充标准化业务指纹。文件自身摘要见测试资源目录的 SHA256SUMS。

量表样本替换了 report/outcome ID 与时间，重新计算脱敏输入及 Prompt 哈希，并以 `not_original_fingerprint: true` 明示。它们与生产原始业务指纹是两个基线，不应互相替代。Profile 来源为已有公开固定资源，使用前已核对其指纹与生产一致。

下一步是 QS 结构化人格事实及版本化输入契约；本批不代表整个 P0 或 MBTI 方案完成。
