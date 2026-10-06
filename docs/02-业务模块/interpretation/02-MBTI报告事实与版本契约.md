# MBTI 报告事实与版本契约

本文回答哪些人格事实可以进入 AI、单主题与三主题各使用什么版本，以及与量表旧路径如何隔离。核对基线：`c977cb9`，2026-10-06。有限 MBTI 解码、配置编译、正式执行、评测和恢复代码已实现；生产资产安装、真实质量批准、生效发布、参与者生成及页面验收状态需另行核证，不能由本契约推断。

## 精确版本与责任

单人、单次、单标准报告，模型固定 `MBTI_OEJTS / v64-report-202608-v1`，model.kind=`typology`、algorithm=`personality_typology`、runtime.decision_kind=`pole_composition`。

| 对象 | 量表原路径 | MBTI 单报告解读 | MBTI 三主题解读 |
| --- | --- | --- | --- |
| QS 快照 | `qs-report-snapshot/v1` | `qs-report-snapshot/v2` | `qs-report-snapshot/v2` |
| AI 输入 | `ai-explanation-input/v1` | `ai-explanation-input/v2` | `ai-explanation-input/v3` |
| AI Profile 契约 | `ai-explanation-profile/v1` | `ai-explanation-profile/v2` | `ai-explanation-profile/v3` |
| scene_contract_version | 原 v1 无新增默认字段 | `mbti-single-assessment/v1` | `mbti-single-assessment/v2` |
| 模型输出 | `ai-explanation-output/v1` | `ai-explanation-output/v1` | `ai-explanation-output/v2` |
| Artifact 封装 | `qs-ai-artifact/v1` | `qs-ai-artifact/v1` | `qs-ai-artifact/v2` |
| 评测 input construction | `qs-published-snapshot-v1` | `qs-published-snapshot-v2` | `qs-published-snapshot-v3` |
| 正式 Session workflow | `qs-published-snapshot-v1` | `qs-published-snapshot-v2` | `qs-published-snapshot-v2` |
| 受控根 Suite 版本 | 保留固定 v6/published 来源 | `participant-mbti-single / v1` | `participant-mbti-single / three-topic-v1` |

这些版本各表达不同协议，不与 ModelRoute v2 或 candidate_v2 隐式联动。文件名 profile-v1.json 指根资产修订，不表示 Profile schema v1。读取快照 v2 只分派人格解码器；发布清单中的 Profile 决定单主题或三主题输入和输出策略。

QS 是标准类型、方向、强度、raw_score 及 match_percent 的唯一事实生产者。qs-ai 校验字段、范围、轴与类型一致性，**不根据 raw_score 重算偏好或强度，也不重算 match_percent**。偏好强度显式标记为 `preference_strength_not_confidence`，不能写成置信度。公开人格材料不证明本地测量等价于其他产品，也不产生用户个体的新测量事实。

## 完整快照投影

合成协议向量见 [snapshot.json](../../../tests/fixtures/personality_contract/snapshot.json) 及 [来源说明](../../../tests/fixtures/personality_contract/README.md)。所有对象使用精确字段集合，拒绝未知字段和重复 JSON key。

| 对象 | 允许字段及约束 |
| --- | --- |
| 顶层 | schema_version、source、model、runtime、conclusion、dimensions、suggestions、model_extra |
| source | report_id、outcome_id、report_type、report_template_version、content_schema_version、builder_identity、generated_at；ID 为正 uint64 规范十进制字符串；report_type=standard，时间带时区 |
| model | kind、algorithm、code、version、title；身份精确匹配上表模型 |
| runtime | 仅 decision_kind=pole_composition |
| dimension | code、kind、name、raw_score、pole_facts、description、suggestion；kind=pole，恰好四个独立轴 |
| pole_facts | schema_version=mbti-pole-facts/v1、left_pole、right_pole、preference、strength、min_score、max_score、threshold、composition_order，均必填 |
| model_extra | kind=personality_type、type_code、type_name、one_liner、match_percent、commentary；type_code 与四轴已保存偏好一致 |
| suggestion | source_index、category、content、dimension_code；索引为唯一非负整数，dimension_code 为空或已知轴 |

四轴身份与顺序固定：EI(I/E)、SN(S/N)、TF(F/T)、JP(J/P)。raw_score 和两极范围为 8–40，阈值 24，composition_order 为 1–4。preference 必须为对应两极之一；strength 和 match_percent 是 0–100 有限数值。零为有效事实，缺失、null、布尔值、字符串、非有限数值均拒绝；方向只取保存事实，不按阈值二次判断。

报告建议最多 50 条；轴、类型和报告描述保留标准来源。原始答案、账户资料、稀有度和图片等不能进入该投影。QS 应先白名单投影，不能发送完整持久化对象。缺 pole_facts 的历史报告明确失败，不能默认强度为零，也不能靠 description 文本解析补造事实。

## AI 输入、引用与隐私

规范组装结果含 schema_version、scene_contract_version、source、profile、context、facts；三主题增加独立 reference_material。供应商只收到 context/facts，三主题另收到冻结参考区；report/outcome ID、Profile 元数据留在服务端规范输入与指纹。

按 composition_order 排序四轴，按 source_index 排序报告建议，保留原始小数，不从描述中的四舍五入百分比替代数值。轴引用为 `dimension:EI` 等，报告建议为 `suggestion:report:<index+1>`，轴建议为 `suggestion:dimension:<code>`。总体与类型结果分别采用 `overall_result`、`model_result`。

locale 和 focus areas 必须符合固定 Profile 策略；最多三个唯一且允许的 focus。四轴全部保留，没有排除轴、常模补充或其他人格模型的自由扩展。

[输入 Schema v2](../../../integrations/qs_server/schemas/ai-explanation-input-v2.schema.json) 与 [v3](../../../integrations/qs_server/schemas/ai-explanation-input-v3.schema.json) 是 qs-ai 编写的资产，各自清单明示来源及摘要：[单主题清单](../../../integrations/qs_server/schemas/mbti-input-manifest.json)、[三主题清单](../../../integrations/qs_server/schemas/mbti-thematic-input-manifest.json)。不伪装为 QS 原始导出。原 v1 清单和 Schema 保留；显式版本读取，缺失或摘要损坏不回落 v1/latest。生产读取数据库不可变资产与冻结清单，初始化文件不是运行回退源。

JSON Schema 验证结构；四轴唯一性、顺序、偏好与类型一致性由固定版本代码补充。错误只携带安全分类，不暴露原报告或 Pydantic/jsonschema 输入正文。

## 发布选择、输出与恢复

MBTI selector 必须精确匹配 participant/typology/pole_composition/模型代码/模型版本。人格与量表通配不互相匹配，未发布或暂停不能回落量表。scene 保存在原 Profile，两个 MBTI 场景共享该模型 selector 槽位；发布使用当前批准 Profile 的场景，不能另猜版本。

单主题继续使用 output v1 的数量、跨维度引用、标准建议来源及安全规则；不能用 model_result 替代原要求的维度依据，也拒绝其他人格类型标签。有限类型字符串检查不能发现所有改写的事实错误，仍需原生语义评测及实际人工审核。

回执编解码保存原 Profile、策略、输入指纹、Prompt 消息、模型参数及响应，恢复从原回执读取；单主题和三主题均拒绝与原冻结 Profile 不一致的策略。已保存响应不二次调用，dispatched/unknown 未保存响应不推断未发出。三主题额外参考约束见 [专项契约](03-MBTI三主题与参考材料契约.md)。

## 选择代价与 Verify

有限版本组合避免场景误路由与隐式升级，代价是新模型/根参考包必须建立新协议并重新评测。精确投影保护隐私与事实所有权，代价是历史缺字段报告不能直接生成。Schema 加业务验证提供可重复拒绝，但不把合法结构等同内容质量。

源码：[有限场景](../../../src/qs_ai/domain/governance/scenes.py)、[快照解码/组装](../../../src/qs_ai/application/interpretation/mbti_input.py)、[Profile 三版本解码](../../../src/qs_ai/infrastructure/qs_server/profiles.py)、[选择器](../../../src/qs_ai/application/interpretation/selection.py)、[回执编解码](../../../src/qs_ai/infrastructure/persistence/model_call_codec.py)。

验证：[16 类型、零值/小数/隐私/错误来源/摘要](../../../tests/test_mbti_contract.py)、[Go→Python 投影](../../../tests/test_mbti_snapshot_interop.py)、[原响应恢复与量表字节](../../../tests/test_mbti_runtime.py)、[三主题输入](../../../tests/test_mbti_themes_input.py)。互操作测试固定 QS 源码用于合成投影，不引入生产源码依赖；本地测试不替代真实模型、部署、质量审核、业务生成、授权和恢复验收。
