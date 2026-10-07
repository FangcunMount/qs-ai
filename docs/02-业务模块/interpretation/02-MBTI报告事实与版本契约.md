# MBTI 报告事实与版本契约

MBTI 解读解释的是 **QS 已保存的四轴偏好、强度和人格类型**。qs-ai 核对这些事实能否共同组成一份合法输入，再根据已接受的 Profile 选择单主题或三主题解释；它不重新评分，不从描述里补数字，也不把偏好强度改称置信度。历史报告缺少结构化 `pole_facts` 时，页面可能仍能展示标准报告，但该报告不能直接进入这条 AI 协议。

本文沿 [合成 ISFJ 快照](../../../tests/fixtures/personality_contract/snapshot.json) 解释事实、版本和引用如何关联。该样例的身份、时间和部分文本由测试构造；[来源说明](../../../tests/fixtures/personality_contract/README.md) 明确它不是生产快照，也不能用它给历史报告补事实。qs-ai 源码按 `766b2aa` 核对；QS 投影按本地 `2ccc2de44` 核对，互操作 CI 的固定 QS 版本另见后文。参考包选择和三主题输出结构见 [三主题与参考材料契约](03-MBTI三主题与参考材料契约.md)。

## 先确认解释的是哪个模型、哪份报告

当前支持的是一个有限场景：单人、单次测评、单份标准报告。模型身份必须完整匹配有限名单；以下代码块和 ISFJ 样例继续指基础版：

```text
model.kind      = typology
model.algorithm = personality_typology
model.code      = MBTI_OEJTS
model.version   = v64-report-202608-v1
runtime.decision_kind = pole_composition
```

探索版新增独立绑定 `MBTI_FC_93 / v55-report-202608-v1`，问卷为冻结的 `MBTI_FC_93 / 8.0.1`。其 EI 为 E/I、SN 为 S/N、TF 为 T/F、JP 为 J/P；前三轴原始范围 0–23，JP 为 0–24，阈值均为 11.5。QS 从已冻结二选一问卷验证 93 题的 0/1 计分范围，不改变历史评分、偏好方向或强度。基础版原有 I/E、S/N、F/T、J/P 与 8/40/24 契约保持不变。

探索版三主题使用新增 `ai-explanation-input/v4`；输出仍为 `ai-explanation-output/v2`，共享三主题结构，不共享发布指针。代码只接受这两个完整模型版本组合，不允许用 `latest`、基础版轴边界或当前模型编辑头代替。探索版旧报告没有 `pole_facts` 时继续只读展示；需新测评形成冻结事实，不能补写旧报告。

这些值来自报告及其已提交 Outcome，而不是当前模型编辑头。`model.version` 固定测量模型身份；`report_template_version` 固定标准报告模板；`content_schema_version` 标记报告内容协议；三者允许不同值，不能相互替换。

QS 的 `reportsource.ResolveCurrent` 先按当前报告目录中的 source ID 读取标准 Report，再按该 Report 的 OutcomeID 读取已提交 Outcome，核对 Assessment/Testee/组织、模型与 runtime。目录缺失返回 not_ready；目录损坏、Report/Outcome 关联不一致则失败，不能扫描历史报告或最新模型来凑一个“可用来源”。AI 快照再由 `aibridge.reportSnapshot → mbtiReportSnapshot` 白名单投影。

在样例中，报告是 `99`，Outcome 是 `101`，内容协议是 `report-content/v1`。QS 发给 AI 的外层 EvidenceItem 相应保留：

```text
report_id = 99
source_version = report-content/v1:101
facts[0].ref = standard_report
facts[0].value = <完整 qs-report-snapshot/v2 JSON 字符串>
```

**`source_version` 的实际构造是 `content_schema_version + ":" + outcome_id`。** 它不包含模型版本、报告模板版本或 generated_at，也不是递增的报告 revision。AI 的 [`report_selector`](../../../src/qs_ai/application/interpretation/selection.py) 与 [`prepare_report_input`](../../../src/qs_ai/application/interpretation/preparation.py) 同时核对外层 report_id/source_version 和 JSON 内 source；重新计算 EvidenceSet 哈希也不能绕过这层来源绑定。

QS 当前报告选择发生在原请求产生时；一旦 AI 接单保存 EvidenceSet，之后的生成使用原快照，不跟随 QS 当前报告目录变化。执行前和接受成果前的当前授权仍单独复核。冻结、授权与原事务的完整链路见 [解读与成果设计](01-解读冻结输入与成果设计.md)。

## 四轴数值怎样成为可引用事实

样例保留的标准事实如下。这里的数值用于说明投影，没有请求模型推断一个新的测量结果：

| 顺序 | 轴身份与两极 | raw_score | 已保存 preference | 已保存 strength | AI 引用 |
| --- | --- | --- | --- | --- | --- |
| 1 | EI：I / E | 15 | I | 56.25 | `dimension:EI` |
| 2 | SN：S / N | 23 | S | 6.25 | `dimension:SN` |
| 3 | TF：F / T | 20 | F | 25 | `dimension:TF` |
| 4 | JP：J / P | 12 | J | 75 | `dimension:JP` |

类型由保存的 preference 按 `composition_order` 连成 `ISFJ`；`model_extra.type_code` 必须与这四个字母一致。`model_extra.match_percent=40.625` 同样原值保留，qs-ai 不计算该百分比，也不将它与某个轴的 strength 混为一个指标。

[`decode_mbti_snapshot`](../../../src/qs_ai/application/interpretation/mbti_input.py) 核对的是结构、范围和一致性：每个轴恰好出现一次，两极和顺序固定，raw_score 在 8–40，min/max/threshold 为 8/40/24，strength 和 match_percent 在 0–100。它没有根据 raw_score 与 threshold 重新判方向，也没有从两者重算强度。

例如测试把 EI raw_score 改为 24、strength 改为 0，并保留已保存 preference=I，组装后仍保留 I 与 0。另一个测试把 strength 改为 6.25，也按原数值投影。这证明输入层接受 QS 的权威数字并检查协议；不证明这些合成修改重新通过了 QS 测量算法或标准报告生成验收。

每个投影轴增加固定语义标签 `strength_semantics=preference_strength_not_confidence`。零和 0.0 都是有效已知值；缺失、null、布尔值、数字字符串、NaN/Infinity 或越界值被拒绝。`strength=0` 与“没有 PoleFacts”有完全不同的含义，不能用默认零抹掉历史缺失。

## 快照白名单具体保留什么

MBTI 快照顶层只有 `schema_version / source / model / runtime / conclusion / dimensions / suggestions / model_extra`。专用解码器逐层要求精确字段集合；原 JSON 还经过重复 key 检查。增加账户、原作答、图片 URL 或其他未声明字段，即使其它事实完整，也会拒绝。

| 对象 | 必需字段与关键校验 |
| --- | --- |
| `source` | report_id、outcome_id、report_type、report_template_version、content_schema_version、builder_identity、generated_at；ID 是正 uint64 的规范十进制字符串、无前导零，report_type=standard，时间带时区 |
| `model` / `runtime` | kind、algorithm、code、version、title / 仅 decision_kind；完整匹配上述有限模型身份 |
| `dimensions[]` | code、kind、name、raw_score、pole_facts、description、suggestion；恰好四轴，kind=pole；说明和建议可为空字符串，不能作为缺数字时的解析来源 |
| `pole_facts` | schema_version、left_pole、right_pole、preference、strength、min_score、max_score、threshold、composition_order；schema 固定 `mbti-pole-facts/v1`，数值与身份按前节校验 |
| `model_extra` | kind、type_code、type_name、one_liner、match_percent、commentary；kind=personality_type，类型字母与四轴偏好一致 |
| `suggestions[]` | source_index、category、content、dimension_code；最多 50 条，source_index 唯一非负整数，dimension_code 为 null 或四轴之一 |

QS 的白名单也有自己的接受边界：Report 与 Outcome 的模型必须一致，PoleFacts 必须结构完整，ModelExtra 不能是 special 类型，建议关联轴必须已知。缺少全部 PoleFacts 的旧报告在标准报告领域仍可保持兼容读取，但 `mbtiReportSnapshot` 明确返回不适用；部分缺失或错误则视为不一致。不能把“旧报告可读”推导成“旧报告可生成 AI”。

文本长度、纯文本检查和输入 Schema 还会继续限制内容。顶层 conclusion、轴 description/suggestion、类型 one_liner/commentary 可以为空，不意味着数值字段也可缺省。标准报告最多 50 条建议是快照边界；组装后的标准建议列表还受输入 Schema 约束，结构合格不会跳过发布输入验证。

## 一份快照为什么有两份组装 JSON

[`assemble_mbti`](../../../src/qs_ai/application/interpretation/mbti_input.py) 按 composition_order 排四轴、按 source_index 排报告建议，再形成 [`AssembledInput`](../../../src/qs_ai/application/interpretation/input_values.py)：

| 值 | 保留内容 | 谁使用 |
| --- | --- | --- |
| `canonical_json` | schema_version、scene_contract_version、原 source、Profile id/version/fingerprint、context、facts；三主题另有 reference_material | 服务端输入 Schema、input fingerprint、审计与成果接受重建 |
| `provider_payload` | 单主题只含 context/facts；三主题增加独立 reference_material | Prompt 的数据消息，供应商收到的报告投影 |

source 中 report/outcome ID、Profile 身份和摘要保留在规范输入，未放进 provider payload；供应商仍会看到模型身份、类型、四轴事实和标准描述。因此隐私边界是具体白名单，不能笼统说“供应商看不到元数据”或“只发送分数”。

`context` 固定 `current_assessment_only / participant`。locale 默认 zh-CN，校验语言标签格式和最长 35 字符，Profile 没有 locale 允许列表。focus 最多三个、不能重复，必须在 Profile 的 allowed_focus_areas 内；有 focus 时 personalization_scope 为 `assessment_result_and_focus_areas`，否则为 `assessment_result_only`。focus 只影响解释上下文，不移除轴，不补常模或额外测评事实。当前正式报告图使用默认 locale 与空 focus，组装器支持参数并不表示所有入站请求都能自由设置它们。

标准建议引用保留来源身份：报告 source_index=0 对应 `suggestion:report:1`；若原索引是 3，对应 `suggestion:report:4`，不是重排后第二项。轴建议非空时产生 `suggestion:dimension:EI` 等引用；报告/轴建议按 category、正文和关联轴去重，因此不是每个原条目都会变成不同建议。`overall_result` 对应标准结论，`model_result` 对应类型结果，轴引用对应原轴事实。

输入层存在两个摘要层次：EvidenceSet 对原快照字符串及外层 items 求 hash；input fingerprint 对组装后的 canonical_json 求 hash。把原 JSON 的四轴列表倒序，会改变原字符串和证据摘要，但排序组装后仍得到同一规范输入。改原 Profile、来源 ID 或事实数值，则规范输入也变化。不能只拿一个 hash 代替原证据、来源和配置的全部核验。

## 版本表表达不同边界，不按同一个数字升级

[`decode_published_profile`](../../../src/qs_ai/infrastructure/qs_server/profiles.py) 校验 Profile 的 published envelope、严格字段和 canonical fingerprint，产生对应 InputPolicy。快照 v2 表明进入 MBTI 解码；**原接受 Profile 的类型决定单主题或三主题**。完整组合如下：

| 协议对象 | 量表原路径 | MBTI 单主题 | MBTI 三主题 |
| --- | --- | --- | --- |
| QS 快照 | `qs-report-snapshot/v1` | `qs-report-snapshot/v2` | `qs-report-snapshot/v2` |
| AI Profile schema | `ai-explanation-profile/v1` | `ai-explanation-profile/v2` | `ai-explanation-profile/v3` |
| scene contract | 不新增 MBTI scene 字段 | `mbti-single-assessment/v1` | `mbti-single-assessment/v2` |
| AI 规范输入 | `ai-explanation-input/v1` | `ai-explanation-input/v2` | `ai-explanation-input/v3` |
| 模型内容输出 | `ai-explanation-output/v1` | `ai-explanation-output/v1` | `ai-explanation-output/v2` |
| 成果封装 | `qs-ai-artifact/v1` | `qs-ai-artifact/v1` | `qs-ai-artifact/v2` |
| 评测 Suite input construction | `qs-published-snapshot-v1` | `qs-published-snapshot-v2` | `qs-published-snapshot-v3` |
| 正式 Session workflow | `qs-published-snapshot-v1` | `qs-published-snapshot-v2` | `qs-published-snapshot-v2` |
| 受控 MBTI 根 Suite | 使用原量表 Suite 身份 | `participant-mbti-single / v1` | `participant-mbti-single / three-topic-v1` |

例如三主题接受一份快照 v2，Session 仍为 workflow v2，因为这个标签区分量表/MBTI 事实读取。编译原三主题 Profile 后，得到 input v3、output v2，并要求 Suite 的 input construction v3。两个 `qs-published-snapshot-v*` 字符串出现在不同上下文，不能只按名字把 Session 和 Suite 版本混为同一个字段。

Profile 自己的 `version` 是资产修订，`schema_version` 是协议版本：根文件名 profile-v1.json 也可能保存 Profile schema v2/v3。ModelRoute v2 是供应商绑定协议，candidate_v2 是评测执行模式；它们与 MBTI 的 v2/v3 输入没有隐式开关关系。

MBTI Profile 强制保留全部 EI/SN/TF/JP，维度 min/max 均为 4，无 excluded codes、不含 norm context、必须包含 model_result。三主题先复用单主题组装，保持相同 facts，再按原 type_code 选择冻结参考区；通用人格材料没有变成新的个人测量事实。

## 发布选择与 Schema 必须对上原组合

基础版 MBTI 的 selector 精确为 `(participant, typology, pole_composition, MBTI_OEJTS, v64-report-202608-v1)`；探索版为独立的 `(participant, typology, pole_composition, MBTI_FC_93, v55-report-202608-v1)`。[`ReleaseSelector.admission_candidates`](../../../src/qs_ai/domain/governance/publication.py) 对 typology 只返回自身，没有量表的“精确→模型通配→全量表通配”候选序列。缺少 active Publication、禁用当前槽位或配置损坏，会明确拒绝，不能用量表发布补缺。

单主题与三主题共用这个模型 selector 槽位，scene 不在 selector key 里。一次新请求由当前发布 Profile 选择场景，已接受请求继续使用原 Publication。把单主题指针切到三主题发布，不会让先前已接单 Session 自动增加参考材料或换输出结构。

[`compile_configuration`](../../../src/qs_ai/infrastructure/persistence/mysql/execution_configurations.py) 重读原生成资产、与 Manifest/发布证据比对，再核对 Suite construction、input/output Schema 身份与上述组合。单主题必须 input v2/output v1，三主题必须 input v3/output v2；只改 Profile 标签或 Manifest 中的版本号不能绕过该检查。

文件输入 Schema 的来源也明确：[v2 Schema](../../../integrations/qs_server/schemas/ai-explanation-input-v2.schema.json) 与 [单主题清单](../../../integrations/qs_server/schemas/mbti-input-manifest.json)、[v3 Schema](../../../integrations/qs_server/schemas/ai-explanation-input-v3.schema.json) 与 [三主题清单](../../../integrations/qs_server/schemas/mbti-thematic-input-manifest.json) 均注明 qs-ai 编写。它们不是 QS 原始导出；[`load_input_schema`](../../../src/qs_ai/infrastructure/qs_server/input_schema.py) 按显式版本核对文件字节 SHA、schema_version 和 JSON Schema 自身合法性，缺失、摘要损坏、未知版本均失败。

正式发布执行读取数据库不可变资产和原冻结清单，不用初始化文件作为丢失资产的回退。JSON Schema 证明声明结构；四轴唯一性、两极顺序、类型与偏好一致、建议引用等仍由固定版本代码补充。评测案例的 provider projection 没有 QS report/outcome 来源，专用评测输入 validator 只检查合成投影，不能给它编造真实报告 ID。

## 从正确事实到正式成果，仍有接受边界

单主题输出继续遵守 output v1 的 Profile 数量、跨维度依据、标准建议来源及安全规则。一个 integrated insight 只引用 model_result，不能替代 Profile 要求的至少两个不同维度；提及与原 ISFJ 不同的 INTJ/ENTP 等类型标签，会被有限字符串检查拒绝为 `mbti_type_conflict`。此检查能识别类型字母冲突，不能识别所有自然语言改写错误。

三主题有自己的 output v2 和 reference 引用规则，成果封装 v2 在模型内容之外保留原选定参考正文与摘要。两者都必须从原 frozen input 和 ModelResponse 构建候选，并在 `finish` 接受事务中重新核对输入/来源/配置/响应及 Claim，详见前篇 [成果设计](01-解读冻结输入与成果设计.md)。

恢复也遵守原组合。[`JSONModelCallCodec`](../../../src/qs_ai/infrastructure/persistence/model_call_codec.py) 保存原 Profile definition、InputPolicy、PreparedExplanation、Prompt 消息和 Route；解码时按原 Profile schema 重建 policy，拒绝与原 Profile 不同的 eligible codes、norm/model-result 开关、scene 或参考投影。读到原 `response_received` 就复用，不加载当前参考包或再次调用；dispatched/unknown 继续未知结果边界。量表原 FrozenGeneration 的编码字节保持原路径，不因增加 MBTI policy 就默默改写旧回执。

## 三个失败例子确定责任边界

| 例子 | 当前证据 | 应如何处理 |
| --- | --- | --- |
| 历史标准报告只有 description 的“偏好强度 56%”，没有 pole_facts | QS 原报告可读，缺少 AI 必须的结构化事实 | 返回不适用；不能解析描述、查当前模型或使用合成快照补齐 |
| 四轴 preference 连成 ISFJ，model_extra 却是 INTJ；或 strength=`"56.25"` | 快照的权威事实彼此冲突/字段类型错误 | 解码拒绝；接单绑定映射到 `admission_input_invalid`，不创建模型 Job |
| 快照正确，但只发布了量表 Profile，或三主题绑定单主题 Schema | 无精确 MBTI 发布 / 固定生成组合错误 | 前者 `configuration_unavailable`；后者确定配置拒绝，不回落旧场景 |

输入解码的安全错误不携带原报告正文；确定输入/配置拒绝可持久重放，数据库/网络/commit 故障则应回滚原事务并按技术失败处理。合法快照、可用发布与结构合格输出是必要条件，仍不等于评测质量批准、参与者内容验收或上线生效。

## 代码与测试怎样查证

| 需要证明的行为 | 实现或固定入口 | 测试证明范围 |
| --- | --- | --- |
| 16 种类型、零值/小数、四轴身份、未知字段与来源拒绝 | [mbti_input.py](../../../src/qs_ai/application/interpretation/mbti_input.py)、[scenes.py](../../../src/qs_ai/domain/governance/scenes.py) | [test_mbti_contract.py](../../../tests/test_mbti_contract.py) 使用合成向量；保留原事实、不重新评分，错误不泄漏输入正文 |
| Profile v2/v3 和固定输入/输出组合 | [profiles.py](../../../src/qs_ai/infrastructure/qs_server/profiles.py)、[configuration compiler](../../../src/qs_ai/infrastructure/persistence/mysql/execution_configurations.py) | [test_evaluation_input_version.py](../../../tests/test_evaluation_input_version.py) 与 [正式输入版本集成](../../../tests/integration/test_published_input_version.py)；后者须隔离 MySQL，不证明当前已发布 |
| 三主题新增参考而保持原 facts | [mbti_themes_input.py](../../../src/qs_ai/application/interpretation/mbti_themes_input.py) | [test_mbti_themes_input.py](../../../tests/test_mbti_themes_input.py) 验证全部类型、原参考选择、各冻结投影篡改与恢复不重发 |
| MBTI 不能换绑量表；原回执和原响应可重建 | [preparation.py](../../../src/qs_ai/application/interpretation/preparation.py)、[model_call_codec.py](../../../src/qs_ai/infrastructure/persistence/model_call_codec.py) | [test_mbti_runtime.py](../../../tests/test_mbti_runtime.py) 检查恢复替身、Artifact 封装、类型与维度引用、量表原请求字节 |
| Go 投影真实字节被 Python 接受且事实不变 | QS [快照投影](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/snapshot_mbti.go)、[PoleFacts](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/domain/interpretation/report/pole_facts.go)；[固定互操作 CI](../../../.github/workflows/ci.yml) | [test_mbti_snapshot_interop.py](../../../tests/test_mbti_snapshot_interop.py) 消费 Go 导出的向量；未提供 `QS_AI_SNAPSHOT_VECTOR_OUT` 时会 skip，不能把普通本地测试当成互操作通过 |

CI 的报告投影 checkout 固定为 QS `e4c93f7a9ea68ec0a00e34a682f959dc69a895c7`，运行 `TestMBTISnapshotContractVector` 导出合成向量后交给 Python；它与 MQ 互操作的另一 checkout 分开。本篇另核查当前 QS [报告源解析](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/interpretation/reportsource/resolver.go)，与上述快照投影和 PoleFacts 一同界定来源规则。

这些证据保护协议和实现：生产是否有完整 PoleFacts、根资产是否安装、哪份 Publication 已生效、供应商回答是否忠实、QS 是否已保存成果、参与者页面是否可用，须分别绑定实际报告、发布、Run/Invocation 和验收记录核证。
