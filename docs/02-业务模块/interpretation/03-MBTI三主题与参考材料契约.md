# MBTI 三主题与参考材料契约

三主题解读以同一份标准报告为事实依据，把性格理解、职业探索和关系沟通一起交付。报告中的 `ISFJ`、四轴方向和强度属于本次测量结果；“可以试着先整理想法再沟通”来自有限参考材料和探索任务，不能写成用户已经具有的经历或能力。系统把这两类依据分别冻结、分别引用，再把原参考正文随成果交给 QS。

本文沿用 [事实与版本契约](02-MBTI报告事实与版本契约.md)中的合成 ISFJ 报告，说明材料如何被选中、文案怎样引用、成果如何接收。示例不是生产报告或已批准文案。qs-ai 源码按 `766b2aa` 核对，QS 接收与读取按本地 `2ccc2de44` 核对；这里只确认代码及契约，不据此推断部署、真实模型质量、发布或设备展示状态。

## 同一事实输入，增加三个主题和独立参考区

本场景仍限定 `MBTI_OEJTS / v64-report-202608-v1`、单人、单次测评、单份标准报告。三主题由一次生成调用输出，不为每个主题创建 Session、Run 或另一次模型请求。

| 边界 | 三主题固定值 | 含义 |
| --- | --- | --- |
| 报告快照 | `qs-report-snapshot/v2` | 四轴、类型和来源与单主题相同，没有升级测量算法 |
| Profile / scene | `ai-explanation-profile/v3` / `mbti-single-assessment/v2` | 原接受配置显式携带有限参考包，选择三主题路径 |
| 规范输入 / 模型输出 | `ai-explanation-input/v3` / `ai-explanation-output/v2` | 输入增加 reference_material；输出改为三个固定 section |
| 成果封装 | `qs-ai-artifact/v2` | 原内容之外，携带服务端生成的选中参考正文及摘要 |

[`assemble_mbti_themes`](../../../src/qs_ai/application/interpretation/mbti_themes_input.py) 先复用单主题组装器，逐字段保留 `facts`，再根据报告 `model_result.type_code` 选材料。规范输入保留 source、Profile 身份、context、facts 和 reference_material；供应商数据消息只含 `context / facts / reference_material`。通用材料不会写入 facts，也不会补进标准报告建议。正式 Session 的 workflow 仍是 `qs-published-snapshot-v2`；评测 Suite 的 construction 标记为 `qs-published-snapshot-v3`，二者用途不同。

输出顶层只有 `schema_version / scene_contract_version / summary / sections / limitations`。完整 [输出 Schema](../../../integrations/qs_server/schemas/ai-explanation-output-v2.schema.json) 规定：

| 位置 | 字段与数量 | 业务内容 |
| --- | --- | --- |
| summary | content、basis=report_fact、1–6 条 evidence_refs | 概括本次报告，不能增加参考引用或模型自报的来源 |
| sections | 恰好三节，依次 personality、career、relationships | 自我理解；任务和工作环境探索；表达、倾听与共同协商 |
| 每节 insights | 2–3 条；title、content、basis、两类 refs | 有解释正文，不能只换标题复述同一段四轴文本 |
| 每节 reflection_questions | 1–2 条；question、basis=exploration、两类 refs | 让本人核对符合及不符合的实际情境 |
| 每节 actions | 1–2 条；title、goal、1–3 个 steps、basis=exploration、两类 refs | 具体、可选择、可撤销的尝试 |
| limitations | 1–5 条非重复短文本 | 本次结果、通用参考、非诊断及非确定性边界 |

Schema 拒绝未知字段、空白及 `<`、`>`；title 最长 160 字符，content 最长 600，问题、目标、步骤及限制最长 300。每个条目最多六条报告引用、四条参考引用。当前根 Profile 仍将整个模型输出限制在 8000 字符内，Route 的 token、超时和容量限制也继续生效；增加主题不增加额度。Profile 中保留的旧 insight/suggestion 配置不把新版 sections 改回旧 v1 列表规则。

## ISFJ 为什么选出这十二条材料

原报告保存 EI=I、SN=S、TF=F、JP=J。选取函数只按这些方向过滤，既不按强度排序，也不因为 SN 强度只有 6.25 就删去 S 材料。当前根包为三个主题 × 四轴八端，共 24 条；每个主题留下四条，共 12 条：

| 原报告依据 | personality 条目 | career 条目 | relationships 条目 |
| --- | --- | --- | --- |
| `dimension:EI`，I | `personality.ei.i` | `career.ei.i` | `relationships.ei.i` |
| `dimension:SN`，S | `personality.sn.s` | `career.sn.s` | `relationships.sn.s` |
| `dimension:TF`，F | `personality.tf.f` | `career.tf.f` | `relationships.tf.f` |
| `dimension:JP`，J | `personality.jp.j` | `career.jp.j` | `relationships.jp.j` |

例如 `career.ei.i` 的正文讨论独立准备与讨论交替时哪种安排有助于投入任务，来源为 `preferences / careers`，边界要求结合兴趣、技能、价值观和现实条件，不能推断岗位适配、招聘结论或职业成就。`relationships.ei.i` 讨论“如果希望先整理想法，怎样约定稍后继续沟通”，不能据此假定用户已有伴侣、正在冲突或需要某种配对。

[`mbti-reference-material/v1`](../../../src/qs_ai/domain/governance/mbti_references.py) 的完整包包含版本、固定模型身份、sources 和 entries。每条 entry 有 `entry_id / topic / axis / pole / content / source_ids / usage_boundary`；每个 source 有 `source_id / title / url / accessed_on / support_scope`。材料校验要求：

- 24–48 条唯一条目，完整覆盖三主题、四轴的八个方向；一条只能绑定一个主题、轴和方向。
- 1–8 个唯一来源，每条连接 1–3 个存在的来源，每个来源都被使用。URL 要是无账号信息的 HTTPS 地址，查阅日是规范日期；校验不访问网页，也没有批准字段。
- 正文最长 1000 字符，使用边界最长 500；规范化材料 JSON 及整个 Profile 分别受 128 KiB 上限约束。

“每次十二条”是**当前根包**的性质，通用解码器允许同一主题、轴和方向有多个不同条目，选择函数会全部保留。QS 接收端另要求选中条目为 12–24 条，且覆盖三主题 × 四轴；AI 允许的任意 24–48 条包并不自动满足这条下游数量限制。当前根每型恰选 12 条，与 QS 兼容；新材料必须同时验证双方契约。

选中对象采用 `mbti-reference-selection/v1`，保留材料 version、模型、type_code、选中 entries 和被使用的 sources。解码器按 source_id、entry_id 排序，条目的 source_ids 也规范排序；`canonical_json` 使用键排序、UTF-8、无多余空格的 JSON。摘要是这段 JSON 字节的 SHA-256，投影额外加入 `fingerprint`，摘要正文自身不包含 fingerprint。

当前 `three-topic-v1` 根的 ISFJ 选择含四个来源、12 条材料，规范正文为 7149 个 UTF-8 字节，摘要为 `sha256:8623135a4e7d6c17022b45c5e7f4c6f9efd12dc5519ca42f30a5f5ad83d3572e`。这是从固定根计算出的参考子集身份，不是报告摘要、整份 Profile 摘要或审核结论。

## 文案必须说明自己依据什么

报告引用是对象 `{kind, ref}`；材料引用是字符串 `reference:<entry_id>`。模型只输出稳定 ID，不输出 source_id、来源标题、URL 或“已审核”标签。服务端沿 entry.source_ids 解析冻结 source 元数据。

| basis | 可以写的内容 | 引用义务 |
| --- | --- | --- |
| report_fact | “本次报告类型为 ISFJ；SN 轴偏好为 S，强度为 6.25。” | 至少一条本次真实报告引用；reference_refs 必须为空 |
| general_reference | “S 的通用释义关注具体信息与已有经验；这可以作为核对自己的线索。” | 真实报告引用与本主题适用材料引用都要有；措辞不能说这是已测出的能力 |
| exploration | “可以比较先看实例和先看整体两种学习顺序，记录各自帮助与困难。” | 两类引用都要有；情境是可尝试的假设，不能当作已发生经历 |

summary 只允许 report_fact。Schema 也允许 insight 使用 report_fact，但当前根 Prompt 要求 section 的 insights 只用 general_reference 或 exploration，把报告概括放在 summary。核对问题与行动始终为 exploration；不能将一个问题标为“测量事实”。

下面是 personality 节的一条 insight 片段，**不是完整输出**。它有解释、核对及反例空间，SN 的弱偏好没有被改写成固定标签：

```json
{
  "title": "用具体信息核对自己的理解方式",
  "content": "本次 SN 轴偏好为 S，强度为 6.25。S 的通用释义关注具体信息与已有经验；你可以把它作为核对线索，看看理解新内容时实例是否有帮助，也记录先看整体更有帮助的情境。",
  "basis": "general_reference",
  "evidence_refs": [{"kind": "dimension", "ref": "dimension:SN"}],
  "reference_refs": ["reference:personality.sn.s"]
}
```

career 节可以据 `dimension:EI + reference:career.ei.i` 提出“先独立写两点想法，再参与一次讨论，比较哪一环节有帮助”；relationships 节可以据 `dimension:EI + reference:relationships.ei.i` 提出以下行动。它没有假定存在伴侣或冲突，也没有替别人决定沟通方式：

```json
{
  "title": "试着约定思考与继续沟通的时间",
  "goal": "如果需要先整理想法，在对方同意的前提下保留继续交流的机会。",
  "steps": ["遇到需要讨论的小事时，先询问对方是否方便稍后继续。", "双方愿意时约定一个时间，之后核对这种安排是否有帮助。"],
  "basis": "exploration",
  "evidence_refs": [{"kind": "dimension", "ref": "dimension:EI"}],
  "reference_refs": ["reference:relationships.ei.i"]
}
```

引用存在还不代表陈述得到支持。把第一段改成“你善于执行细节，因此适合会计”，两类 ID 仍可能存在，却越过材料边界并制造能力/岗位适配结论。把关系段写成“你与某类型更匹配”也没有报告或来源支持。这类问题必须进入语义评测与人工审核，不能靠追加免责声明抵消正文违规。

## 引用校验检查到哪里，质量检查从哪里开始

[`validate_mbti_themes_output`](../../../src/qs_ai/application/interpretation/mbti_themes_output.py) 使用原 PreparedExplanation，不读当前参考目录：先核对 frozen provider payload 等于原 Profile 选中的材料，再解析 summary 与各节条目。具体失败有不同含义：

| 输出问题 | 本地失败类别 | 校验依据 |
| --- | --- | --- |
| 两个主题、错顺序、必需引用为空、report_fact 带参考、模型附 source_url | `output_schema_invalid` | 完整 output v2 Schema 的字段、数量和 basis 规则 |
| `suggestion:missing` 或不存在的报告引用 | `unresolved_evidence` | 原 facts 中实际可用的维度、标准建议、总体和类型结果引用 |
| ISFJ 引 E 条目；personality 引 career 条目；未知 entry | `unresolved_topic_reference` | 本次选中集合与 section.topic |
| 引 `personality.ei.i`，只附 `dimension:JP` | `unresolved_topic_reference` | 每条参考必须有对应轴 `dimension:EI` 或 `model_result` 支持 |
| 输入材料正文、来源或摘要与冻结 Profile 不符 | `input_reference_invalid` | 重新选择并逐值比较整个 reference projection |
| 文中另出现 INTJ 等不同类型标签 | `mbti_type_conflict` | 有限类型标签检查；不是对所有自然语言错误的识别 |

同一个条目可引用多个材料，但每一条都分别需要对应轴或 model_result。只有 overall_result、standard_suggestion 或不相关轴不满足该义务。对应轴有据，并不自动证明相邻文案忠实于该条材料；基础安全检查也只覆盖已声明的禁词类别和限制语句。

供应商结构化输出与本地校验也不同。当前 [DeepSeek Responses 适配](../../../src/qs_ai/infrastructure/qs_server/deepseek_request.py) 把 prefixItems 的三节顺序投影成通用 section 数组，把 oneOf 投影成 anyOf，并省略部分完整 Schema 约束。返回结果仍由本地完整 Schema 检查顺序、数量和互斥条件；远端接受请求不能代替这道检查。

三主题根 Suite 保留七组 × 五候选和一个预检，候选计划为 35 个。每个生成候选经独立裁判评测；若所有生成与裁判调用都正常完成，对应 35+35 次模型调用，预检是本地检查。七组样例不代表全部十六类型的实际内容质量已经验证。

根 [语义 Prompt](../../../integrations/qs_server/evaluation/mbti-themes/semantic-v1.md) 新增六项硬义务：三个主题有实质解释、引用支持相邻声明、事实与材料区分、职业仅作探索、关系仅作沟通、问题行动具体。它仍要求不得改测量/补事实、弱偏好措辞匹配、避免身份本质化与数据注入等共同义务。五个裁判评分字段保留；此场景的 cross_dimension_quality 指三主题解释质量，不要求旧版每条至少两轴。高评分不能抵消硬义务失败，真实审核与发布仍按 [评测证据链](../evaluation/01-评测候选门槛与审核设计.md)进行。

## 冻结字节怎样进入成果和恢复

生成、参考与成果不是一段可以相互替换的“原文”：

| 持久值 | 如何形成 | 摘要/恢复用途 |
| --- | --- | --- |
| ModelCall.request_json | 编码原 FrozenGeneration：原 Profile/策略、规范输入、provider payload、Prompt 消息、Route、Schema 等 | 恢复时核对原配置与三个输入投影，禁止用当前资料替换 |
| ModelCall.response_json | 编码 ModelResponse：raw_output、validation_output、normalization、调用/请求/模型身份、用量等 | 保存适配器返回的结构化响应记录；不是原 HTTP 响应的逐字节抓包 |
| Artifact.content_json | validation_output 经完整解析、引用和安全检查后，以 UTF-8 紧凑 JSON 再序列化 | content_fingerprint 对这段实际字符串字节求 SHA-256；不直接 hash raw_output |
| Artifact.reference_material_json | 从原冻结 Profile 按原报告类型选择，生成 mbti-reference-selection/v1 canonical_json | reference_material_fingerprint 对这段规范字节求 SHA-256；不是初始化文件的原字节，也不含投影里的 fingerprint 字段 |

[`build_artifact`](../../../src/qs_ai/application/execution/artifact.py) 先从原 EvidenceSet 重组输入，必须与已冻结组装结果相同，再绑定响应身份与内容。成果保留原 report_id/source_version、Profile/Prompt/Route/input 摘要及验证器版本。三主题验证器为 `qs-ai-output-mbti-three-topic/v1`，Artifact v2 额外保存选中材料字符串和摘要，而来源 URL 始终位于这个服务端字段中。

接受成果的 [`execution.finish`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 在有效 claim 下，用持久 request/response 重建候选并要求完全相等；整个 Artifact 还受 128 KiB 投递上限约束。通过后将 payload 写入 `interpretation_artifacts`，与 Session complete、Run/Job 完成和结果事件写入同一事务。表对 session_id 与 run_id 各有唯一约束，不能给同一个执行追加一份不同参考正文的成果。

例如响应已经保存、Worker 在成果提交前退出，接手 Worker 解码原 request 和 response，重新构造同一成果，不查询当前 Profile/网页，也不再次调用模型。Codec 拒绝 Profile/策略、canonical input、provider payload、数据消息或 input fingerprint 被篡改。只有 dispatched/unknown、未保存响应时，恢复保留 provider_result_unknown；已保存 failed 则复用原 failure_code。缺少材料不会回落单主题或当前目录。业务重试是另一条受控命令，不能由恢复自动授权。

## QS 接收、参与者读取和页面展示各证明什么

QS 的 [ValidateArtifact](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/artifact.go) 核对 completed 事件、Session 关联、各类摘要及 content_json 字节摘要。output v2 还要求正确验证器身份和完整 Schema；Artifact v2 必须配 output v2，并通过 [参考校验](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/mbti_references.go)：

- 参考字符串非空且不超过 128 KiB，其摘要必须等于收到的字符串字节；版本、模型、类型、来源字段、12–24 条适用材料与完整主题/轴覆盖均合法。
- 输出中的 reference ID 存在于该选择、属于对应主题，每条带对应轴或 model_result；source 链接均存在、无重复和未使用来源。
- 这些检查不访问公开网页，不批准材料理论，也不重新生成原报告或独立证明其语义质量。原 Profile/事实选择的一致性由 qs-ai 的冻结链负责。

QS 为历史兼容仍允许无参考字段的 Artifact v1 承载通过 Schema 的 output v2；当前 qs-ai 三主题生产路径总是构造 Artifact v2。不能将“output v2 可解析”当成“成果已携带冻结来源”。维护接收测试时要同时检查 envelope、内容和参考字段。

QS [MySQL 接收事务](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/infra/mysql/aibridge/store.go) 再核对原 request 的 actor、testee、绑定 Session、assessment_id、report_id 和 source_version；事件 ID 或 request/version 重复必须有相同 payload hash。请求投影、Inbox 和业务回执由宿主接收事务提交。AI 完成、Broker 确认与这笔 QS 业务接收不是同一个节点，消息投递边界见 [可靠消息](../../03-基础设施/messaging/README.md)。

参与者读取先复核当前访问权及原请求归属，再返回已保存结果。[apiserver gRPC](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/transport/grpc/service/participant_ai_explanation.go) 返回 content_json、artifact_id、report_id/source_version 以及原 reference_material_json/fingerprint；[collection REST 对象](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/collection-server/port/aiexplanation/workflow.go) 分别暴露 `content` 与 `reference_material` 对象，不把内部 Artifact 全部返回给页面。

页面解析 reference ID → entry → source_ids → source，可以展示原材料正文、来源标题/地址、accessed_on、support_scope 与 usage_boundary。读取接口已有这些字段只证明数据可达，不能证明小程序已经显示正确、三个主题可读或引用点击正常；正式设备展示需要另行验收。2026-10-02 是根资料记录的历史查阅日期，不是本次文档改写重新核查网站的日期。

## 资料更新与验证范围

参考包嵌入不可变 Profile，改正文、来源或边界都会改变 Profile 及相关输入身份。普通 Profile 注册和 Suite 派生只能继承原场景与完整材料；不能把旧单主题重新贴为三主题，也不能替换参考后沿用旧案例。需要更新材料时，应建立新的受控根和匹配案例，重新完成评测、审核与发布；原 Session、响应和成果继续使用各自原版本。两个 MBTI 根共用同一模型 selector 的 publication 槽，切换新版会影响后续准入，原已接受任务仍绑定原配置，详见 [模板与方案来源契约](../governance/02-MBTI模板与方案来源契约.md)。

输出 Schema 权威来源为 QS `api/schema/interpretation/ai-explanation-output-v2.schema.json`，原引入提交为 `bc7e1af6ab61ea553b082fb8013655ba5ed5c461`。2026-10-06 本地比对上述 QS 基线与 qs-ai 镜像逐字节一致，SHA-256 为 `db6ba195348ea9d48cf0d321ed31efc2deb27a4d0a81a61a06fe955fb615b34b`。既有 schemas/manifest.json 只覆盖 v1，不能拿它证明 v2 镜像或当前部署。

| 现有测试 | 直接证明的范围 | 不能据此宣布的结果 |
| --- | --- | --- |
| [reference](../../../tests/test_mbti_references.py)、[input](../../../tests/test_mbti_themes_input.py) | 16 种方向选择、事实不变、严格字段、材料变更摘要、冻结篡改拒绝、原响应恢复不外发 | 理论资料有效、真实用户事实正确、已部署 |
| [Schema](../../../tests/test_mbti_theme_output_contract.py)、[output](../../../tests/test_mbti_themes_output.py) | 数量/顺序/basis、主题/方向/依据引用、Artifact v2 参考及重建一致 | 文案被来源实质支持、职业/关系质量通过 |
| [root](../../../tests/test_mbti_themes_root.py)、[evaluation](../../../tests/test_mbti_themes_evaluation.py)、[registration](../../../tests/test_mbti_themes_registration.py) | 固定根字节、35 槽位、六项义务保持 pending_semantic、裁判输入与派生保护 | 裁判实测通过、真实人工批准或 publication 生效 |
| QS mbti_output/mbti_references、participant_ai_explanation 与 collection ai_workflow_client 测试 | 接收结构、参考摘要/适用性拒绝、原字段读取与透传 | 真实 MQ 业务事务提交或正式小程序呈现 |

维护时按“原资料身份 → 选择及两类引用 → 模型/成果字节 → QS 接收与读取 → 实际语义与设备展示”逐层保留证据。有限引用结构便于复现错误，但内容支持和发布资格仍需要自己的真实证据。
