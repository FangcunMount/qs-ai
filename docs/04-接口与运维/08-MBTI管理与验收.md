# MBTI 管理与验收

三主题 MBTI 开放需要串起五份证据：精确模板可用、冻结方案完成评测与独立审核、共享发布指针指向该批准版本、参与者原请求被 QS 持久接收、正式客户端读到同一成果及其原参考正文。安装成功、合成测试通过或容器健康，只能证明其中一层。

本文以一次 `MBTI_OEJTS / v64-report-202608-v1` 标准报告为对象。三主题是自我理解、职业探索、关系沟通；不支持招聘适配、配对、诊断、多次报告趋势。核对日期为 2026-10-06：AI 业务源码 `766b2aa`、QS `2ccc2de`、IAM `c8fc1fe`、Operating `8eefb9e`、小程序 `681cd05`。以下是受控操作的参数和判定规则；示例身份、版本必须用目标环境读回值替换，执行记录另绑定具体环境与日期。

## 1. 先确认调用方与环境能够完成现行契约

管理请求从 QS `/internal/v2/interpretation/ai-workflow` 进入。QS 用登录 access token、当前 active Operator 成员资格和 IAM 无条件权限形成组织/操作者 scope；读方案/评测/发布需要 AuditInterpretation，写方案、双职责审核和发布需要 OrgAdmin。AI 收到 `qs-apiserver.svc` mTLS 委托后再核对资源、版本和规则。浏览器 JSON 不能指定 actor、组织或审核时间，初始化 CLI 的 imported_by 也不携带这些在线权限。委托来源见 [治理接口](03-治理接口与可信委托.md)。

开始前应保存目标环境、各服务/客户端构建、操作者范围、数据库迁移与 MQ 配置、原 MBTI 和量表 publication 读回。readiness 或 mTLS 探针成功不能证明用户写权限、模型额度或客户端兼容；部署前置条件见 [部署迁移](05-部署迁移与兼容回滚.md)。

当前源代码中有明确的 **Operating 停止点**，不能按旧页面文案直接宣称三主题管理可用：

| 现行调用方代码 | 实际行为 | 验收停止点 |
| --- | --- | --- |
| [SolutionWorkspace][ops-solutions]，`/operations/ai-governance/solutions?aiScene=mbti` | 目录只接受 scene `mbti-single-assessment/v1`；出现三主题 v2 项会报“首版模板目录不完整”；无发布时按 selector 取第一项 | 该构建不能正确选择三主题模板；反复刷新或安装模板不能修复消费者契约 |
| [评测 hook][ops-evaluation] | Start/Cancel 正文没有 command_id，并把响应按同步 EvaluationState 处理 | QS 现行执行入口要求命令 UUID，返回 202 operation；需兼容构建才可验收页面启动/取消 |
| [冻结输入阅读][ops-input]、[候选阅读][ops-candidate] | 分别只识别 input v1、output v1；三主题使用 input v3、output v2 | 原 JSON 回退不是可审阅的三主题工作区，不能据此完成真实内容审核验收 |
| [小程序 API][client-api]、[三主题组件][client-content] | 已有 MQ 提交、output v2、原参考选择集与展开正文代码 | 可进入受控接入验证，但仍须绑定实际正式版本并真机核验 |

下文给出后端已实现的准确步骤，供具备现行契约的受控调用方取证。发现上述缺口时停止对应 UI 验收并登记具体构建，不通过改库、伪造 scope 或转调旧 gRPC Start 绕过。

## 2. 安装固定根，并从目录核对三主题来源

先确认固定依赖已经存在且指纹相符：执行策略 `release-evaluation-bounded-recovery/v2`、门槛策略 `release-gates/v2`、语义输出 Schema v1、生成 Route `balanced_text_v1/v8`、裁判 Route `semantic_judge_v1/v5`。[安装器](../../src/qs_ai/bootstrap/import_mbti_assets.py)复用这些资产，不创建模型 Route、额度或 publication。依赖缺失或内容变更时应停止，不能替换成目录中的另一个同类资产。

三主题维护命令的有效参数形状为：

```sh
python -m qs_ai.bootstrap.import_mbti_assets \
  --root three-topic-v1 \
  --source-commit 766b2aa2ed20e30b886f6cdcbcfdb5dfccf6821b \
  --imported-by maintenance-ticket:MBTI-001
```

执行者必须先绑定实际待安装构建与获授权维护记录。`source_commit` 只校验 40 位小写 hex 并记 provenance，不自行证明提交存在或构建字节来自它；`imported_by` 是记录字符串。省略 root 会安装旧 `v1` 根，不会升级为三主题。

安装器核对固定 manifest/文件字节，在一笔 SERIALIZABLE 事务内注册并完整校验。成功输出 `import=complete`、实际 inserted 数和 `activated=false`；同身份同内容重复不刷新首次 provenance，同身份异内容或晚期校验失败整笔回滚，失败输出 `import=failed`、error_type 并退出 1。不要把 inserted=0 当失败，也不要把 complete 当已上线。细节见 [模板来源契约](../02-业务模块/governance/02-MBTI模板与方案来源契约.md)。

随后 GET `/internal/v2/interpretation/ai-workflow/solutions`，必须在 `data.templates` 读到以下精确项：

```json
{
  "name":"MBTI 三主题单次解读首版",
  "template_ref":{"id":"participant-mbti-single","version":"three-topic-v1","fingerprint":"sha256:027fbe31e18e08786390920dfc1233e76e7a54ac68198385f6391c8634ce2a4b"},
  "scene_contract_version":"mbti-single-assessment/v2",
  "selector":{"audience":"participant","model_kind":"typology","decision_kind":"pole_composition","model_code":"MBTI_OEJTS","model_version":"v64-report-202608-v1"},
  "published":false,
  "reason":"requires_evaluation_review_and_publication"
}
```

目录项来自 DB 原资产的完整校验，没有 installed=true 字段。已知根未安装时目录省略该项，直接 Create 返回 INVALID_ARGUMENT；未知根引用返回 NOT_FOUND。根行在而依赖损坏会使读取失败，不使用部署文件或旧模板补齐。目录里的 published=false 是“此模板来源需评测审核发布”的标记；当前是否发布须另查 publication 指针。

## 3. 创建方案、冻结，再启动原 Run

首次采用三主题须显式选择上述 template_ref。旧单次 v1 来源不能通过 Save 改成三主题；同槽位已有旧 publication 时，也不能盲目从它克隆后声称完成升级。publication 或本机构 Run 来源可用于其自身场景的修改版本，三类来源必须恰选一类。

例如客户端先持久保存方案和 Create 命令 UUID，再发：

```http
POST /internal/v2/interpretation/ai-workflow/solutions/00000000-0000-4000-8000-000000000010/create
Authorization: Bearer <具备当前 OrgAdmin 的 access token>
Content-Type: application/json

{"command_id":"00000000-0000-4000-8000-000000000011","title":"MBTI 三主题方案","reason":"建立待评测的三主题首版","template_ref":{"id":"participant-mbti-single","version":"three-topic-v1","fingerprint":"sha256:027fbe31e18e08786390920dfc1233e76e7a54ac68198385f6391c8634ce2a4b"}}
```

读回 source.template_ref、scene、Profile/输入/输出与生成/裁判身份。允许的 Prompt 和模型参数修改通过 Save 的方案 revision 与 Prompt revision CAS；参考包、Profile 场景不在可变字段内。保存后刷新当前 head，Prepare 用独立 command_id 和实际 expected_revision。若本例刚创建未保存、revision=1：

```json
{"command_id":"00000000-0000-4000-8000-000000000012","expected_revision":1,"reason":"冻结三主题方案用于完整评测"}
```

这是 POST `/solutions/{solution_id}/prepare` 的正文，不需要 confirm。读回 `prepared.run_id`、完整 release/Manifest 和 `prepared.plan`；本例稳定 Run 为 `f4799033-c25d-5ec5-ba48-529d5b314214`，由 uuid5(solution_id, "evaluation") 生成。计划子对象应为：

```json
{"generation_case_count":7,"candidates_per_case":5,"candidate_count":35,"preflight_case_count":1,"max_generation_invocations":70,"max_semantic_invocations":70}
```

这里是一项本地预检和 35 个候选槽位。正常无恢复时 35 次生成 + 35 次裁判；两个 70 是各阶段全 Run 上限，不能理解成已调用 140 次。Prepare 只创建 requested Run、冻结资产，尚未付费执行；prepared 方案不可继续编辑。首次来源、双 revision、事务与失响应回执见 [方案设计](../02-业务模块/governance/01-方案资产与原子准备设计.md)。

确认计划、当前模型能力和额度后，GET 原 `/evaluations/{run_id}`；仅当实际 requested/version=1 时，POST `/evaluations/{run_id}/start`：

```json
{"command_id":"00000000-0000-4000-8000-000000000021","expected_version":1,"reason":"执行已核对的三主题评测计划","confirm":true}
```

QS 返回 202 `data.status=submitted`、operation_id/command_id 与 status_url。先按原 status_url 读 AI 首接单决定，再读同 Run 状态、slot/执行证据。202 不是 collecting，更不是评测通过。Create/Save/Prepare 超时查 `/solutions/commands/{command_id}`；Start 超时查原 operation，不换命令 UUID。版本冲突先读回，不能自动把 expected_version 改大后再发。

## 4. 完成评测与两类独立审核，再固定结论

候选阅读必须同时打开原冻结报告、该候选、原执行及语义逐条决策。三主题冻结组合应为 Profile `ai-explanation-profile/v3`、scene `mbti-single-assessment/v2`、input `ai-explanation-input/v3`、output `ai-explanation-output/v2`；报告来源仍由 QS 标准报告产生，不从生成文案重建事实。[报告与版本](../02-业务模块/interpretation/02-MBTI报告事实与版本契约.md)、[三主题参考契约](../02-业务模块/interpretation/03-MBTI三主题与参考材料契约.md)说明具体字段。

| 读回情况 | 需要保存的原证据 | 停止/推进规则 |
| --- | --- | --- |
| 未齐 35 个槽位、预检失败或 unresolved unknown | SlotPlan、checkpoint/claim、dispatch/receipt、预算和最后 transition | 不进入发布；unknown 先核证原结果并走显式处置，不能重复 Start 重发原调用 |
| 内容有效但语义硬断言/分数不达标 | candidate_id、原 normalized_output、语义 execution/逐条 obligation、G4 结果 | 质量失败不授权替换生成；修改方案需要新冻结 Run，不能用技术重试筛掉差候选 |
| 明确可恢复的技术失败 | 原 stage/code、尝试 ordinal、预算与既有恢复授权 | 只用支持的受控恢复；生成和裁判恢复规则不同，充值/改草稿不改变旧 Run |
| 候选齐、进入 awaiting_review | 同一 Run version 下的候选及原语义证据 | 分别完成 assessment_semantics 与 safety_product，不把“已有候选”当批准 |

35 个候选各需两条职责审核，共 70 条；同候选两个 actor 必须不同，目前两类写入均要求 QS OrgAdmin。单批一职责、1～35 个不同候选，actor/UTC 时间由服务端形成。逐项审核报告事实、各主题的引用支持、探索边界；委托 AI 代读要保留实际披露，不能代签成独立人工阅读。任一拒绝、缺审核或未闭合证据都会阻止批准。

先 GET `/evaluations/{run_id}/gates?expected_version=<当前版本>`，保存 G1～G4 和 passed；再以同 version、显式 expected_passed、reason、confirm 做 Finalize。服务端事务内复算，不能用 confirm 跳过硬断言、均值或审核。approved/rejected 只固定 Run 结论；ReopenReview 有有限条件、不会重跑生成。完整门槛、响应未知和取消排空规则见 [评测设计](../02-业务模块/evaluation/01-评测候选门槛与审核设计.md)、[排障与恢复](06-排障与受控恢复.md)。

## 5. 单独发布到共享 MBTI 槽位

先 GET `/publications`，query 完整携带 `audience=participant`、`model_kind=typology`、`decision_kind=pole_composition`、`model_code=MBTI_OEJTS`、`model_version=v64-report-202608-v1`。读取失败不能当“从未发布”；从未发布为 version=0/active_publication_id 空，停用后则为大于零的 version/空指针。

这是全局共享 selector，旧 v1 与三主题 v2 共用同一槽位，机构没有私人 active 指针。变更会影响后续匹配的新准入，必须明确其影响范围，并保存独立量表 selector 的原指针作为兼容对照。

Publish 正文各字段只从相应读回组成：

| 字段 | 取值来源 |
| --- | --- |
| command_id、reason、confirm=true | 客户端预先持久保存的本次独立发布决定 |
| expected.selector/version/active_publication_id | 刚读回的精确 MBTI 槽位，不猜 version、不把空指针当缺字段 |
| run_id、run_version | 原 approved Run 和其当前 version |
| release_fingerprint | 同一 Run 的原冻结 release 指纹；不能用 template fingerprint 代替 |

POST `/publications/publish` 后保存原回执、publication_id、指针版本、actor/time 与 release 关联，再 GET 当前槽位和该 history 版本核对。失响应先查 `/publications/commands/{command_id}`；CAS 失败停止并重新判断他人的变更，不能自动覆盖。Publish 会重读批准、候选/语义/审核与不可变资产，不只检查 approved 字符串。详见 [发布证据设计](../02-业务模块/publication/01-发布证据与生效指针设计.md)。

发布完成仍须做参与者预检和真实生成。已接单 Session 继续使用原 publication/configuration；后续 Publish/Disable 不把它换绑，也不赋予旧被拒绝请求新的可重试配置。

## 6. 参与者从当前来源发起，并核对同一原操作

使用具备当前 IAM ProfileLink/Testee 关系的专用参与者会话，通过正常 collection 入口读取：

```http
GET /api/v1/assessments/42/ai-workflows/source?testee_id=7
Authorization: Bearer <该参与者会话>
```

以 QS MBTI 合成测试中 report=99、Outcome=101、ContentSchemaVersion=report-content/v1 的身份为例，有可用发布时 data 形状为：

```json
{"status":"ready","report_id":"99","source_version":"report-content/v1:101","ai_eligibility":{"status":"available"}}
```

实际报告身份必须从授权读取取得；ready 只说明报告可冻结，ai_eligibility=available 才表示此次无预留的 AI 预检通过。`publication_missing`、额度/模型能力不可用或预检依赖故障应停止新发起，不能复制管理员凭据或直接提交自己的 MBTI 类型/四轴事实。即使预检 available，真正受理仍会重检。

客户端先保存稳定 request_id 和 source report，再 POST `/assessments/42/ai-workflows?testee_id=7`：

```json
{"request_id":"00000000-0000-4000-8000-000000000031","report_id":"99"}
```

QS 202 的 operation_id/command_id 应等于这个 request_id，status 为 submitted。按返回 status_url 可核对原命令决定；业务结果另 GET `/assessments/42/ai-workflows/{request_id}?testee_id=7`。当前小程序把提交显示为 pending，并轮询这个授权结果路径，**不跟随 status_url 查询命令操作**。其页面 pending/generating/generated 对应公开业务投影，不能用页面文字代替 Inbox 首决定。

超时、刷新、切后台再回来都保留原 request/report；相同提交和查询不得形成第二次供应商调用。源码中的本地指针按账号/Assessment/Testee 分区，保存失败不发送，旧账号和过期页面响应不得进入新页面。AI 在执行前及接受成果前回查 QS 当前访问；关系撤销时查询和后续接受均按当时权限处理。QS 不共享 AI activeRun。完整授权、原拒绝与 MQ 对账见 [QS 接入](02-QS接入与消息契约.md)。

## 7. 核验原成果、参考正文与正式设备展示

完成必须对上同一 request/report/source、artifact_id、AI 原 publication/configuration、dispatch/response receipt、成果事件与 QS 原业务 STORED。Broker confirm、consumer ACK、AI 本地 completed 和 QS 返回 completed 各是不同节点；原成果进入 QS 接收事务并产生准确 Stored ACK 后，才可报告结果交付。消息边界见 [可靠消息](../03-基础设施/messaging/README.md)。

公开结果须为 output v2，三 sections 按 personality/career/relationships，summary 的 basis 为 report_fact，主题内容区分 report_fact/general_reference/exploration；QS DTO 不直接暴露内部 Run、原供应商响应或完整 Artifact。检查 reference_material 是该成果冻结的 `mbti-reference-selection/v1` 原选择集；例如 ISFJ 应选择三个主题 × 四轴的 12 条对应材料，引用不得串到别的极性、类型或主题。

QS 校验 Artifact/参考字符串的原字节摘要和绑定，小程序再验证选择集结构、指纹格式与条目引用，**客户端不重新计算原字符串 SHA**。验证时保留服务端原摘要，不能解析重编码后重新算一个摘要替代。sources.accessed_on 是材料当时核对日期，不表示客户端刚访问或重新验证了外站。

| 正式设备步骤 | 预期可见证据 | 停止点 |
| --- | --- | --- |
| 正常报告入口打开同请求结果 | 标准报告仍可读；AI 标识、三主题、问题与行动、使用边界明确 | 只有原 JSON、空主题、错误类型或缺披露不能通过展示验收 |
| 展开“查看参考依据” | 原 content、usage_boundary、source title/url/support_scope/accessed_on；对应本项主题/四轴 | 对不上原选择集，或用最新参考包补旧记录，停止并查原 Artifact |
| 隐藏/返回、刷新、关闭后重进 | 同 request/artifact；不重复发起；报告变更后 source_state 标记旧成果 | 旧报告成果被包装成当前结果，或重复调用，停止 |
| 换无关账号、撤销/恢复关系 | 当前授权拒绝/恢复按真实关系生效；无旧账号内容闪现 | 管理 token 成功不能替代参与者授权验收 |
| 对照真实量表原路径 | 原量表指针及 input/output v1、展示与读取保持可用 | MBTI 槽发布不应成为量表行为改变的理由 |

新三主题结果缺原参考正文应先核对接收契约，不能只凭 component 渲染成功关闭验收。小程序对旧 v2 记录仍允许缺参考并明确显示“当前记录未保留完整参考正文与来源”，这是旧记录读取兼容，不是新链路的完成标准。

## 8. 暂停、恢复与证据缺口

暂停用精确 MBTI selector 的 Disable：原命令、预期 version/active ID、reason 和 confirm。回退用 Rollback 选择保留的 publication，重新核验原证据并形成新的指针版本；旧单次根和三主题根不能靠改库互转。停用后新准入不可回落量表，已有原接单工作/成果与当前访问权另行核验。

重启或恢复先绑定原 dispatch/receipt。已持久回执可以恢复同成果；无原结果的 dispatch 保持 unknown，替代尝试须风险确认和预算/规则授权。兼容镜像须在隔离环境验证三主题 Profile、输入、参考、成果和持久消息的解码；不得删除原证据或改状态强制成功。操作细节见 [部署迁移](05-部署迁移与兼容回滚.md)、[排障](06-排障与受控恢复.md)。

| 已有源码/测试证据 | 能证明的范围与限制 |
| --- | --- |
| [themes root](../../tests/test_mbti_themes_root.py)、[references](../../tests/test_mbti_references.py)、[input](../../tests/test_mbti_themes_input.py)、[output](../../tests/test_mbti_themes_output.py) | 固定字节、16 类型选择、无 fallback、原参考冻结/回执恢复、错误引用拒绝；本地契约测试 |
| [初始化集成](../../tests/integration/test_mbti_initialization.py) | 隔离 MySQL 的同内容重放、首次 provenance、冲突/晚期失败全回滚；无自动批准/发布 |
| [方案完整链集成](../../tests/integration/test_mbti_solutions.py) | 隔离 MySQL 下方案→35 候选/裁判→审核→发布→原冻结成果；生成/裁判及 user:42/43 审核是合成替身，不能证明真实质量或人工阅读 |
| [QS MBTI 投影][qs-snapshot-tests]、[成果][qs-output-tests]、[参考][qs-reference-tests]、[source][qs-source-tests] | 原报告事实/成果/参考绑定及授权先行；Go 合成报告不是生产报告 |
| [小程序 API 测试][client-api-tests]、[参考测试][client-reference-tests]、[内容组件测试][client-content-tests]、[页面生命周期测试][client-lifecycle-tests] | 原身份、MQ pending、引用解析、展开文本、账号/页面隔离；组件测试不是微信真机或正式发布验收 |

取证记录至少绑定环境/构建、Run/version/release、审核实际来源、publication/version、参与者 request/report/source/artifact 与 Stored、客户端正式版本/设备/时间。无法与原身份绑定的历史截图、日期记录或当前健康状态保留为 unknown；缺真实模型质量、独立人工审核、部署、参与者交付或设备展示中的任何一层，就报告该层未完成。

[ops-solutions]: https://github.com/FangcunMount/qs-operating-system/blob/8eefb9e0660bc1ffeef6dde31805a9b8caaa5623/src/pages/operations/ai-governance/workspaces/product/SolutionWorkspace.tsx
[ops-evaluation]: https://github.com/FangcunMount/qs-operating-system/blob/8eefb9e0660bc1ffeef6dde31805a9b8caaa5623/src/pages/operations/ai-governance/workspaces/native/useNativeEvaluation.ts
[ops-input]: https://github.com/FangcunMount/qs-operating-system/blob/8eefb9e0660bc1ffeef6dde31805a9b8caaa5623/src/pages/operations/ai-governance/workspaces/product/FrozenInputReading.tsx
[ops-candidate]: https://github.com/FangcunMount/qs-operating-system/blob/8eefb9e0660bc1ffeef6dde31805a9b8caaa5623/src/pages/operations/ai-governance/workspaces/product/CandidateReading.tsx
[client-api]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/services/api/aiExplanationApi.ts
[client-content]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/modules/assessment/components/ai-explanation/MBTIThreeTopicContent.tsx
[qs-snapshot-tests]: https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/snapshot_mbti_test.go
[qs-output-tests]: https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/mbti_output_test.go
[qs-reference-tests]: https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/mbti_references_test.go
[qs-source-tests]: https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/application/aibridge/participant_source_test.go
[client-api-tests]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/services/api/__tests__/aiExplanationApi.test.js
[client-reference-tests]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/services/api/__tests__/mbtiReferences.test.js
[client-content-tests]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/modules/assessment/components/ai-explanation/__tests__/AIExplanationContent.test.jsx
[client-lifecycle-tests]: https://github.com/FangcunMount/qs-collection-system/blob/681cd05f23418d028e61676e7158139078455f62/src/modules/assessment/pages/__tests__/ReportPageLifecycle.test.jsx
