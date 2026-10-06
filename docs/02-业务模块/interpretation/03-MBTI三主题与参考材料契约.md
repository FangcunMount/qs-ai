# MBTI 三主题输出契约

本文回答三主题内容、报告事实与通用参考如何区分，以及引用、成果与回执如何冻结。核对基线：`c977cb9`，2026-10-06；三主题契约、有限根资产、原生评测与冻结响应恢复已实现。具体部署、真实评测/人工审核/发布、正式参与者生成、授权与恢复及小程序展示须分别绑定环境和日期核证，本次状态为 unknown。合成测试不替代生产质量证据，不将现有任务转换为新版。

## 产品与字段

本场景一起提供性格特征与自我理解、职业发展探索、恋爱婚姻中的沟通与相处。适用模型仍为 `MBTI_OEJTS / v64-report-202608-v1`，单人、单次、单报告。

新输出为 `ai-explanation-output/v2`，场景为 `mbti-single-assessment/v2`。`summary` 只说明本次报告结果，带真实报告引用；`sections` 固定依次为 `personality`、`career`、`relationships`，每节 2–3 条解读、1–2 个核对问题和 1–2 条行动；`limitations` 保留使用边界。

每条解读含 `title`、`content`、`basis`、`evidence_refs` 和 `reference_refs`。报告引用继续沿用本次维度、类型结果、总体结果和标准建议。参考引用格式为 `reference:<stable-entry-id>`，只能指向该任务冻结的适用参考条目。

| basis | 语义 | 引用规则 |
| --- | --- | --- |
| report_fact | 本次标准报告实际提供的结果 | 至少一条真实报告引用，参考引用必须为空 |
| general_reference | 有来源的通用偏好解释，供用户核对 | 真实报告引用与适用参考条目引用都必须存在 |
| exploration | 从参考解释提出的情境探索，不是个体测量结果 | 真实报告引用与适用参考条目引用都必须存在 |

核对问题固定为 `exploration`，字段为 `question` 及两类引用；行动同样固定为 `exploration`，包含 `title`、`goal`、1–3 条 `steps` 及两类引用。文字须有实际内容，不能为空白或 HTML。

模型输出只携带稳定引用 ID，不填写来源 URL、来源标题或审核状态。成果封装保存原冻结的选取参考正文及摘要，下游据此提供来源元数据，不读取当前目录或外部网页，不信任模型提供的来源。成果封装为 `qs-ai-artifact/v2`，模型输出为 `ai-explanation-output/v2`，两者不是同一个契约。

## 参考材料与恢复

有限 Profile v3 JSON 正文保存固定参考包，复用现有不可变 Profile、组织范围、摘要与治理。新版 input v3 区分 `facts` 和 `reference_material`；网页或通用释义不能写入标准报告事实。

准备时按本次偏好选取适用参考并冻结选中正文、来源和引用身份。确定性检查必须验证引用存在、主题和偏好适用性；语义评测与实际人工审核必须验证解释受来源支持。Schema 本身不证明上述内容正确。

`domain/governance/mbti_references.py` 提供有限参考材料契约 `mbti-reference-material/v1`。正文只支持本地固定模型和四轴；每条条目绑定一个主题、一个轴与一个偏好方向，三主题的八种方向全部覆盖，24–48 条。来源最多八个，包含公开 HTTPS 地址、查阅日期及能支持的范围；材料及整个 Profile 均受既有 128 KiB 上限约束。没有批准字段，结构合法不等于来源或内容已审核。业务派生必须继承来源参考包；不能通过普通 Profile 注册修改参考或把旧场景升级为新场景。

按报告类型选取后的 `mbti-reference-selection/v1` 保存原版本、模型、类型、适用条目及其来源，生成独立摘要；不读取文件、网页或当前目录。引用校验拒绝未选中条目、主题不匹配及没有对应轴或类型结果依据的引用。真实报告依据的存在性仍由后续输入/输出校验负责，本模块不生产测评事实。16 类型测试使用合成内容，不构成生产理论素材或质量验收。

本场景不另建通用知识库、新的执行任务或供应商链路。三个主题仍由同一次模型调用生成，现有输出、超时和容量上限不自动增加。

## 冻结运行时与版本兼容

Profile v3 绑定固定 scene v2、input v3 和 output v2。输入复用原始四轴与类型投影，只增加单独的冻结参考区；旧 Profile、报告快照和消息字节不变。新参考数据只有在明确的新场景下才能进入 Prompt 数据消息，不改 native Prompt 包格式。

收到回执后从原请求恢复 Profile、参考正文、选取摘要和模型参数，不读取当前参考包。解码时核对 Profile 与策略、输入指纹、三份数据投影和原消息；成果构造仍重新绑定本次原报告，拒绝篡改及跨报告复用。缺少或损坏参考不回退旧版本或文件。

输出确定性门槛拒绝未知来源条目、错误主题、未选中偏好、缺失实际报告依据和不同类型标签。报告事实不能携带参考作为测量事实。语义支持、三个主题的实质内容及职业/关系边界仍需新版原生评测和人工审核，本地结构校验不能替代它们。

DeepSeek 请求继续使用原协议和资源限制。新输出的 `prefixItems` 在供应商可表达子集内投影为通用 section 数组，`oneOf` 投影为 `anyOf`；顺序、数量、独占条件和引用由本地完整 Schema 严格检查。不会修改完整 Schema 或旧 v1 请求格式。

评测输入新增明确的 `qs-published-snapshot-v3` 构造标记，按冻结套件的原 Profile 逐条核对参考正文、来源及摘要，不能临时替换材料。合成案例保持独立身份，不制造 QS 报告来源；新版 Prompt 渲染显式接收冻结参考区。套件派生保留来源场景、Profile Schema 及完整参考包，改变这些内容需要新根案例契约，不能重新贴标旧案例。七组×五候选及预检义务不变。代码接线不等于三主题语义与发布质量门槛通过。

## 兼容与验收

输出 Schema 权威来源为 QS 的 `api/schema/interpretation/ai-explanation-output-v2.schema.json`，原始源提交 `bc7e1af6ab61ea553b082fb8013655ba5ed5c461`；qs-ai 保存[逐字节镜像](../../../integrations/qs_server/schemas/ai-explanation-output-v2.schema.json)，该来源提交来自既有契约记录；现有 schemas/manifest.json 只覆盖 v1，不能当作 v2 来源清单，v2 字节一致性需要单独固定源版本核证。原始源提交标识契约来源，不代表当前部署版本。测试合成样例只验证结构，不能作为真实质量样本或已审核参考内容。

既有量表和 MBTI output v1、Profile、Prompt 和回执不变；旧场景继续拒绝新输出。新方案必须显式继承三主题根版本或同场景来源，经完整评测、审核及独立发布指针生效；不得用旧场景重新贴标。存在三主题代码或模板不代表任一组织、模型或版本已经获准使用。

必须保持独立的技术执行、语义质量、实际人工批准、业务发布和正式小程序展示证据。职业部分不推断能力或岗位适配，关系部分不推断伴侣及婚姻结果；主题内容不能只是同一四轴文本换标题。

## 根身份与语义义务

三主题根 Suite 为 `participant-mbti-single / three-topic-v1`，指纹 `sha256:027fbe31e18e08786390920dfc1233e76e7a54ac68198385f6391c8634ce2a4b`。完整版本组合见 [事实与版本契约](02-MBTI报告事实与版本契约.md)；模板采用完整返回引用，见 [模板与来源](../governance/02-MBTI模板与方案来源契约.md)。

当前初始化材料包含三主题、四轴八端共 24 条短参考，每个本次类型选相符方向的 12 条。根材料说明和适用范围见 [根资产说明](../../../integrations/qs_server/evaluation/mbti-themes/README.md)。这些是有限版本的本地固定材料；不能把历史查阅日期当作当前网页核证，也不能从公开材料推导本次用户具备未测量的经历、能力、适配或关系结果。

三主题原生套件固定 7 个案例 × 5 候选及一个预检。保留五项裁判字段，`cross_dimension_quality` 在该场景定义为三主题解释质量；原量表和旧 MBTI 定义保留。新增硬语义义务核对主题实质内容、参考忠实、事实与通用参考区分、职业探索、关系沟通、问题及行动可执行性。ID 存在和离线结构测试都不能证明义务通过。

## 设计选择与失败边界

采用嵌入 Profile 的有限参考包而非运行时网页/通用知识库，使原调用的来源和范围可重建，代价是资料更新必须新根、重新评测和审核。同一调用生成三主题复用原执行/容量边界，代价是输出空间和主题质量须在原限制下验证，不自动加额度。用模型稳定 ID 加服务端原参考正文，避免模型伪造来源元数据，代价是下游须兼容 Artifact v2。

缺少、损坏或篡改原 Profile/选取参考/输入指纹，主题或偏好不匹配、未选中条目、报告依据缺失均明确失败，不回落旧 output v1 或当前目录。原响应保存后可从原冻结材料重建成果，不二次外发模型；未保存响应的 dispatched/unknown 保留不确定状态。

## 实现依据与 Verify

源码：[有限参考与选择](../../../src/qs_ai/domain/governance/mbti_references.py)、[三主题输入](../../../src/qs_ai/application/interpretation/mbti_themes_input.py)、[三主题输出校验](../../../src/qs_ai/application/interpretation/mbti_themes_output.py)、[原回执解码](../../../src/qs_ai/infrastructure/persistence/model_call_codec.py)、[Artifact v2](../../../src/qs_ai/application/execution/artifact.py)、[Suite 版本](../../../src/qs_ai/infrastructure/qs_server/evaluation_suite.py)。

验证：[参考适用性](../../../tests/test_mbti_references.py)、[输入](../../../tests/test_mbti_themes_input.py)、[输出](../../../tests/test_mbti_themes_output.py)、[根](../../../tests/test_mbti_themes_root.py)、[原生评测](../../../tests/test_mbti_themes_evaluation.py)、[派生保护](../../../tests/test_mbti_themes_registration.py)。维护时分别核证版本镜像、完整参考摘要、主题与两类引用规则、原响应恢复、资产派生限制，再核证真实模型内容、实际双角色签名、独立发布、QS 业务接收和设备展示。
