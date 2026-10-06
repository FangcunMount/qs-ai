# 输入与输出机器规范

ai-explanation-output-v1.schema.json 原样取自 manifest.json 固定的 QS 提交，运行时校验文件摘要，CI 与固定 QS 工作区逐字比较。该 manifest 只覆盖原 v1 来源；qs-ai 自主 input v2/v3 由各 MBTI manifest 固定，output v2 的真实文件摘要记录在 retained-assets.json。Schema 随 wheel/镜像发布，不从网络动态加载。

QSOutputParser 使用该 Schema 严格解析；应用层 validate_output 再应用冻结输入引用和发布策略：引用必须存在、每条洞察满足不同维度数量、建议来源和分类受限、建议数量/动作数/总字符数受限，禁止配置不允许的祖先后代组合。返回 DeterministicOutput，不代表正式成果或语义安全通过。

相对旧 Go 确定性校验的明确差异：

- 以已有 JSON Schema 为准，必需集合字段不得省略或为 null；旧 Go 零值解码有部分放行行为。
- 拒绝重复 JSON 字段及非 JSON 数字常量。
- 层级检查沿输入中完整可知的 parent_ref 链执行，覆盖祖父/孙辈；旧 Go 的此处确定性检查只检查直接父子。符合 v6 Prompt 已声明的祖先后代限制，不推断输入中不存在的关系。

测试涵盖 Schema、引用、Profile 数量、第二条违规洞察和祖先链，并与原 Go Validate 对照合法候选、未知引用、直接父子组合。测试候选是合成内容，不是质量验收案例。

原 QS 固定对照在确定性校验后绑定 safety.DeterministicGate，而非在线语义模型。该门槛由 qs-ai application/interpretation/safety.py 实现：保留七类中英文禁用表述、空白/大小写归一化、64 字符否定窗口与转折边界，版本保留 ai-explanation-safety-deterministic-zh-en/v2。check_safety 接收 DeterministicOutput，返回带双重校验版本的 SafetyCheckedOutput；正式 Artifact 接受由持久执行事务进一步核对原调用、证据和当前授权。

原 Go 对照增加完整候选的合法/禁用因果表述/缺少边界/转折推翻边界四类。语义评测与实际审核是独立的发布质量证据，此确定性门槛不是完整语义安全保证。合成输出和原 Go 对照不替代真实模型质量或生产输出验收。

## 输入规范迁移及已知差异

`ai-explanation-input-v1.schema.json` 从同一固定 QS 提交 `1b52081ea42c94dc5653ce91e8c7a1db9f85fc44` 原样提取；manifest 的 `input` 记录来源及 SHA-256。`load_input_schema` 校验字节与 schema_version。发布绑定执行从原 publication 的资产读取规范，并在接单、生成及持久成果接受时校验完整输入。

旧 Go assembler 在绑定建议时使用 `append([]string(nil), refs...)`，空 refs 会序列化为 null；原 Schema 只允许 array。当前 [input.py](../../../src/qs_ai/application/interpretation/input.py)统一将所有量表输入的空 standard_suggestion_refs 编码为 `[]`，不再按会话绑定版本保留 null。固定 Go 对照在 [test_input_assembly.py](../../../tests/test_input_assembly.py)先将旧 null 规范化为 `[]` 后比较；这是明确的对照兼容处理，不是与旧 Go JSON 字节相同。

发布执行仍通过 `qs-published-snapshot-v1` 和匹配套件记录输入构造身份，但该标记不是当前空数组修正的唯一适用条件。已经保存的调用 JSON/指纹保持原证据，恢复须读取原冻结请求/回执，不能以新组装结果覆盖它们。provider_payload 仍仅含 context/facts，完整输入规范不直接套在该投影上。

这是版本化的输入编码契约；质量、真实授权和生产恢复需准确环境的独立证据。原评测套件保留原输入字节；独立派生套件现已固定新输入构造版本与 Schema 身份，必须通过新 Run 重新评测和批准。旧 Run 批准不能用于新输入执行。套件身份与验证边界见 [评测资源](../evaluation/README.md#发布执行输入契约套件)，真实质量和业务对账仍需验收。


## 人格与三主题版本

| 场景 | 输入 | 输出 | 身份材料 |
| --- | --- | --- | --- |
| 量表发布执行 | `ai-explanation-input/v1` | `ai-explanation-output/v1` | [原 QS manifest](manifest.json) |
| MBTI 报告解读 `mbti-single-assessment/v1` | `ai-explanation-input/v2` | `ai-explanation-output/v1` | [MBTI manifest](mbti-input-manifest.json) |
| MBTI 三主题 `mbti-single-assessment/v2` | `ai-explanation-input/v3` | `ai-explanation-output/v2` | [三主题 manifest](mbti-thematic-input-manifest.json) |

v2/v3 是 qs-ai 自主新增契约，不伪造为原 QS 导出。每次运行绑定原完整版本/摘要，旧资产与原调用不重写。三主题输出固定 personality/career/relationships，正文区分报告 evidence_refs 与冻结参考 reference_refs；结构/引用合法不代表参考忠实、语义质量或发布批准。

完整生命周期与版本组合从 [文档总入口](../../../docs/README.md)进入；MBTI 实际参与者、QS 接收和端侧验收见 [专项操作](../../../docs/04-接口与运维/08-MBTI管理与验收.md)。机器 Schema/manifest 仍留在本目录。
