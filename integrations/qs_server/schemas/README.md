# 输出契约迁移

ai-explanation-output-v1.schema.json 原样取自 manifest.json 固定的 QS 提交，运行时校验文件摘要，CI 与固定 QS 工作区逐字比较。Schema 随 wheel/镜像发布，不从网络动态加载。

QSOutputParser 使用该 Schema 严格解析；应用层 validate_output 再应用冻结输入引用和发布策略：引用必须存在、每条洞察满足不同维度数量、建议来源和分类受限、建议数量/动作数/总字符数受限，禁止配置不允许的祖先后代组合。返回 DeterministicOutput，不代表正式成果或语义安全通过。

相对旧 Go 确定性校验的明确差异：

- 以已有 JSON Schema 为准，必需集合字段不得省略或为 null；旧 Go 零值解码有部分放行行为。
- 拒绝重复 JSON 字段及非 JSON 数字常量。
- 层级检查沿输入中完整可知的 parent_ref 链执行，覆盖祖父/孙辈；旧 Go 的此处确定性检查只检查直接父子。符合 v6 Prompt 已声明的祖先后代限制，不推断输入中不存在的关系。

测试涵盖 Schema、引用、Profile 数量、第二条违规洞察和祖先链，并与原 Go Validate 对照合法候选、未知引用、直接父子组合。测试候选是合成内容，不是质量验收案例。

QS 现有执行流水线在确定性校验后调用 SafetyEvaluator，实际绑定 safety.DeterministicGate，而非在线语义模型。该门槛现已迁移到 application/interpretation/safety.py：保留七类中英文禁用表述、空白/大小写归一化、64 字符否定窗口与转折边界，版本保留 ai-explanation-safety-deterministic-zh-en/v2。check_safety 接收 DeterministicOutput，返回带双重校验版本的 SafetyCheckedOutput；仍未持久接受 Artifact。

原 Go 对照增加完整候选的合法/禁用因果表述/缺少边界/转折推翻边界四类。语义评测与人工证据仍属于后续 Prompt 发布验证体系；此规则门槛不是完整语义安全保证。此前将运行时 SafetyEvaluator 笼统称为语义安全门槛不准确，以此处源码核对为准。当前未调用模型、未完成生产输出验收。

## 输入规范迁移及已知差异

`ai-explanation-input-v1.schema.json` 从同一固定 QS 提交 `1b52081ea42c94dc5653ce91e8c7a1db9f85fc44` 原样提取；manifest 的 `input` 记录来源及 SHA-256。`load_input_schema` 校验字节与 schema_version。发布绑定执行从原 publication 的资产读取规范，并在接单、生成及持久成果接受时校验完整输入。

现有 QS assembler 在绑定建议时使用 `append([]string(nil), refs...)`，空 refs 会序列化为 null。原 Schema 的 standard_suggestion_refs 只允许 array；固定输入样本中有两个维度因此不符合规范。`qs-snapshot-v1` 会话及默认基线组装保留原编码，以便已有调用恢复时可重建同一输入和指纹。

新接单标记 `qs-published-snapshot-v1`，仅该执行版本将空引用编码为 `[]`。输入规范文件、已有会话及调用 JSON 均不改写；新输入指纹随内容变化。测试同时保留旧 Go 完整输入对照，并显式验证新版本仅有这项集合编码差异、满足原规范。切回接单开关不改变已接受会话的执行版本。provider_payload 仍仅含 context/facts，完整输入规范不直接套在该投影上。

这是可审查的代码契约修正，尚无真实模型质量与历史生产恢复验收。固定评测套件仍保留原输入字节；新输入构造版本对应的案例、套件版本及质量对账必须在生产发布准入前补齐，不能沿用旧 Run 的批准声称新映射已经通过业务验收。
