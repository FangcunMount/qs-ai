# 输出契约迁移

ai-explanation-output-v1.schema.json 原样取自 manifest.json 固定的 QS 提交，运行时校验文件摘要，CI 与固定 QS 工作区逐字比较。Schema 随 wheel/镜像发布，不从网络动态加载。

QSOutputParser 使用该 Schema 严格解析；应用层 validate_output 再应用冻结输入引用和发布策略：引用必须存在、每条洞察满足不同维度数量、建议来源和分类受限、建议数量/动作数/总字符数受限，禁止配置不允许的祖先后代组合。返回 DeterministicOutput，不代表正式成果或语义安全通过。

相对旧 Go 确定性校验的明确差异：

- 以已有 JSON Schema 为准，必需集合字段不得省略或为 null；旧 Go 零值解码有部分放行行为。
- 拒绝重复 JSON 字段及非 JSON 数字常量。
- 层级检查沿输入中完整可知的 parent_ref 链执行，覆盖祖父/孙辈；旧 Go 的此处确定性检查只检查直接父子。符合 v6 Prompt 已声明的祖先后代限制，不推断输入中不存在的关系。

测试涵盖 Schema、引用、Profile 数量、第二条违规洞察和祖先链，并与原 Go Validate 对照合法候选、未知引用、直接父子组合。测试候选是合成内容，不是质量验收案例。

QS 现有执行流水线在确定性校验后还调用 SafetyEvaluator。该独立门槛尚未迁移，不能跳过后接受 Artifact；当前未调用模型、未完成生产输出验收。
