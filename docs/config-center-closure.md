# 配置中心收尾契约

新增只读 RPC：AssetCatalog.References、QuotaManagement.Status，QS 均复用审计权限。
组织及操作者来自可信 QS 上下文，不接受调用者自行指定组织。

References 指定 kind（execution_policy/gate_policy）、完整 reference（id/version/fingerprint）、
usage_kind（suite/evaluation/publication）、limit（1–50）和 cursor。结果只有引用对象的类型、
身份、版本及摘要，不返回报告、Prompt 或审核正文。按当前组织隔离；共享初始化套件可见。
游标绑定组织及原查询，不能跨查询复用。未知资产 NOT_FOUND；非法参数 INVALID_ARGUMENT；
依赖失败 UNAVAILABLE。这是当前引用查询，不是删除许可，不创建可变引用索引。

Status 返回部署配置类别、脱敏版本指纹、模型名单、有效额度版本和组织资产集合状态。
不输出地址、证书内容或路径、凭据或由凭据计算的指纹；不得用全量 Settings dump 作为状态。
版本指纹只覆盖明确公开的字段。部署类重启生效，额度后续准入生效，资产按固定快照生效。

生产操作验收：真实管理员通过 QS 入口读取额度、以不改变有效值的版本更新验证回执和
回退；另建未发布测试裁判草稿/案例版本进行准备，不批准、不发布、不改动当前 v6 指针。
记录真实组织、操作者、命令、版本与回执；缺少管理员会话时不能使用合成授权替代。
