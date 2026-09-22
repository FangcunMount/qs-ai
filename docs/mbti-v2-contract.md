# MBTI v2 兼容读取契约（P1）

本批提供人格快照解码、AI 输入组装、Profile v2 解码、精确发布选择器及独立输入 Schema。**尚未开放生产 MBTI 准入，也未导入、评测或发布人格资产。**

基于 main `befa988`，承接 [真实基线](mbti-contract-baseline.md) 和 QS PR #129 的结构化标准报告事实。没有数据库迁移、供应商调用、运行配置修改或业务数据写入。

## 协议与生效边界

| 对象 | 本批固定值 | 约束 |
| --- | --- | --- |
| QS 快照 | `qs-report-snapshot/v2` | 仅精确 MBTI 模型；不接受量表或其他人格模型 |
| AI 输入 | `ai-explanation-input/v2` | 显式 `scene_contract_version` |
| Profile | `ai-explanation-profile/v2` | 场景版本 `mbti-single-assessment/v1` |
| 模型 | `MBTI_OEJTS / v64-report-202608-v1` | typology / personality_typology / pole_composition |
| 输出 | `ai-explanation-output/v1` | 原跨维度引用、安全规则与数量边界保持 |

以上版本与路线 v2、候选执行 candidate_v2 无隐含关系。旧 v1 Profile 的字段集合和规范化 JSON 不增加默认字段；原量表资源、输入字节、Prompt 消息和指纹保留。

`assemble_input` 以解码后的策略类型选择输入实现；不能凭快照 v2 偷换量表策略。`snapshot_selector` 提供有限场景分派；MBTI 的 model_code/model_version 必填且精确匹配。人格选择器与量表通配选择器互不匹配，未发布/暂停不能回退到量表。

目前 `report_selector` 的线上准入仍只接受 v1；工作流、Profile 注册写入、套件准备和初始化仍保留原有 v1 门槛。新增纯读取能力不能证明生产 v2 已可运行。后续应在冻结配置读取、输入/输出校验、恢复及对应资产齐全之后接通 `qs-published-snapshot-v2`，才能使 QS v2 快照进入人格执行。QS P1 已实现事实投影，但兼容读取发布本身不开放人格准入。

## 快照字段

完整例子见 [合成协议向量](../tests/fixtures/personality_contract/snapshot.json)，其来源限制见 [说明](../tests/fixtures/personality_contract/README.md)。

- 顶层只允许 schema_version、source、model、runtime、conclusion、dimensions、suggestions、model_extra。
- source 保留 report_id、outcome_id、report_type、内容/模板版本、构建器与带时区生成时间。ID 为正的 uint64 十进制字符串。
- model 和 runtime 固定上述模型与机制身份。
- dimensions 恰好四个独立轴。每轴只允许 code、kind、name、raw_score、pole_facts、description、suggestion。
- pole_facts 使用 QS `mbti-pole-facts/v1`：left_pole、right_pole、preference、strength、min_score、max_score、threshold、composition_order 均必填。
- 轴与顺序固定 EI(I/E)、SN(S/N)、TF(F/T)、JP(J/P)，范围 8–40、阈值 24。强度必须是 0–100 有限数值，零是有效事实；空、缺失、布尔值和字符串均拒绝。
- model_extra 只允许 kind=personality_type、type_code、type_name、one_liner、match_percent、commentary。type_code 必须与四轴已保存偏好一致。
- suggestions 保留原 source_index、category、content、dimension_code；重复来源索引拒绝。

所有对象拒绝未知字段。原始答案、账户资料、稀有度、图片等不能进入该投影。QS 应先完成白名单投影，不能把完整持久化对象原样发送。缺少 pole_facts 的历史报告明确失败，不能默认强度为零。

qs-ai 校验完整性、范围、两极及类型一致性，**不根据 raw_score 重新算偏好或强度，也不重新算 match_percent**。这些值由 QS 标准报告负责。输入显式标明偏好强度不等于置信度。

## AI 输入、Schema 与隐私

组装结果含 schema_version、scene_contract_version、source、profile、context、facts；供应商只接收 context 与 facts，报告/Outcome ID 和 Profile 元数据留在服务端。

四轴按 composition_order 规范排序，保留原始数值，不使用 description 中取整后的百分比。每轴引用为 `dimension:EI` 等；原报告建议保留 `suggestion:report:<index+1>`，轴建议保留 `suggestion:dimension:<code>`。

[输入 Schema v2](../integrations/qs_server/schemas/ai-explanation-input-v2.schema.json) 是本项目新编写的协议资产；[独立清单](../integrations/qs_server/schemas/mbti-input-manifest.json) 明示其来源和摘要，不伪装成 QS 原始导出。原 v1 manifest 和 Schema 不变。

`load_input_schema()` 默认仍读 v1。显式指定 v2 才读取新文件和摘要；缺失、摘要错误、未知版本均失败，不回落 v1 或 latest。该读取器用于初始化与测试；生产运行仍应通过既有 MySQL 不可变资产和冻结清单读取。

JSON Schema 负责结构验证；四轴唯一性、顺序及偏好与类型一致性由固定版本的代码校验补充，不声称 Schema 单独证明全部语义正确。异常边界只返回安全分类信息，不输出原报告或 Pydantic 的输入正文。

## 验证与后续

[新增测试](../tests/test_mbti_contract.py) 覆盖 16 种合法类型、合法零强度、精确小数、混合版本拒绝、缺失/重复轴、偏好冲突、敏感异常、原始字段拒绝、Profile 摘要、来源身份、Schema 损坏/缺失、无通配及跨场景回退。

后续按顺序实施：

P1 已增加 [Go→Python 投影联调](../tests/test_mbti_snapshot_interop.py)：CI 固定 QS 源码提交，执行实际 `reportSnapshot` 后导出合成数据，由 Python 解码、组装及校验 Schema，覆盖精确值、合法零值、隐私白名单及跨场景拒绝。固定提交仅用于测试，不引入生产源码依赖。

1. 冻结执行读取与恢复支持 v2，输出场景验证；保持量表已接单任务配置不变。
2. 完成 P1 两端兼容部署核证后，再继续 P2 后端闭环。
3. MBTI 根 Prompt/Profile/Schema/裁判/七组×五候选及预检、首版模板受控初始化。
4. 方案准备、能力预检、Operating 与小程序接入；真实评测、人工审核、正式业务生成与恢复验收分别记录。

当前 CI/本地测试不代表部署、质量审核或真实 MBTI 解读通过。
