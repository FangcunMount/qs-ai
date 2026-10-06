# MBTI 模板与方案来源契约

本文回答首页模板从哪里来、模板怎样建立方案、为何模板不等于批准，以及场景和来源怎样保留。核对基线：`c977cb9`，2026-10-06；某环境是否初始化、批准和发布需另行实时核证。

## 模板边界

模板是受控初始化的固定配置来源，运行时读取 MySQL 原字节和全部依赖摘要；它从不代表质量审核或 active publication。只允许下列两个精确的全局根引用，禁止 latest、私有派生套件和猜摘要。

| 模板名 | Suite 引用 | scene_contract_version | 根 Suite 指纹 |
| --- | --- | --- | --- |
| MBTI 单次解读首版 | `participant-mbti-single / v1` | `mbti-single-assessment/v1` | `sha256:3353f945b75346b869c638a1034356ac602ea7f049cb0d2a6e55262538b9cacd` |
| MBTI 三主题单次解读首版 | `participant-mbti-single / three-topic-v1` | `mbti-single-assessment/v2` | `sha256:027fbe31e18e08786390920dfc1233e76e7a54ac68198385f6391c8634ce2a4b` |

两个模板的 selector 都是 participant / typology / pole_composition / MBTI_OEJTS / v64-report-202608-v1。模板各自有固定输入、输出、Profile、Prompt、语义义务和来源；不能把单主题 v1 方案重新贴标成三主题。两者的版本组合见 [报告事实契约](../interpretation/02-MBTI报告事实与版本契约.md)。

## 读取与创建

`SolutionManagement.List` 的既有 `qs-ai-solutions/v1` 响应提供 `templates` 数组。每个已安装根明确列举：name、精确 template_ref{id,version,fingerprint}、scene_contract_version、selector、`published=false`、`reason=requires_evaluation_review_and_publication`。未安装根逐项省略，两项都未安装时为空；数据库失败和损坏资产明确失败，不能返回“未安装”。

Create 的 publication_id、source_run_id、template_ref 是互斥且必选其一的来源。command_id、title、reason、solution UUID 和可信组织/操作者仍必需，没有新增独立工作区或 RPC。创建三主题必须选择列表返回的 three-topic-v1 entry，复制其完整引用。

```json
{
  "command_id": "00000000-0000-4000-8000-000000000101",
  "title": "MBTI 三主题首版验收",
  "reason": "基于固定模板评测，完成审核后另行发布",
  "template_ref": {
    "id": "participant-mbti-single",
    "version": "three-topic-v1",
    "fingerprint": "<copy exact catalog fingerprint>"
  }
}
```

服务端先确认该引用属于受控根，再读取 Suite、原 Profile/Prompt/Route/Schema 和冻结策略，核验原摘要与完整来源。根 Suite 指纹绑定原 release 依赖，不能在已有名称下接受变化的 Prompt/Schema。缺失资产不自动导入，不读 initializer 文件，不回落其他根或历史预发布修订。

## 工作区、准备与隔离

新工作区/read/list 保留 scene_contract_version；source 保存精确 template_ref，publication/run source ID 为 null。source_release 固定全部原依赖，prepared 初始 null，source_reviews 初始为空。Save/Prepare 使用既有 CAS、命令回执和模型能力，完整事务见 [治理设计](01-方案资产与原子准备设计.md)。

Prepare 固定七组 × 五候选及预检，创建未启动 Run；不会模型调用、审核或发布。模板继承方案可以调整允许的 Prompt 和模型参数，同场景兼容 Suite/语义 Prompt；不能改变来源 scene、selector、Profile schema、输入/输出版本或三主题参考材料。更改根知识材料需要新根协议和独立质量验证。

QS 可信授权保持：工作区、草稿命令及回执按组织隔离，另一个操作者不能读原命令回执。旧 Create 命令缺少 template_ref 时，存储正文省略该字段，使原 publication/run 命令仍可回放；原量表来源与指纹不改。

## 错误与不确定结果

| 情况 | 语义 |
| --- | --- |
| 来源非法或多选 | INVALID_ARGUMENT |
| 未知/未安装模板、不可访问工作区或回执 | NOT_FOUND |
| revision 过期、相同 command_id 不同正文 | ABORTED |
| 固定依赖缺失或损坏 | 配置错误，明确失败，无回退 |
| 意外数据库临时失败 | UNAVAILABLE，按原 command_id 核对 |

客户端不能因暂时失败换新 command_id 重做不确定创建或准备；也不能把模板列举、准备成功或合成审核结果显示为已开放。

## 选择、限制与验证

精确模板来源减少首版搭建步骤，代价是不能任选私有/派生 Suite 作为模板，也不能修改原根引用。来源与场景继承保护旧资产，代价是场景升级必须新建方案。模板内容结构合法不能证明参考来源支持或产品质量。

源码：[精确目录与读取](../../../src/qs_ai/infrastructure/persistence/mysql/solution_templates.py)、[根身份](../../../src/qs_ai/infrastructure/qs_server/evaluation_suite.py)、[Create 命令](../../../src/qs_ai/application/governance/solutions.py)、[来源与准备](../../../src/qs_ai/infrastructure/persistence/mysql/solution_assets.py)、[根三主题资产说明](../../../integrations/qs_server/evaluation/mbti-themes/README.md)。

验证：[源互斥及精确引用](../../../tests/test_solutions.py)、[MBTI 方案集成](../../../tests/integration/test_mbti_solutions.py)、[初始化](../../../tests/integration/test_mbti_initialization.py)、[三主题根](../../../tests/test_mbti_themes_root.py)。隔离数据库测试使用合成生成/裁判完成 35 候选及原响应投影恢复，能证明事务、隔离和重复调用保护；不能证明真实 70 次模型调用的内容质量、人工批准或生产发布。
