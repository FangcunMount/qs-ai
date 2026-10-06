# 模型执行适配与原调用回执

现行接入材料；源码基线 `c977cb9`。部署选择供应商地址/凭据，MySQL 冻结路线与发布资产决定模型、参数、输入和校验版本。当前生产模型及质量状态须使用日期绑定证据。

## 适配与编排

[ModelGatewayRouter](../../src/qs_ai/infrastructure/models/router.py)核对冻结路线与部署 binding，按 DeepSeek Responses 或智谱 Chat Completions 等已验证 adapter_contract 发送。[responses.py](../../src/qs_ai/infrastructure/models/responses.py)保留原始响应与分类边界；实际模型通过 LangChain 模型适配执行，生成/评测由 [report 图](../../src/qs_ai/infrastructure/workflows/report.py)和[evaluation 图](../../src/qs_ai/infrastructure/workflows/evaluation.py)组织节点。

LangGraph 不拥有业务恢复权，不安装业务 checkpointer。应用执行用例、MySQL 租约/fence、CAS、dispatch 标记和原回执决定恢复，数据库事务不跨模型网络请求。

## 协议限制与失败

网关执行单次异步发送、总超时、响应大小限制、禁用重定向与隐式环境代理，检查完成状态、模型、消息/用量并保留原输出、供应商响应 ID、调用 ID 和时延。不默认宣称供应商支持幂等重发或按调用 ID 查询，不在超时后静默切模型。

失败只输出固定 code。连接失败和合法恢复名单仍须经完整持久策略核对；读写超时、传输中断、响应过大、服务端 5xx/408 和非终态响应保守分类为未知。retryable 不意味着可以直接再发收费请求。取消向外传播，执行层发送前先持久 dispatch 证据。

缺失 usage 保留 None，不补零；已知单层 JSON 包装才可解包，原输出字节保持。解析拒绝重复 JSON 字段和非 JSON 数字，随后应用原 Schema、引用、数量和安全校验。协议成功不是语义质量通过，也不是正式 Artifact。

## durable dispatch 与成果

[DurableGeneration](../../src/qs_ai/application/execution/generation.py)冻结完整 PreparedExplanation、路线及 Schema。首次创建 dispatch 标记者获得发送权；后续恢复读取原请求和回执，不使用当前 publication 或新模型参数。标记后、网络发送前崩溃也保守为未知，dispatched/unknown 不自动重发；租约转移后旧执行者不能覆盖新 owner 的证据。

原回执恢复仍用原版本的结构、事实引用和安全规则验证。成果接受核对候选与原模型调用/证据/输入/资产的关联，当前授权及租约有效时，将不可变 Artifact、completed 状态和同版本结果事件同事务提交。取消/失去 fence 后拒绝迟到写入；HTTP 200 不能直接当成成果。

MQ relay 投递原结果事件，最终业务 ACK 才确认交付。旧 gRPC receiver/scanner 已退役；主链路见 [QS 接入与消息契约](../../docs/04-接口与运维/02-QS接入与消息契约.md)。

## 路线来源与部署

[routes/manifest.json](routes/manifest.json)保存固定来源文件摘要，balanced_text_v1/v8 与 semantic_judge_v1/v5 是保留的迁移/初始化基线，不是唯一可管理版本。MySQL 不可变资产与 publication 的完整冻结清单是执行权威，缺失或字节损坏拒绝，不从文件补齐。

地址和凭据不纳入业务路线指纹；改模型、超时、token、推理参数或 adapter contract 需新的资产版本、匹配套件和独立评测批准，不能只改环境变量复用旧指纹。部署模型目录的 verified 是技术能力核证，不能当作质量或任意场景批准。

[GenerationProvider](../../src/qs_ai/bootstrap/providers/generation.py)按开关装配 PublishedReportWorkflow，禁用时 UnconfiguredWorkflow；启用依赖由启动预检核对。统一 server 必须满足 MQ/TLS/数据库条件，生成和评测关闭仍保持消息接收与结果投递。配置见 [configs](../../configs/README.md)。

## 验证定位

[真实 MySQL 工作流回归](../../tests/integration/test_report_workflow.py)、[发布配置回归](../../tests/integration/test_execution_configurations.py)和[单进程回归](../../tests/test_single_server.py)分别覆盖持久契约。模型 MockTransport、合成权限、临时证书和固定 Go 对照不能替代真实供应商质量、当前授权、QS 业务 ACK、客户端展示或受控生产恢复。
