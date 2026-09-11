# 首个模型路线的执行适配

当前迁移目标为已观测的 balanced_text_v1/v8、DeepSeek deepseek-v4-pro、Responses、json_schema、reasoning none、120000 ms、12000 output tokens。ModelRoute 保存非秘密参数及与原 QS 字段顺序一致的指纹算法；地址、密钥由外部注入，不纳入指纹。尚未将这些参数启用到生产。

DeepSeekResponses 已实现单次异步 HTTP 发送、总超时、响应大小限制、禁用重定向、完成状态/模型/消息数量/用量检查，保留原始输出、兼容解包结果、供应商响应 ID、调用 ID 和时延。它不自动重试、不实现调用 ID 查询，不允许路线宣称这些能力。

失败分类只输出固定 code，不暴露供应商正文、地址或凭据。连接失败可重试；读写超时、传输中断、响应过大及服务端 5xx/408 保守标记 result_unknown。queued/in_progress 也视为尚无最终结果。结果未知必须经后续持久执行协议处理，不能因为 retryable 就重新收费调用。适配器的取消仍传播给调用方，执行层必须在发送前持久记录调用状态，才能在进程取消/崩溃后恢复未知状态。

相对旧 QS 的明确变化：缺失 usage 保留 None，不伪造零用量；部分服务端失败/非终态更保守地标记未知。供应商 JSON 包装只移除已知单层，保留内部原格式以维持字符数限制，仍经过完整输出及安全规则校验。

model_calls 通过迁移 0005 增加每 Run 唯一的调用记录。发送前提交 dispatched 标记和冻结请求；只有首次创建标记的执行者获得发送权。调用回执在当前租约及 fence 下保存，成功响应、已知失败和未知结果分别记录；领取权转移后可读取旧回执，但不能重发或覆盖旧调用。发送标记之后、实际网络发送之前发生崩溃也保守地视为结果未知，这是当前供应商不支持幂等重发或按调用 ID 查询的限制。

DurableGeneration 将持久接口与模型网关接起来：冻结完整 PreparedExplanation、路线及 Schema，保存 ModelResponse；恢复时读取原始冻结版本及回执，而不是采用当前发布参数。dispatched/unknown 不重新发送，failed 保留失败原因，取消和响应提交失败留下待核实的调用。恢复后的输出仍须交给原版本对应的结构、引用及安全校验，再进入正式成果事务。

数据库集成覆盖并发创建、连接重建读取、领取权转移、旧执行者迟到、回执不可覆盖、失败分类、取消、数据库提交失败及配置变化后的原版本恢复。该协调器尚未接入真实 Workflow；不能据此宣称模型运行中断恢复已端到端完成。

本地 MockTransport 测试只验证构造、发送次数、分类和解析；未访问真实供应商。尚待 Worker/调用记录/成果事务接线、配置与凭据部署、真实案例验收。没有将 HTTP 成功直接转为正式 Artifact。

build_artifact 将恢复后的响应与当前任务冻结证据重新绑定，再运行完整输出 Schema、事实引用及确定性安全校验。只有全部通过才返回 ArtifactCandidate，包含稳定成果 ID、内容指纹、证据/输入/Profile/Prompt/路线及校验器版本和供应商回执 ID。

迁移 0006 增加 interpretation_artifacts。finish 在当前租约内复核候选与已保存调用/证据的关联和内容指纹，将完整成果、completed 状态、同版本 Outbox 一并提交；无完整成果不能完成。取消后和已完成后拒绝迟到写入，已完成状态不能再取消。结果事件以新增 proto 字段 artifact_json 承载完整候选信封，序列化大小上限为 128 KiB；重复投递使用同一已保存事件。

MySQL 集成测试验证成果和事件同步保存、Outbox 写入失败时整体回滚、取消后拒绝迟到成果。QS 接收端仍待配套更新，实际 Workflow 与常驻进程也尚未启用，不可将这些离线测试视为正式成果已送达生产 QS。

完整成果跨语言验证位于 tests/integration/test_artifact_delivery.py：使用 MySQL 保存的候选及 Outbox、Python GRPCResultReceiver、独立 Go 进程中的真实 Results 接收器和 QS MySQL 投影，在测试 CA 下建立 mTLS；覆盖完成事件先于旧状态、确认丢失、接收器重启及相同成果重投。本地 MySQL 8.4 已通过，CI 固定 QS 成果接收版本 b13c21e02d0d2abf97062c221aeb69565047f419 并纳入 8.0.36/8.4 矩阵。原 Prompt 来源和旧协议兼容测试保留原固定提交，不用新代码替换历史对照基线。该验证没有调用真实模型，也未证明生产授权、客户端展示或 M2 全部验收完成。

ReportWorkflow 已组合准备输入、持久生成、成果校验并返回 WorkflowResult，由 ExecuteNext 在再次授权后提交成果。供应商未知结果统一呈现 provider_result_unknown，格式/安全不合格呈现具体失败码，数据库失败与取消继续传播以保留恢复语义。MySQL 集成测试通过成功、Schema 不合格、安全规则拒绝、执行前撤权、调用后撤权及未知超时六个分支；仍使用模型和权限测试替身。GenerationProvider 已支持按配置组装正式 Workflow；generation.enabled 默认为 false，默认仍选择 UnconfiguredWorkflow。常驻进程与生产启用尚待完成。


运行配置集中在 configs/default.yaml 的 generation 节点：启用开关、Profile ID/版本、供应商地址、模型路线及参数。QS_AI_MODEL_API_KEY 仅通过环境变量或显式测试参数提供，禁止写入 YAML；启用生成必须同时配置 HTTPS 地址、非空凭据和 grpc.access_address。HTTP 客户端随请求作用域释放，禁用重定向和隐式环境代理。配置、容器和架构测试覆盖默认关闭、缺配置拒绝、正式组装与 YAML 密钥拒绝；仅组装对象，没有发起真实模型请求。

常驻入口为 `python -m qs_ai.bootstrap.worker --serve` 和 `python -m qs_ai.bootstrap.integration deliver --continuous`，原有探针/单次执行方式保持兼容。worker/delivery 配置各自的并发、空闲等待、最大退避和退出等待；默认并发为 1。收到 SIGTERM/SIGINT 后先停止领取，在途任务继续续租与收尾，超时取消并释放请求作用域。异常日志不输出底层异常正文。首次领取前解析 Workflow/TLS 并检查数据库；尚未加入生产 Compose，也尚未完成进程健康信号及真实进程终止恢复演练。

常驻进程通过 worker/delivery.health_file 配置独立心跳文件，`python -m qs_ai.bootstrap.daemon_health <path>` 检查进程存在且单调时钟心跳未过期。心跳由同一事件循环写入，退出后删除；写入失败会终止受监督的执行循环。它仅为 liveness，不把数据库不可用、持续失败或无业务流量解释为业务健康。已用真实 Python 子进程验证 SIGTERM 正常退出及心跳移除；这不替代模型在途、租约转移与生产恢复演练。

生产发布脚本支持 GitHub Variable `QS_AI_EXECUTION_ENABLED=true` 时合并 deploy/serverA/execution.yaml，增加 worker/delivery；默认 false 时只保留 API/gRPC。启用还要求 Variable `QS_AI_MODEL_ENDPOINT`、Secret `QS_AI_MODEL_API_KEY`；QS 地址由 `QS_AI_QS_ADDRESS` 指定，工作流默认 qs-apiserver:9090。模型密钥只写入 worker 的私有 runtime.json 环境覆盖，不传给 API/gRPC/delivery；沿用发布目录权限和日志抑制。worker/delivery 继承三份只读 TLS 挂载与容器限制、不发布端口，各使用 pool_size=2/max_overflow=2；停止等待分别为 140/20 秒，大于进程默认收尾时间。

15 项部署测试通过，包括 Compose 实际解析服务、证书、端口及健康配置；另用一次性无网络容器与纯合成值验证美元符号转义在运行时可还原。尚未设置生产启用变量或真实模型 Secret，尚未启动生产 worker/delivery。

## 冻结路由版本

`routes/balanced_text_v1-v8.json` 保存上述已观测的非秘密执行参数及按 QS 算法计算的 fingerprint；manifest 校验文件字节。这里的来源是前期运行配置观测，不冒充从 QS 数据库导出的新治理资产或生产审批。

GenerationProvider 校验配置投影与冻结包完全相同后才装配模型调用器。同名同版改模型、超时、Token 或 reasoning 会拒绝，未登记版本也拒绝。endpoint 和 credential 仍通过外部配置提供，不写入包。新增路由必须先完成版本资产与评测/审批流程，不能只修改环境变量复用 v8。

当前仅有固定 v8 基线，尚未实现 MySQL 路由资产仓库、发布选择及管理界面；此保护不代表 M3 完成，也不启用生成。
