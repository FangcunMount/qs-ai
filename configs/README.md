# 统一启动配置

本目录持有部署配置；运行时业务资产由 MySQL 持有。源码基线 `c977cb9`，具体环境值与生产验收由日期绑定的部署证据确认。

## 加载与权威

`default.yaml` 为公共非敏感默认值，`local.yaml`、`production.yaml` 为环境差异。[Settings](../src/qs_ai/config.py)负责类型校验，统一 server、维护命令和 Alembic 使用同一加载规则。

优先级从低到高：默认 YAML → 环境 YAML → 环境变量 → 显式构造参数；嵌套对象逐字段合并。`QS_AI_ENVIRONMENT` 仅允许 local/production，默认 local。配置路径不依赖当前工作目录，wheel/镜像携带 YAML；不自动读取 `.env`。未知键、非法值、缺失文件或环境拼写错误立即失败。

YAML 禁止 database_url 和模型凭据。数据库 URL、供应商凭据由环境/受控 Secret 注入；MQ 私钥和 TLS 通过只读文件挂载。连接池、并发、租约、MQ、地址、模型部署目录与额度默认值/硬上限为重启生效配置；Prompt、Profile、评测套件、发布与组织额度是有版本的业务数据，不能在此代替。

## 常驻入口前置条件

唯一常驻入口为 `uv run python -m qs_ai.bootstrap.server`。必须同时准备数据库 exact Alembic heads、有效 TLS、QS 授权/正文读取地址及显式 MQ 配置。默认 messaging 关闭，直接使用默认值启动 server 会拒绝；本地 Compose 仅提供 MySQL，不能单独组成完整服务。

| 应用环境变量 | 语义 |
| --- | --- |
| `QS_AI_DATABASE_URL` | `mysql+asyncmy` URL；凭据按 URL 编码，禁止日志输出 |
| `QS_AI_DATABASE__POOL_SIZE` / `MAX_OVERFLOW` | 应用共享连接池 |
| `QS_AI_HTTP__PORT` | HTTP 运维监听 |
| `QS_AI_GRPC__BIND_ADDRESS` | AI mTLS gRPC 监听 |
| `QS_AI_GRPC__ACCESS_ADDRESS` | QS 当前授权和 MQ payload endpoint，常驻 MQ 装配必需 |
| `QS_AI_GRPC__CA_FILE` / `QS_AI_GRPC__CERT_FILE` / `QS_AI_GRPC__KEY_FILE` | 三份 TLS 文件 |
| `QS_AI_MESSAGING` | 整个 messaging JSON 对象；含 enabled、nsqd 映射、密钥文件及 max_in_flight |
| `QS_AI_GENERATION__ENABLED` / `QS_AI_EVALUATION__ENABLED` | 生成/评测内部循环开关，不关闭 MQ 投递和 ACK |
| `QS_AI_GRPC__GOVERNANCE_ENABLED` | 治理服务注册开关，不恢复旧 gRPC 执行写入口 |
| `QS_AI_WORKER__LEASE_SECONDS` | 任务租约，至少 3 秒 |
| `QS_AI_DEEPSEEK_API_KEY` / `QS_AI_ZHIPU_API_KEY` | 供应商凭据；旧 `QS_AI_MODEL_API_KEY` 与 DeepSeek 新名并存时须相同 |

`messaging.nsqd` 明确映射 NSQD TCP 到同主机 HTTP 地址，不推断端口；signing_key_file、decrypt_key_files、qs_signer_files、qs_recipient_key_file 分别配置 AI 签名私钥、AI 解密私钥、QS 签名公钥、QS 接收者公钥。启动核对 kid、角色、EC P-256、文件大小与既有失败拓扑，不创建 Topic/Channel。

`grpc.result_address`、delivery 的旧字段仍有兼容配置定义，但当前 server 使用 MQ relay；它们不能重新启用旧 gRPC result scanner。完整字段以 [config.py](../src/qs_ai/config.py) 为准。

## 模型与额度

启用生成/评测需对应供应商绑定、凭据、冻结路线与资产；模型目录中的 verified 是技术能力证据，不等于质量批准或可用于任意场景。地址/凭据归部署，模型路线版本/参数及其指纹归不可变资产，不能靠改环境变量复用旧版本改变语义。

participant_capacity 与 evaluation.daily_provider_calls/max_active_runs 是部署默认额度。可选 quota_ceilings 包含参与者六项、评测两项完整正整数上限；不配置时上限等于默认值，默认值不得超过上限。组织额度修改影响后续准入，已有预留与活动槽位保持原语义；部署降低上限按项限幅，不改历史。

生产工作流变量与应用的嵌套变量不同，例如 QS_AI_EXECUTION_ENABLED 在[打包脚本](../scripts/cd/deploy.py)转换成 QS_AI_GENERATION__ENABLED。部署密钥、MQ 冻结绑定及全部变量见 [serverA](../deploy/serverA/README.md)，开发与隔离检查见 [配置与本地开发](../docs/04-接口与运维/04-配置与本地开发.md)。

生产环境 evaluation.daily_provider_calls 为 2048 次（UTC 日界线）；本地默认仍为 1024。生产未配置独立 quota_ceilings 时，部署上限随默认值为 2048。修改部署预算需发布并重启，仅影响后续准入，不重置当日预留或已有任务；显式组织额度继续按其原版本生效，不会随部署自动提高。
