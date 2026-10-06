# qs-server 契约与来源资产

本目录保存 QS 来源协议、初始化资产及固定对照。源码基线 `c977cb9`；生产状态和授权验收须另行绑定准确环境/版本。

## QS 协议来源

[manifest.json](manifest.json)固定 QS 提交 `1ce436d7ec7c97a2647f3591eeddd9f7cab6aae6` 的[报告与授权协议](https://github.com/FangcunMount/qs-server/blob/1ce436d7ec7c97a2647f3591eeddd9f7cab6aae6/api/grpc/proto/interpretation/interpretation.proto)和[evaluation 协议](https://github.com/FangcunMount/qs-server/blob/1ce436d7ec7c97a2647f3591eeddd9f7cab6aae6/api/grpc/proto/evaluation/evaluation.proto)，记录原文件哈希。协议由 QS 维护，不手改 vendored proto 或生成代码。

```sh
uv run python scripts/generate_qs_proto.py --check
```

[ParticipantReportProbe](../../src/qs_ai/infrastructure/qs_server/report_probe.py)是显示报告传输探针，短期委托由调用方即时传入，不签发/持久化用户 token。显示 DTO 不是完整不可变 AI 事实快照，不能补造缺失 report/source 身份。

[QSAccessSource](../../src/qs_ai/infrastructure/qs_server/access.py)通过 AIWorkflowAccessService.Authorize，以可信 org/subject/Testee/assessment 复核**当前**权限。它与报告探针用途不同，不能借用 collection/worker 证书冒充 AI，不能用 Job 中的短期 token 替代长期恢复授权。

## 事实、资产与运行时

QS 持有 Testee、评分、标准报告与业务权限；AI 接收可信快照并核对关联，从冻结 Profile 投影输入，不能改标准结论。流程见 [QS 接入](../../docs/04-接口与运维/02-QS接入与消息契约.md)。

| 材料 | 权威用途 |
| --- | --- |
| [报告事实快照](report-snapshot.md) | QS→AI 输入信封和不可变事实 |
| [模型执行适配](provider-runtime.md) | 供应商协议、原调用回执与恢复边界 |
| [输入/输出规范](schemas/README.md) | 机器版本、来源、兼容关系与确定性校验 |
| [Prompt 原始资产](prompts/README.md) | 原字节、manifest 和初始化/迁移对照 |
| [评测资源](evaluation/README.md) | 案例、策略与原生套件的来源/身份 |
| [保留资产清单](retained-assets.json) | 原完整来源记录与实际保留文件的区别 |

运行时权威是 MySQL 的精确资产/发布版本。缺失或损坏时拒绝，不从本目录文件兜底，不选 latest。初始化和对账不等于评测、审核、发布或实际参与者验收。
