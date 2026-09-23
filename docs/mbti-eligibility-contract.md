# 报告 AI 能力预检契约

本接口用于区分标准报告就绪与 AI 已开放。它不是授权、容量预留或接单承诺。

## 内部 RPC

`qsai.workflow.v1.Commands.CheckEligibility(EligibilityQuery) → EligibilityStatus`。
沿用现有 mTLS，只接受 `qs-apiserver.svc`。不新增公开 URL。

请求包含现有 Actor、testee_id、assessment_ids 和 EvidenceItem。QS 必须先校验
实际参与者权限与当前报告归属，再从持久标准报告构造快照；不能转发客户端提交的
原始快照或组织身份。当前只支持一个测评。消息大小沿用 gRPC 现有限制。

返回值：

| status | reason_code | 含义 |
| --- | --- | --- |
| available | 空字符串 | 当前精确发布与报告输入校验通过 |
| unavailable | unsupported_scene | 场景不支持或不是单报告 |
| unavailable | unsupported_model_version | 不属于本期固定 MBTI 模型版本 |
| unavailable | source_incomplete | 可信来源不完整或不符合输入契约 |
| unavailable | source_conflict | 来源身份、报告版本或受试者不一致 |
| unavailable | publication_missing | 没有适用发布；初始化模板不等于发布 |
| unavailable | publication_paused | 已暂停适用发布 |
| unavailable | asset_invalid | 发布证据、冻结资产或当前调用绑定不可用 |

非法请求返回 INVALID_ARGUMENT；不受信主体返回 PERMISSION_DENIED；数据库或依赖
临时失败返回 UNAVAILABLE。传输错误不伪装为 publication_missing，也不携带内部异常正文。

实现只执行 SELECT，无会话、任务、回执、额度、供应商请求或发布写入。复用现有
快照解码、精确发布选择、资产指纹和输入组装校验；Start 仍在实际接单事务中重新校验。
不引入跨请求缓存，不向参与者返回资产正文或模型参数。

## QS 与页面接入

QS 保留 `/ai-workflows/source` 的原 status/report_id/source_version，增量返回：

```json
{
  "status": "ready",
  "report_id": "123456",
  "source_version": "standard-v2:123455",
  "ai_eligibility": {"status": "unavailable", "reason_code": "publication_missing"}
}
```

报告未就绪时不请求 AI 预检。暂时不可用通过现有服务错误处理，不变成“不支持”。
小程序缺少预检字段时不能默认开放 MBTI；现有成果仍通过原授权读取入口展示，
不因当前发布暂停而隐藏。量表旧响应兼容须单独覆盖。

发布顺序为 qs-ai → QS 授权代理与生成协议 → 页面。QS 与页面验收尚未完成，
本契约及后端测试不代表四端已上线。
