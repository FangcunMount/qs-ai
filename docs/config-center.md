# 配置中心后端契约与实施台账

范围：qs-ai 后端与必要 QS 代理；Operating 由独立会话维护。基线 0f9d7c6；不另建方案或发布状态机。

## 第一批接口

QuotaManagement: Get / Update / History / Rollback / GetReceipt。QS 组织及操作者来自可信上下文，读使用审计权限，写使用组织管理员权限。

字段：participant（daily_org、daily_user、daily_assessment、active_org、active_user、active_assessment），evaluation（daily_provider_calls、max_active_runs），均为正整数。Get 返回 defaults、ceilings、configured、effective、revision、source、constrained_fields。未配置 revision=0/source=deployment_default；损坏或数据库失败不回退。

Update 是完整替换，携带 command_id、expected_revision、reason、values。Rollback 携带 command_id、expected_revision、target_revision、reason，生成新版本；不删除历史。命令同 ID 不同正文拒绝，回执仅原组织原操作者可读。错误：INVALID_ARGUMENT 非法配置，ABORTED 版本冲突，NOT_FOUND 不存在或不属于当前范围，UNAVAILABLE 依赖失败/结果待核对。

额度修改提交后对后续准入/槽位获取生效；每日预留不重复扣减，已有活跃槽位继续运行。UTC 日界线不变。部署默认值与硬上限初始相同，组织只能调整到上限以内。部署降低上限按项限幅并返回 constrained_fields，不改写历史。

锁顺序：配置写先 participant_admission_locks 后 evaluation_admission_locks；读取有效配置复用业务准入事务，不先锁配置指针再获取业务锁。禁止跨请求额度缓存。

## 第二批边界

语义 Prompt、案例可派生并冻结；执行/门槛策略只读。按原身份、版本、摘要导入；运行不回退当前文件。历史证据不重写。接口扩展复用现有方案协议，只增加可选精确引用，省略时继承来源。

## 验收台账

- [ ] 在线额度存储、管理接口、准入与槽位读取
- [ ] QS 授权代理与互操作
- [ ] 配置状态脱敏读取
- [ ] 文件 I/O 不阻塞事件循环
- [ ] 评测资产初始化、草稿及固定版本读取
- [ ] MySQL 8.0/8.4、并发/恢复/历史回归
- [ ] CI、生产部署与运行时核证
- 页面接入、实际人工审核不计为后端自动通过。

## 当前第一批接线

REST 基址 `/internal/v2/interpretation/ai-workflow/quotas`：GET 根路径、GET `/history?before_revision=...`、GET `/commands/{uuid}`、POST `/update`、POST `/rollback`。

写入示例（无需组织或操作者字段）：

```json
{"command_id":"00000000-0000-4000-8000-000000000001","expected_revision":0,"reason":"控制测试期间用量","values":{"participant":{"daily_org":100,"daily_user":5,"daily_assessment":3,"active_org":10,"active_user":2,"active_assessment":1},"evaluation":{"daily_provider_calls":1024,"max_active_runs":1}}}
```

回退只提供 target_revision，不提供 values；历史分页每页至多20项，next_revision=0 表示结束。UTC 日界线不变。预留表 quota_snapshot=NULL 表示迁移前记录，不以新策略回填历史。

本轮新增迁移 0030，不能直接回滚到只认可 0029 的镜像；需使用兼容该结构的修复版本，不删除额度历史。
