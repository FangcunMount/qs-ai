# 多供应商模型配置与评测接入台账

本项目按已批准 M0–M5 计划实施；技术验证、人工质量批准和正式发布分开记录。

## 基线

- qs-ai: 66d4aa117cf21534704bcf45be45106520ae4b75。
- qs-server: 0358557a2（main）。
- Operating: af6bfa7（main）。
- 专属分支 codex/multi-provider；不修改原主干工作区。
- 原路线、方案、LangChain 定向基线14项通过。

## 首轮候选（不是已启用清单）

| 代号 | API model ID | 协议 | 目标用途 | 证据状态 |
|---|---|---|---|---|
| DQ | deepseek-v4-pro | Responses | 生成、语义 | 旧路径已有生产证据，新路由待验收 |
| DL | deepseek-flash | Responses | 生成 | 官方文档列出；账户及业务待验证 |
| ZQ | glm-5.3 | Chat Completions | 生成、语义 | 官方文档列出；账户及业务待验证 |
| ZL | glm-5.3-flash（待实际 API 确认） | Chat Completions | 生成 | 用户指定 GLM-5.3-Flash；文档参数段提及该系列，但模型枚举未列出，准确 ID、账户及业务待验证 |

模型选择按用户最新指定固定为 DeepSeek V4 Pro、DeepSeek Flash、GLM-5.3、GLM-5.3-Flash。不得因 Flash 不可用而自动替换为 GLM-4.7-Flash；不可用时报告阻塞。

来源：[DeepSeek](https://api-docs.deepseek.com/zh-cn/api/create-response/)、[智谱](https://docs.bigmodel.cn/api-reference/模型-api/对话补全)，2026-09-21核对。

DeepSeek现有生产Secret名为QS_AI_MODEL_API_KEY；智谱预定QS_AI_ZHIPU_API_KEY，已请求用户配置，不记录值。不进行未经账本记录的生成调用。

## 固定执行预算

DQ/DQ、DL/DQ、ZQ/DQ、ZL/DQ、DQ/ZQ五轮；每轮7组×5候选和预检。正常350次调用；按现有各阶段70次上限最多700次，探测单列。当前组织预算待正式入口核对，不自动提高上限或重置已用量。

## 当前批次 M1a：兼容格式与配置基础

- 增加严格v2冻结路线、明确供应商/协议/适配版本/绑定身份。
- 旧v1定义序列化不变；原始冻结请求中route使用显式联合类型，v2回执恢复保留新增字段。
- 格式或适配身份损坏明确拒绝，禁止回落成v1。
- 增加部署绑定/能力配置类型；新模型无验证证据不得标记verified。
- models.v2_writes_enabled默认false，目录默认空；本批不开放新写入、不修改现有运行装配。
- 只接入配置读取和路线解码；M1完整能力接口、方案准备及写开关执行门禁仍待后续批次。
- 无数据库迁移、无生产修改。

## 余项

- M0：真实账户可用性、完整参数矩阵和三端契约示例待完成。
- M1b：目录到方案选择/准备/准入、能力API、目录版本冲突与写入门禁。
- M2：统一Router、GLM适配、回执扩展、错误分类和隔离恢复。
- M3：QS及Operating、五轮正式评测矩阵、管理员独立操作。
- M4：含智谱组合真实人工审核发布、生成展示及生产恢复。
- M5：迁移部署配置、移除临时别名、完成证据。

v2开始写入后，仅允许回滚到已验证支持v2的镜像。保留有效v1读取器和当前线上v6；不自动代签、不降低质量门槛。
