# 单次 MBTI 三主题根版本

本目录是 qs-ai 自主编写的 `participant-mbti-single / three-topic-v1` 初始化材料，不是 QS 导出的报告或已经批准的产品配置。运行时权威来源是按固定身份和摘要读取的 MySQL 资产；本目录不能作为缺失资产的回退。

## 内容及来源

`reference-material-v1.json` 包含三个主题、四轴八端共 24 条短参考。每个本次类型只选择相符方向的 12 条。通用释义与自主编写的观察、职业探索、相处讨论问题分开定位；来源没有证明该用户具备相应经历、能力、岗位适配或关系结果。

公开来源于 2026-10-02 核对：

- [四轴偏好释义](https://www.myersbriggs.org/my-mbti-personality-type/the-mbti-preferences/)：通用方向含义，不能作为个体实测特征或优劣等级。
- [职业探索](https://www.myersbriggs.org/type-in-my-life/personality-type-and-careers/)：工作任务与环境探索边界；不是招聘、职业能力或适配结论。条目中的练习为本项目自主编写。
- [关系与沟通](https://www.myersbriggs.org/type-in-my-life/personality-type-and-relationships/)：沟通与共同决策边界；不是配对或婚姻预测。条目中的讨论问题为本项目自主编写。
- [OEJTS](https://openpsychometrics.org/tests/OEJTS/)：公开替代测量及教育参考定位。其公开版本与本项目 `MBTI_OEJTS / v64-report-202608-v1` 不同，不表示本地量表等价于官方 MBTI 产品，也不借用官方产品的个体效度或批准。

完整参考包嵌入不可变 Profile，原文文件用于初始化核对，摘要同时覆盖内容、来源和边界。更新必须建立新的根版本并重新评测与审核，不改写 `v1` 或旧 Run。

## 执行与审核契约

输入采用 `ai-explanation-input/v3`，输出采用 `ai-explanation-output/v2`；同一调用生成性格、职业和关系三个固定主题。七组事实、五候选、预检、原模型路线、预算、超时和发布门槛保持原值。

新根不沿用旧版“每条至少两轴”的内容目标。保留五项裁判输出字段，其中本场景 `cross_dimension_quality` 明确定义为三主题解释质量；量表与旧 MBTI 的定义不变。新增六项硬语义义务检查内容充实、参考忠实、事实与参考区分、职业探索、关系沟通、问题及行动可执行性。引用 ID 存在和离线测试不能证明这些质量义务已经通过。

只在隔离测试中使用假裁判验证执行、回执和恢复。生产仍需真实 35 候选及预检、实际人工批准、独立 MBTI 发布、QS 接收和正式小程序展示验收。该根当前未批准、未发布；不能以初始化成功替代业务发布。

## 受控初始化

先核对精确版本镜像与已安装的共享模型路线、执行策略、门槛和语义输出 Schema，再运行：

```bash
python -m qs_ai.bootstrap.import_mbti_assets \
  --root three-topic-v1 \
  --source-commit <exact-40-character-commit> \
  --imported-by <authorized-initializer>
```

默认 `--root v1` 保持兼容。新根显式选择；同内容重复导入不写入，同身份异内容拒绝，整个导入事务失败即回滚。此命令不会创建发布、审查结论、业务请求或模型调用。新输出的 QS 与页面兼容完成前不要开放产品发布。

## 成果参考正文与兼容发布

三主题成果采用 `qs-ai-artifact/v2`，新增 `reference_material_json` 和 `reference_material_fingerprint`。正文来自该任务原冻结 Profile、原报告类型选出的参考子集，摘要按原 UTF-8 JSON 字节计算；它不由模型生成，不读取当前参考或外部网页。既有量表及旧 MBTI 仍使用原 `qs-ai-artifact/v1`，不补造不存在的来源。

QS 先上线 v2 接收及读取兼容，再上线生产者；页面分别从原评测冻结输入、正式成果读取相同参考正文、来源与适用边界。三主题资产初始化与产品发布仍需实际内容核对及原审核流程。隔离测试覆盖完整 35 候选、合成审核/发布、准入、冻结回执恢复、成果与投递事件持久化；合成审核不是生产批准。
