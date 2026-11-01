# P. 代码分析报告

## 分析目标与范围

主目标为review：为AI #246生成空旧发布表退役的保护与前向恢复执行包。仅涉及0043→0044的`messaging_outbox`；不处理历史责任关闭、模型重试、其他服务对象或生产发布授权。数据/恢复责任人均为shujie。

## 入口与调用路径、当前职责与依赖

`migrations/versions/0044_empty_publisher.py`→`empty_publisher_retirement.retire`负责唯一单表DROP。`publisher_window`只读capture/verify与单独reconcile入口共用`publisher_material`，只创建自有数据库连接，不启动服务、SDK、调度或模型。正常启动/发布探针继续使用完整`require_runtime_schema`。离线`require_pending_publisher_drop`独立验证43ledger+44物理布局，不开放正常启动的中间态兼容。

## 行为 / 契约 / 不变量边界

43必须有旧表，44必须物理缺席；其他对象的完整结构验证不变。迁移前对所有49个保留表的定义、列顺序、行数及原字节指纹比较，ledger结构单独保护。NULL、空值、二进制、字段边界、重复行均有确定编码。指纹不是业务备份。原目标DDL及43ledger原件用于审计和隔离恢复资格，不授权回放生产43账本。

v2批准拒绝v1散列声明：必须引用同窗口frozen保护、完整writer排除回执和绑定原目标DDL的隔离恢复/前向资格。writer回执是责任人的审查声明，无法自动发现任意外部写入；数据库/process权限可见性和完整入口清单仍需独立复核。当前采集默认frozen=false、execution_approval=false。

## 测试与可观测性现状

原生MySQL8.0.36/8.4矩阵覆盖空库安装、既有43默认拒绝、非空原表、原生witness/二进制ACK保留、DDL前失败消费、真实DROP已提交而ledger未更新、单独ledger前向收敛、保留字节/结构漂移和原消费凭证缺失拒绝，以及当前生产空目标DDL在隔离库的逐字恢复。单元覆盖私有文件、v1/活库/未合格恢复材料拒绝、NULL/二进制编码和独占持久消费。实际CI结果由infra日期执行包绑定，测试夹具中的批准不构成生产批准。

## 分析指标与判定

| 指标 | 判定 | 证据/剩余边界 |
| --- | --- | --- |
| 删除范围 | 绿 | 字面量单表DROP；全目录及引用验证 |
| 凭证和业务原件 | 绿 | 全保留对象定义/内容比较，原生见证/ACK测试 |
| 中间态隔离 | 绿 | 正常启动拒绝；独立恢复仅允许43缺表且原件一致 |
| 审查材料语义 | 黄 | v2拒绝任意散列文件；实际writer范围依赖同窗口审查 |
| 恢复路线 | 黄 | 隔离资格与受控工具；生产镜像digest/窗口回执仍待绑定 |
| 生产执行状态 | 黄 | 未停业务、未执行DDL、未签发生产批准 |

## 主要风险点、初步发现或假设

MySQL DDL隐式提交，不能以事务rollback恢复已删表。原43镜像不能读取44；DROP后不能应用旧schema/全库备份覆盖现库。43缺表而retained漂移时也不能自动补写ledger。发布脚本require-existing不会代办此DDL。SHA散列只能约束批准的字节，不能代替人员对停写/恢复证据的判断。

## 下一步建议与具体维护接口

保持AI自动发布false、QS发布禁用。先审查infra执行包、精确候选/44前向镜像及实际权限/入口；正式窗口再生成停写和保护材料。以下命令是审查模板，`/run/m5`必须为宿主持久挂载、执行UID所有0700；文件0600。路径及摘要必须由窗口清单填入，不可复制本文件生成批准。

只读预检（显式`QS_AI_PUBLISHER_WINDOW_DATABASE_URL`从已审核的原库配置注入，禁止输出凭证）：

```sh
/app/.venv/bin/python -m qs_ai.maintenance.publisher_window capture --operation-id AI246-WINDOW --output /run/m5/live.json
```

正式停写审查后capture增加`--writer-receipt /run/m5/writers.json --writer-sha256 <原件SHA256>`，输出新名称`frozen.json`。全库采集预算为20,000行/128MiB原字段字节/60秒，进程90秒超时，单连接无溢出；超限保持冻结并重新审查预算，不截断成成功。对原目标空表逐字保存SHOW CREATE，ledger定义+43原值与全部保留指纹包含于snapshot。对完整保留业务数据的备份/PITR资格需另行审核，指纹不提供数据恢复。

唯一迁移命令（仅在批准的停写窗口；当前禁止执行）：

```sh
/app/.venv/bin/alembic -x empty_publisher_review=/run/m5/review.json -x reviewed_sha256=<原件SHA256> upgrade 0044_empty_publisher
```

批准schema为`ai-empty-publisher-review/v2`，精确字段见审查器`FIELDS`；保护、writer、恢复证明内容契约见同文件与`publisher_material.writer_receipt`。审查器在5秒表锁内复验空表和批准字节，0700父目录固定FD下EXCL创建、fsync消费凭证，再DROP；持久凭证必须另机保全。

发生未知结果时，只读capture重新分类：43_intact、43_drop_committed_ledger_pending、44_complete；任何非严格布局均拒绝。不得重复原DROP。仅43缺表且所有保留原件/ledger定义一致，另发`ai-publisher-forward-review/v1`批准后才能：

```sh
/app/.venv/bin/python -m qs_ai.maintenance.publisher_window reconcile --review /run/m5/forward.json --reviewed-sha256 <原件SHA256> --output /run/m5/forward-result.json
```

该入口要求原批准SHA及其持久consumed、同源同库新writer回执和当前一小时内恢复批准；锁定ledger行，独占消费新批准，仅条件更新一行43→44。无CREATE/DROP、无业务行恢复。commit回复未知或输出保存失败都先只读实查，不能把无输出当未执行。

DDL成功后、业务重开前`verify --baseline /run/m5/frozen.json --baseline-sha256 <原件SHA256> --output /run/m5/after.json`，须44严格缺表+全部保留字节一致；capture/verify不自己授予重开资格。重开后新事实变化是正常业务，旧快照比较不再是可回放授权，故障只用44兼容前向映像修复。不得用43镜像、downgrade、直接stamp、重建旧表或覆盖旧全备恢复生产。

首次业务重开命令之前，必须在原批准同一持久目录独占写入`review.json.business-reopen-issued`并fsync，再复制到另一保护位置；标记不因启动失败而删除。迁移与F1入口在检查和消费前都拒绝该标记（包括符号链接/异常文件）。正式单一协调会话在W08完整44复验后执行以下受审文件动作，再发实际启动命令；该动作本身不启动服务：

```sh
/app/.venv/bin/python -c 'from qs_ai.maintenance.publisher_material import write_private; write_private("/run/m5/review.json.business-reopen-issued", b"issued-before-business-start-command\n")'
```

取消窗口后恢复原43业务也先记录此标记，永久禁止该operation再次迁移或执行首启动前修复。批准/恢复批准commit或SSH结果未知时原标记和原consumed全部保留。
