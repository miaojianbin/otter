# Otter Manager Channel 全量同步设计

## 1. 目标

在 Channel 列表增加“全量同步”按钮，由 Otter Manager 完成以下操作：

1. 将 Canal 位点游标重新写为源库当前位点。
2. 根据 Channel 配置执行全量数据导入。
3. 全量导入成功后启动 Channel，继续增量同步。

全量同步的建位点、建表和数据导入均由 Manager 执行，不依赖 Node，也不需要检查或干预 Node 的运行状态。

首期仅支持 MySQL 到 MySQL，并保持 JDK 8 兼容。

## 2. 页面入口

在 `channelList.vm` 的 Channel 操作列增加“全量同步”按钮。

按钮规则：

- Channel 状态为 `STOP` 时可点击。
- Channel 为 `START`、`PAUSE` 或正在执行全量同步时不可点击。
- 仅管理员可以执行。
- 点击后弹出删除并重建目标表的确认框。
- 用户确认后创建后台任务并进入任务详情页，避免长时间占用 Web 请求线程。

全量任务执行期间保持 Channel 为 `STOP`。如果执行失败，Channel 继续保持 `STOP`；只有全部数据导入成功后才启动 Channel。

## 3. 执行流程

```text
检查 Channel 为 STOP
        ↓
写入源库当前位点游标
        ↓
收集 Channel 下全部表映射
        ↓
按目标表分组
        ↓
每个目标表只删除、创建一次
        ↓
依次导入该目标表对应的所有源表数据
        ↓
所有目标表导入成功
        ↓
启动 Channel
```

### 3.1 前置检查

Manager 执行以下检查：

1. Channel 存在且当前状态为 `STOP`。
2. 同一 Channel 没有其他正在运行的全量任务。
3. Channel 下至少存在一个 Pipeline 和一个 DataMediaPair。
4. 源端和目标端均为 MySQL 数据源。
5. 源表存在且 Manager 可以读取。
6. Manager 可以在目标库执行 `DROP TABLE`、`CREATE TABLE`、`INSERT` 和 `UPDATE`。

检查失败时不修改游标、不删除目标表，也不启动 Channel。

### 3.2 写入当前位点游标

Manager 根据 Pipeline 的 Canal destination 和 clientId 处理游标：

1. 读取 Canal 配置中的源 MySQL 地址和账号。
2. 查询源库当前 binlog 文件、偏移、GTID 和 serverId。
3. 构造 Canal 使用的 `LogPosition`。
4. 将新 `LogPosition` 写入 ZooKeeper 中的客户端 cursor。

同一 destination 被本 Channel 的多个 Pipeline 使用时，必须为这些 clientId 写入同一个当前位点，避免 Canal 从其他旧 cursor 重新选择已经失效的较早位点。如果该 destination 还被其他 Channel 使用，首期直接阻止执行，避免修改其他 Channel 的游标。

新位点必须在删除目标表和读取任何全量数据之前写入。全量期间源库产生的新变化将在 Channel 启动后从该位点重新播放。

### 3.3 收集并分组表映射

Manager 读取 Channel 下全部 Pipeline 的 DataMediaPair，并按照以下目标标识分组：

```text
目标数据源 ID + 目标 schema + 目标 table
```

一个目标表分组中可以包含一个或多个源表。例如：

```text
source_db.order_01 ─┐
source_db.order_02 ─┼─> target_db.order
source_db.order_03 ─┘
```

目标表的删除和创建以目标表分组为单位执行，而不是以 DataMediaPair 为单位执行。

### 3.4 重建目标表

对于每个目标表分组：

1. 按 DataMediaPair ID 排序。
2. 选择 ID 最小的 DataMediaPair 对应源表作为建表模板。业务语义上可以使用任意源表，固定选择规则只是为了保证每次执行结果一致。
3. 执行 `SHOW CREATE TABLE source_schema.source_table` 获取源表结构。
4. 将 DDL 中的源 schema 和表名替换为目标 schema 和表名。
5. 执行 `DROP TABLE IF EXISTS target_schema.target_table`。
6. 执行改写后的 `CREATE TABLE`。

无论有多少个源表映射到同一个目标表，步骤 5 和步骤 6 都只执行一次。

如果同组源表中将导入的字段不在建表模板中，Manager 在删除目标表前终止任务，不执行目标表删除。

首期仅改写 DDL 中的 schema 和表名，不自动转换数据库类型，也不处理自定义建表脚本。

### 3.5 导入全量数据

目标表创建完成后，Manager 按 DataMediaPair ID 顺序导入同组的所有源表：

1. 根据 DataMediaPair 和 ColumnPair 计算源字段与目标字段映射。
2. 使用流式 ResultSet 分批读取源表，避免全部加载到 Manager 内存。
3. 使用 PreparedStatement 批量写入目标表。
4. MySQL 写入使用 `INSERT ... ON DUPLICATE KEY UPDATE`，以支持多个源表之间可能出现的重复主键，并兼容后续增量重放。
5. 每批提交后更新任务的已处理表数和已导入行数。
6. 当前源表完成后继续导入同一目标表分组中的下一个源表。
7. 当前目标表分组全部完成后，再处理下一个目标表分组。

字段映射规则限定为可安全重建目标表的子集：

- 配置 ColumnPair 时，仅支持同名字段的包含或排除。
- 没有配置 ColumnPair 时按同名字段写入。
- 过滤器、关联查询和改名字段映射会在删除目标表前终止任务。
- 首期不执行 Node 中的自定义 resolver、filter 或处理器；发现此类配置时提示不支持并终止。

### 3.6 启动 Channel

所有目标表分组均完成后：

1. 再次确认 ZooKeeper 中的新游标仍然存在。
2. 调用现有 `channelService.startChannel(channelId)`。
3. 启动请求成功后将全量任务标记为 `SUCCESS`。

如果 Channel 启动失败：

- 全量数据不需要重新导入。
- 任务标记为 `FAILED`，阶段为 `START_CHANNEL`。
- 页面提供“重新启动 Channel”按钮。

## 4. 多源表写入同一目标表

多源表合并是本功能必须支持的场景，处理规则如下：

1. 以目标数据源、schema 和 table 作为唯一目标表标识。
2. 每个目标表只选择一个源表生成 DDL。
3. 每个目标表只执行一次 DROP 和 CREATE。
4. 同组所有源表数据依次追加或 upsert 到该目标表。
5. 不并行导入同一目标表，避免重复主键竞争和不可预测的覆盖顺序。
6. 不同目标表首期也按顺序执行，保持实现简单。
7. 出现重复主键时，后导入的源表数据覆盖先导入的数据；顺序固定为 DataMediaPair ID 升序。

例如三个源表映射到一个目标表时，执行顺序为：

```text
DROP target.order
CREATE target.order            # 仅一次
导入 source.order_01
导入 source.order_02
导入 source.order_03
START Channel
```

## 5. 最小任务状态

为了支持页面进度和防止重复点击，只保留一个简单的全量任务记录，不设计复杂恢复状态机。

任务状态：

- `RUNNING`：正在执行，当前阶段记录在 stage 字段。
- `SUCCESS`：全量导入并启动 Channel 成功。
- `FAILED`：位点、DDL 或数据导入失败。

任务至少记录：

- taskId、channelId、状态和当前阶段。
- 已导入行数和总行数。
- 创建时间、更新时间和错误信息。

Manager 重启后，不自动从中间批次续传。未完成任务标记为 `FAILED`，Channel 保持 `STOP`，管理员重新点击全量同步从头执行。

## 6. 失败处理

统一规则：

- 写入游标失败：立即停止，不操作目标表。
- 源表结构检查失败：立即停止，不操作目标表。
- 删除或创建目标表失败：任务失败，Channel 保持 `STOP`。
- 数据导入失败：任务失败，Channel 保持 `STOP`。
- 任意目标表失败：不再处理后续目标表。
- 只有所有目标表数据导入成功后才启动 Channel。
- Channel 启动失败不重复全量，只允许重新启动 Channel。

由于目标表会被删除，失败时可能存在部分目标表已完成、部分目标表未完成的情况。重新执行任务时会再次写入新的当前游标，并从第一个目标表开始重新删除、建表和导入。

## 7. 代码改动位置

本次实现涉及：

### Manager Web

- 修改 `channelList.vm` 增加按钮和状态控制。
- 增加 `FullSyncAction` 接收创建任务和重新启动 Channel 请求。
- 增加简单任务详情页面。

### Manager Biz

- 增加 `FullSyncService` 和 `FullSyncServiceImpl`。
- 增加当前位点读取及 ZooKeeper cursor 写入逻辑。
- 增加目标表分组、DDL 改写和流式数据导入逻辑。
- 增加独立单线程后台执行器。
- 增加简单的 FullSyncTask DAO。

### Manager 数据库

- 增加一张 `FULL_SYNC_TASK` 表保存状态、进度和错误。
- 更新全新安装 SQL，并提供独立升级 SQL。

Node 模块、Node 配置和 Node 部署包不需要修改。

## 8. 测试要点

1. Channel 为 STOP 时按钮可用，其他状态不可用。
2. 游标被重写为源 MySQL 当前 binlog/GTID 位点。
3. 单源表到单目标表可以完成 DROP、CREATE 和全量导入。
4. 多个源表到同一目标表时，目标表只 DROP 和 CREATE 一次。
5. 多源表数据全部进入同一目标表。
6. 多个不同目标表分别只重建一次。
7. 全量期间持续写入源库，Channel 启动并追平后数据一致。
8. DDL 或数据导入失败时 Channel 不启动。
9. 全量成功但 Channel 启动失败时，可以单独重新启动 Channel。
10. Node 不参与全量处理，Node 模块不需要修改。

## 9. 验收标准

1. 用户只需在停止状态的 Channel 上点击一次“全量同步”。
2. Manager 自动完成写当前游标、重建目标表、导入全部历史数据和启动 Channel。
3. 多源表映射到同一目标表时不会重复删除或创建目标表。
4. 全量期间产生的源库变化能在 Channel 启动后从新游标继续同步。
5. 任一步骤失败时保留明确错误信息，并且不会错误启动 Channel。
