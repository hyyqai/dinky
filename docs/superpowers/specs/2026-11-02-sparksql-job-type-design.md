# Dinky 新增 SparkSQL 作业类型（最小闭环）设计稿

- 日期：2026-11-02
- 分支：1.2.4
- 状态：待评审（评审通过前不写业务代码）

## 1. 背景与目标

Dinky 1.2.4 原生只有 Flink SQL / Flink Jar / JDBC 查询 / UDF 等作业类型，没有 Spark 相关代码。
iServ 环境已部署 Spark 3.5.3 Standalone（`spark-master` / `spark-worker` / `spark-thrift`）与 Paimon。

本次目标是最小闭环：

1. 数据开发 → 项目 → 创建作业 → **作业类型新增 `SparkSql`**，可创建、保存、再次打开；
2. 在编辑器写 SparkSQL 点"运行"，**由 Dinky 自己通过 `spark-submit` 提交到 `spark://spark-master:7077`**（client 模式，driver 跑在 Dinky 容器内）；
3. Spark Master UI 的 Applications 中能看到该作业，worker 上能分配 executor；
4. SELECT 结果（列 + 行）回显到 Dinky 的结果表格。

## 2. 非目标（本版明确不做）

- 停止/取消 Spark 作业、运维中心作业历史、Spark 作业监控；
- DolphinScheduler 调度 SparkSQL；
- Spark Jar 作业、SparkSqlEnv、Spark UDF；
- 语法校验 / explain / 血缘 / Catalog 浏览；
- 多 Spark 版本（只支持 3.5.x）、YARN / K8s / standalone-cluster 提交模式；
- 流式作业、作业排队与并发控制；
- 任务级 Spark 参数与最大返回行数配置面板（本版只用服务端全局配置）；
- Dinky 变量机制（`${var}` / 提交变量）与 FlinkSQLEnv 前置 SQL（与现有 JDBC 查询作业保持一致，原样交给 Spark；含 `${}` 的脚本会由 Spark 报错）；
- Document 模块的 SparkSql 文档模板（模板区为空是可接受状态）。

## 3. 约束与环境事实

| 项 | 值 |
|---|---|
| Spark | 3.5.3，Scala 2.12，Standalone |
| Master | `spark://spark-master:7077`（**只在 `iserv_net` 内可达**，未映射到宿主机） |
| 提交模式 | client（driver 在 Dinky 容器内，结果才能回到 Dinky） |
| Dinky 容器 | hostname `dinky`，与 Spark 同在 `iserv_net`，`mem_limit=2.5g`，JVM 堆约 1g |
| Java | Dinky 镜像为 JRE 8 + JDK 8；Spark 3.5 支持 Java 8（若现场有问题，可通过配置 `java-home` 指向 JDK 11） |
| Hadoop | `HADOOP_CONF_DIR=/etc/hadoop/conf` 已挂载 |
| Paimon | Spark 镜像自带 Paimon 3.5_2.12-1.4.2 与 catalog 默认配置；Runner 侧仍允许通过 `dinky.spark.conf` 显式下发 |
| 资源 | worker 资源池 1536m；`spark-thrift` 常驻会占用资源，验收时可能需要 `docker compose stop spark-thrift` |

## 4. 总体方案

### 4.1 为什么用 `spark-submit` 子进程

不把 Spark 嵌进 Dinky JVM，也不走 Thrift Server，而是在 Dinky 进程里以**子进程**方式执行：

```
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class org.dinky.spark.runner.SparkSqlRunner \
  --name Dinky-<taskName> \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --conf ... \
  /opt/dinky/lib/dinky-spark-runner-<version>.jar \
  --request-file /tmp/dinky-spark/<uuid>/request.json \
  --result-file  /tmp/dinky-spark/<uuid>/result.json
```

理由：

1. Spark 3.5 与 Dinky 内 Flink/Netty/Jackson/Guava 依赖同 JVM 冲突风险高，子进程天然隔离；
2. `--master` / `--deploy-mode client` / `--driver-memory` 与部署文档的验收判断完全一致；
3. driver 内存独立可控，不影响 Dinky 主 JVM；
4. Runner jar 只编译依赖 Spark API（`provided`），体积只有几十 KB。

代价：结果通过临时 JSON 文件回传；提交是同步阻塞的（与现有 JDBC 查询作业一致）。

### 4.2 端到端流程

```
前端 运行按钮
  → PUT /api/catalogue/saveOrUpdateCatalogueAndTask（先保存 SQL）
  → GET /api/task/submitTask?id=xx
  → TaskServiceImpl.submitTask
      → prepareTask
      → executeJob
          → BaseTask.getTask(task)  // @SupportDialect(SPARK_SQL)
              → SparkSqlTask.execute()
                  → SparkSubmitService.submit(task)
                      → 写 request.json（语句列表 + maxRows）
                      → 启动 spark-submit 子进程（client）
                          → Runner: SparkSession → 逐条 spark.sql() → 写 result.json
                      → 读取 result.json
                      → 组装 JobResult.result = SelectResult(columns,rowData)
  → 前端 isSparkSql 分支：更新结果表格
```

Spark 作业本身在 Master UI 显示为 `Dinky-<taskName>`，RUNNING → FINISHED。

### 4.3 结果与错误模型

Runner 输出（`result.json`）：

```json
{
  "success": true,
  "appId": "app-20261102103000-0001",
  "error": null,
  "results": [
    {
      "columns": ["count(1)"],
      "rows": [[10]],
      "truncated": false,
      "statement": "SELECT count(1) FROM paimon.ods.orders"
    }
  ],
  "startTime": "2026-11-02T10:30:00",
  "endTime": "2026-11-02T10:30:04"
}
```

Dinky 侧映射：

- 多条语句时，取**最后一条有结果集的语句**组装 `SelectResult`（前端结果面板只消费单个 `result`）；
- `JobResult.status = SUCCESS/FAILED`，`success` 与之一致；
- `JobResult.jobId = appId`，`JobResult.result = SelectResult`；
- 失败时 `error` = runner 报告的异常摘要；拿不到 result.json 时回退为子进程日志最后 100 行；
- 最大返回行数只取服务端全局 `dinky.spark.result-max-rows`；`TaskDTO.maxRowNum` 的默认 100 不参与 Spark 路径，避免默认值覆盖全局配置。

类型转换约定：null / boolean / 数字原样；Date/Timestamp/BigDecimal 转字符串；Array/Map/Struct 转字符串；保证前端表格可渲染。

## 5. 变更清单

### 5.1 后端

| 文件/模块 | 变更 |
|---|---|
| `dinky-common/.../config/Dialect.java` | 新增 `SPARK_SQL("SparkSql")`；`isCommonSql` / `isUDF` / `isFlinkSql` / `isJarDialect` 均保持返回 false |
| 新模块 `dinky-spark-runner/` | `SparkSqlRunner` 主类；`spark-sql_2.12:3.5.3` 依赖为 `provided`；Java 8 目标；`finalName` 固定为 `dinky-spark-runner`（不带版本号） |
| `dinky-assembly/src/main/assembly/package.xml` | 把 `dinky-spark-runner-*.jar` 打到发布包 `lib/` |
| `pom.xml` + `dinky-assembly/pom.xml` | 注册新模块 |
| `dinky-admin/.../service/spark/SparkSubmitService.java` | 组装命令、启动子进程、超时、解析结果、清理临时目录 |
| `dinky-admin/.../service/spark/SparkProperties.java` | `@ConfigurationProperties(prefix="dinky.spark")`；含 `runner-jar`（默认 `$DINKY_HOME/lib/spark/dinky-spark-runner.jar`） |
| `dinky-admin/.../service/task/SparkSqlTask.java` | `@SupportDialect(Dialect.SPARK_SQL) extends BaseTask`；`execute()` 调 `SparkSubmitService` |
| `dinky-admin/src/main/resources/application.yml` | 增加 `dinky.spark.*` 默认值（默认 `enabled=false`，路径/地址留空） |
| `dinky-admin/.../service/catalogue/factory/CatalogueFactory.java` | `resetTask` 对 `SPARK_SQL` 不写入 Flink 默认值（保持现状即可），不做特殊处理 |

说明：

- `TaskServiceImpl.submitTask` 的现有流程可直接复用：`BaseTask.getTask()` 按方言自动分发；`SparkSqlTask` 同步返回结果后，`submitTask` 会把 `jobInstanceId`（为 null）写回任务，MyBatis-Plus 默认忽略 null，不会破坏原值。
- 进程切面 `@ExecuteProcess(ProcessType.FLINK_SUBMIT)` 与 `@ProcessStep(SUBMIT_EXECUTE)` 保持复用，本版不新增流程枚举；界面上的流程名仍是 FlinkSubmit（可接受的过渡态）。

### 5.2 前端

| 文件 | 变更 |
|---|---|
| `dinky-web/src/services/constants.tsx` | `DIALECT.SPARK_SQL = 'sparksql'` |
| `dinky-web/src/pages/DataStudio/constants.tsx` | `JOB_TYPE` 新增分组 `Spark Job Type` → `{ value: 'SparkSql', label: 'SparkSql' }` |
| `dinky-web/src/pages/DataStudio/function.tsx` | `getTabIcon` 映射 SparkSql（复用 SQL 图标）；`matchLanguage` 映射到 `sql` |
| `dinky-web/src/pages/DataStudio/utils.tsx` | 新增 `isSparkSql(dialect)` 辅助函数，**不并入 `isSql`**，避免影响数据库选择器/Catalog |
| `dinky-web/src/pages/DataStudio/CenterTabContent/SqlTask/index.tsx` | ① 运行结果预览条件加 `isSparkSql`；② "校验"按钮对 SparkSql 隐藏；③ SparkSql 运行成功后状态置 `SUCCESS`、不置 `isRunning`（避免出现无效的停止按钮）；④ `handleSubmit` 增加 `result` 判空（`handleGetOption` 在失败时返回 `undefined`，现有代码会抛 JS 错）；⑤ 发布/下线按钮与 DolphinScheduler 推送按钮对 SparkSql 隐藏 |
| `dinky-web/src/pages/DataStudio/Toolbar/Service/Result/index.tsx` | SparkSql 走"预览结果"分支，不请求 Flink 作业历史 |
| `dinky-web/src/pages/DataStudio/Toolbar/Service/index.tsx` | 方言节点条件加 `isSparkSql`，让结果面板正常归类 |

明确不改：

- `SelectDb`（数据源选择器）不显示给 SparkSql；
- `Toolbar/Catalog` 不显示 SparkSql 的元数据树；
- `ExecutionHistory` / `TableData` / 血缘 / DAG 不对 SparkSql 显示；
- `TaskConfig` 配置页签不对 SparkSql 显示（本版无任务级配置入口）；
- `pages/DataStudio/Toolbar/Project/constants.tsx` 的 Document 模板 `JOB_TYPE` 不改（SparkSql 暂无文档模板）。

### 5.3 部署（仓库外，iServ）

Dinky 镜像需要带 Spark 发行版（不用重复下载，用已有镜像 `COPY --from`）：

```dockerfile
COPY --from=iserv/spark:3.5.3 /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
```

配置（追加到镜像内 `/opt/dinky/config/application.yml`，或挂载覆盖）：

```yaml
dinky:
  spark:
    enabled: true
    home: /opt/spark
    runner-jar: /opt/dinky/lib/spark/dinky-spark-runner.jar
    master: spark://spark-master:7077
    deploy-mode: client
    driver-memory: 512m
    executor-memory: 512m
    executor-cores: 1
    cores-max: 1
    result-max-rows: 1000
    submit-timeout-seconds: 600
    conf:
      spark.driver.host: dinky
      spark.driver.bindAddress: 0.0.0.0
      spark.sql.extensions: org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
      spark.sql.catalog.paimon: org.apache.paimon.spark.SparkCatalog
      spark.sql.catalog.paimon.warehouse: hdfs://cdh:8020/paimon
```

> `cores-max=1` 是保守默认值，避免与 `spark-thrift` 争抢 1536m 的 worker 资源池。

## 6. 关键实现细节

### 6.1 任务创建

- 前端 `JOB_TYPE` 增加选项后，`type='SparkSql'` 随创建请求提交；后端 `saveOrUpdateCatalogueAndTask` 会把 `Catalogue.type` 与 `Task.dialect` 都设为该值。
- `JobModal` 的 `isFlinkJob('SparkSql') === false`，不会初始化 Flink 专用字段（运行模式、并行度、envId 等），符合预期。
- `TemplateSelect` 按 `subtype=SparkSql` 查询文档模板，查不到则为空，不报错。

### 6.2 SQL 拆分

后端用现有 `SqlUtil.getStatements(statement)` 拆分（已处理单引号内分号与注释），把语句数组写入 `request.json`；Runner 按顺序在同一 `SparkSession` 中执行，保证 `CREATE TEMPORARY VIEW` 等前后依赖。

本版不做 `${var}` 替换，也不拼接 `envId` 指向的 FlinkSQLEnv；语句原样下发，行为与现有 JDBC 查询作业一致。若用户脚本中含 `${...}`，由 Spark 按自身变量替换规则处理（未定义会报错），错误会原样回显。

### 6.3 Runner

- `main`：解析参数 → 读 `request.json` → `SparkSession.builder().appName(appName).getOrCreate()` → 逐条 `spark.sql(stmt)` → 对每条语句 `df.limit(maxRows + 1).collectAsList()` 触发执行并取数（DDL/DML 返回空结果，不影响后续语句）→ 写 `result.json`（先写 `.tmp` 再原子 rename）→ `spark.stop()`。
- 顶层 try/catch：异常写入 `result.json` 的 `error`，退出码非 0；确保 Dinky 总能拿到结构化结果。
- 不依赖 Dinky 的任何 jar，只用 Spark 自带类与 Jackson（与 Spark 3.5 自带版本一致）。

### 6.4 SparkSubmitService

- 校验：`home/bin/spark-submit` 存在、`runner-jar` 存在、`master` 非空；不满足返回明确错误（不静默降级到 local）。
- 参数优先级：全局配置 → `dinky.spark.conf` → 任务 `configJson.customConfig` 中以 `spark.` 开头的键（后者覆盖前者）。
- 默认注入 `spark.driver.host`（默认取本机 hostname，即 `dinky`）与 `spark.driver.bindAddress=0.0.0.0`，保证 worker 上的 executor 能回连 driver；二者均可被配置覆盖。
- `ProcessBuilder` 传参数组，不经过 shell，避免注入；stdout/stderr 合并重定向到 `/tmp/dinky-spark/<uuid>/submit.log`。
- `waitFor(timeout)`；超时 `destroyForcibly()` 并返回失败。
- 临时目录：成功即删；失败保留 `submit.log` 与 `result.json`，便于定位（路径写入错误信息）。

### 6.5 前端运行链路

- "运行"按钮对 SparkSql 已经可见（现有 `notIncludes` 条件），点击后先保存再调用 `submitTask`。
- 结果预览条件从 `isSql(dialect)` 扩展为 `isSql(dialect) || isSparkSql(dialect)`。
- "校验"按钮（`handleCheck` → explain）对 SparkSql 隐藏，避免调用 Flink explain。
- 运行成功后对 SparkSql 直接把状态置 `SUCCESS`，不进入 `isRunning`，避免出现点了会失败的"停止"按钮。

## 7. 验收标准

环境准备：`spark-master` / `spark-worker` healthy；如需腾资源先 `docker compose stop spark-thrift`。

1. 创建作业弹窗的"作业类型"中出现 `SparkSql`，可创建并保存，刷新后仍在；
2. 打开作业，编辑器按 SQL 高亮；运行
   `SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(CAST(amount*100 AS BIGINT)),0) FROM paimon.ods.orders;`
   后 Dinky 结果表格出现一行三列，数值与基线一致（`10 / 55 / 247390`）；
3. 运行期间/结束后，`http://192.168.168.94:18082` 的 Applications 中出现 `Dinky-<任务名>`，状态 RUNNING → FINISHED，worker 上可见 executor；
4. 故意写错 SQL，Dinky 返回明确错误信息，不出现"成功但无结果"；
5. 多语句（如先 `CREATE TEMPORARY VIEW` 再 `SELECT`）能按顺序执行；
6. 回归：FlinkSql / FlinkJar / JDBC 查询作业的创建、保存、运行不受影响；作业类型下拉原有选项不变。

## 8. 风险与已知取舍

| 风险 | 说明 / 应对 |
|---|---|
| Java 8 运行 Spark 3.5 | Spark 3.5 官方支持 Java 8；若现场出现类/运行时问题，配置 `dinky.spark.java-home` 指向 JDK 11 |
| driver 回连 | 默认 `spark.driver.host=dinky`，依赖 compose 网络 DNS；若宿主机部署需改成可达 IP |
| worker 资源被 Thrift 占用 | 验收时停 `spark-thrift`；默认 `cores-max=1` |
| 结果集过大 | `result-max-rows` 截断并标记 `truncated`；本版不做分页 |
| HTTP 同步阻塞 | 提交请求会等到 Spark 作业结束；超时可配。大查询在浏览器侧表现为长时间 loading |
| 无法取消 | 本版无停止入口；进程超时会强制结束 |
| 作业历史缺失 | 不写 `JobInstance`，运维中心看不到 Spark 作业——已列为非目标 |
| 异常中断残留 | 子进程异常退出时保留日志目录，便于排查；不影响 Dinky 主进程 |
| 变量机制缺失 | 本版不解析 `${var}`、不前置 FlinkSQLEnv；与 JDBC 作业一致，需要在验收与用户说明中写清 |
| 失败详情有限 | 失败时前端只弹 toast（含错误摘要与 `submit.log` 路径），结果面板/Output 不展示结构化失败详情 |
| 无文档模板 | Document 模块不提供 SparkSql 子类型，创建作业时模板区为空 |

## 9. 评审问题处置（2026-11-02）

| # | 问题 | 处置 |
|---|---|---|
| 1 | 是否支持 `${var}` 变量与 FlinkSQLEnv 前置 SQL | **不支持**。与 JDBC 查询作业现状一致，语句原样下发；文档、验收、用户说明中注明 |
| 2 | 任务级 `customConfig` / `maxRowNum` 入口与优先级 | 本版**不开放**任务级配置页签；最大行数只取全局 `dinky.spark.result-max-rows`。**不读取** `configJson.customConfig`（安全审查：任意 `spark.*` 键可导致 client 模式 driver 代码执行），所有 Spark 参数只来自服务端 `dinky.spark.*` |
| 3 | 提交失败的呈现程度 | 不改 `submitTask` 通用契约：Spark 返回 FAILED 的 `JobResult`（不抛异常），沿用现有 toast；前端补 `result` 判空；错误信息带 `submit.log` 路径。本版不在结果面板做结构化错误展示 |
| 4 | Runner jar 路径与版本号 | jar `finalName` 固定为 `dinky-spark-runner`，发布到 `lib/`；新增 `dinky.spark.runner-jar` 配置键，默认 `$DINKY_HOME/lib/spark/dinky-spark-runner.jar`，不使用通配匹配 |
| 5 | `enabled=false` 时前端是否展示 SparkSql | **始终展示**；后端在提交时校验配置并返回明确错误。本部署设置 `enabled=true` |
| 6 | 是否增加 SparkSql 文档模板子类型 | **不增加**，模板区为空可接受 |
| 7 | 发布/下线、DS 推送按钮是否隐藏 | **对 SparkSql 隐藏**，避免发布后"运行不保存"以及调度非目标带来的误解 |

## 10. 留给实现者的决定

- Runner 内 Spark 会话是否 `enableHiveSupport()`（默认先不启用，Paimon catalog 不依赖它）；
- result.json 的字段命名与 `SelectResult` 的映射细节；
- 临时目录命名、日志保留策略、并发提交保护（同一任务重复点击时是否直接拒绝）；
- 单元测试范围（建议至少覆盖：SQL 拆分、spark-submit 命令组装、result.json 解析）；
- 是否在结果面板显示 `truncated` 提示（建议显示）。
