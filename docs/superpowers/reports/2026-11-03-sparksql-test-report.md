# SparkSQL 作业类型（最小闭环）测试报告

- 日期：2026-11-03
- 分支：1.2.4
- 代码基线：`139f716b2 fix: 补全脱敏规则并覆盖日志 tail`
- 发布包：`build/dinky-release-1.20-1.2.4.tar.gz`（160 MB）
- 发布包 SHA256：`a8042677cd47569d7044532df3e3fc567a84ca161cb7304ae4820259e07522b7`

## 1. 测试范围

本次验证覆盖 SparkSQL 最小闭环的代码单元与构建产物，不包含服务器端端到端验收（见第 8 节）：

- 方言识别与回归（`Dialect.SPARK_SQL`）
- Runner 的请求/响应 JSON、行类型转换、文件安全、脱敏、行数限制
- 提交服务的配置 fail-fast、命令组装、结果解析、截断标记、超时、结果文件大小限制、日志 tail
- 作业分发（`BaseTask.getTask` → `SparkSqlTask`）
- 前端常量、编辑器映射、运行/结果/按钮可见性
- 发布包构建与包内容

## 2. 测试环境

| 项 | 值 |
|---|---|
| 操作系统 | macOS（Darwin） |
| Maven | 3.8.4（wrapper） |
| Maven JVM | OpenJDK 17.0.15 |
| 前端 | pnpm 9.15.4 / TypeScript |
| Spark 依赖 | 3.5.3（Scala 2.12，`provided`） |
| 目标字节码 | Java 8 |

## 3. 后端自动化测试

| 模块 | 测试类 | 用例数 | 结果 |
|---|---|---|---|
| dinky-common | `DialectSparkSqlTest` | 2 | 2/2 通过 |
| dinky-spark-runner | `RunnerRequestTest` | 2 | 2/2 通过 |
| dinky-spark-runner | `RunnerResponseTest` | 1 | 1/1 通过 |
| dinky-spark-runner | `RowConverterTest` | 4 | 4/4 通过 |
| dinky-spark-runner | `RunnerFilesTest` | 3 | 3/3 通过 |
| dinky-spark-runner | `SparkSqlRunnerTest` | 7 | 7/7 通过 |
| dinky-admin | `SparkSubmitServiceTest` | 15 | 15/15 通过 |
| dinky-admin | `SparkSqlTaskTest` | 1 | 1/1 通过 |
| **合计** | | **35** | **35/35 通过** |

关键用例：

- `submitFailsFastWhenDisabled` / `submitFailsFastWhenMasterMissing` / `submitFailsFastWhenDeployModeIsNotClient`
- `submitFailsWhenSparkSubmitMissing` / `submitFailsWhenRunnerJarMissing` / `submitFailsOnTimeout`
- `parseResultUsesLastSelectResult` / `parseResultKeepsTruncationFlag` / `parseResultSkipsNullResultEntries` / `parseResultOnMalformedJsonFails`
- `readsBoundedTail` / `readTailRedactsSecrets` / `rejectsOversizedResultFile`
- `ignoresTaskLevelSparkConf`（任务级 `spark.*` 配置不透传）
- `RunnerFilesTest`：符号链接拒绝、0600 权限、拒绝覆盖结果文件
- `SparkSqlRunnerTest`：脱敏（`=`/`=>`/空格、单双引号、反引号、SQL 双写引号、JDBC userinfo）、失败摘要不含堆栈、单元格/行数上限

测试日志保存在 `.superpowers/sdd/2026-11-03-sparksql-job-type/test-logs/`（本地，未入库）。

## 4. 前端检查

| 检查 | 命令 | 结果 |
|---|---|---|
| 改动文件类型检查 | `pnpm run tsc` + 按文件过滤 | 本次 7 个改动文件 **0 错误** |
| 格式检查 | `pnpm exec prettier -c <7 files>` | 全部通过 |
| 全量 tsc | `pnpm run tsc` | 仓库既有 251 个错误（本次改动未引入；均为历史问题，如 `About/QRCode.tsx` 引用不存在的 `VERSION`） |
| ESLint | `pnpm exec eslint ...` | 仓库 `.eslintrc.js` 与本地 ESLint 9.39.4 不兼容，未能执行；以 tsc + prettier 代替 |

改动文件：

- `services/constants.tsx`
- `pages/DataStudio/utils.tsx`
- `pages/DataStudio/function.tsx`
- `pages/DataStudio/constants.tsx`
- `pages/DataStudio/CenterTabContent/SqlTask/index.tsx`
- `pages/DataStudio/Toolbar/Service/Result/index.tsx`
- `pages/DataStudio/Toolbar/Service/index.tsx`

## 5. 构建验证

命令：

```bash
./mvnw clean package -Dmaven.test.skip=true \
  -P '!jdk11,aliyun,prod,web,flink-1.20,flink-single-version,npm-taobao' \
  -Dspotless.check.skip=true
```

结果：`BUILD SUCCESS`。

> 注意：Maven 运行在 JDK 11+ 时，仓库的 `jdk11` profile 会自动激活并把字节码目标改为 11。
> 发布包要跑在 Java 8 容器里，必须显式 `-P '!jdk11'`；已核验 `dinky-admin` 与
> `dinky-spark-runner` 的 class major version 均为 52。

包内容核验：

- `lib/dinky-admin-1.2.4.jar` 存在，含 `deploy-mode` 校验、`truncationFlag`、日志 tail 脱敏
- `lib/spark/dinky-spark-runner.jar` 存在，含 runner 主类、文件安全、脱敏规则
- `html` 与 admin jar 内 `static` 均包含 `Spark Job Type` / `sparksql`

## 6. 安全审查处置

| 轮次 | 发现 | 处置 |
|---|---|---|
| 1 | 临时文件/符号链接、SQL 日志、结果写入、行数上限 | 已修复：`RunnerFiles`（NOFOLLOW、0600、原子移动、拒绝覆盖）、不打印 SQL、行/语句/单元格上限 |
| 2 | 任务级 `spark.*` 配置注入（HIGH）、结果/日志无界读取 | 已修复：移除 `customConfig` 透传、64MB 结果上限、64KB 日志 tail、byte[] 预截断 |
| 3 | 错误响应含堆栈、日志脱敏不完整 | 已修复：API 只返回异常类+脱敏消息，堆栈仅写 `submit.log`；补充 `OPTIONS` 脱敏 |
| 4 | 脱敏的引号/转义变体 | 已修复：统一正则覆盖单双引号、反引号、SQL 双写引号、JDBC userinfo；`readTail` 也脱敏 |
| - | client 模式在 Dinky 容器内执行用户 SQL | 已记录为特权能力：登录 + task owner 校验 + 显式 `enabled=true`；容器与 0700 临时目录为隔离边界（spec 风险表） |

## 7. 未覆盖与已知遗留

- 未在本地启动 Spark 做 `local[1]` 集成测试：Maven 运行在 Java 17，Spark 3.5 需要 `--add-opens`，测试环境不稳定；多语句执行改由服务器 E2E 覆盖。
- 未执行仓库全量单元测试（存在与本次无关的历史用例/环境依赖）。
- 已记录但本版未修的 Minor：重复列名覆盖、`ATOMIC_MOVE` 无降级、失败 workdir 无定期清理、`--conf` 出现在进程命令行、中断标记未恢复、null 单元格筛选 JS 错误、失败后结果面板保留旧结果、`SparkSqlTask` 多余 `@Slf4j`。

## 8. 端到端验收计划（服务器 192.168.168.94）

1. 用 1.2.4 官方基础镜像 + 本次发布包构建 `iserv/dinky`，重启 Dinky；
2. `docker compose stop spark-thrift` 释放 worker 资源；
3. Dinky UI 新建 `SparkSql` 作业，执行：

```sql
SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(CAST(amount*100 AS BIGINT)),0)
FROM paimon.ods.orders;
```

4. 期望结果 `10 / 55 / 247390`；
5. Spark Master UI（`http://192.168.168.94:18082`）Applications 出现 `Dinky-<任务名>`；
6. 多语句验证：`CREATE OR REPLACE TEMPORARY VIEW` + `SELECT`；
7. 回归：FlinkSql / JDBC 作业创建与运行不受影响；
8. 验收完成后恢复 `spark-thrift`。

## 9. 部署中发现的构建问题

首次部署时 Dinky 容器循环重启，日志为：

```
java.lang.UnsupportedClassVersionError: org/dinky/Dinky has been compiled by a more
recent version of the Java Runtime (class file version 55.0), this version of the
Java Runtime only recognizes class file versions up to 52.0
```

原因：本机 Maven 运行在 JDK 17，仓库 `jdk11` profile（`<jdk>[11,)</jdk>`）自动激活，
把发布包编译为 Java 11（class 55），而 Dinky 镜像运行 Java 8（class 52）。

处理：使用 `-P '!jdk11,...'` 重新构建，核验 class major=52 后重新部署。

## 10. 结论

代码级测试与构建验证通过（35/35，构建成功，产物核验通过）。端到端验收待服务器部署后执行。
