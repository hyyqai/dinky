/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.service.spark;

import org.dinky.data.dto.TaskDTO;
import org.dinky.data.result.SelectResult;
import org.dinky.job.Job;
import org.dinky.job.JobResult;
import org.dinky.utils.JsonUtils;
import org.dinky.utils.SqlUtil;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

import cn.hutool.core.io.FileUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class SparkSubmitService {

    public static final String RUNNER_MAIN_CLASS = "org.dinky.spark.runner.SparkSqlRunner";

    private static final int MAX_LOG_TAIL_BYTES = 64 * 1024;

    private static final Pattern SECRET_PATTERN =
            Pattern.compile("(?i)[\"'`]?(password|passwd|pwd|secret|token|access[._-]?key)[\"'`]?\\s*(?:=>|[=:])?\\s*"
                    + "('(?:[^']|'')*'|\"(?:[^\"\\\\]|\\\\.)*\"|`[^`]*`|\\S+)");

    private final SparkProperties properties;

    public JobResult submit(TaskDTO task) {
        LocalDateTime startTime = LocalDateTime.now();
        String validationError = validate();
        if (validationError != null) {
            return failed(task.getStatement(), startTime, validationError);
        }

        String[] statements = SqlUtil.getStatements(task.getStatement());
        if (statements.length == 0) {
            return failed(task.getStatement(), startTime, "statement is empty");
        }

        Path workDir = null;
        try {
            workDir = createWorkDir(task.getId());
            Path requestFile = workDir.resolve("request.json");
            Path resultFile = workDir.resolve("result.json");
            Path logFile = workDir.resolve("submit.log");
            writeRequest(requestFile, task, statements);

            ProcessBuilder processBuilder = new ProcessBuilder(buildCommand(task, requestFile, resultFile));
            processBuilder.redirectErrorStream(true);
            processBuilder.redirectOutput(logFile.toFile());
            applyEnvironment(processBuilder);

            Process process = processBuilder.start();
            boolean finished = process.waitFor(properties.getSubmitTimeoutSeconds(), TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                return failed(
                        task.getStatement(),
                        startTime,
                        "Spark job timeout after " + properties.getSubmitTimeoutSeconds() + "s, log: " + logFile);
            }

            if (!Files.isRegularFile(resultFile, LinkOption.NOFOLLOW_LINKS)) {
                return failed(
                        task.getStatement(),
                        startTime,
                        "Spark job finished with exit code " + process.exitValue() + " but no result file, log: "
                                + logFile + "\n" + readTail(logFile, MAX_LOG_TAIL_BYTES));
            }

            String json = readResultFile(resultFile);
            JobResult result = parseResult(json, task.getStatement(), startTime, logFile.toString());
            if (result.isSuccess()) {
                FileUtil.del(workDir.toFile());
            }
            return result;
        } catch (Exception e) {
            log.error("Spark submit failed, task: {}", task.getName(), e);
            return failed(
                    task.getStatement(),
                    startTime,
                    "Spark submit failed: " + e.getMessage() + (workDir == null ? "" : ", work dir: " + workDir));
        }
    }

    public List<String> buildCommand(TaskDTO task, Path requestFile, Path resultFile) {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(properties.getHome(), "bin", "spark-submit").toString());
        command.add("--master");
        command.add(properties.getMaster());
        command.add("--deploy-mode");
        command.add(properties.getDeployMode());
        command.add("--class");
        command.add(RUNNER_MAIN_CLASS);
        command.add("--name");
        command.add("Dinky-" + task.getName());
        command.add("--driver-memory");
        command.add(properties.getDriverMemory());
        command.add("--executor-memory");
        command.add(properties.getExecutorMemory());
        command.add("--executor-cores");
        command.add(String.valueOf(properties.getExecutorCores()));
        for (Map.Entry<String, String> entry : mergedConf().entrySet()) {
            command.add("--conf");
            command.add(entry.getKey() + "=" + entry.getValue());
        }
        command.add(resolveRunnerJar().toString());
        command.add("--request-file");
        command.add(requestFile.toString());
        command.add("--result-file");
        command.add(resultFile.toString());
        return command;
    }

    public JobResult parseResult(String json, String statement, LocalDateTime startTime, String logPath) {
        SparkResultJson parsed;
        try {
            parsed = JsonUtils.parseObject(json, SparkResultJson.class);
        } catch (Exception e) {
            return failed(
                    statement,
                    startTime,
                    "Failed to parse Spark result, log: " + logPath + ", error: " + e.getMessage());
        }
        if (parsed == null) {
            return failed(statement, startTime, "Failed to parse Spark result, log: " + logPath);
        }
        if (!parsed.isSuccess()) {
            String error = StringUtils.defaultIfBlank(parsed.getError(), "Spark job failed");
            return failed(statement, startTime, error + " (log: " + logPath + ")");
        }

        SelectResult lastResult = null;
        if (parsed.getResults() != null) {
            for (SparkResultJson.Result item : parsed.getResults()) {
                if (item != null
                        && item.getColumns() != null
                        && !item.getColumns().isEmpty()) {
                    lastResult = toSelectResult(parsed.getAppId(), item);
                }
            }
        }
        if (lastResult == null) {
            lastResult = new SelectResult(parsed.getAppId(), new ArrayList<>(), new LinkedHashSet<>());
        }

        JobResult result = new JobResult();
        result.setStatement(statement);
        result.setStatus(Job.JobStatus.SUCCESS);
        result.setSuccess(true);
        result.setJobId(parsed.getAppId());
        result.setResult(lastResult);
        result.setStartTime(startTime);
        result.setEndTime(LocalDateTime.now());
        return result;
    }

    private SelectResult toSelectResult(String appId, SparkResultJson.Result item) {
        LinkedHashSet<String> columns = new LinkedHashSet<>(item.getColumns());
        List<Map<String, Object>> rowData = new ArrayList<>();
        if (item.getRows() != null) {
            for (List<Object> row : item.getRows()) {
                Map<String, Object> rowMap = new LinkedHashMap<>();
                for (int i = 0; i < item.getColumns().size(); i++) {
                    rowMap.put(item.getColumns().get(i), i < row.size() ? row.get(i) : null);
                }
                rowData.add(rowMap);
            }
        }
        SelectResult selectResult = new SelectResult(appId, rowData, columns);
        selectResult.setTruncationFlag(item.isTruncated());
        return selectResult;
    }

    private String validate() {
        if (!properties.isEnabled()) {
            return "Spark support is disabled. Set dinky.spark.enabled=true";
        }
        if (StringUtils.isBlank(properties.getMaster())) {
            return "dinky.spark.master is required";
        }
        if (!"client".equalsIgnoreCase(properties.getDeployMode())) {
            return "dinky.spark.deploy-mode must be client, got: " + properties.getDeployMode();
        }
        Path sparkSubmit = Paths.get(properties.getHome(), "bin", "spark-submit");
        if (!Files.isRegularFile(sparkSubmit) || !Files.isExecutable(sparkSubmit)) {
            return "spark-submit not found under dinky.spark.home: " + properties.getHome();
        }
        Path runnerJar = resolveRunnerJar();
        if (!Files.isRegularFile(runnerJar)) {
            return "Spark runner jar not found: " + runnerJar + ". Set dinky.spark.runner-jar";
        }
        return null;
    }

    private Map<String, String> mergedConf() {
        Map<String, String> merged = new LinkedHashMap<>(properties.getConf());
        merged.putIfAbsent("spark.driver.host", localHostName());
        merged.putIfAbsent("spark.driver.bindAddress", "0.0.0.0");
        merged.putIfAbsent("spark.cores.max", String.valueOf(properties.getCoresMax()));
        return merged;
    }

    private String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private Path resolveRunnerJar() {
        if (StringUtils.isNotBlank(properties.getRunnerJar())) {
            return Paths.get(properties.getRunnerJar());
        }
        String dinkyHome = System.getenv("DINKY_HOME");
        if (StringUtils.isBlank(dinkyHome)) {
            dinkyHome = System.getProperty("user.dir");
        }
        return Paths.get(dinkyHome, "lib", "spark", "dinky-spark-runner.jar");
    }

    private Path createWorkDir(Integer taskId) throws IOException {
        Path root = Paths.get(System.getProperty("java.io.tmpdir"), "dinky-spark");
        Files.createDirectories(root);
        Path workDir = Files.createTempDirectory(root, "task-" + (taskId == null ? "unknown" : taskId) + "-");
        if (Files.getFileStore(workDir).supportsFileAttributeView(PosixFileAttributeView.class)) {
            Files.setPosixFilePermissions(workDir, PosixFilePermissions.fromString("rwx------"));
        }
        return workDir;
    }

    private void writeRequest(Path requestFile, TaskDTO task, String[] statements) throws IOException {
        SparkRequestJson request = new SparkRequestJson();
        request.setAppName("Dinky-" + task.getName());
        request.setStatements(Arrays.asList(statements));
        request.setMaxRows(properties.getResultMaxRows());

        Files.createFile(requestFile);
        if (Files.getFileStore(requestFile).supportsFileAttributeView(PosixFileAttributeView.class)) {
            Files.setPosixFilePermissions(requestFile, PosixFilePermissions.fromString("rw-------"));
        }
        Files.write(requestFile, JsonUtils.toJsonString(request).getBytes(StandardCharsets.UTF_8));
    }

    private void applyEnvironment(ProcessBuilder processBuilder) {
        Map<String, String> environment = processBuilder.environment();
        if (StringUtils.isNotBlank(properties.getHadoopConfDir())) {
            environment.put("HADOOP_CONF_DIR", properties.getHadoopConfDir());
        }
        if (StringUtils.isNotBlank(properties.getJavaHome())) {
            environment.put("JAVA_HOME", properties.getJavaHome());
        }
    }

    String readResultFile(Path resultFile) throws IOException {
        long size = Files.size(resultFile);
        if (size > properties.getMaxResultBytes()) {
            throw new IOException("Spark result file is too large: " + size + " bytes, limit "
                    + properties.getMaxResultBytes() + " bytes");
        }
        return new String(Files.readAllBytes(resultFile), StandardCharsets.UTF_8);
    }

    String readTail(Path logFile, int maxBytes) {
        try (RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "r")) {
            long length = file.length();
            long start = Math.max(0, length - maxBytes);
            file.seek(start);
            byte[] buffer = new byte[(int) (length - start)];
            file.readFully(buffer);
            String text = new String(buffer, StandardCharsets.UTF_8);
            if (start > 0) {
                int firstNewline = text.indexOf('\n');
                if (firstNewline >= 0 && firstNewline + 1 < text.length()) {
                    text = text.substring(firstNewline + 1);
                }
            }
            return SECRET_PATTERN.matcher(text).replaceAll("$1=***");
        } catch (Exception e) {
            return "";
        }
    }

    private JobResult failed(String statement, LocalDateTime startTime, String error) {
        JobResult result = new JobResult();
        result.setStatement(statement);
        result.setStatus(Job.JobStatus.FAILED);
        result.setSuccess(false);
        result.setError(error);
        result.setStartTime(startTime);
        result.setEndTime(LocalDateTime.now());
        return result;
    }
}
