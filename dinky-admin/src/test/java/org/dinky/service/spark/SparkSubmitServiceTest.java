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

import static org.assertj.core.api.Assertions.assertThat;

import org.dinky.data.dto.TaskDTO;
import org.dinky.data.ext.ConfigItem;
import org.dinky.data.model.ext.TaskExtConfig;
import org.dinky.data.result.SelectResult;
import org.dinky.job.Job;
import org.dinky.job.JobResult;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class SparkSubmitServiceTest {

    private SparkProperties properties() {
        SparkProperties p = new SparkProperties();
        p.setEnabled(true);
        p.setHome("/opt/spark");
        p.setRunnerJar("/opt/dinky/lib/spark/dinky-spark-runner.jar");
        p.setMaster("spark://spark-master:7077");
        p.setDeployMode("client");
        p.setDriverMemory("512m");
        p.setExecutorMemory("512m");
        p.setExecutorCores(1);
        p.setCoresMax(1);
        p.setResultMaxRows(1000);
        HashMap<String, String> conf = new HashMap<>();
        conf.put("spark.driver.host", "dinky");
        conf.put("spark.executor.memory", "512m");
        p.setConf(conf);
        return p;
    }

    private TaskDTO task() {
        TaskDTO task = new TaskDTO();
        task.setId(9);
        task.setName("spark_demo");
        task.setDialect("SparkSql");
        task.setStatement("select 1; select 2");
        TaskExtConfig config = new TaskExtConfig();
        config.setCustomConfig(Arrays.asList(
                new ConfigItem("spark.executor.memory", "768m"), new ConfigItem("not.spark.key", "ignored")));
        task.setConfigJson(config);
        return task;
    }

    @Test
    public void buildCommandContainsRequiredArgsAndOverrides() {
        SparkSubmitService service = new SparkSubmitService(properties());
        List<String> command = service.buildCommand(task(), Paths.get("/tmp/req.json"), Paths.get("/tmp/res.json"));

        assertThat(command).containsSubsequence("--master", "spark://spark-master:7077");
        assertThat(command).containsSubsequence("--deploy-mode", "client");
        assertThat(command).containsSubsequence("--class", "org.dinky.spark.runner.SparkSqlRunner");
        assertThat(command).containsSubsequence("--name", "Dinky-spark_demo");
        assertThat(command).containsSubsequence("--driver-memory", "512m");
        assertThat(command).containsSubsequence("--executor-memory", "512m");
        assertThat(command).contains("spark.driver.host=dinky");
        assertThat(command).contains("spark.executor.memory=768m");
        assertThat(command).doesNotContain("not.spark.key=ignored");
        assertThat(command).contains("/opt/dinky/lib/spark/dinky-spark-runner.jar");
        assertThat(command.subList(command.size() - 4, command.size()))
                .containsExactly("--request-file", "/tmp/req.json", "--result-file", "/tmp/res.json");
    }

    @Test
    public void parseResultUsesLastSelectResult() {
        SparkSubmitService service = new SparkSubmitService(properties());
        String json = "{\"success\":true,\"appId\":\"app-1\",\"results\":["
                + "{\"statement\":\"create table t\",\"columns\":[],\"rows\":[]},"
                + "{\"statement\":\"select a,b\",\"columns\":[\"a\",\"b\"],\"rows\":[[1,\"x\"],[2,\"y\"]]}]}";

        JobResult result = service.parseResult(json, "select a,b", LocalDateTime.now(), "/tmp/submit.log");

        assertThat(result.getStatus()).isEqualTo(Job.JobStatus.SUCCESS);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getJobId()).isEqualTo("app-1");
        assertThat(result.getResult()).isInstanceOf(SelectResult.class);
        SelectResult selectResult = (SelectResult) result.getResult();
        assertThat(selectResult.getColumns()).containsExactly("a", "b");
        assertThat(selectResult.getRowData()).hasSize(2);
        assertThat(selectResult.getRowData().get(0)).containsEntry("a", 1).containsEntry("b", "x");
    }

    @Test
    public void parseResultOnMalformedJsonFails() {
        SparkSubmitService service = new SparkSubmitService(properties());
        JobResult result = service.parseResult("{not-json", "select 1", LocalDateTime.now(), "/tmp/submit.log");
        assertThat(result.getStatus()).isEqualTo(Job.JobStatus.FAILED);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getError()).contains("/tmp/submit.log");
    }

    @Test
    public void submitFailsFastWhenMasterMissing() {
        SparkProperties p = properties();
        p.setMaster("");
        SparkSubmitService service = new SparkSubmitService(p);
        JobResult result = service.submit(task());
        assertThat(result.getStatus()).isEqualTo(Job.JobStatus.FAILED);
        assertThat(result.getError()).contains("dinky.spark.master");
    }

    @Test
    public void submitFailsFastWhenDisabled() {
        SparkProperties p = properties();
        p.setEnabled(false);
        SparkSubmitService service = new SparkSubmitService(p);
        JobResult result = service.submit(task());
        assertThat(result.getStatus()).isEqualTo(Job.JobStatus.FAILED);
        assertThat(result.getError()).contains("dinky.spark.enabled");
    }
}
