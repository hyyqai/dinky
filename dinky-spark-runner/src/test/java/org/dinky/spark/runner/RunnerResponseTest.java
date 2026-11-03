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

package org.dinky.spark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RunnerResponseTest {

    @Test
    public void serializesAllFields() throws Exception {
        StatementResult result = new StatementResult();
        result.setStatement("select a");
        result.setColumns(Collections.singletonList("a"));
        result.setRows(Collections.singletonList(Arrays.asList((Object) 1)));

        RunnerResponse response = new RunnerResponse();
        response.setSuccess(true);
        response.setAppId("app-1");
        response.setResults(Collections.singletonList(result));

        String json = new ObjectMapper().writeValueAsString(response);

        assertThat(json).contains("\"success\":true");
        assertThat(json).contains("\"appId\":\"app-1\"");
        assertThat(json).contains("\"columns\":[\"a\"]");
        assertThat(json).contains("\"rows\":[[1]]");
    }
}
