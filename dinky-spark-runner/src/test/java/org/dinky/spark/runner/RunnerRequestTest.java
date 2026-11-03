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

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RunnerRequestTest {

    @Test
    public void parsesRequestJson() throws Exception {
        String json = "{\"appName\":\"Dinky-demo\",\"statements\":[\"select 1\",\"select 2\"],\"maxRows\":5}";
        RunnerRequest request = new ObjectMapper().readValue(json, RunnerRequest.class);

        assertThat(request.getAppName()).isEqualTo("Dinky-demo");
        assertThat(request.getStatements()).containsExactly("select 1", "select 2");
        assertThat(request.getMaxRows()).isEqualTo(5);
    }

    @Test
    public void defaultsMaxRowsWhenMissing() throws Exception {
        RunnerRequest request = new ObjectMapper().readValue("{\"statements\":[\"select 1\"]}", RunnerRequest.class);
        assertThat(request.getMaxRows()).isEqualTo(1000);
    }
}
