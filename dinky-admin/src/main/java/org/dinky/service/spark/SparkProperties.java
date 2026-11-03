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

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "dinky.spark")
public class SparkProperties {

    private boolean enabled = false;

    private String home = "/opt/spark";

    private String runnerJar = "";

    private String master = "";

    private String deployMode = "client";

    private String driverMemory = "512m";

    private String executorMemory = "512m";

    private int executorCores = 1;

    private int coresMax = 1;

    private int resultMaxRows = 1000;

    private int submitTimeoutSeconds = 600;

    private int maxResultBytes = 64 * 1024 * 1024;

    private String javaHome = "";

    private String hadoopConfDir = "";

    private Map<String, String> conf = new LinkedHashMap<>();
}
