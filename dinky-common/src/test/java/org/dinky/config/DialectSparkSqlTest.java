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

package org.dinky.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class DialectSparkSqlTest {

    @Test
    public void sparkSqlIsRecognized() {
        assertThat(Dialect.get("SparkSql")).isEqualTo(Dialect.SPARK_SQL);
        assertThat(Dialect.get("sparksql")).isEqualTo(Dialect.SPARK_SQL);
    }

    @Test
    public void sparkSqlIsNotFlinkOrCommonSql() {
        assertThat(Dialect.isFlinkSql("SparkSql", true)).isFalse();
        assertThat(Dialect.isCommonSql("SparkSql")).isFalse();
        assertThat(Dialect.isUDF("SparkSql")).isFalse();
        assertThat(Dialect.isJarDialect("SparkSql")).isFalse();
    }
}
