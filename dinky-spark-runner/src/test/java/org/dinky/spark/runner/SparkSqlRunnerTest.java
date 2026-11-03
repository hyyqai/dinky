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

public class SparkSqlRunnerTest {

    @Test
    public void normalizeMaxRowsClampsToSafeRange() {
        assertThat(SparkSqlRunner.normalizeMaxRows(-5)).isEqualTo(1);
        assertThat(SparkSqlRunner.normalizeMaxRows(0)).isEqualTo(1);
        assertThat(SparkSqlRunner.normalizeMaxRows(1000)).isEqualTo(1000);
        assertThat(SparkSqlRunner.normalizeMaxRows(Integer.MAX_VALUE)).isEqualTo(SparkSqlRunner.MAX_ROWS);
    }

    @Test
    public void statementLabelDoesNotExposeSqlText() {
        String label = SparkSqlRunner.statementLabel(2, "select * from t where password='secret'");
        assertThat(label).doesNotContain("select").doesNotContain("password");
        assertThat(label).contains("#2").contains("chars");
    }

    @Test
    public void redactsQuotedAndJdbcSecrets() {
        String redacted = SparkSqlRunner.redact("failed: 'password' => 'secret' url=jdbc:mysql://user:secret@host/db");
        assertThat(redacted).doesNotContain("secret");
        assertThat(redacted).contains("password=***");
        assertThat(redacted).contains("user:***@");
    }

    @Test
    public void redactsSecretsFromErrorText() {
        String redacted = SparkSqlRunner.redact("failed: password='secret' and token: abc");
        assertThat(redacted).doesNotContain("secret").doesNotContain("abc");
    }
}
