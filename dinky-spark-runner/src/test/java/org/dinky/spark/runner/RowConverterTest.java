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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import org.junit.Test;

public class RowConverterTest {

    @Test
    public void convertsCommonTypes() {
        Row row = RowFactory.create(null, "text", 7, new BigDecimal("12.34"), Timestamp.valueOf("2026-11-02 10:30:00"));

        List<Object> values = RowConverter.convert(row);

        assertThat(values.get(0)).isNull();
        assertThat(values.get(1)).isEqualTo("text");
        assertThat(values.get(2)).isEqualTo(7);
        assertThat(values.get(3)).isEqualTo("12.34");
        assertThat(values.get(4)).isEqualTo("2026-11-02 10:30:00.0");
    }

    @Test
    public void truncatesHugeCell() {
        char[] chars = new char[RowConverter.MAX_CELL_CHARS + 10];
        java.util.Arrays.fill(chars, 'a');
        String converted = (String)
                RowConverter.convert(RowFactory.create(new String(chars))).get(0);
        assertThat(converted).endsWith("...(truncated)");
        assertThat(converted.length()).isLessThanOrEqualTo(RowConverter.MAX_CELL_CHARS + 20);
    }

    @Test
    public void complexValueBecomesString() {
        Row row = RowFactory.create(new int[] {1, 2});
        assertThat(RowConverter.convert(row).get(0)).isInstanceOf(String.class);
    }
}
