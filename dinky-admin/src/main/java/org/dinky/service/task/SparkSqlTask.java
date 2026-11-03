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

package org.dinky.service.task;

import org.dinky.config.Dialect;
import org.dinky.data.annotations.SupportDialect;
import org.dinky.data.dto.TaskDTO;
import org.dinky.job.JobResult;
import org.dinky.service.spark.SparkSubmitService;

import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SupportDialect(Dialect.SPARK_SQL)
public class SparkSqlTask extends BaseTask {

    public SparkSqlTask(TaskDTO task) {
        super(task);
    }

    @Override
    public JobResult execute() {
        return SpringUtil.getBean(SparkSubmitService.class).submit(task);
    }

    @Override
    public boolean stop() {
        return false;
    }
}
