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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RunnerFilesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void readsRegularRequestFile() throws Exception {
        Path request = folder.newFile("request.json").toPath();
        Files.write(
                request,
                "{\"appName\":\"Dinky-demo\",\"statements\":[\"select 1\"],\"maxRows\":5}"
                        .getBytes(StandardCharsets.UTF_8));

        RunnerRequest parsed = RunnerFiles.readRequest(request);

        assertThat(parsed.getAppName()).isEqualTo("Dinky-demo");
        assertThat(parsed.getStatements()).containsExactly("select 1");
    }

    @Test
    public void rejectsSymlinkRequestFile() throws Exception {
        Path real = folder.newFile("real.json").toPath();
        Files.write(real, "{\"statements\":[\"select 1\"]}".getBytes(StandardCharsets.UTF_8));
        Path link = folder.getRoot().toPath().resolve("request-link.json");
        try {
            Files.createSymbolicLink(link, real);
        } catch (UnsupportedOperationException | IOException e) {
            Assume.assumeNoException("symlinks unsupported", e);
        }

        assertThatThrownBy(() -> RunnerFiles.readRequest(link)).isInstanceOf(IOException.class);
    }

    @Test
    public void writesResultWithOwnerOnlyPermissionAndRefusesOverwrite() throws Exception {
        Path result = folder.getRoot().toPath().resolve("result.json");
        RunnerResponse response = new RunnerResponse();
        response.setSuccess(true);
        response.setAppId("app-1");
        response.setResults(Collections.emptyList());

        RunnerFiles.writeResult(result, response);

        assertThat(new String(Files.readAllBytes(result), StandardCharsets.UTF_8))
                .contains("\"appId\" : \"app-1\"");
        if (Files.getFileStore(result)
                .supportsFileAttributeView(java.nio.file.attribute.PosixFileAttributeView.class)) {
            assertThat(Files.getPosixFilePermissions(result)).isEqualTo(PosixFilePermissions.fromString("rw-------"));
        }
        assertThatThrownBy(() -> RunnerFiles.writeResult(result, response)).isInstanceOf(IOException.class);
    }
}
