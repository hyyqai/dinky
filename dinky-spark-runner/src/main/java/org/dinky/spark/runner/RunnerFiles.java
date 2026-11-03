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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class RunnerFiles {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RunnerFiles() {}

    public static RunnerRequest readRequest(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Request file is not a regular file: " + path);
        }
        return MAPPER.readValue(path.toFile(), RunnerRequest.class);
    }

    public static void writeResult(Path resultFile, RunnerResponse response) throws IOException {
        Path directory = resultFile.toAbsolutePath().getParent();
        if (directory == null || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Result directory is not a directory: " + directory);
        }
        if (Files.exists(resultFile, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Result file already exists: " + resultFile);
        }
        Path tmp = Files.createTempFile(directory, "dinky-spark-result-", ".tmp");
        try {
            if (Files.getFileStore(tmp).supportsFileAttributeView(PosixFileAttributeView.class)) {
                Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------"));
            }
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), response);
            Files.move(tmp, resultFile, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
