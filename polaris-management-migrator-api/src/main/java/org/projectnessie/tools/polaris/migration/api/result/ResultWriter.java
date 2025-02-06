/*
 * Copyright (C) 2025 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.projectnessie.tools.polaris.migration.api.result;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
public class ResultWriter implements AutoCloseable {

    private final CSVPrinter printer;

    private final Object fileLock = new Object();

    public ResultWriter(File file) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT
                .builder()
                .setHeader("entityName", "entityType", "status", "reason", "properties")
                .get();
        this.printer = new CSVPrinter(new FileWriter(file), format);
    }

    public void writeResult(EntityMigrationResult result) {
        synchronized (fileLock) {
            try {
                printer.printRecord(result.entityName(), result.entityType(), result.status(), result.reason(), result.properties());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.printer.close();
    }

}
