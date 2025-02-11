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

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe container for synchronized log writing logic.
 */
public class MigrationLog {

    private final Queue<EntityMigrationLog> logs = new ConcurrentLinkedQueue<>();

    private final CSVPrinter printer;

    private final Object fileLock = new Object();

    private final Gson gson;

    public MigrationLog(File file) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT
                .builder()
                .setHeader("entityType", "description", "status", "reason", "properties")
                .get();
        this.printer = new CSVPrinter(new FileWriter(file), format);
        this.gson = new Gson();
    }

    public MigrationReport generateReport() {
        return new MigrationReport(logs);
    }

    public Queue<EntityMigrationLog> getLogs() {
        return logs;
    }

    /**
     * Append the migration log to the file and in-memory log store.
     * @param log
     */
    public void append(EntityMigrationLog log) {
        synchronized (fileLock) {
            logs.add(log);

            try {
                printer.printRecord(
                        log.entityType(),
                        log.entityDescription(),
                        log.status(),
                        log.reason(),
                        gson.toJson(log.properties())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws IOException {
        this.printer.close();
    }

}
