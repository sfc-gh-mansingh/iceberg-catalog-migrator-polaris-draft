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

package org.projectnessie.tools.catalog.migration.cli.polaris.workspace;

//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVPrinter;
//import org.apache.commons.csv.CSVRecord;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Workspace implements Closeable {

//    private final Map<EntityPath, WorkspaceRecord> records;
//
//    private final CSVPrinter printer;
//
//    public Workspace(File file) throws IOException {
//        this.records = new HashMap<>();
//
//        if (file.exists()) {
//            CSVFormat readFormat = CSVFormat.DEFAULT
//                    .builder()
//                    .setHeader("path", "status", "reason", "signature")
//                    .setSkipHeaderRecord(true)
//                    .get();
//
//            FileReader reader = new FileReader(file);
//            Iterable<CSVRecord> records = readFormat.parse(reader);
//
//            for (CSVRecord record : records) {
//                EntityPath path = new EntityPath(record.get("path"));
//                Status status = Status.valueOf(record.get("status"));
//                String reason = record.get("reason");
//                String signature = record.get("signature");
//                this.put(new WorkspaceRecord(path, status, reason, signature), false);
//            }
//
//            reader.close();
//        }
//
//        CSVFormat writeFormat = CSVFormat.DEFAULT
//                .builder()
//                .setHeader("path", "status", "reason", "signature")
//                .get();
//
//        FileWriter fileWriter = new FileWriter(file);
//        fileWriter.write(""); // clear file
//        this.printer = new CSVPrinter(fileWriter, writeFormat);
//    }
//
//    public WorkspaceRecord get(EntityPath entityId) {
//        return records.get(entityId);
//    }
//
//    public void put(WorkspaceRecord record) {
//        put(record, true);
//    }
//
//    public void put(WorkspaceRecord record, boolean writeToFile) {
//        WorkspaceRecord workspaceRecord = this.records.putIfAbsent(record.path(), record);
//        if (workspaceRecord != null) {
//            throw new IllegalStateException("Cannot append record with duplicate path: " + workspaceRecord.path());
//        }
//
//        if (writeToFile) {
//            try {
//                printer.printRecord(
//                        record.path(),
//                        record.status().name(),
//                        record.reason(),
//                        record.signature()
//                );
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//
    @Override
    public void close() throws IOException {
        // this.printer.close();
    }

}
