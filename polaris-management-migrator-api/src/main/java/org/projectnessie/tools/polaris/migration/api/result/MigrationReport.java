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

import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MigrationReport {

    private final Queue<EntityMigrationLog> logs;

    public MigrationReport(Queue<EntityMigrationLog> logs) {
        this.logs = logs;
    }

    @Override
    public String toString() {
        Map<ManagementEntityType, List<EntityMigrationLog>> logsByType = new HashMap<>();

        StringBuilder representation = new StringBuilder();

        for (ManagementEntityType type : ManagementEntityType.values()) {
            logsByType.put(type, new ArrayList<>());
        }

        // aggregate the logs by entity type
        for (EntityMigrationLog log : logs) {
            logsByType.get(log.entityType()).add(log);
        }

        for (ManagementEntityType type : ManagementEntityType.values()) {

            if (logsByType.get(type).isEmpty()) {
                continue;
            }

            Map<TaskStatus, List<EntityMigrationLog>> logsByStatus = new HashMap<>();

            for (TaskStatus status : TaskStatus.values()) {
                logsByStatus.put(status, new ArrayList<>());
            }

            // further aggregate the logs by their status
            for (EntityMigrationLog log : logsByType.get(type)) {
                logsByStatus.get(log.status()).add(log);
            }

            representation.append(String.format("Entity: %s\n", type.name()));
            representation.append(String.format("* TOTAL RESULTS: %s\n", logsByType.get(type).size()));

            for (TaskStatus status : Arrays.stream(TaskStatus.values()).sorted().toList()) {
                representation.append(String.format(
                        "* %s: %s/%s (%.2f%%)\n",
                        status.name(),
                        logsByStatus.get(status).size(),
                        logsByType.get(type).size(),
                        ((double) logsByStatus.get(status).size() / logsByType.get(type).size()) * 100.0
                ));
            }

        }

        return representation.toString();
    }

}
