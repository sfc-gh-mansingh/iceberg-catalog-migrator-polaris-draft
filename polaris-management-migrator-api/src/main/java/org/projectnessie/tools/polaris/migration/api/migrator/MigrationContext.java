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

package org.projectnessie.tools.polaris.migration.api.migrator;

import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;

import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

/**
 * Each migration has its own context to store the current migration state
 * and inject dependencies as needed from the root context object
 */
public class MigrationContext {

    private final PolarisManagementDefaultApi source;

    private final PolarisManagementDefaultApi target;

    private final PriorityQueue<MigrationTask<?>> taskQueue;

    private final MigrationLog migrationLog;

    private final ExecutorService executorService;

    public MigrationContext(
            PolarisManagementDefaultApi source,
            PolarisManagementDefaultApi target,
            MigrationLog migrationLog,
            ExecutorService executorService
    ) {
        this.source = source;
        this.target = target;
        this.migrationLog = migrationLog;
        this.executorService = executorService;
        this.taskQueue = new PriorityQueue<>((t1, t2) -> {
            // If t2 depends on t1, t1 should be prioritized (i.e., t1 < t2)
            boolean t2DependsOnT1 = t2.dependsOn().stream()
                    .anyMatch(t -> t.isAssignableFrom(t1.getClass()));

            if (t2DependsOnT1) return -1;

            // If t1 depends on t2, t2 should be prioritized (i.e., t2 < t1)
            boolean t1DependsOnT2 = t1.dependsOn().stream()
                    .anyMatch(t -> t.isAssignableFrom(t2.getClass()));

            if (t1DependsOnT2) return 1;

            // Otherwise, consider them equal priority
            return 0;
        });
    }

    public PolarisManagementDefaultApi source() {
        return this.source;
    }

    public PolarisManagementDefaultApi target() {
        return this.target;
    }

    public PriorityQueue<MigrationTask<?>> taskQueue() {
        return this.taskQueue;
    }

    public MigrationLog migrationLog() {
        return this.migrationLog;
    }

    public ExecutorService executor() {
        return this.executorService;
    }

}
