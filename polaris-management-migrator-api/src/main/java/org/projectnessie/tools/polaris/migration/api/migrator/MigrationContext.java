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
import org.projectnessie.tools.polaris.migration.api.result.ResultWriter;

import java.util.List;
import java.util.PriorityQueue;

public class MigrationContext {

    private final PolarisManagementDefaultApi source;

    private final PolarisManagementDefaultApi target;

    private final PriorityQueue<MigrationTask<?>> taskQueue;

    private final ResultWriter resultWriter;

    public MigrationContext(
            PolarisManagementDefaultApi source,
            PolarisManagementDefaultApi target,
            ResultWriter resultWriter
    ) {
        this.source = source;
        this.target = target;
        this.resultWriter = resultWriter;
        this.taskQueue = new PriorityQueue<>((t1, t2) -> {
            // If t2 depends on t1, t1 should be prioritized (i.e., t1 < t2)
            for (Class<? extends MigrationTask<?>> taskClass : t2.dependsOn()) {
                if (taskClass.isAssignableFrom(t1.getClass())) {
                    return -1;
                }
            }

            // If t1 depends on t2, t2 should be prioritized (i.e., t2 < t1)
            for (Class<? extends MigrationTask<?>> taskClass : t1.dependsOn()) {
                if (taskClass.isAssignableFrom(t2.getClass())) {
                    return 1;
                }
            }

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

    public ResultWriter resultWriter() {
        return this.resultWriter;
    }

}
