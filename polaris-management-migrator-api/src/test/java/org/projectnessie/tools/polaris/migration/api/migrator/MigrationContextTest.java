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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.CatalogRolesMigrationTask;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.CatalogsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.GrantsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;

import java.util.List;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;

public class MigrationContextTest {

    @Test
    public void ensureDependenciesOrderedFirstInTaskQueue() {
        MigrationContext migrationContext = new MigrationContext(
                Mockito.mock(PolarisManagementDefaultApi.class),
                Mockito.mock(PolarisManagementDefaultApi.class),
                Mockito.mock(MigrationLog.class),
                Executors.newSingleThreadExecutor()
        );

        CatalogsMigrationTask catalogsMigrationTask = Mockito.mock(CatalogsMigrationTask.class);
        CatalogRolesMigrationTask rolesMigrationTask = Mockito.mock(CatalogRolesMigrationTask.class);
        GrantsMigrationTask grantsMigrationTask = Mockito.mock(GrantsMigrationTask.class);

        when(catalogsMigrationTask.dependsOn()).thenReturn(List.of());
        when(rolesMigrationTask.dependsOn()).thenReturn(List.of(CatalogsMigrationTask.class));
        when(grantsMigrationTask.dependsOn()).thenReturn(List.of(CatalogsMigrationTask.class, CatalogRolesMigrationTask.class));

        // enqueue them out of order
        migrationContext.taskQueue().add(grantsMigrationTask);
        migrationContext.taskQueue().add(catalogsMigrationTask);
        migrationContext.taskQueue().add(rolesMigrationTask);

        MigrationTask<?> firstTask = migrationContext.taskQueue().poll();
        MigrationTask<?> secondTask = migrationContext.taskQueue().poll();
        MigrationTask<?> thirdTask = migrationContext.taskQueue().poll();

        Assertions.assertInstanceOf(CatalogsMigrationTask.class, firstTask);
        Assertions.assertInstanceOf(CatalogRolesMigrationTask.class, secondTask);
        Assertions.assertInstanceOf(GrantsMigrationTask.class, thirdTask);
    }

}
