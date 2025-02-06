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
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.CatalogsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.PrincipalRolesMigrationTask;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.PrincipalsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ResultWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ManagementMigrator {

    private final PolarisManagementDefaultApi source;

    private final PolarisManagementDefaultApi target;

    private final ResultWriter resultWriter;

    private final ExecutorService executor;

    public ManagementMigrator(
            PolarisManagementDefaultApi source,
            PolarisManagementDefaultApi target,
            ResultWriter resultWriter,
            int numberOfThreads
    ) {
        this.source = source;
        this.target = target;
        this.resultWriter = resultWriter;
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
    }

    private MigrationContext context() {
        return new MigrationContext(source, target, resultWriter, executor);
    }

    public List<EntityMigrationResult> migrateAll() throws Exception {
        MigrationContext migrationContext = context();

        return this.migrate(migrationContext,
                new CatalogsMigrationTask(migrationContext, true, true, true),
                new PrincipalsMigrationTask(migrationContext, true),
                new PrincipalRolesMigrationTask(migrationContext)
        );
    }

    public List<EntityMigrationResult> migrateCatalogs(
            boolean migrateCatalogRoles,
            boolean migrateCatalogRoleAssignments,
            boolean migrateGrants
    ) throws Exception {
        MigrationContext migrationContext = context();

        return this.migrate(migrationContext,
                new CatalogsMigrationTask(migrationContext, migrateCatalogRoles, migrateGrants, migrateCatalogRoleAssignments)
        );
    }

    public List<EntityMigrationResult> migratePrincipals(
            boolean migratePrincipalRoleAssignments
    ) throws Exception {
        MigrationContext migrationContext = context();

        return this.migrate(migrationContext,
                new PrincipalsMigrationTask(migrationContext, migratePrincipalRoleAssignments)
        );
    }

    public List<EntityMigrationResult> migratePrincipalRoles() throws Exception {
        MigrationContext migrationContext = context();

        return this.migrate(migrationContext,
                new PrincipalRolesMigrationTask(migrationContext)
        );
    }

    private List<EntityMigrationResult> migrate(MigrationContext context, MigrationTask<?>... initialTasks) throws Exception {
        for (MigrationTask<?> task : initialTasks) {
            context.taskQueue().add(task);
        }

        List<EntityMigrationResult> results = this.execute(context);
        context.resultWriter().close();
        return results;
    }

    private List<EntityMigrationResult> execute(MigrationContext context) {
        List<EntityMigrationResult> results = new ArrayList<>();

        while (!context.taskQueue().isEmpty()) {
            List<EntityMigrationResult> taskResults = context.taskQueue().poll().migrate();
            results.addAll(taskResults);
        }

        return results;
    }

}
