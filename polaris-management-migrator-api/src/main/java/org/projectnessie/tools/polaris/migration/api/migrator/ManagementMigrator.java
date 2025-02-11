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
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Migrator service class to run configurations of different Polaris entity migrations.
 */
public class ManagementMigrator {

    private final PolarisManagementDefaultApi source;

    private final PolarisManagementDefaultApi target;

    private final MigrationLog migrationLog;

    private final ExecutorService executor;

    public ManagementMigrator(
            PolarisManagementDefaultApi source,
            PolarisManagementDefaultApi target,
            MigrationLog migrationLog,
            int numberOfThreads
    ) {
        this.source = source;
        this.target = target;
        this.migrationLog = migrationLog;
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
    }

    private MigrationContext createContext() {
        return new MigrationContext(source, target, migrationLog, executor);
    }

    /**
     * Migrate all Polaris entities.
     * @return the log of the migration events that took place
     * @throws Exception
     */
    public MigrationLog migrateAll() throws Exception {
        MigrationContext migrationContext = createContext();

        return this.migrate(migrationContext,
                new CatalogsMigrationTask(migrationContext, true, true, true),
                new PrincipalsMigrationTask(migrationContext, true),
                new PrincipalRolesMigrationTask(migrationContext)
        );
    }

    /**
     * Migrate all Polaris catalog entities and possibly their hierarchical subentities
     * @param migrateCatalogRoles true if the migration of the catalog roles is desired
     * @param migrateCatalogRoleAssignments true if the migration of catalog role assignments to principal roles is desired
     * @param migrateGrants true if the migration of grants under catalog roles is desired
     * @return the log of migration events that took place
     * @throws Exception
     */
    public MigrationLog migrateCatalogs(
            boolean migrateCatalogRoles,
            boolean migrateCatalogRoleAssignments,
            boolean migrateGrants
    ) throws Exception {
        MigrationContext migrationContext = createContext();

        return this.migrate(migrationContext,
                new CatalogsMigrationTask(migrationContext, migrateCatalogRoles, migrateGrants, migrateCatalogRoleAssignments)
        );
    }

    /**
     * Migrate all Polaris principal entities
     * @param migratePrincipalRoleAssignments true if migration of assignment of principals to principal roles is desired
     * @return the log of migration events that took place
     * @throws Exception
     */
    public MigrationLog migratePrincipals(
            boolean migratePrincipalRoleAssignments
    ) throws Exception {
        MigrationContext migrationContext = createContext();

        return this.migrate(migrationContext,
                new PrincipalsMigrationTask(migrationContext, migratePrincipalRoleAssignments)
        );
    }

    /**
     * Migrate all Polaris principal role entities
     * @return the log of migration events that took place
     * @throws Exception
     */
    public MigrationLog migratePrincipalRoles() throws Exception {
        MigrationContext migrationContext = createContext();

        return this.migrate(migrationContext,
                new PrincipalRolesMigrationTask(migrationContext)
        );
    }

    private MigrationLog migrate(MigrationContext context, MigrationTask<?>... initialTasks) throws Exception {
        for (MigrationTask<?> task : initialTasks) {
            context.taskQueue().add(task);
        }

        this.execute(context);
        context.migrationLog().close();

        return context.migrationLog();
    }

    private void execute(MigrationContext context) {
        while (!context.taskQueue().isEmpty()) {
            context.taskQueue().poll().migrate();
        }
    }

}
