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

import java.util.ArrayList;
import java.util.List;

public class ManagementMigrator {

    private final PolarisManagementDefaultApi source;

    private final PolarisManagementDefaultApi target;

    private final ResultWriter resultWriter;

    public ManagementMigrator(
            PolarisManagementDefaultApi source,
            PolarisManagementDefaultApi target,
            ResultWriter resultWriter
    ) {
        this.source = source;
        this.target = target;
        this.resultWriter = resultWriter;
    }

    public List<EntityMigrationResult> migrateAll() {
        MigrationContext migrationContext = new MigrationContext(source, target, resultWriter);

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
    ) {
        MigrationContext migrationContext = new MigrationContext(source, target, resultWriter);

        return this.migrate(migrationContext,
                new CatalogsMigrationTask(migrationContext, migrateCatalogRoles, migrateGrants, migrateCatalogRoleAssignments)
        );
    }

    public List<EntityMigrationResult> migratePrincipals(
            boolean migratePrincipalRoleAssignments
    ) {
        MigrationContext migrationContext = new MigrationContext(source, target, resultWriter);

        return this.migrate(migrationContext,
                new PrincipalsMigrationTask(migrationContext, migratePrincipalRoleAssignments)
        );
    }

    public List<EntityMigrationResult> migratePrincipalRoles() {
        MigrationContext migrationContext = new MigrationContext(source, target, resultWriter);

        return this.migrate(migrationContext,
                new PrincipalRolesMigrationTask(migrationContext)
        );
    }

    private List<EntityMigrationResult> migrate(MigrationContext context, MigrationTask<?>... initialTasks) {
        for (MigrationTask<?> task : initialTasks) {
            context.taskQueue().add(task);
        }

        return this.execute(context);
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
