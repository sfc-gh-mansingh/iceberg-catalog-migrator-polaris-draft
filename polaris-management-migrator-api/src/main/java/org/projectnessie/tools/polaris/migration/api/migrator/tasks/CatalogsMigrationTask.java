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

package org.projectnessie.tools.polaris.migration.api.migrator.tasks;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationResult;

import java.util.ArrayList;
import java.util.List;

public class CatalogsMigrationTask extends MigrationTask<Catalog> {

    private final List<Catalog> candidatesForCatalogRoleMigration;

    private final boolean migrateCatalogRoles;

    private final boolean migrateGrants;

    private final boolean migrateCatalogRoleAssignments;

    public CatalogsMigrationTask(
            MigrationContext context,
            boolean migrateCatalogRoles,
            boolean migrateGrants,
            boolean migrateCatalogRoleAssignments
    ) {
        super(ManagementEntityType.CATALOG, context);
        this.candidatesForCatalogRoleMigration = new ArrayList<>();
        this.migrateCatalogRoles = migrateCatalogRoles;
        this.migrateGrants = migrateGrants;
        this.migrateCatalogRoleAssignments = migrateCatalogRoleAssignments;
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of();
    }

    @Override
    protected List<Catalog> getEntities() {
        return context.source().listCatalogs().getCatalogs();
    }

    @Override
    protected void createEntity(Catalog catalog) throws Exception {
        candidatesForCatalogRoleMigration.add(catalog);
        this.context.target().createCatalog(new CreateCatalogRequest().catalog(catalog));
    }

    @Override
    protected ImmutableEntityMigrationResult.Builder prepareResult(Catalog catalog, Exception e) {
        return ImmutableEntityMigrationResult.builder()
                .entityName(catalog.getName())
                .putProperties("entityVersion", catalog.getEntityVersion().toString());
    }

    @Override
    public List<EntityMigrationResult> migrate() {
        List<EntityMigrationResult> results = super.migrate();

        if (migrateCatalogRoles) {
            for (Catalog catalog : candidatesForCatalogRoleMigration) {
                CatalogRolesMigrationTask catalogRoleMigrationTask = new CatalogRolesMigrationTask(context, catalog.getName(), migrateGrants, migrateCatalogRoleAssignments);
                context.taskQueue().add(catalogRoleMigrationTask);
            }
        }

        return results;
    }

}
