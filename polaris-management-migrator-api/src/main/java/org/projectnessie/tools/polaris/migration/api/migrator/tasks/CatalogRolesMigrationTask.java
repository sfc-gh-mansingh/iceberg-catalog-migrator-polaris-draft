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

import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationResult;

import java.util.ArrayList;
import java.util.List;

public class CatalogRolesMigrationTask extends MigrationTask<CatalogRole> {

    private final String catalogName;

    private final boolean migrateGrants;

    private final List<CatalogRole> candidatesForSubentityMigration;

    private final boolean migratePrincipalRoleAssignments;

    protected CatalogRolesMigrationTask(
            MigrationContext context,
            String catalogName,
            boolean migrateGrants,
            boolean migratePrincipalRoleAssignments
    ) {
        super(ManagementEntityType.CATALOG_ROLE, context);
        this.catalogName = catalogName;
        this.migrateGrants = migrateGrants;
        this.candidatesForSubentityMigration = new ArrayList<>();
        this.migratePrincipalRoleAssignments = migratePrincipalRoleAssignments;
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of(CatalogsMigrationTask.class);
    }

    @Override
    protected List<CatalogRole> getEntities() {
        return context.source().listCatalogRoles(catalogName).getRoles();
    }

    @Override
    protected void createEntity(CatalogRole catalogRole) throws Exception {
        this.candidatesForSubentityMigration.add(catalogRole);
        context.target().createCatalogRole(catalogName, new CreateCatalogRoleRequest().catalogRole(catalogRole));
    }

    @Override
    protected ImmutableEntityMigrationResult.Builder prepareResult(CatalogRole catalogRole, Exception e) {
        return ImmutableEntityMigrationResult.builder()
                .entityName(catalogRole.getName())
                .putProperties("catalogName", catalogName);
    }

    @Override
    public List<EntityMigrationResult> migrate() {
        List<EntityMigrationResult> results = super.migrate();

        if (migrateGrants) {
            for (CatalogRole catalogRole : candidatesForSubentityMigration) {
                GrantsMigrationTask grantMigrationTask = new GrantsMigrationTask(context, catalogName, catalogRole.getName());
                context.taskQueue().add(grantMigrationTask);
            }
        }

        if (migratePrincipalRoleAssignments) {
            for (CatalogRole catalogRole : candidatesForSubentityMigration) {
                CatalogRolesAssignmentMigrationTask catalogRoleAssignmentMigrationTask = new CatalogRolesAssignmentMigrationTask(
                        context, catalogName, catalogRole.getName());
                context.taskQueue().add(catalogRoleAssignmentMigrationTask);
            }
        }

        return results;
    }

}
