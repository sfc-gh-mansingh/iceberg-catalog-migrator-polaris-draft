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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CatalogRolesMigrationTask extends MigrationTask<CatalogRole> {

    private final String catalogName;

    private final boolean migrateGrants;

    // contains the catalog role entities for which grants and principal role assignments
    // should be computed
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
        // must wait for catalogs to finish migrating before we migrate catalog roles
        return List.of(CatalogsMigrationTask.class);
    }

    @Override
    protected List<CatalogRole> listEntities() {
        return context.source().listCatalogRoles(catalogName).getRoles();
    }

    @Override
    protected void createEntity(CatalogRole catalogRole) throws Exception {
        this.candidatesForSubentityMigration.add(catalogRole);
        context.target().createCatalogRole(catalogName, new CreateCatalogRoleRequest().catalogRole(catalogRole));
    }

    @Override
    protected String getDescription(CatalogRole catalogRole) {
        return String.format("Catalog role (%s) under catalog (%s)", catalogRole.getName(), catalogName);
    }

    @Override
    protected Map<String, String> properties() {
        return Map.of(
                "catalogName", catalogName
        );
    }

    @Override
    protected Map<String, String> properties(CatalogRole catalogRole) {
        return Map.of(
                "catalogRoleName", catalogRole.getName(),
                "catalogName", catalogName
        );
    }

    @Override
    public void migrate() {
        super.migrate();

        if (migrateGrants) {
            // enqueue grant migration tasks
            for (CatalogRole catalogRole : candidatesForSubentityMigration) {
                GrantsMigrationTask grantMigrationTask = new GrantsMigrationTask(context, catalogName, catalogRole.getName());
                context.taskQueue().add(grantMigrationTask);
            }
        }

        if (migratePrincipalRoleAssignments) {
            // enqueue assignments to principal roles migration tasks
            for (CatalogRole catalogRole : candidatesForSubentityMigration) {
                CatalogRolesAssignmentMigrationTask catalogRoleAssignmentMigrationTask = new CatalogRolesAssignmentMigrationTask(
                        context, catalogName, catalogRole.getName());
                context.taskQueue().add(catalogRoleAssignmentMigrationTask);
            }
        }
    }

}
