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
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;

import java.util.List;
import java.util.Map;

public class CatalogRolesAssignmentMigrationTask extends MigrationTask<PrincipalRole> {

    private final String catalogName;

    private final String catalogRoleName;

    public CatalogRolesAssignmentMigrationTask(MigrationContext context, String catalogName, String catalogRoleName) {
        super(ManagementEntityType.CATALOG_ROLE_ASSIGNMENT, context);
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        // Catalog role assignments to principal roles require all catalogs, catalog roles, and principal role
        // migrations to take place first
        return List.of(CatalogsMigrationTask.class, CatalogRolesMigrationTask.class, PrincipalRolesMigrationTask.class);
    }

    @Override
    protected List<PrincipalRole> listEntities() {
        return context.source().listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName).getRoles();
    }

    @Override
    protected void createEntity(PrincipalRole principalRole) throws Exception {
        CatalogRole catalogRole = new CatalogRole().name(catalogRoleName);
        context.target().assignCatalogRoleToPrincipalRole(
                principalRole.getName(),
                catalogName,
                new GrantCatalogRoleRequest().catalogRole(catalogRole)
        );
    }

    @Override
    protected String getDescription(PrincipalRole principalRole) {
        return String.format("Assignment of catalog role (%s) under catalog (%s) to principal role (%s)",
                catalogRoleName, catalogName, principalRole.getName());
    }

    @Override
    protected Map<String, String> properties() {
        return Map.of(
                "catalogName", catalogName,
                "catalogRoleName", catalogRoleName
        );
    }

    @Override
    protected Map<String, String> properties(PrincipalRole principalRole) {
        return Map.of(
                "catalogName", catalogName,
                "catalogRoleName", catalogRoleName,
                "principalRoleName", principalRole.getName()
        );
    }

}
