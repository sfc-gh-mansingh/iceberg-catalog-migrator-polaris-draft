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
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationResult;

import java.util.List;

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
        return List.of(CatalogsMigrationTask.class, CatalogRolesMigrationTask.class);
    }

    @Override
    protected List<PrincipalRole> getEntities() {
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
    protected ImmutableEntityMigrationResult.Builder prepareResult(
            PrincipalRole principalRole,
            Exception e
    ) {
        return ImmutableEntityMigrationResult.builder().entityName(principalRole.getName());
    }

}
