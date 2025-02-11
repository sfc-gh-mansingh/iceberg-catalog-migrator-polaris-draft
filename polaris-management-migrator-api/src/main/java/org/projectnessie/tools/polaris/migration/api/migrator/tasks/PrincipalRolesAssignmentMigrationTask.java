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

import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;

import java.util.List;
import java.util.Map;

public class PrincipalRolesAssignmentMigrationTask extends MigrationTask<PrincipalRole> {

    private final String principalName;

    public PrincipalRolesAssignmentMigrationTask(MigrationContext context, String principalName) {
        super(ManagementEntityType.PRINCIPAL_ROLE_ASSIGNMENT, context);
        this.principalName = principalName;
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of(PrincipalsMigrationTask.class, PrincipalRolesMigrationTask.class);
    }

    @Override
    protected List<PrincipalRole> listEntities() {
        return context.source().listPrincipalRolesAssigned(principalName).getRoles();
    }

    @Override
    protected void createEntity(PrincipalRole principalRole) throws Exception {
        context.target().assignPrincipalRole(principalName, new GrantPrincipalRoleRequest().principalRole(principalRole));
    }

    @Override
    protected String getDescription(PrincipalRole principalRole) {
        return String.format("Assignment of principal role (%s) to principal (%s)",
                principalRole.getName(), principalName);
    }

    @Override
    protected Map<String, String> properties() {
        return Map.of("principalName", principalName);
    }

    @Override
    protected Map<String, String> properties(PrincipalRole principalRole) {
        return Map.of(
                "principalName", principalName,
                "principalRoleName", principalRole.getName()
        );
    }

}
