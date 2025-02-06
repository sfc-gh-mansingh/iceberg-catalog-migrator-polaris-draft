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

import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.Principal;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationResult;

import java.util.ArrayList;
import java.util.List;

public class PrincipalsMigrationTask extends MigrationTask<Principal> {

    private final boolean migratePrincipalRoleAssignments;

    private final List<Principal> candidatesForRoleAssignmentMigration;

    public PrincipalsMigrationTask(MigrationContext context, boolean migratePrincipalRoleAssignments) {
        super(ManagementEntityType.PRINCIPAL, context);
        this.migratePrincipalRoleAssignments = migratePrincipalRoleAssignments;
        this.candidatesForRoleAssignmentMigration = new ArrayList<>();
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of();
    }

    @Override
    protected List<Principal> getEntities() {
        return context.source().listPrincipals().getPrincipals();
    }

    @Override
    protected void createEntity(Principal principal) throws Exception {
        this.candidatesForRoleAssignmentMigration.add(principal);
        context.target().createPrincipal(new CreatePrincipalRequest().principal(principal));
    }

    @Override
    public List<EntityMigrationResult> migrate() {
        List<EntityMigrationResult> results = super.migrate();

        if (migratePrincipalRoleAssignments) {
            for (Principal principal : candidatesForRoleAssignmentMigration) {
                PrincipalRolesAssignmentMigrationTask principalRolesAssignmentMigrationTask =
                        new PrincipalRolesAssignmentMigrationTask(context, principal.getName());
                context.taskQueue().add(principalRolesAssignmentMigrationTask);
            }
        }

        return results;
    }

    @Override
    protected ImmutableEntityMigrationResult.Builder prepareResult(Principal principal, Exception e) {
        return ImmutableEntityMigrationResult.builder()
                .entityName(principal.getName())
                .putProperties("entityVersion", principal.getEntityVersion().toString());
    }

}
