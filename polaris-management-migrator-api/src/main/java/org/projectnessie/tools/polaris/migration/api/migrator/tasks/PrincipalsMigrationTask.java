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
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrincipalsMigrationTask extends MigrationTask<Principal> {

    private final boolean migratePrincipalRoleAssignments;

    private final List<Principal> candidatesForRoleAssignmentMigration;

    private final Map<Principal, PrincipalWithCredentials> targetPrincipalBySourcePrincipal;

    public PrincipalsMigrationTask(MigrationContext context, boolean migratePrincipalRoleAssignments) {
        super(ManagementEntityType.PRINCIPAL, context);
        this.migratePrincipalRoleAssignments = migratePrincipalRoleAssignments;
        this.candidatesForRoleAssignmentMigration = new ArrayList<>();
        this.targetPrincipalBySourcePrincipal = new HashMap<>();
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of();
    }

    @Override
    protected List<Principal> listEntities() {
        return context.source().listPrincipals().getPrincipals();
    }

    @Override
    protected void createEntity(Principal principal) throws Exception {
        this.candidatesForRoleAssignmentMigration.add(principal);
        PrincipalWithCredentials targetPrincipal = context.target()
                .createPrincipal(new CreatePrincipalRequest().principal(principal));

        targetPrincipalBySourcePrincipal.put(principal, targetPrincipal);
    }

    @Override
    protected String getDescription(Principal principal) {
        return principal.getName();
    }

    @Override
    protected Map<String, String> properties() {
        return Map.of();
    }

    @Override
    protected Map<String, String> properties(Principal principal) {
        Map<String, String> properties = new HashMap<>();
        properties.put("principalName", principal.getName());

        if (targetPrincipalBySourcePrincipal.containsKey(principal)) {
            PrincipalWithCredentials targetPrincipal = targetPrincipalBySourcePrincipal.get(principal);
            properties.put("sourceClientId", principal.getClientId() == null ? "" : principal.getClientId());
            properties.put("targetClientId", targetPrincipal.getCredentials().getClientId() == null ? "" : targetPrincipal.getCredentials().getClientId());
            properties.put("targetClientSecret", targetPrincipal.getCredentials().getClientSecret() == null ? "" : targetPrincipal.getCredentials().getClientSecret());
        }

        return properties;
    }

    @Override
    public void migrate() {
        super.migrate();

        if (migratePrincipalRoleAssignments) {
            for (Principal principal : candidatesForRoleAssignmentMigration) {
                PrincipalRolesAssignmentMigrationTask principalRolesAssignmentMigrationTask =
                        new PrincipalRolesAssignmentMigrationTask(context, principal.getName());
                context.taskQueue().add(principalRolesAssignmentMigrationTask);
            }
        }
    }

}
