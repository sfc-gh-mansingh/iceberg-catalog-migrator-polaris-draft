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

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationContext;
import org.projectnessie.tools.polaris.migration.api.migrator.MigrationTask;

import java.util.List;
import java.util.Map;

public class GrantsMigrationTask extends MigrationTask<GrantResource> {

    private final String catalogName;

    private final String catalogRoleName;

    public GrantsMigrationTask(MigrationContext context, String catalogName, String catalogRoleName) {
        super(ManagementEntityType.GRANT, context);
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
    }

    @Override
    public List<Class<? extends MigrationTask<?>>> dependsOn() {
        return List.of(CatalogsMigrationTask.class, CatalogRolesMigrationTask.class);
    }

    @Override
    protected List<GrantResource> listEntities() {
        return context.source().listGrantsForCatalogRole(catalogName, catalogRoleName).getGrants();
    }

    @Override
    protected void createEntity(GrantResource grant) throws Exception {
        context.target().addGrantToCatalogRole(catalogName, catalogRoleName, new AddGrantRequest().grant(grant));
    }

    @Override
    protected String getDescription(GrantResource grant) {
        return String.format("Grant (%s) of type (%s) for catalog role (%s) under catalog (%s)",
                getGrantPrivilege(grant), grant.getType(), catalogRoleName, catalogName);
    }

    private String getGrantPrivilege(GrantResource grant) {
        String privilege  = "";

        switch (grant) {
            case CatalogGrant c -> privilege = c.getPrivilege().getValue();
            case NamespaceGrant n -> privilege = n.getPrivilege().getValue();
            case TableGrant t -> privilege = t.getPrivilege().getValue();
            case ViewGrant v -> privilege = v.getPrivilege().getValue();
            default -> {}
        }

        return privilege;
    }

    @Override
    protected Map<String, String> properties() {
        return Map.of(
                "catalogName", catalogName,
                "catalogRoleName", catalogRoleName
        );
    }

    @Override
    protected Map<String, String> properties(GrantResource grant) {
        return Map.of(
                "type", grant.getType().getValue(),
                "privilege", getGrantPrivilege(grant),
                "catalogName", catalogName,
                "catalogRoleName", catalogRoleName
        );
    }

}
