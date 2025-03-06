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

package org.projectnessie.tools.polaris.migration.api;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.projectnessie.tools.polaris.migration.api.callbacks.SynchronizationEventListener;
import org.projectnessie.tools.polaris.migration.api.planning.SynchronizationPlan;
import org.projectnessie.tools.polaris.migration.api.planning.SynchronizationPlanner;

import java.util.ArrayList;
import java.util.List;

public class PolarisSynchronizer {

    private final SynchronizationPlanner syncPlanner;

    private final SynchronizationEventListener syncEventListener;

    private final PolarisService source;

    private final PolarisService target;

    public PolarisSynchronizer(
            SynchronizationPlanner synchronizationPlanner,
            SynchronizationEventListener syncEventListener,
            PolarisService source,
            PolarisService target
    ) {
        this.syncPlanner = synchronizationPlanner;
        this.syncEventListener = syncEventListener;
        this.source = source;
        this.target = target;
    }

    public void syncPrincipalRoles() {
        List<PrincipalRole> principalRolesSource;

        try {
            principalRolesSource = source.listPrincipalRoles();
            syncEventListener.onListPrincipalRolesSource(principalRolesSource, null);
        } catch (Exception e) {
            syncEventListener.onListPrincipalRolesSource(null, e);
            return;
        }

        List<PrincipalRole> principalRolesTarget;

        try {
            principalRolesTarget = target.listPrincipalRoles();
            syncEventListener.onListPrincipalRolesTarget(principalRolesTarget, null);
        } catch (Exception e) {
            syncEventListener.onListPrincipalRolesTarget(null, e);
            return;
        }

        SynchronizationPlan<PrincipalRole> principalRoleSyncPlan = syncPlanner.planPrincipalRoleSync(principalRolesSource, principalRolesTarget);

        for (PrincipalRole principalRole : principalRoleSyncPlan.entitiesToCreate()) {
            try {
                target.createPrincipalRole(principalRole, false);
                syncEventListener.onCreatePrincipalRole(principalRole, null);
            } catch (Exception e) {
                syncEventListener.onCreatePrincipalRole(principalRole, e);
            }
        }

        for (PrincipalRole principalRole : principalRoleSyncPlan.entitiesToOverwrite()) {
            try {
                target.createPrincipalRole(principalRole, true);
                syncEventListener.onOverwritePrincipalRole(principalRole, null);
            } catch (Exception e) {
                syncEventListener.onOverwritePrincipalRole(principalRole, e);
            }
        }

        for (PrincipalRole principalRole : principalRoleSyncPlan.entitiesToRemove()) {
            try {
                target.removePrincipalRole(principalRole.getName());
                syncEventListener.onRemovePrincipalRole(principalRole, null);
            } catch (Exception e) {
                syncEventListener.onRemovePrincipalRole(principalRole, e);
            }
        }
    }

    public void syncAssigneePrincipalRolesForCatalogRole(String catalogName, String catalogRoleName) {
        List<PrincipalRole> principalRolesSource;

        try {
            principalRolesSource = source.listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName);
            syncEventListener.onListAssigneePrincipalRolesForCatalogRoleSource(
                    catalogName, catalogRoleName, principalRolesSource, null);
        } catch (Exception e) {
            syncEventListener.onListAssigneePrincipalRolesForCatalogRoleSource(
                    catalogName, catalogRoleName, null, e);
            return;
        }

        List<PrincipalRole> principalRolesTarget;

        try {
            principalRolesTarget = target.listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName);
            syncEventListener.onListAssigneePrincipalRolesForCatalogRoleTarget(
                    catalogName, catalogRoleName, principalRolesTarget, null);
        } catch (Exception e) {
            syncEventListener.onListAssigneePrincipalRolesForCatalogRoleTarget(
                    catalogName, catalogRoleName, null, e);
            return;
        }

        SynchronizationPlan<PrincipalRole> assignedPrincipalRoleSyncPlan = syncPlanner.planAssignPrincipalRolesToCatalogRolesSync(
                catalogName, catalogRoleName, principalRolesSource, principalRolesTarget);

        for (PrincipalRole principalRole : assignedPrincipalRoleSyncPlan.entitiesToCreate()) {
            try {
                target.assignCatalogRoleToPrincipalRole(principalRole.getName(), catalogName, catalogRoleName);
                syncEventListener.onAssignPrincipalRoleToCatalogRole(catalogName, catalogRoleName, principalRole, null);
            } catch (Exception e) {
                syncEventListener.onAssignPrincipalRoleToCatalogRole(catalogName, catalogRoleName, principalRole, e);
            }
        }

        for (PrincipalRole principalRole : assignedPrincipalRoleSyncPlan.entitiesToOverwrite()) {
            try {
                target.assignCatalogRoleToPrincipalRole(principalRole.getName(), catalogName, catalogRoleName);
                syncEventListener.onAssignPrincipalRoleToCatalogRole(catalogName, catalogRoleName, principalRole, null);
            } catch (Exception e) {
                syncEventListener.onAssignPrincipalRoleToCatalogRole(catalogName, catalogRoleName, principalRole, e);
            }
        }

        for (PrincipalRole principalRole : assignedPrincipalRoleSyncPlan.entitiesToRemove()) {
            try {
                target.removeCatalogRoleFromPrincipalRole(principalRole.getName(), catalogName, catalogRoleName);
                syncEventListener.onRemovePrincipalRoleFromCatalogRole(catalogName, catalogRoleName, principalRole, null);
            } catch (Exception e) {
                syncEventListener.onRemovePrincipalRoleFromCatalogRole(catalogName, catalogRoleName, principalRole, e);
            }
        }
    }

    public void syncCatalogs() {
        List<Catalog> catalogsSource;

        try {
            catalogsSource = source.listCatalogs();
            syncEventListener.onListCatalogsSource(catalogsSource, null);
        } catch (Exception e) {
            syncEventListener.onListCatalogsSource(null, e);
            return;
        }

        List<Catalog> catalogsTarget;

        try {
            catalogsTarget = target.listCatalogs();
            syncEventListener.onListCatalogsTarget(catalogsTarget, null);
        } catch (Exception e) {
            syncEventListener.onListCatalogsTarget(null, e);
            return;
        }

        SynchronizationPlan<Catalog> catalogSyncPlan = syncPlanner.planCatalogSync(catalogsSource, catalogsTarget);

        for (Catalog catalog : catalogSyncPlan.entitiesToCreate()) {
            try {
                target.createCatalog(catalog, false);
                syncEventListener.onCreateCatalog(catalog, null);
            } catch (Exception e) {
                syncEventListener.onCreateCatalog(catalog, e);
            }
        }

        for (Catalog catalog : catalogSyncPlan.entitiesToOverwrite()) {
            try {
                target.createCatalog(catalog, true);
                syncEventListener.onOverwriteCatalog(catalog, null);
            } catch (Exception e) {
                syncEventListener.onOverwriteCatalog(catalog, e);
            }
        }

        for (Catalog catalog : catalogSyncPlan.entitiesToRemove()) {
            try {
                target.removeCatalogCascade(catalog.getName());
                syncEventListener.onRemoveCatalog(catalog, null);
            } catch (Exception e) {
                syncEventListener.onRemoveCatalog(catalog, e);
            }
        }

        List<Catalog> catalogsToSync = new ArrayList<>();
        catalogsToSync.addAll(catalogSyncPlan.entitiesToCreate());
        catalogsToSync.addAll(catalogSyncPlan.entitiesToOverwrite());

        for (Catalog catalog : catalogsToSync) {
            syncCatalogRoles(catalog.getName());
        }
    }

    public void syncCatalogRoles(String catalogName) {
        List<CatalogRole> catalogRolesSource;

        try {
            catalogRolesSource = source.listCatalogRoles(catalogName);
            syncEventListener.onListCatalogRolesSource(catalogName, catalogRolesSource, null);
        } catch (Exception e) {
            syncEventListener.onListCatalogRolesSource(catalogName, null, e);
            return;
        }

        List<CatalogRole> catalogRolesTarget;

        try {
            catalogRolesTarget = target.listCatalogRoles(catalogName);
            syncEventListener.onListCatalogRolesTarget(catalogName, catalogRolesTarget, null);
        } catch (Exception e) {
            syncEventListener.onListCatalogRolesTarget(catalogName, null, e);
            return;
        }

        SynchronizationPlan<CatalogRole> catalogRoleSyncPlan = syncPlanner.planCatalogRoleSync(catalogName, catalogRolesSource, catalogRolesTarget);

        for (CatalogRole catalogRole : catalogRoleSyncPlan.entitiesToCreate()) {
            try {
                target.createCatalogRole(catalogName, catalogRole, false);
                syncEventListener.onCreateCatalogRole(catalogName, catalogRole, null);
            } catch (Exception e) {
                syncEventListener.onCreateCatalogRole(catalogName, catalogRole, e);
            }
        }

        for (CatalogRole catalogRole : catalogRoleSyncPlan.entitiesToOverwrite()) {
            try {
                target.createCatalogRole(catalogName, catalogRole, true);
                syncEventListener.onOverwriteCatalogRole(catalogName, catalogRole, null);
            } catch (Exception e) {
                syncEventListener.onOverwriteCatalogRole(catalogName, catalogRole, e);
            }
        }

        for (CatalogRole catalogRole : catalogRoleSyncPlan.entitiesToRemove()) {
            try {
                target.removeCatalogRole(catalogName, catalogRole.getName());
                syncEventListener.onRemoveCatalogRole(catalogName, catalogRole, null);
            } catch (Exception e) {
                syncEventListener.onRemoveCatalogRole(catalogName, catalogRole, e);
            }
        }

        List<CatalogRole> catalogRolesToSync = new ArrayList<>();
        catalogRolesToSync.addAll(catalogRoleSyncPlan.entitiesToCreate());
        catalogRolesToSync.addAll(catalogRoleSyncPlan.entitiesToOverwrite());

        for (CatalogRole catalogRole : catalogRolesToSync) {
            syncAssigneePrincipalRolesForCatalogRole(catalogName, catalogRole.getName());
            syncGrants(catalogName, catalogRole.getName());
        }
    }

    private void syncGrants(String catalogName, String catalogRoleName) {
        List<GrantResource> grantsSource;

        try {
            grantsSource = source.listGrants(catalogName, catalogRoleName);
            syncEventListener.onListGrantsSource(catalogName, catalogRoleName, grantsSource, null);
        } catch (Exception e) {
            syncEventListener.onListGrantsSource(catalogName, catalogRoleName, null, e);
            return;
        }

        List<GrantResource> grantsTarget;

        try {
            grantsTarget = target.listGrants(catalogName, catalogRoleName);
            syncEventListener.onListGrantsTarget(catalogName, catalogRoleName, grantsTarget, null);
        } catch (Exception e) {
            syncEventListener.onListGrantsTarget(catalogName, catalogRoleName, null, e);
            return;
        }

        SynchronizationPlan<GrantResource> grantSyncPlan = syncPlanner.planGrantSync(catalogName, catalogRoleName, grantsSource, grantsTarget);

        for (GrantResource grant : grantSyncPlan.entitiesToCreate()) {
            try {
                target.addGrant(catalogName, catalogRoleName, grant);
                syncEventListener.onAddGrant(catalogName, catalogRoleName, grant, null);
            } catch (Exception e) {
                syncEventListener.onAddGrant(catalogName, catalogRoleName, grant, e);
            }
        }

        for (GrantResource grant : grantSyncPlan.entitiesToOverwrite()) {
            try {
                target.addGrant(catalogName, catalogRoleName, grant);
                syncEventListener.onAddGrant(catalogName, catalogRoleName, grant, null);
            } catch (Exception e) {
                syncEventListener.onAddGrant(catalogName, catalogRoleName, grant, e);
            }
        }

        for (GrantResource grant : grantSyncPlan.entitiesToRemove()) {
            try {
                target.revokeGrant(catalogName, catalogRoleName, grant);
                syncEventListener.onRevokeGrant(catalogName, catalogRoleName, grant, null);
            } catch (Exception e) {
                syncEventListener.onRevokeGrant(catalogName, catalogRoleName, grant, e);
            }
        }
    }

}
