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

package org.projectnessie.tools.catalog.migration.cli.polaris;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.projectnessie.tools.polaris.migration.api.callbacks.SynchronizationEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SyncPolarisSynchronizationEventListener implements SynchronizationEventListener {

    private final Logger consoleLog = LoggerFactory.getLogger("console-log");

    @Override
    public void onListPrincipalRolesSource(List<PrincipalRole> principalRolesSource, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} principal-roles from source.", principalRolesSource.size());
        } else {
            consoleLog.error("Listing principal-roles from source failed.", e);
        }
    }

    @Override
    public void onListPrincipalRolesTarget(List<PrincipalRole> principalRolesTarget, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} principal-roles from target.", principalRolesTarget.size());
        } else {
            consoleLog.error("Listing principal-roles from target failed.", e);
        }
    }

    @Override
    public void onCreatePrincipalRole(PrincipalRole principalRole, Exception e) {
        if (e == null) {
            consoleLog.info("Created principal-role {}.", principalRole.getName());
        } else {
            consoleLog.error("Failed to create principal-role {}.", principalRole.getName(), e);
        }
    }

    @Override
    public void onOverwritePrincipalRole(PrincipalRole principalRole, Exception e) {
        if (e == null) {
            consoleLog.info("Overwrote principal-role {}.", principalRole.getName());
        } else {
            consoleLog.error("Failed to overwrite principal-role {}.", principalRole.getName(), e);
        }
    }

    @Override
    public void onRemovePrincipalRole(PrincipalRole principalRole, Exception e) {
        if (e == null) {
            consoleLog.info("Removed principal-role {}.", principalRole.getName());
        } else {
            consoleLog.error("Failed to remove principal-role {}.", principalRole.getName(), e);
        }
    }

    @Override
    public void onListCatalogsSource(List<Catalog> catalogsFromSource, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} catalogs from source.", catalogsFromSource.size(), e);
        } else {
            consoleLog.error("Listing catalogs from source failed.", e);
        }
    }

    @Override
    public void onListCatalogsTarget(List<Catalog> catalogsFromTarget, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} catalogs from target.", catalogsFromTarget.size());
        } else {
            consoleLog.error("Listing catalogs from target failed.", e);
        }
    }

    @Override
    public void onCreateCatalog(Catalog catalog, Exception e) {
        if (e == null) {
            consoleLog.info("Created catalog {}.", catalog.getName());
        } else {
            consoleLog.error("Failed to create catalog {}.", catalog.getName(), e);
        }
    }

    @Override
    public void onOverwriteCatalog(Catalog catalog, Exception e) {
        if (e == null) {
            consoleLog.info("Overwrote catalog {}.", catalog.getName());
        } else {
            consoleLog.error("Failed to overwrite catalog {}.", catalog.getName(), e);
        }
    }

    @Override
    public void onRemoveCatalog(Catalog catalog, Exception e) {
        if (e == null) {
            consoleLog.info("Removed catalog {}.", catalog.getName());
        } else {
            consoleLog.error("Failed to remove catalog {}.", catalog.getName(), e);
        }
    }

    @Override
    public void onListCatalogRolesSource(String catalogName, List<CatalogRole> catalogRoleFromSource, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} catalog-roles from source for catalog {}.", catalogRoleFromSource.size(), catalogName);
        } else {
            consoleLog.error("Listing catalog-roles from source failed for catalog {}.", catalogName, e);
        }
    }

    @Override
    public void onListCatalogRolesTarget(String catalogName, List<CatalogRole> catalogRolesFromTarget, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} catalog-roles from target for catalog {}.", catalogRolesFromTarget.size(), catalogName);
        } else {
            consoleLog.error("Listing catalog-roles from target failed for catalog {}.", catalogName, e);
        }
    }

    @Override
    public void onCreateCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        if (e == null) {
            consoleLog.info("Created catalog-role {} for catalog {}.", catalogRole.getName(), catalogName);
        } else {
            consoleLog.error("Failed to create catalog-role {} for catalog {}.", catalogRole.getName(), catalogName, e);
        }
    }

    @Override
    public void onOverwriteCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        if (e == null) {
            consoleLog.info("Overwrote catalog-role {} for catalog {}.", catalogRole.getName(), catalogName);
        } else {
            consoleLog.error("Failed to overwrite catalog-role {} for catalog {}.", catalogRole.getName(), catalogName, e);
        }
    }

    @Override
    public void onRemoveCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        if (e == null) {
            consoleLog.info("Removed catalog-role {} for catalog {}.", catalogRole.getName(), catalogName);
        } else {
            consoleLog.error("Failed to remove catalog-role {} for catalog {}.", catalogRole.getName(), catalogName, e);
        }
    }

    @Override
    public void onListAssigneePrincipalRolesForCatalogRoleSource(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleSource, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} assigned principal roles from source for catalog-role {} in catalog {}.",
                    assigneePrincipalRolesForCatalogRoleSource.size(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Listing assigned principal roles from source failed for catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onListAssigneePrincipalRolesForCatalogRoleTarget(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleTarget, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} assigned principal roles from target for catalog-role {} in catalog {}.",
                    assigneePrincipalRolesForCatalogRoleTarget.size(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Listing assigned principal roles from target failed for catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onAssignPrincipalRoleToCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e) {
        if (e == null) {
            consoleLog.info("Assigned principal-role {} to catalog-role {} in catalog {}.",
                    principalRole.getName(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Failed to assign principal-role {} to catalog-role {} in catalog {}.",
                    principalRole.getName(), catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onRemovePrincipalRoleFromCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e) {
        if (e == null) {
            consoleLog.info("Removed principal-role {} from catalog-role {} in catalog {}.",
                    principalRole.getName(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Failed to remove principal-role {} from catalog-role {} in catalog {}.",
                    principalRole.getName(), catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onListGrantsSource(String catalogName, String catalogRoleName, List<GrantResource> grantsFromSource, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} grants from source for catalog-role {} in catalog {}.",
                    grantsFromSource.size(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Listing grants from source failed for catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onListGrantsTarget(String catalogName, String catalogRoleName, List<GrantResource> grantsFromTarget, Exception e) {
        if (e == null) {
            consoleLog.info("Identified {} grants from target for catalog-role {} in catalog {}.",
                    grantsFromTarget.size(), catalogRoleName, catalogName);
        } else {
            consoleLog.error("Listing grants from target failed for catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onAddGrant(String catalogName, String catalogRoleName, GrantResource grant, Exception e) {
        if (e == null) {
            consoleLog.info("Added grant to catalog-role {} in catalog {}.", catalogRoleName, catalogName);
        } else {
            consoleLog.error("Failed to add grant to catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

    @Override
    public void onRevokeGrant(String catalogName, String catalogRoleName, GrantResource grant, Exception e) {
        if (e == null) {
            consoleLog.info("Revoked grant from catalog-role {} in catalog {}.", catalogRoleName, catalogName);
        } else {
            consoleLog.error("Failed to revoke grant from catalog-role {} in catalog {}.", catalogRoleName, catalogName, e);
        }
    }

}
