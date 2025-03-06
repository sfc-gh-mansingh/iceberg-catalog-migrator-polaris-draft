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

package org.projectnessie.tools.polaris.migration.api.callbacks;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PrincipalRole;

import java.util.List;

public class NoOpSyncEventListener implements SynchronizationEventListener {

    @Override
    public void onListPrincipalRolesSource(List<PrincipalRole> principalRolesSource, Exception e) {
        // do nothing
    }

    @Override
    public void onListPrincipalRolesTarget(List<PrincipalRole> principalRolesTarget, Exception e) {
        // do nothing
    }

    @Override
    public void onCreatePrincipalRole(PrincipalRole principalRole, Exception e) {
        // do nothing
    }

    @Override
    public void onOverwritePrincipalRole(PrincipalRole principalRole, Exception e) {
        // do nothing
    }

    @Override
    public void onRemovePrincipalRole(PrincipalRole principalRole, Exception e) {
        // do nothing
    }

    @Override
    public void onListCatalogsSource(List<Catalog> catalogsFromSource, Exception e) {
        // do nothing
    }

    @Override
    public void onListCatalogsTarget(List<Catalog> catalogsFromTarget, Exception e) {
        // do nothing
    }

    @Override
    public void onCreateCatalog(Catalog catalog, Exception e) {
        // do nothing
    }

    @Override
    public void onOverwriteCatalog(Catalog catalog, Exception e) {
        // do nothing
    }

    @Override
    public void onRemoveCatalog(Catalog catalog, Exception e) {
        // do nothing
    }

    @Override
    public void onListCatalogRolesSource(String catalogName, List<CatalogRole> catalogRoleFromSource, Exception e) {
        // do nothing
    }

    @Override
    public void onListCatalogRolesTarget(String catalogName, List<CatalogRole> catalogRolesFromTarget, Exception e) {
        // do nothing
    }

    @Override
    public void onCreateCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        // do nothing
    }

    @Override
    public void onOverwriteCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        // do nothing
    }

    @Override
    public void onRemoveCatalogRole(String catalogName, CatalogRole catalogRole, Exception e) {
        // do nothing
    }

    @Override
    public void onListAssigneePrincipalRolesForCatalogRoleSource(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleSource, Exception e) {
        // do nothing
    }

    @Override
    public void onListAssigneePrincipalRolesForCatalogRoleTarget(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleTarget, Exception e) {
        // do nothing
    }

    @Override
    public void onAssignPrincipalRoleToCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e) {
        // do nothing
    }

    @Override
    public void onRemovePrincipalRoleFromCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e) {
        // do nothing
    }

    @Override
    public void onListGrantsSource(String catalogName, String catalogRoleName, List<GrantResource> grantsFromSource, Exception e) {
        // do nothing
    }

    @Override
    public void onListGrantsTarget(String catalogName, String catalogRoleName, List<GrantResource> grantsFromTarget, Exception e) {
        // do nothing
    }

    @Override
    public void onAddGrant(String catalogName, String catalogRole, GrantResource grant, Exception e) {
        // do nothing
    }

    @Override
    public void onRevokeGrant(String catalogName, String catalogRole, GrantResource grant, Exception e) {
        // do nothing
    }

}
