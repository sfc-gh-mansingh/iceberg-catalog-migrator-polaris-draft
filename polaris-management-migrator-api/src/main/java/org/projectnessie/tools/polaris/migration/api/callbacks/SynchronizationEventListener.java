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

public interface SynchronizationEventListener {

    void onListPrincipalRolesSource(List<PrincipalRole> principalRolesSource, Exception e);

    void onListPrincipalRolesTarget(List<PrincipalRole> principalRolesTarget, Exception e);

    void onCreatePrincipalRole(PrincipalRole principalRole, Exception e);

    void onOverwritePrincipalRole(PrincipalRole principalRole, Exception e);

    void onRemovePrincipalRole(PrincipalRole principalRole, Exception e);

    void onListCatalogsSource(List<Catalog> catalogsFromSource, Exception e);

    void onListCatalogsTarget(List<Catalog> catalogsFromTarget, Exception e);

    void onCreateCatalog(Catalog catalog, Exception e);

    void onOverwriteCatalog(Catalog catalog, Exception e);

    void onRemoveCatalog(Catalog catalog, Exception e);

    void onListCatalogRolesSource(String catalogName, List<CatalogRole> catalogRoleFromSource, Exception e);

    void onListCatalogRolesTarget(String catalogName, List<CatalogRole> catalogRolesFromTarget, Exception e);

    void onCreateCatalogRole(String catalogName, CatalogRole catalogRole, Exception e);

    void onOverwriteCatalogRole(String catalogName, CatalogRole catalogRole, Exception e);

    void onRemoveCatalogRole(String catalogName, CatalogRole catalogRole, Exception e);

    void onListAssigneePrincipalRolesForCatalogRoleSource(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleSource, Exception e);

    void onListAssigneePrincipalRolesForCatalogRoleTarget(String catalogName, String catalogRoleName, List<PrincipalRole> assigneePrincipalRolesForCatalogRoleTarget, Exception e);

    void onAssignPrincipalRoleToCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e);

    void onRemovePrincipalRoleFromCatalogRole(String catalogName, String catalogRoleName, PrincipalRole principalRole, Exception e);

    void onListGrantsSource(String catalogName, String catalogRoleName, List<GrantResource> grantsFromSource, Exception e);

    void onListGrantsTarget(String catalogName, String catalogRoleName, List<GrantResource> grantsFromTarget, Exception e);

    void onAddGrant(String catalogName, String catalogRoleName, GrantResource grant, Exception e);

    void onRevokeGrant(String catalogName, String catalogRoleName, GrantResource grant, Exception e);

}
