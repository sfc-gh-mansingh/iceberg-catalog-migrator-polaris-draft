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

package org.projectnessie.tools.polaris.migration.api.planning;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PrincipalRole;

import java.util.List;

public interface SynchronizationPlanner {

    SynchronizationPlan<PrincipalRole> planPrincipalRoleSync(List<PrincipalRole> principalRolesOnSource, List<PrincipalRole> principalRolesOnTarget);

    SynchronizationPlan<Catalog> planCatalogSync(List<Catalog> catalogsOnSource, List<Catalog> catalogsOnTarget);

    SynchronizationPlan<CatalogRole> planCatalogRoleSync(String catalogName, List<CatalogRole> catalogRolesOnSource, List<CatalogRole> catalogRolesOnTarget);

    SynchronizationPlan<GrantResource> planGrantSync(String catalogName, String catalogRoleName, List<GrantResource> grantsOnSource, List<GrantResource> grantsOnTarget);

    SynchronizationPlan<PrincipalRole> planAssignPrincipalRolesToCatalogRolesSync(String catalogName, String catalogRoleName, List<PrincipalRole> assignedPrincipalRolesOnSource, List<PrincipalRole> assignedPrincipalRolesOnTarget);

}
