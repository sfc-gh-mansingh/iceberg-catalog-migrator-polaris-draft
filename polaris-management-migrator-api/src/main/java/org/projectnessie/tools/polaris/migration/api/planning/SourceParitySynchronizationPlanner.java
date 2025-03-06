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
import java.util.Set;
import java.util.stream.Collectors;

public class SourceParitySynchronizationPlanner implements SynchronizationPlanner {

    @Override
    public SynchronizationPlan<PrincipalRole> planPrincipalRoleSync(List<PrincipalRole> principalRolesOnSource, List<PrincipalRole> principalRolesOnTarget) {
        Set<String> sourcePrincipalRoleNames = principalRolesOnSource.stream().map(PrincipalRole::getName).collect(Collectors.toSet());
        Set<String> targetPrincipalRoleNames = principalRolesOnTarget.stream().map(PrincipalRole::getName).collect(Collectors.toSet());

        List<PrincipalRole> principalRolesOnlyOnSource = principalRolesOnSource
                .stream()
                .filter(role -> !targetPrincipalRoleNames.contains(role.getName()))
                .toList();

        List<PrincipalRole> principalRolesOnBoth = principalRolesOnTarget
                .stream()
                .filter(role -> sourcePrincipalRoleNames.contains(role.getName()))
                .toList();

        List<PrincipalRole> principalRolesOnlyOnTarget = principalRolesOnTarget
                .stream()
                .filter(role -> !sourcePrincipalRoleNames.contains(role.getName()))
                .toList();

        return new SynchronizationPlan<>(principalRolesOnlyOnSource, principalRolesOnBoth, principalRolesOnlyOnTarget);
    }

    @Override
    public SynchronizationPlan<Catalog> planCatalogSync(List<Catalog> catalogsOnSource, List<Catalog> catalogsOnTarget) {
        Set<String> sourceCatalogNames = catalogsOnSource.stream().map(Catalog::getName).collect(Collectors.toSet());
        Set<String> targetCatalogNames = catalogsOnTarget.stream().map(Catalog::getName).collect(Collectors.toSet());

        List<Catalog> catalogsOnlyOnSource = catalogsOnSource
                .stream()
                .filter(catalog -> !targetCatalogNames.contains(catalog.getName()))
                .toList();

        List<Catalog> catalogsOnBoth = catalogsOnTarget
                .stream()
                .filter(catalog -> sourceCatalogNames.contains(catalog.getName()))
                .toList();

        List<Catalog> catalogsOnlyOnTarget = catalogsOnTarget
                .stream()
                .filter(catalog -> !sourceCatalogNames.contains(catalog.getName()))
                .toList();

        return new SynchronizationPlan<>(catalogsOnlyOnSource, catalogsOnBoth, catalogsOnlyOnTarget);
    }

    @Override
    public SynchronizationPlan<CatalogRole> planCatalogRoleSync(String catalogName, List<CatalogRole> catalogRolesOnSource, List<CatalogRole> catalogRolesOnTarget) {
        Set<String> sourceCatalogRoleNames = catalogRolesOnSource.stream().map(CatalogRole::getName).collect(Collectors.toSet());
        Set<String> targetCatalogRoleNames = catalogRolesOnTarget.stream().map(CatalogRole::getName).collect(Collectors.toSet());

        List<CatalogRole> catalogRolesOnlyOnSource = catalogRolesOnSource
                .stream()
                .filter(catalogRole -> !targetCatalogRoleNames.contains(catalogRole.getName()))
                .filter(catalogRole -> !catalogRole.getName().equals("catalog_admin")) // do not try to create catalog_admin
                .toList();

        List<CatalogRole> catalogRolesOnBoth = catalogRolesOnTarget
                .stream()
                .filter(catalogRole -> sourceCatalogRoleNames.contains(catalogRole.getName()))
                .filter(catalogRole -> !catalogRole.getName().equals("catalog_admin")) // do not try to overwrite catalog_admin
                .toList();

        List<CatalogRole> catalogRolesOnlyOnTarget = catalogRolesOnTarget
                .stream()
                .filter(catalogRole -> !sourceCatalogRoleNames.contains(catalogRole.getName()))
                .filter(catalogRole -> !catalogRole.getName().equals("catalog_admin")) // do not try to remove catalog_admin
                .toList();

        return new SynchronizationPlan<>(catalogRolesOnlyOnSource, catalogRolesOnBoth, catalogRolesOnlyOnTarget);
    }

    @Override
    public SynchronizationPlan<GrantResource> planGrantSync(String catalogName, String catalogRoleName, List<GrantResource> grantsOnSource, List<GrantResource> grantsOnTarget) {
        Set<GrantResource> grantsSourceSet = Set.copyOf(grantsOnSource);
        Set<GrantResource> grantsTargetSet = Set.copyOf(grantsOnTarget);

        List<GrantResource> grantsOnlyOnSource = grantsOnSource
                .stream()
                .filter(grant -> !grantsTargetSet.contains(grant))
                .toList();

        List<GrantResource> grantsOnBoth = grantsOnTarget
                .stream()
                .filter(grantsSourceSet::contains)
                .toList();

        List<GrantResource> grantsOnlyOnTarget = grantsOnTarget
                .stream()
                .filter(grant -> !grantsSourceSet.contains(grant))
                .toList();

        return new SynchronizationPlan<>(grantsOnlyOnSource, grantsOnBoth, grantsOnlyOnTarget);
    }

    @Override
    public SynchronizationPlan<PrincipalRole> planAssignPrincipalRolesToCatalogRolesSync(String catalogName, String catalogRoleName, List<PrincipalRole> assignedPrincipalRolesOnSource, List<PrincipalRole> assignedPrincipalRolesOnTarget) {
        Set<String> sourcePrincipalRoleNames = assignedPrincipalRolesOnSource.stream().map(PrincipalRole::getName).collect(Collectors.toSet());

        List<PrincipalRole> principalRolesOnlyOnTarget = assignedPrincipalRolesOnTarget
                .stream()
                .filter(role -> !sourcePrincipalRoleNames.contains(role.getName()))
                .toList();

        // can do the same assignment twice, so we will just plan all of these as 'creates'
        // because there is no concept of overwriting here
        return new SynchronizationPlan<>(assignedPrincipalRolesOnSource, List.of(), principalRolesOnlyOnTarget);
    }

}
