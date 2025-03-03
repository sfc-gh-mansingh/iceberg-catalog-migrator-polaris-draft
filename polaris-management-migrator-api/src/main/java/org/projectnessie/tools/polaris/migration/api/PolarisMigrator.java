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
import org.projectnessie.tools.polaris.migration.api.workspace.EntityPath;
import org.projectnessie.tools.polaris.migration.api.workspace.SignatureService;
import org.projectnessie.tools.polaris.migration.api.workspace.Status;
import org.projectnessie.tools.polaris.migration.api.workspace.Workspace;
import org.projectnessie.tools.polaris.migration.api.workspace.WorkspaceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class PolarisMigrator {

    private final Workspace workspace;

    private final PolarisService source;

    private final PolarisService target;

    private final SignatureService signatureService;

    private final Logger log = LoggerFactory.getLogger(PolarisMigrator.class);

    public PolarisMigrator(
            Workspace workspace,
            Workspace previousWorkspace,
            PolarisService source,
            PolarisService target
    ) {
        this.workspace = workspace;
        this.source = source;
        this.target = target;
        this.signatureService = new SignatureService(previousWorkspace);
    }

    private <T> List<T> doList(EntityPath entityPath, Status failureStatus, Callable<List<T>> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            workspace.put(new WorkspaceRecord(entityPath, failureStatus, e));
            return null;
        }
    }

    private void doModify(EntityPath entityPath, boolean isOverwrite, Object entity, Runnable runnable) {
        try {
            runnable.run();
            String signature = signatureService.createSignature(entity);

            if (isOverwrite) {
                workspace.put(new WorkspaceRecord(entityPath, Status.OVERWRITTEN, signature));
            } else {
                workspace.put(new WorkspaceRecord(entityPath, Status.CREATED, signature));
            }
        } catch (Exception e) {
            if (isOverwrite) {
                workspace.put(new WorkspaceRecord(entityPath, Status.OVERWRITE_FAILED, e));
            } else {
                workspace.put(new WorkspaceRecord(entityPath, Status.CREATE_FAILED, e));
            }
        }
    }

    private void doRemove(EntityPath entityPath, Runnable runnable) {
        try {
            runnable.run();
            workspace.put(new WorkspaceRecord(entityPath, Status.REMOVED));
        } catch (Exception e) {
            workspace.put(new WorkspaceRecord(entityPath, Status.REMOVE_FAILED, e));
        }
    }

//    public PrincipalWithCredentials createMigratorPrincipalOnSource() {
//        Principal principal = new Principal()
//                .name("migrator-principal-" + workspace.getRunId());
//
//        this.source.createPrincipal(principal);
//
//        PrincipalRole principalRole = new PrincipalRole()
//                .name("migrator-principal-role-" + workspace.getRunId());
//
//        this.source.createPrincipalRole(principalRole);
//
//        this.source.assignPrincipalRole(principal.getName(), principalRole.getName());
//
//        List<Catalog> catalogs = this.source.listCatalogs();
//
//        for (Catalog catalog : catalogs) {
//            CatalogRole catalogRole = new CatalogRole()
//                    .name("migrator-catalog-role-" + workspace.getRunId());
//
//            this.source.createCatalogRole(catalog.getName(), );
//        }
//    }

    private List<Catalog> listCatalogsFromSource() {
        return doList(EntityPath.catalogs(), Status.LIST_FROM_SOURCE_FAILED, source::listCatalogs);
    }

    private List<Catalog> listCatalogsFromTarget() {
        return doList(EntityPath.catalogs(), Status.LIST_FROM_TARGET_FAILED, target::listCatalogs);
    }

    private void createCatalogOnTargetIfChanged(Catalog catalog, boolean overwrite) {
        EntityPath catalogPath = EntityPath.catalog(catalog.getName());

        if (signatureService.hasChanged(catalogPath, catalog)) {
            doModify(catalogPath, overwrite, catalog, () -> target.createCatalog(catalog, overwrite));
        } else {
            workspace.put(new WorkspaceRecord(catalogPath, Status.NOT_MODIFIED, signatureService.createSignature(catalog)));
        }
    }

    private void removeCatalogOnTarget(String catalogName) {
        doRemove(EntityPath.catalog(catalogName), () -> target.removeCatalogCascade(catalogName));
    }

    public void migrateCatalogs() {
        List<Catalog> catalogsOnSource = listCatalogsFromSource();

        if (catalogsOnSource == null) return;

        List<Catalog> catalogsOnTarget = listCatalogsFromTarget();

        if (catalogsOnTarget == null) return;

        Set<String> catalogNamesOnSource = catalogsOnSource.stream()
                .map(Catalog::getName)
                .collect(Collectors.toSet());

        Set<String> catalogNamesOnTarget = catalogsOnTarget.stream()
                .map(Catalog::getName)
                .collect(Collectors.toSet());

        for (Catalog catalog : catalogsOnSource) {
            boolean overwrite = catalogNamesOnTarget.contains(catalog.getName());
            createCatalogOnTargetIfChanged(catalog, overwrite);
        }

        List<Catalog> catalogsToCleanupOnTarget = catalogsOnTarget
                .stream()
                .filter(catalog -> !catalogNamesOnSource.contains(catalog.getName()))
                .toList();

        for (Catalog catalog : catalogsToCleanupOnTarget) {
            removeCatalogOnTarget(catalog.getName());
        }

        for (Catalog catalog : catalogsOnSource) {
            migrateCatalogRoles(catalog.getName());
        }
    }

    private List<CatalogRole> listCatalogRolesFromSource(String catalogName) {
        return doList(
                EntityPath.catalogRoles(catalogName), Status.LIST_FROM_SOURCE_FAILED, () -> source.listCatalogRoles(catalogName));
    }

    private List<CatalogRole> listCatalogRolesFromTarget(String catalogName) {
        return doList(
                EntityPath.catalogRoles(catalogName), Status.LIST_FROM_TARGET_FAILED, () -> target.listCatalogRoles(catalogName));
    }

    private void createCatalogRoleOnTargetIfChanged(String catalogName, CatalogRole catalogRole, boolean overwrite) {
        EntityPath catalogRolePath = EntityPath.catalogRole(catalogName, catalogRole.getName());

        if (signatureService.hasChanged(catalogRolePath, catalogRole)) {
            doModify(
                    EntityPath.catalogRole(catalogName, catalogRole.getName()),
                    overwrite,
                    catalogRole,
                    () -> target.createCatalogRole(catalogName, catalogRole, overwrite)
            );
        } else {
            workspace.put(new WorkspaceRecord(catalogRolePath, Status.NOT_MODIFIED, signatureService.createSignature(catalogRole)));
        }

    }

    private void removeCatalogRoleOnTarget(String catalogName, String catalogRoleName) {
        doRemove(
                EntityPath.catalogRole(catalogName, catalogRoleName),
                () -> target.removeCatalogRole(catalogName, catalogRoleName)
        );
    }

    public void migrateCatalogRoles(String catalogName) {
        List<CatalogRole> catalogRolesOnSource = listCatalogRolesFromSource(catalogName);

        if (catalogRolesOnSource == null) return;

        List<CatalogRole> catalogRolesOnTarget = listCatalogRolesFromTarget(catalogName);

        if (catalogRolesOnTarget == null) return;

        Set<String> catalogRoleNamesOnSource = catalogRolesOnSource.stream()
                .map(CatalogRole::getName)
                .collect(Collectors.toSet());

        Set<String> catalogRoleNamesOnTarget = catalogRolesOnTarget.stream()
                .map(CatalogRole::getName)
                .collect(Collectors.toSet());

        for (CatalogRole catalogRole : catalogRolesOnSource) {
            if (catalogRole.getName().equals("catalog_admin")) {
                workspace.put(new WorkspaceRecord(EntityPath.catalogRole(catalogName, "catalog_admin"), Status.SKIPPED));
                continue;
            }

            boolean overwrite = catalogRoleNamesOnTarget.contains(catalogRole.getName());
            createCatalogRoleOnTargetIfChanged(catalogName, catalogRole, overwrite);
        }

        List<CatalogRole> catalogRolesToCleanupOnTarget = catalogRolesOnTarget.stream()
                .filter(catalogRole -> !catalogRoleNamesOnSource.contains(catalogRole.getName()))
                .toList();

        for (CatalogRole catalogRole : catalogRolesToCleanupOnTarget) {
            removeCatalogRoleOnTarget(catalogName, catalogRole.getName());
        }

        for (CatalogRole catalogRole : catalogRolesOnSource) {
            migrateGrants(catalogName, catalogRole.getName());
        }
    }

    private Set<GrantResource> listGrantsFromSource(String catalogName, String catalogRoleName) {
        List<GrantResource> grantResources = doList(
                EntityPath.grants(catalogName, catalogRoleName),
                Status.LIST_FROM_SOURCE_FAILED,
                () -> source.listGrants(catalogName, catalogRoleName)
        );

        return grantResources == null ? null : Set.copyOf(grantResources);
    }

    private Set<GrantResource> listGrantsFromTarget(String catalogName, String catalogRoleName) {
        List<GrantResource> grantResources = doList(
                EntityPath.grants(catalogName, catalogRoleName),
                Status.LIST_FROM_TARGET_FAILED,
                () -> target.listGrants(catalogName, catalogRoleName)
        );

        return grantResources == null ? null : Set.copyOf(grantResources);
    }

    private void addGrantOnTargetIfChanged(String catalogName, String catalogRoleName, GrantResource grant) {
        EntityPath grantPath = EntityPath.grant(catalogName, catalogRoleName, grant);

        if (signatureService.hasChanged(grantPath, grant)) {
            doModify(
                    EntityPath.grant(catalogName, catalogRoleName, grant),
                    false,
                    grant,
                    () -> target.addGrant(catalogName, catalogRoleName, grant)
            );
        } else {
            workspace.put(new WorkspaceRecord(grantPath, Status.NOT_MODIFIED, signatureService.createSignature(grant)));
        }
    }

    private void revokeGrantOnTarget(String catalogName, String catalogRoleName, GrantResource grant) {
        doRemove(
                EntityPath.grant(catalogName, catalogRoleName, grant),
                () -> target.revokeGrant(catalogName, catalogRoleName, grant)
        );
    }

    public void migrateGrants(String catalogName, String catalogRoleName) {
        Set<GrantResource> grantsOnSource = listGrantsFromSource(catalogName, catalogRoleName);
        if (grantsOnSource == null) return;

        Set<GrantResource> grantsOnTarget = listGrantsFromTarget(catalogName, catalogRoleName);
        if (grantsOnTarget == null) return;

        grantsOnSource
                .forEach(grant -> addGrantOnTargetIfChanged(catalogName, catalogRoleName, grant));

        grantsOnTarget
                .stream()
                .filter(grant -> !grantsOnSource.contains(grant))
                .forEach(grant -> revokeGrantOnTarget(catalogName, catalogRoleName, grant));
    }

}
