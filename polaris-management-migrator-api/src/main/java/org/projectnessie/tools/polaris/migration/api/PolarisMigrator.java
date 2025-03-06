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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigrator;
//import org.projectnessie.tools.polaris.migration.api.workspace.EntityPath;
//import org.projectnessie.tools.polaris.migration.api.workspace.SignatureService;
//import org.projectnessie.tools.polaris.migration.api.workspace.Status;
//import org.projectnessie.tools.polaris.migration.api.workspace.Workspace;
//import org.projectnessie.tools.polaris.migration.api.workspace.WorkspaceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class PolarisMigrator {

//    private final Workspace workspace;
//
//    private final PolarisService source;
//
//    private final PolarisService target;
//
//    private final SignatureService signatureService;
//
//    private final static String POLARIS_MIGRATOR_PRINCIPAL_PROPERTY = "POLARIS_MIGRATOR_PRINCIPAL";
//
//    private final static String MIGRATOR_NAME = "polaris-migrator";
//
//    private final Logger log = LoggerFactory.getLogger(PolarisMigrator.class);
//
//    public PolarisMigrator(
//            Workspace workspace,
//            Workspace previousWorkspace,
//            PolarisService source,
//            PolarisService target
//    ) {
//        this.workspace = workspace;
//        this.source = source;
//        this.target = target;
//        this.signatureService = new SignatureService(previousWorkspace);
//    }
//
//    private <V> V doList(EntityPath entityPath, Status failureStatus, Callable<V> callable) {
//        try {
//            return callable.call();
//        } catch (Exception e) {
//            workspace.put(new WorkspaceRecord(entityPath, failureStatus, e));
//            return null;
//        }
//    }
//
//    private void doModify(EntityPath entityPath, boolean isOverwrite, Object entity, Runnable runnable) {
//        try {
//            runnable.run();
//            String signature = signatureService.createSignature(entity);
//
//            if (isOverwrite) {
//                workspace.put(new WorkspaceRecord(entityPath, Status.OVERWRITTEN, signature));
//            } else {
//                workspace.put(new WorkspaceRecord(entityPath, Status.CREATED, signature));
//            }
//        } catch (Exception e) {
//            if (isOverwrite) {
//                workspace.put(new WorkspaceRecord(entityPath, Status.OVERWRITE_FAILED, e));
//            } else {
//                workspace.put(new WorkspaceRecord(entityPath, Status.CREATE_FAILED, e));
//            }
//        }
//    }
//
//    private void doRemove(EntityPath entityPath, Runnable runnable) {
//        try {
//            runnable.run();
//            workspace.put(new WorkspaceRecord(entityPath, Status.REMOVED));
//        } catch (Exception e) {
//            workspace.put(new WorkspaceRecord(entityPath, Status.REMOVE_FAILED, e));
//        }
//    }
//
////    public PrincipalWithCredentials createMigratorPrincipal(PolarisService polaris) {
////        if (polaris.principalExists(MIGRATOR_NAME)) {
////            polaris.removePrincipal(MIGRATOR_NAME);
////        }
////
////        Principal prototypeMigratorPrincipal = new Principal()
////                .name(MIGRATOR_NAME)
////                .putPropertiesItem(POLARIS_MIGRATOR_PRINCIPAL_PROPERTY, "");
////
////        PrincipalWithCredentials migratorPrincipal = polaris.createPrincipal(prototypeMigratorPrincipal);
////
////        if (polaris.principalRoleExists(MIGRATOR_NAME)) {
////            polaris.removePrincipalRole(MIGRATOR_NAME);
////        }
////
////        PrincipalRole migratorPrincipalRole = new PrincipalRole()
////                .name(MIGRATOR_NAME)
////                .putPropertiesItem(POLARIS_MIGRATOR_PRINCIPAL_PROPERTY, "");
////
////        polaris.createPrincipalRole(migratorPrincipalRole);
////
////        polaris.assignPrincipalRole(migratorPrincipal.getPrincipal().getName(), migratorPrincipalRole.getName());
////
////        List<Catalog> catalogs = polaris.listCatalogs();
////
////        for (Catalog catalog : catalogs) {
////            CatalogRole migratorCatalogRole = new CatalogRole()
////                    .name(MIGRATOR_NAME)
////                    .putPropertiesItem(POLARIS_MIGRATOR_PRINCIPAL_PROPERTY, "");
////
////            try {
////                polaris.createCatalogRole(
////                        catalog.getName(),
////                        migratorCatalogRole,
////                        polaris.catalogRoleExists(catalog.getName(), MIGRATOR_NAME)
////                );
////
////
////                polaris.assignCatalogRole(migratorPrincipalRole.getName(), catalog.getName(), migratorCatalogRole.getName());
////
////                GrantResource migratorGrantResource = new CatalogGrant()
////                        .type(GrantResource.TypeEnum.CATALOG)
////                        .privilege(CatalogPrivilege.CATALOG_MANAGE_METADATA);
////
////                polaris.addGrant(catalog.getName(), migratorCatalogRole.getName(), migratorGrantResource);
////            } catch (Exception e) {
////                // TODO: ADD LOG
////                continue;
////            }
////        }
////
////        return migratorPrincipal;
////    }
//
//    private List<Catalog> listCatalogsFromSource() {
//        return doList(EntityPath.catalogs(), Status.LIST_FROM_SOURCE_FAILED, source::listCatalogs);
//    }
//
//    private List<Catalog> listCatalogsFromTarget() {
//        return doList(EntityPath.catalogs(), Status.LIST_FROM_TARGET_FAILED, target::listCatalogs);
//    }
//
//    private void createCatalogOnTargetIfChanged(Catalog catalog, boolean overwrite) {
//        EntityPath catalogPath = EntityPath.catalog(catalog.getName());
//
//        if (signatureService.hasChanged(catalogPath, catalog)) {
//            doModify(catalogPath, overwrite, catalog, () -> target.createCatalog(catalog, overwrite));
//        } else {
//            workspace.put(new WorkspaceRecord(catalogPath, Status.NOT_MODIFIED, signatureService.createSignature(catalog)));
//        }
//    }
//
//    private void removeCatalogOnTarget(String catalogName) {
//        doRemove(EntityPath.catalog(catalogName), () -> target.removeCatalogCascade(catalogName));
//    }
//
//    public void migrateCatalogs() {
//        List<Catalog> catalogsOnSource = listCatalogsFromSource();
//
//        if (catalogsOnSource == null) return;
//
//        List<Catalog> catalogsOnTarget = listCatalogsFromTarget();
//
//        if (catalogsOnTarget == null) return;
//
//        Set<String> catalogNamesOnSource = catalogsOnSource.stream()
//                .map(Catalog::getName)
//                .collect(Collectors.toSet());
//
//        Set<String> catalogNamesOnTarget = catalogsOnTarget.stream()
//                .map(Catalog::getName)
//                .collect(Collectors.toSet());
//
//        for (Catalog catalog : catalogsOnSource) {
//            boolean overwrite = catalogNamesOnTarget.contains(catalog.getName());
//            createCatalogOnTargetIfChanged(catalog, overwrite);
//        }
//
//        List<Catalog> catalogsToCleanupOnTarget = catalogsOnTarget
//                .stream()
//                .filter(catalog -> !catalogNamesOnSource.contains(catalog.getName()))
//                .toList();
//
//        for (Catalog catalog : catalogsToCleanupOnTarget) {
//            removeCatalogOnTarget(catalog.getName());
//        }
//
//        for (Catalog catalog : catalogsOnSource) {
//            migrateTables(catalog.getName(), sourceMigratorPrincipal, targetMigratorPrincipal);
//            migrateCatalogRoles(catalog.getName());
//        }
//    }
//
//    private List<CatalogRole> listCatalogRolesFromSource(String catalogName) {
//        return doList(
//                EntityPath.catalogRoles(catalogName), Status.LIST_FROM_SOURCE_FAILED, () -> source.listCatalogRoles(catalogName));
//    }
//
//    private List<CatalogRole> listCatalogRolesFromTarget(String catalogName) {
//        return doList(
//                EntityPath.catalogRoles(catalogName), Status.LIST_FROM_TARGET_FAILED, () -> target.listCatalogRoles(catalogName));
//    }
//
//    private void createCatalogRoleOnTargetIfChanged(String catalogName, CatalogRole catalogRole, boolean overwrite) {
//        EntityPath catalogRolePath = EntityPath.catalogRole(catalogName, catalogRole.getName());
//
//        if (signatureService.hasChanged(catalogRolePath, catalogRole)) {
//            doModify(
//                    EntityPath.catalogRole(catalogName, catalogRole.getName()),
//                    overwrite,
//                    catalogRole,
//                    () -> target.createCatalogRole(catalogName, catalogRole, overwrite)
//            );
//        } else {
//            workspace.put(new WorkspaceRecord(catalogRolePath, Status.NOT_MODIFIED, signatureService.createSignature(catalogRole)));
//        }
//
//    }
//
//    private void removeCatalogRoleOnTarget(String catalogName, String catalogRoleName) {
//        doRemove(
//                EntityPath.catalogRole(catalogName, catalogRoleName),
//                () -> target.removeCatalogRole(catalogName, catalogRoleName)
//        );
//    }
//
//    public void migrateCatalogRoles(String catalogName) {
//        List<CatalogRole> catalogRolesOnSource = listCatalogRolesFromSource(catalogName);
//
//        if (catalogRolesOnSource == null) return;
//
//        List<CatalogRole> catalogRolesOnTarget = listCatalogRolesFromTarget(catalogName);
//
//        if (catalogRolesOnTarget == null) return;
//
//        Set<String> catalogRoleNamesOnSource = catalogRolesOnSource.stream()
//                .map(CatalogRole::getName)
//                .collect(Collectors.toSet());
//
//        Set<String> catalogRoleNamesOnTarget = catalogRolesOnTarget.stream()
//                .map(CatalogRole::getName)
//                .collect(Collectors.toSet());
//
//        for (CatalogRole catalogRole : catalogRolesOnSource) {
//            if (catalogRole.getName().equals("catalog_admin")) {
//                workspace.put(new WorkspaceRecord(EntityPath.catalogRole(catalogName, "catalog_admin"), Status.SKIPPED));
//                continue;
//            }
//
//            boolean overwrite = catalogRoleNamesOnTarget.contains(catalogRole.getName());
//            createCatalogRoleOnTargetIfChanged(catalogName, catalogRole, overwrite);
//        }
//
//        List<CatalogRole> catalogRolesToCleanupOnTarget = catalogRolesOnTarget.stream()
//                .filter(catalogRole -> !catalogRoleNamesOnSource.contains(catalogRole.getName()))
//                .toList();
//
//        for (CatalogRole catalogRole : catalogRolesToCleanupOnTarget) {
//            removeCatalogRoleOnTarget(catalogName, catalogRole.getName());
//        }
//
//        for (CatalogRole catalogRole : catalogRolesOnSource) {
//            migrateGrants(catalogName, catalogRole.getName());
//        }
//    }
//
//    private Set<GrantResource> listGrantsFromSource(String catalogName, String catalogRoleName) {
//        List<GrantResource> grantResources = doList(
//                EntityPath.grants(catalogName, catalogRoleName),
//                Status.LIST_FROM_SOURCE_FAILED,
//                () -> source.listGrants(catalogName, catalogRoleName)
//        );
//
//        return grantResources == null ? null : Set.copyOf(grantResources);
//    }
//
//    private Set<GrantResource> listGrantsFromTarget(String catalogName, String catalogRoleName) {
//        List<GrantResource> grantResources = doList(
//                EntityPath.grants(catalogName, catalogRoleName),
//                Status.LIST_FROM_TARGET_FAILED,
//                () -> target.listGrants(catalogName, catalogRoleName)
//        );
//
//        return grantResources == null ? null : Set.copyOf(grantResources);
//    }
//
//    private void addGrantOnTargetIfChanged(String catalogName, String catalogRoleName, GrantResource grant) {
//        EntityPath grantPath = EntityPath.grant(catalogName, catalogRoleName, grant);
//
//        if (signatureService.hasChanged(grantPath, grant)) {
//            doModify(
//                    EntityPath.grant(catalogName, catalogRoleName, grant),
//                    false,
//                    grant,
//                    () -> target.addGrant(catalogName, catalogRoleName, grant)
//            );
//        } else {
//            workspace.put(new WorkspaceRecord(grantPath, Status.NOT_MODIFIED, signatureService.createSignature(grant)));
//        }
//    }
//
//    private void revokeGrantOnTarget(String catalogName, String catalogRoleName, GrantResource grant) {
//        doRemove(
//                EntityPath.grant(catalogName, catalogRoleName, grant),
//                () -> target.revokeGrant(catalogName, catalogRoleName, grant)
//        );
//    }
//
//    public void migrateGrants(String catalogName, String catalogRoleName) {
//        Set<GrantResource> grantsOnSource = listGrantsFromSource(catalogName, catalogRoleName);
//        if (grantsOnSource == null) return;
//
//        Set<GrantResource> grantsOnTarget = listGrantsFromTarget(catalogName, catalogRoleName);
//        if (grantsOnTarget == null) return;
//
//        grantsOnSource
//                .forEach(grant -> addGrantOnTargetIfChanged(catalogName, catalogRoleName, grant));
//
//        grantsOnTarget
//                .stream()
//                .filter(grant -> !grantsOnSource.contains(grant))
//                .forEach(grant -> revokeGrantOnTarget(catalogName, catalogRoleName, grant));
//    }
//
//    private org.apache.iceberg.catalog.Catalog initializeSourceCatalog(String catalogName, PrincipalWithCredentials sourceMigratorPrincipal) {
//        return doList(EntityPath.tables(catalogName), Status.LIST_FROM_SOURCE_FAILED,
//                () -> source.initializeCatalog(catalogName, sourceMigratorPrincipal));
//    }
//
//    private org.apache.iceberg.catalog.Catalog initializeTargetCatalog(String catalogName, PrincipalWithCredentials targetMigratorPrincipal) {
//        return doList(EntityPath.tables(catalogName), Status.LIST_FROM_TARGET_FAILED,
//                () -> target.initializeCatalog(catalogName, targetMigratorPrincipal));
//    }
//
//    private Set<TableIdentifier> listTablesFromSource(String catalogName, CatalogMigrator catalogMigrator) {
//        return doList(EntityPath.tables(catalogName), Status.LIST_FROM_SOURCE_FAILED,
//                () -> catalogMigrator.getMatchingTableIdentifiersFromSource(null));
//    }
//
//    private Set<TableIdentifier> listTablesFromTarget(String catalogName, CatalogMigrator catalogMigrator) {
//        return doList(EntityPath.tables(catalogName), Status.LIST_FROM_TARGET_FAILED,
//                () -> catalogMigrator.getMatchingTableIdentifiersFromTarget(null));
//    }
//
//    private void registerTableToTarget(
//            String catalogName, TableIdentifier tableIdentifier, boolean existsOnTarget, CatalogMigrator catalogMigrator) {
//        doModify(EntityPath.table(catalogName, tableIdentifier), existsOnTarget, null, () -> {
//            if (existsOnTarget)
//                catalogMigrator.dropTableFromTargetCatalog(tableIdentifier);
//
//            catalogMigrator.registerTable(tableIdentifier);
//        });
//    }
//
//    private void removeTableFromTarget(String catalogName, TableIdentifier tableIdentifier, CatalogMigrator catalogMigrator) {
//        doRemove(EntityPath.table(catalogName, tableIdentifier), () -> catalogMigrator.dropTableFromTargetCatalog(tableIdentifier));
//    }
//
//    public void migrateTables(
//            String catalogName,
//            PrincipalWithCredentials sourceMigratorPrincipal,
//            PrincipalWithCredentials targetMigratorPrincipal
//    ) {
//        org.apache.iceberg.catalog.Catalog sourceCatalog = initializeSourceCatalog(catalogName, sourceMigratorPrincipal);
//
//        if (sourceCatalog == null) return;
//
//        org.apache.iceberg.catalog.Catalog targetCatalog = initializeTargetCatalog(catalogName, targetMigratorPrincipal);
//
//        if (targetCatalog == null) return;
//
//        CatalogMigrator catalogMigrator = ImmutableCatalogMigrator.builder()
//                .sourceCatalog(sourceCatalog)
//                .targetCatalog(targetCatalog)
//                .deleteEntriesFromSourceCatalog(false)
//                .enableStacktrace(true)
//                .build();
//
//        Set<TableIdentifier> tablesOnSource = listTablesFromSource(catalogName, catalogMigrator);
//
//        if (tablesOnSource == null) return;
//
//        Set<TableIdentifier> tablesOnTarget = listTablesFromTarget(catalogName, catalogMigrator);
//
//        if (tablesOnTarget == null) return;
//
//        for (TableIdentifier tableIdentifier : tablesOnSource) {
//            registerTableToTarget(catalogName, tableIdentifier, tablesOnTarget.contains(tableIdentifier), catalogMigrator);
//        }
//
//        tablesOnTarget
//                .stream()
//                .filter(tableIdentifier -> !tablesOnSource.contains(tableIdentifier))
//                .forEach(table -> removeTableFromTarget(catalogName, table, catalogMigrator));
//
//        if (sourceCatalog instanceof AutoCloseable autoCloseableSource) {
//            try {
//                autoCloseableSource.close();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        if (targetCatalog instanceof AutoCloseable autoCloseableTarget) {
//            try {
//                autoCloseableTarget.close();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

}
