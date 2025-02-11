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

package org.projectnessie.tools.polaris.migration.api.migrator;

import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationLog;
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ManagementMigratorCatalogsTest {

    @Test
    public void copyCatalogAndCatalogRolesAndGrantsRecursively() throws Exception {
        PolarisManagementDefaultApi mockSource = Mockito.mock(PolarisManagementDefaultApi.class);
        PolarisManagementDefaultApi mockTarget = Mockito.mock(PolarisManagementDefaultApi.class);

        Catalog catalog = new Catalog()
                .type(Catalog.TypeEnum.INTERNAL)
                .name("test-catalog")
                .properties(new CatalogProperties().defaultBaseLocation("s3://test-catalog-bucket/location"))
                .storageConfigInfo(
                        new StorageConfigInfo()
                                .storageType(StorageConfigInfo.StorageTypeEnum.S3)
                                .addAllowedLocationsItem("s3://test-catalog-bucket/location")
                );


        Catalogs catalogs = new Catalogs()
                .catalogs(List.of(catalog));

        // Return test catalog when catalogs listed
        when(mockSource.listCatalogs()).thenReturn(catalogs);
        doNothing().when(mockTarget).createCatalog(any());

        CatalogRole catalogRole = new CatalogRole()
                .name("test-catalog-role");

        CatalogRoles catalogRoles = new CatalogRoles()
                .roles(List.of(catalogRole));

        // Return test catalog role when catalog roles listed for test catalog
        when(mockSource.listCatalogRoles(catalog.getName())).thenReturn(catalogRoles);
        doNothing().when(mockTarget).createCatalogRole(eq(catalog.getName()), any());

        GrantResource grantResource = new NamespaceGrant()
                .type(GrantResource.TypeEnum.NAMESPACE)
                .privilege(NamespacePrivilege.NAMESPACE_CREATE)
                .namespace(List.of("ns"));

        GrantResources grantResources = new GrantResources()
                .grants(List.of(grantResource));

        // Return test grant when grants listed for test catalog role
        when(mockSource.listGrantsForCatalogRole(catalog.getName(), catalogRole.getName()))
                .thenReturn(grantResources);
        doNothing().when(mockTarget).addGrantToCatalogRole(eq(catalog.getName()), eq(catalogRole.getName()), any());

        // capture migration logs
        final List<EntityMigrationLog> logs = new ArrayList<>();

        MigrationLog mockMigrationLog = Mockito.mock(MigrationLog.class);
        doAnswer(invocation -> {
            EntityMigrationLog log = invocation.getArgument(0);
            logs.add(log);
            return null;
        }).when(mockMigrationLog).append(any());

        ManagementMigrator migrator = new ManagementMigrator(mockSource, mockTarget, mockMigrationLog, 1);
        migrator.migrateCatalogs(true, false, true);

        EntityMigrationLog catalogMigrationLog = logs.stream()
                .filter(l -> l.entityType() == ManagementEntityType.CATALOG)
                .findFirst()
                .get();

        EntityMigrationLog catalogRoleMigrationLog = logs.stream()
                .filter(l -> l.entityType() == ManagementEntityType.CATALOG_ROLE)
                .findFirst()
                .get();

        EntityMigrationLog grantMigrationLog = logs.stream()
                .filter(l -> l.entityType() == ManagementEntityType.GRANT)
                .findFirst()
                .get();

        Assertions.assertEquals(catalogMigrationLog.properties().get("catalogName"), catalog.getName());
        Assertions.assertEquals(catalogRoleMigrationLog.properties().get("catalogRoleName"), catalogRole.getName());
        Assertions.assertEquals(grantMigrationLog.properties().get("privilege"), NamespacePrivilege.NAMESPACE_CREATE.getValue());
    }

    @Test
    public void copyPrincipalAndPrincipalRoleAssignmentRecursively() throws Exception {
        PolarisManagementDefaultApi mockSource = Mockito.mock(PolarisManagementDefaultApi.class);
        PolarisManagementDefaultApi mockTarget = Mockito.mock(PolarisManagementDefaultApi.class);

        Principal principal = new Principal()
                .name("test-principal");

        Principals principals = new Principals().principals(List.of(principal));

        // Return test principal when principals listed
        when(mockSource.listPrincipals()).thenReturn(principals);
        doAnswer(invocation ->
                new PrincipalWithCredentials()
                        .principal(principal)
                        .credentials(new PrincipalWithCredentialsCredentials().clientId("clientId").clientSecret("secret"))
        ).when(mockTarget).createPrincipal(any());

        PrincipalRole principalRole = new PrincipalRole()
                .name("test-principal-role");

        // Return test principal roles when assigned principal roles listed for test principal
        PrincipalRoles principalRoles = new PrincipalRoles().roles(List.of(principalRole));

        when(mockSource.listPrincipalRolesAssigned(principal.getName())).thenReturn(principalRoles);
        doNothing().when(mockTarget).assignPrincipalRole(eq("test-principal-role"), any());

        final List<EntityMigrationLog> logs = new ArrayList<>();

        // Capture migration log
        MigrationLog mockMigrationLog = Mockito.mock(MigrationLog.class);
        doAnswer(invocation -> {
            EntityMigrationLog log = invocation.getArgument(0);
            logs.add(log);
            return null;
        }).when(mockMigrationLog).append(any());

        ManagementMigrator migrator = new ManagementMigrator(mockSource, mockTarget, mockMigrationLog, 1);
        migrator.migratePrincipals(true);

        EntityMigrationLog principalMigrationLog = logs.stream()
                .filter(l -> l.entityType() == ManagementEntityType.PRINCIPAL)
                .findFirst()
                .get();

        EntityMigrationLog principalRoleAssignmentLog = logs.stream()
                .filter(l -> l.entityType() == ManagementEntityType.PRINCIPAL_ROLE_ASSIGNMENT)
                .findFirst()
                .get();

        Assertions.assertEquals(principalMigrationLog.properties().get("principalName"), principal.getName());
        Assertions.assertEquals(principalRoleAssignmentLog.properties().get("principalRoleName"), principalRole.getName());
    }



}
