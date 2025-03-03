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

package org.projectnessie.tools.polaris.migration.api.idemp;

import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;

import java.util.List;

public class PolarisService {

    private final PolarisManagementDefaultApi api;

    public PolarisService(PolarisManagementDefaultApi api) {
        this.api = api;
    }

    public PrincipalWithCredentials createPrincipal(Principal principal) {
        CreatePrincipalRequest request = new CreatePrincipalRequest().principal(principal);
        return this.api.createPrincipal(request);
    }

    public void assignPrincipalRole(String principalName, String principalRoleName) {
        GrantPrincipalRoleRequest request = new GrantPrincipalRoleRequest()
                .principalRole(new PrincipalRole().name(principalRoleName));
        this.api.assignPrincipalRole(principalName, request);
    }

    public void createPrincipalRole(PrincipalRole principalRole) {
        CreatePrincipalRoleRequest request = new CreatePrincipalRoleRequest().principalRole(principalRole);
        this.api.createPrincipalRole(request);
    }

    public void assignCatalogRole(String principalRoleName, String catalogName, String catalogRoleName) {
        GrantCatalogRoleRequest request = new GrantCatalogRoleRequest()
                .catalogRole(new CatalogRole().name(catalogRoleName));
        this.api.assignCatalogRoleToPrincipalRole(principalRoleName, catalogName, request);
    }

    public List<Catalog> listCatalogs() {
        return this.api.listCatalogs().getCatalogs();
    }

    public void createCatalog(Catalog catalog, boolean overwrite) {
        if (overwrite) {
            removeCatalogCascade(catalog.getName());
        }

        CreateCatalogRequest request = new CreateCatalogRequest().catalog(catalog);
        this.api.createCatalog(request);
    }

    public void removeCatalogCascade(String catalogName) {
        List<CatalogRole> catalogRoles = listCatalogRoles(catalogName);

        // remove catalog roles under catalog
        for (CatalogRole catalogRole : catalogRoles) {
            if (catalogRole.getName().equals("catalog_admin")) continue;

            removeCatalogRole(catalogName, catalogRole.getName());
        }

        this.api.deleteCatalog(catalogName);
    }

    public List<CatalogRole> listCatalogRoles(String catalogName) {
        return this.api.listCatalogRoles(catalogName).getRoles();
    }

    public void createCatalogRole(String catalogName, CatalogRole catalogRole, boolean overwrite) {
        if (overwrite) {
            removeCatalogRole(catalogName, catalogRole.getName());
        }

        CreateCatalogRoleRequest request = new CreateCatalogRoleRequest().catalogRole(catalogRole);
        this.api.createCatalogRole(catalogName, request);
    }

    public void removeCatalogRole(String catalogName, String catalogRoleName) {
        this.api.deleteCatalogRole(catalogName, catalogRoleName);
    }

    public List<GrantResource> listGrants(String catalogName, String catalogRoleName) {
        return this.api.listGrantsForCatalogRole(catalogName, catalogRoleName).getGrants();
    }

    public void addGrant(String catalogName, String catalogRoleName, GrantResource grant) {
        AddGrantRequest addGrantRequest = new AddGrantRequest().grant(grant);
        this.api.addGrantToCatalogRole(catalogName, catalogRoleName, addGrantRequest);
    }

    public void revokeGrant(String catalogName, String catalogRoleName, GrantResource grant) {
        RevokeGrantRequest revokeGrantRequest = new RevokeGrantRequest().grant(grant);
        this.api.revokeGrantFromCatalogRole(catalogName, catalogRoleName, false, revokeGrantRequest);
    }

}
