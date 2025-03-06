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

import com.snowflake.polaris.management.ApiException;
import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.http.HttpStatus;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.rest.RESTCatalog;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PolarisService {

    private final PolarisManagementDefaultApi api;

    private final Map<String, String> catalogProperties;

    public PolarisService(PolarisManagementDefaultApi api, Map<String, String> catalogProperties) {
        this.api = api;
        this.catalogProperties = catalogProperties;
    }

    public PrincipalWithCredentials createPrincipal(Principal principal) {
        CreatePrincipalRequest request = new CreatePrincipalRequest().principal(principal);
        return this.api.createPrincipal(request);
    }

    public Principal getPrincipal(String principalName) {
        return this.api.getPrincipal(principalName);
    }

    public boolean principalExists(String principalName) {
        try {
            getPrincipal(principalName);
            return true;
        } catch (ApiException apiException) {
            if (apiException.getCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
            throw apiException;
        }
    }

    public void removePrincipal(String principalName) {
        this.api.deletePrincipal(principalName);
    }

    public void assignPrincipalRole(String principalName, String principalRoleName) {
        GrantPrincipalRoleRequest request = new GrantPrincipalRoleRequest()
                .principalRole(new PrincipalRole().name(principalRoleName));
        this.api.assignPrincipalRole(principalName, request);
    }

    public void createPrincipalRole(PrincipalRole principalRole, boolean overwrite) {
        if (overwrite) {
            removePrincipalRole(principalRole.getName());
        }
        CreatePrincipalRoleRequest request = new CreatePrincipalRoleRequest().principalRole(principalRole);
        this.api.createPrincipalRole(request);
    }

    public List<PrincipalRole> listPrincipalRoles() {
        return this.api.listPrincipalRoles().getRoles();
    }

    public List<PrincipalRole> listAssigneePrincipalRolesForCatalogRole(String catalogName, String catalogRoleName) {
        return this.api.listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName).getRoles();
    }

    public void assignCatalogRoleToPrincipalRole(String principalRoleName, String catalogName, String catalogRoleName) {
        GrantCatalogRoleRequest request = new GrantCatalogRoleRequest()
                .catalogRole(new CatalogRole().name(catalogRoleName));
        this.api.assignCatalogRoleToPrincipalRole(principalRoleName, catalogName, request);
    }

    public void removeCatalogRoleFromPrincipalRole(String principalRoleName, String catalogName, String catalogRoleName) {
        this.api.revokeCatalogRoleFromPrincipalRole(principalRoleName, catalogName, catalogRoleName);
    }

    public PrincipalRole getPrincipalRole(String principalRoleName) {
        return this.api.getPrincipalRole(principalRoleName);
    }

    public boolean principalRoleExists(String principalRoleName) {
        try {
            getPrincipalRole(principalRoleName);
            return true;
        } catch (ApiException apiException) {
            if (apiException.getCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
            throw apiException;
        }
    }

    public void removePrincipalRole(String principalRoleName) {
        this.api.deletePrincipalRole(principalRoleName);
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

    public CatalogRole getCatalogRole(String catalogName, String catalogRoleName) {
        return this.api.getCatalogRole(catalogName, catalogRoleName);
    }

    public boolean catalogRoleExists(String catalogName, String catalogRoleName) {
        try {
            getCatalogRole(catalogName, catalogRoleName);
            return true;
        } catch (ApiException apiException) {
            if (apiException.getCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
            throw apiException;
        }
    }

    public void assignCatalogRole(String principalRoleName, String catalogName, String catalogRoleName) {
        GrantCatalogRoleRequest request = new GrantCatalogRoleRequest()
                .catalogRole(new CatalogRole().name(catalogRoleName));
        this.api.assignCatalogRoleToPrincipalRole(principalRoleName, catalogName, request);
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

    public org.apache.iceberg.catalog.Catalog initializeCatalog(String catalogName, PrincipalWithCredentials migratorPrincipal) {
        Map<String, String> currentCatalogProperties = new HashMap<>(catalogProperties);
        currentCatalogProperties.put("warehouse", catalogName);

        String clientId = migratorPrincipal.getCredentials().getClientId();
        String clientSecret = migratorPrincipal.getCredentials().getClientSecret();
        currentCatalogProperties.putIfAbsent("credential", String.format("%s:%s", clientId, clientSecret));
        currentCatalogProperties.putIfAbsent("scope", "PRINCIPAL_ROLE:ALL");

        return CatalogUtil.loadCatalog(
                RESTCatalog.class.getName(), "SOURCE_CATALOG_REST", currentCatalogProperties, null);
    }

}
