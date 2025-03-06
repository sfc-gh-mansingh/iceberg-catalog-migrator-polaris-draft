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

package org.projectnessie.tools.catalog.migration.cli.polaris.workspace;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.projectnessie.tools.polaris.migration.api.EntityType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record EntityPath(String... parts) {

    public EntityPath(String path) {
        this(path.split("/"));
    }

    public EntityPath {
        if (parts.length == 0) throw new IllegalArgumentException("Cannot create an empty entity path.");
    }

    public static EntityPath catalogs() {
        return new EntityPath(EntityType.CATALOG.name());
    }

    public static EntityPath catalog(String catalogName) {
        return new EntityPath(EntityType.CATALOG.name(), catalogName);
    }

    public static EntityPath catalogRoles(String catalogName) {
        return new EntityPath(EntityType.CATALOG.name(), catalogName, EntityType.CATALOG_ROLE.name());
    }

    public static EntityPath catalogRole(String catalogName, String catalogRoleName) {
        return new EntityPath(EntityType.CATALOG.name(), catalogName, EntityType.CATALOG_ROLE.name(), catalogRoleName);
    }

    public static EntityPath grants(String catalogName, String catalogRoleName) {
        return new EntityPath(EntityType.CATALOG.name(), catalogName, EntityType.CATALOG_ROLE.name(), catalogRoleName, EntityType.GRANT.name());
    }

    public static EntityPath grant(String catalogName, String catalogRoleName, GrantResource grant) {
        List<String> parts = new ArrayList<>(
                List.of(EntityType.CATALOG.name(), catalogName, EntityType.CATALOG_ROLE.name(), catalogRoleName)
        );

        if (grant instanceof CatalogGrant c) {
            parts.add(EntityType.CATALOG_GRANT.name());
            parts.add(c.getPrivilege().getValue());
        } else if (grant instanceof NamespaceGrant ns) {
            parts.add(EntityType.NAMESPACE_GRANT.name());
            parts.addAll(ns.getNamespace());
            parts.add(ns.getPrivilege().getValue());
        } else if (grant instanceof  TableGrant t) {
            parts.add(EntityType.TABLE_GRANT.name());
            parts.addAll(t.getNamespace());
            parts.add(t.getTableName());
            parts.add(t.getPrivilege().getValue());
        } else if (grant instanceof ViewGrant v) {
            parts.add(EntityType.VIEW_GRANT.name());
            parts.addAll(v.getNamespace());
            parts.add(v.getViewName());
            parts.add(v.getPrivilege().getValue());
        }

        return new EntityPath(parts.toArray(new String[0]));
    }

    public static EntityPath tables(String catalogName) {
        return new EntityPath(EntityType.CATALOG.name(), catalogName, EntityType.TABLE.name());
    }

    public static EntityPath table(String catalogName, TableIdentifier tableIdentifier) {
        List<String> parts = new ArrayList<>(List.of(EntityType.CATALOG.name(), catalogName, EntityType.NAMESPACE.name()));
        parts.addAll(Arrays.stream(tableIdentifier.namespace().levels()).toList());
        parts.addAll(List.of(EntityType.TABLE.name(), tableIdentifier.name()));
        return new EntityPath(parts.toArray(new String[0]));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < parts.length - 1; i++) {
            builder.append(parts[i]);
            builder.append('/');
        }

        builder.append(parts[parts.length - 1]);

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EntityPath that && Arrays.equals(parts, that.parts);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(parts);
    }

}
