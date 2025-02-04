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

package org.projectnessie.tools.catalog.migration.cli;

import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.projectnessie.tools.polaris.migration.api.ManagementMigrationUtil;
import org.projectnessie.tools.polaris.migration.api.migrator.ManagementMigrator;
import org.projectnessie.tools.polaris.migration.api.result.ResultWriter;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "migrate-polaris",
        mixinStandardHelpOptions = true,
        versionProvider = CLIVersionProvider.class,
        sortOptions = false,
        description = "Bulk migrate Polaris specific entities. Entities will not be removed from the source Polaris instance."
)
public class MigratePolarisCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = "--source-polaris-properties",
            required = true,
            split = ","
    )
    private Map<String, String> sourcePolarisProperties;

    @CommandLine.Option(
            names = "--target-polaris-properties",
            required = true,
            split = ","
    )
    private Map<String, String> targetPolarisProperties;

    @CommandLine.Option(
            names = "--output-file",
            required = true
    )
    String outputFilePath;

    private ManagementMigrator migrator() throws IOException {
        PolarisManagementDefaultApi sourceApi =
                ManagementMigrationUtil.buildPolarisManagementClient(sourcePolarisProperties);

        PolarisManagementDefaultApi targetApi =
                ManagementMigrationUtil.buildPolarisManagementClient(targetPolarisProperties);

        File file = new File(outputFilePath);

        ResultWriter resultWriter = new ResultWriter(file);

        return new ManagementMigrator(sourceApi, targetApi, resultWriter);
    }

    @Override
    public Integer call() throws Exception {
        PolarisManagementDefaultApi sourceApi =
                ManagementMigrationUtil.buildPolarisManagementClient(sourcePolarisProperties);

        PolarisManagementDefaultApi targetApi =
                ManagementMigrationUtil.buildPolarisManagementClient(targetPolarisProperties);

        File file = new File(outputFilePath);

        try (ResultWriter resultWriter = new ResultWriter(file)) {
            ManagementMigrator migrator = new ManagementMigrator(sourceApi, targetApi, resultWriter);
            migrator.migrateAll();
        }

        return 0;
    }

    @CommandLine.Command(name = "catalogs")
    public Integer migrateCatalogs(
            @CommandLine.Option(names = { "--migrate-catalog-roles" }) boolean migrateCatalogRoles,
            @CommandLine.Option(names = { "--migrate-grants" }) boolean migrateGrants,
            @CommandLine.Option(names = { "--migrate-catalog-role-assignments" }) boolean migrateCatalogRoleAssignments
    ) throws IOException {
        ManagementMigrator migrator = migrator();
        migrator.migrateCatalogs(migrateCatalogRoles, migrateCatalogRoleAssignments, migrateGrants);
        return 0;
    }

    @CommandLine.Command(name = "principals")
    public Integer migratePrincipals(
            @CommandLine.Option(names = { "--migrate-principal-role-assignments" }) boolean migratePrincipalRoleAssignments
    ) throws IOException {
        ManagementMigrator migrator = migrator();
        migrator.migratePrincipals(migratePrincipalRoleAssignments);
        return 0;
    }

    @CommandLine.Command(name = "principal-roles")
    public Integer migratePrincipals() throws IOException {
        ManagementMigrator migrator = migrator();
        migrator.migratePrincipalRoles();
        return 0;
    }

}
