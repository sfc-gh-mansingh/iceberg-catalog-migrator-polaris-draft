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
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.ManagementMigrationUtil;
import org.projectnessie.tools.polaris.migration.api.migrator.ManagementMigrator;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ResultWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "migrate-polaris",
        mixinStandardHelpOptions = true,
        versionProvider = CLIVersionProvider.class,
        sortOptions = false,
        description = "Bulk migrate Polaris specific entities. Entities will not be removed from the source Polaris instance."
)
public class MigratePolarisCommand implements Callable<Integer> {

    private final Logger LOG = LoggerFactory.getLogger("console-log");

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

    @CommandLine.Option(
            names = "--concurrency",
            defaultValue = "1"
    )
    int numberOfThreads;

    private ManagementMigrator migrator() throws IOException {
        PolarisManagementDefaultApi sourceApi =
                ManagementMigrationUtil.buildPolarisManagementClient(sourcePolarisProperties);

        PolarisManagementDefaultApi targetApi =
                ManagementMigrationUtil.buildPolarisManagementClient(targetPolarisProperties);

        File file = new File(outputFilePath);

        ResultWriter resultWriter = new ResultWriter(file);

        return new ManagementMigrator(sourceApi, targetApi, resultWriter, numberOfThreads);
    }

    @Override
    public Integer call() throws Exception {
        ManagementMigrator migrator = migrator();
        printStatistics(migrator.migrateAll());

        return 0;
    }

    @CommandLine.Command(name = "catalogs")
    public Integer migrateCatalogs(
            @CommandLine.Option(names = { "--migrate-catalog-roles" }) boolean migrateCatalogRoles,
            @CommandLine.Option(names = { "--migrate-grants" }) boolean migrateGrants,
            @CommandLine.Option(names = { "--migrate-catalog-role-assignments" }) boolean migrateCatalogRoleAssignments
    ) throws Exception {
        ManagementMigrator migrator = migrator();
        printStatistics(migrator.migrateCatalogs(migrateCatalogRoles, migrateCatalogRoleAssignments, migrateGrants));
        return 0;
    }

    @CommandLine.Command(name = "principals")
    public Integer migratePrincipals(
            @CommandLine.Option(names = { "--migrate-principal-role-assignments" }) boolean migratePrincipalRoleAssignments
    ) throws Exception {
        ManagementMigrator migrator = migrator();
        printStatistics(migrator.migratePrincipals(migratePrincipalRoleAssignments));
        return 0;
    }

    @CommandLine.Command(name = "principal-roles")
    public Integer migratePrincipalRoles() throws Exception {
        ManagementMigrator migrator = migrator();
        printStatistics(migrator.migratePrincipalRoles());
        return 0;
    }

    private void printStatistics(List<EntityMigrationResult> results) {
        LOG.info("Statistics:");
        Map<ManagementEntityType, List<EntityMigrationResult>> resultsByType = new HashMap<>();

        for (EntityMigrationResult result : results) {
            resultsByType
                    .computeIfAbsent(result.entityType(), k -> new ArrayList<>())
                    .add(result);
        }

        for (ManagementEntityType type : resultsByType.keySet()) {
            List<EntityMigrationResult> resultsForType = resultsByType.get(type);

            LOG.info("Type: {}", type.name());
            LOG.info("\tTotal = {}", resultsForType.size());

            Map<String, List<EntityMigrationResult>> resultsByStatus = new TreeMap<>();

            for (EntityMigrationResult result : resultsForType) {
                resultsByStatus
                        .computeIfAbsent(result.status().toString(), k -> new ArrayList<>())
                        .add(result);
            }

            for (String status : resultsByStatus.keySet()) {
                LOG.info("\tResults with status {} = {}/{}", status, resultsByStatus.get(status).size(), resultsForType.size());
            }
        }
    }

}
