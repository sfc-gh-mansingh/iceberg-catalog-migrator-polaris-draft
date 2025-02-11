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
import org.apache.commons.lang.time.StopWatch;
import org.projectnessie.tools.polaris.migration.api.ManagementMigrationUtil;
import org.projectnessie.tools.polaris.migration.api.migrator.ManagementMigrator;
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

@CommandLine.Command(
        name = "copy-polaris",
        mixinStandardHelpOptions = true,
        versionProvider = CLIVersionProvider.class,
        sortOptions = false,
        description = "Bulk copy Polaris management entities. Entities will not be removed from the source Polaris instance."
)
public class CopyPolarisCommand implements Callable<Integer> {

    private final Logger LOG = LoggerFactory.getLogger("console-log");

    @CommandLine.Option(
            names = "--source-polaris-properties",
            required = true,
            split = ",",
            scope = CommandLine.ScopeType.INHERIT,
            description = {
                    "Properties for connecting to source Polaris instance, expressed as a comma seperated list of key-value pairs.",
                    "Available Options:",
                    "\turi - the base level uri of the management api for the source Polaris instance.",
                    "\t\tExample: uri=http://localhost/polaris/api/management/v1",
                    "\taccess-token - the access token to authenticate to the source Polaris instance",
                    "\tclient_id - (Note: required if access-token not provided) the client id for the principal the tool will assume to carry out the copy",
                    "\tclient_secret - (Note: required if access-token not provided) the client secret for the principal the tool will assume to carry out the copy",
                    "\tscope - (Note: required if access-token not provided) the scope to authenticate with against the source Polaris instance",
                    "\toauth2-server-uri - (Note: required if access-token not provided) the oauth2-server-uri to authenticate against to obtain an access token for the source Polaris instance"
            }
    )
    private Map<String, String> sourcePolarisProperties;

    @CommandLine.Option(
            names = "--target-polaris-properties",
            required = true,
            split = ",",
            scope = CommandLine.ScopeType.INHERIT,
            description = {
                    "Properties for connecting to target Polaris instance, expressed as a comma seperated list of key-value pairs.",
                    "Available Options:",
                    "\turi - the base level uri of the management api for the target Polaris instance.",
                    "\t\tExample: uri=http://localhost/polaris/api/management/v1",
                    "\taccess-token - the access token to authenticate to the target Polaris instance",
                    "\tclient_id - (Note: required if access-token not provided) the client id for the principal the tool will assume to carry out the copy",
                    "\tclient_secret - (Note: required if access-token not provided) the client secret for the principal the tool will assume to carry out the copy",
                    "\tscope - (Note: required if access-token not provided) the scope to authenticate with against the target Polaris instance",
                    "\toauth2-server-uri - (Note: required if access-token not provided) the oauth2-server-uri to authenticate against to obtain an access token for the target Polaris instance"
            }
    )
    private Map<String, String> targetPolarisProperties;

    @CommandLine.Option(
            names = "--output-file",
            required = true,
            scope = CommandLine.ScopeType.INHERIT,
            description = "The output CSV file to write the copy result log to."
    )
    String outputFilePath;

    @CommandLine.Option(
            names = "--concurrency",
            defaultValue = "1",
            scope = CommandLine.ScopeType.INHERIT,
            description = "The number of threads to use to pool to schedule blocking I/O concurrently."
    )
    int numberOfThreads;

    private ManagementMigrator migrator() throws Exception {
        PolarisManagementDefaultApi sourceApi =
                ManagementMigrationUtil.buildPolarisManagementClient(sourcePolarisProperties);

        PolarisManagementDefaultApi targetApi =
                ManagementMigrationUtil.buildPolarisManagementClient(targetPolarisProperties);

        File outputDirectory = new File(outputFilePath);

        MigrationLog migrationLog = new MigrationLog(outputDirectory);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                migrationLog.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        return new ManagementMigrator(sourceApi, targetApi, migrationLog, numberOfThreads);
    }


    @Override
    public Integer call() throws Exception {
        doTimedRunWithStatistics(m -> {
            try {
                return m.migrateAll();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return 0;
    }

    @CommandLine.Command(
            name = "catalogs",
            description = "Copy catalogs and optionally catalog subentities like catalog roles, grants, " +
                    "and assignments of catalog roles to principal roles."
    )
    public Integer migrateCatalogs(
            @CommandLine.Option(names = { "--migrate-catalog-roles" }) boolean migrateCatalogRoles,
            @CommandLine.Option(names = { "--migrate-grants" }) boolean migrateGrants,
            @CommandLine.Option(names = { "--migrate-catalog-role-assignments" }) boolean migrateCatalogRoleAssignments
    ) throws Exception {
        doTimedRunWithStatistics(m -> {
            try {
                return m.migrateCatalogs(migrateCatalogRoles, migrateCatalogRoleAssignments, migrateGrants);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return 0;
    }

    @CommandLine.Command(
            name = "principals",
            description = "Copy principals and optionally principal assignments to principal roles."
    )
    public Integer migratePrincipals(
            @CommandLine.Option(names = { "--migrate-principal-role-assignments" }) boolean migratePrincipalRoleAssignments
    ) throws Exception {
        doTimedRunWithStatistics(m -> {
            try {
                return m.migratePrincipals(migratePrincipalRoleAssignments);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return 0;
    }

    @CommandLine.Command(
            name = "principal-roles",
            description = "Copy principal roles."
    )
    public Integer migratePrincipalRoles() throws Exception {
        doTimedRunWithStatistics(m -> {
            try {
                return m.migratePrincipalRoles();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return 0;
    }

    private void doTimedRunWithStatistics(Function<ManagementMigrator, MigrationLog> runMigrator) throws Exception {
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        ManagementMigrator migrator = migrator();
        MigrationLog log = runMigrator.apply(migrator);
        stopWatch.stop();

        LOG.info("Migration report:\n{}", log.generateReport());

        LOG.info("Migration took {}", stopWatch);
    }

}
