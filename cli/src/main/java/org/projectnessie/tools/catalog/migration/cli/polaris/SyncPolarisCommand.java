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
package org.projectnessie.tools.catalog.migration.cli.polaris;

import java.util.concurrent.Callable;

import org.projectnessie.tools.catalog.migration.cli.CLIVersionProvider;
import org.projectnessie.tools.catalog.migration.cli.polaris.options.SourcePolarisOptions;
import org.projectnessie.tools.catalog.migration.cli.polaris.options.TargetPolarisOptions;
import org.projectnessie.tools.polaris.migration.api.PolarisService;
import org.projectnessie.tools.polaris.migration.api.PolarisSynchronizer;
import org.projectnessie.tools.polaris.migration.api.callbacks.SynchronizationEventListener;
import org.projectnessie.tools.polaris.migration.api.planning.SourceParitySynchronizationPlanner;
import org.projectnessie.tools.polaris.migration.api.planning.SynchronizationPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "sync-polaris",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    sortOptions = false,
    description =
        "Idempotent synchronization of one Polaris instance to another. Entities will not be removed from the source Polaris instance.")
public class SyncPolarisCommand implements Callable<Integer> {

    private final Logger consoleLog = LoggerFactory.getLogger("console-log");

    @CommandLine.ArgGroup(
            exclusive = false,
            multiplicity = "1",
            heading = "Source Polaris options: %n"
    )
    private SourcePolarisOptions sourcePolarisOptions;

    @CommandLine.ArgGroup(
            exclusive = false,
            multiplicity = "1",
            heading = "Target Polaris options: %n"
    )
    private TargetPolarisOptions targetPolarisOptions;

    @Override
    public Integer call() throws Exception {
        SynchronizationPlanner synchronizationPlanner = new SourceParitySynchronizationPlanner();
        SynchronizationEventListener syncEventListener = new SyncPolarisSynchronizationEventListener();

        PolarisService source = sourcePolarisOptions.buildService();
        PolarisService target = targetPolarisOptions.buildService();

        PolarisSynchronizer synchronizer = new PolarisSynchronizer(synchronizationPlanner, syncEventListener, source, target);

        synchronizer.syncPrincipalRoles();
        synchronizer.syncCatalogs();

        return 0;
    }

}
