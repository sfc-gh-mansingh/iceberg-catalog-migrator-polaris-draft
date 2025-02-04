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

import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationResult;
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class MigrationTask<T> {

    protected Logger LOG = LoggerFactory.getLogger("console-log");

    protected ManagementEntityType entityType;

    protected MigrationContext context;

    protected MigrationTask(ManagementEntityType entityType, MigrationContext context) {
        this.entityType = entityType;
        this.context = context;
    }

    public abstract List<Class<? extends MigrationTask<?>>> dependsOn();

    protected abstract List<T> getEntities();

    protected abstract void createEntity(T entity) throws Exception;

    protected abstract ImmutableEntityMigrationResult.Builder prepareResult(T entity, Exception e);

    public List<EntityMigrationResult> migrate() {
        LOG.info("Retrieving {}s from source.", entityType.name());

        List<T> entitiesOnSource;

        try {
            entitiesOnSource = getEntities();
        } catch (Exception e) {
            EntityMigrationResult result = ImmutableEntityMigrationResult.builder()
                    .entityType(entityType)
                    .entityName(this.getClass().getName())
                    .status(EntityMigrationResult.MigrationStatus.FAILED_RETRIEVAL)
                    .reason(e.toString())
                    .build();

            LOG.debug("[{}] Migration of {} {}", result.status().name(), entityType.name(), result.entityName());

            context.resultWriter().writeResult(result);

            return List.of(result);
        }

        LOG.info("Identified {} {}s from source.", entitiesOnSource.size(), entityType.name());

        if (!entitiesOnSource.isEmpty()) {
            LOG.info("Starting {} migration.", entityType.name());
        }

        List<EntityMigrationResult> results = new ArrayList<>();

        int completedMigrations = 0;

        for (T entity : entitiesOnSource) {
            completedMigrations++;
            ImmutableEntityMigrationResult.Builder migrationResultBuilder;

            try {
                createEntity(entity);

                migrationResultBuilder = prepareResult(entity, null)
                        .entityType(entityType)
                        .status(EntityMigrationResult.MigrationStatus.SUCCESS)
                        .reason("");
            } catch (Exception e) {
                migrationResultBuilder = prepareResult(entity, e)
                        .entityType(entityType)
                        .status(EntityMigrationResult.MigrationStatus.FAILED_MIGRATION)
                        .reason(e.toString());
            }

            EntityMigrationResult result = migrationResultBuilder.build();

            LOG.info(
                    "[{}] Migration of {} {} - {}/{}",
                    result.status().name(),
                    entityType.name(),
                    result.entityName(),
                    completedMigrations,
                    entitiesOnSource.size()
            );

            context.resultWriter().writeResult(result);

            results.add(migrationResultBuilder.build());
        }

        return results;
    }

}
