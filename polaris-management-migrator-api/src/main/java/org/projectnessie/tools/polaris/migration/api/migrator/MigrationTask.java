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

import com.snowflake.polaris.management.ApiException;
import org.apache.http.HttpStatus;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.result.ImmutableEntityMigrationLog;
import org.projectnessie.tools.polaris.migration.api.result.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic unit of migration task.
 * @param <T> The type of the entity to migrate
 */
public abstract class MigrationTask<T> {

    protected ManagementEntityType entityType;

    protected MigrationContext context;

    protected Logger LOG = LoggerFactory.getLogger("console-log");

    protected MigrationTask(ManagementEntityType entityType, MigrationContext context) {
        this.entityType = entityType;
        this.context = context;
    }

    /**
     * Get the list of types of dependencies that must complete migration before this task can execute.
     * @return the list of dependencies for this task type
     */
    public abstract List<Class<? extends MigrationTask<?>>> dependsOn();

    /**
     * List the entities to be migrated by this task from the source catalog.
     * @return the entities to be migrated by this task
     */
    protected abstract List<T> listEntities();

    /**
     * Create an individual entity determined in {@link MigrationTask#listEntities()} on the target catalog.
     * @param entity the entity to
     * @throws Exception
     */
    protected abstract void createEntity(T entity) throws Exception;

    /**
     * Create a description of the entity based on its type and associated information.
     * @param entity the entity to describe
     * @return the description of the entity
     */
    protected abstract String getDescription(T entity);

    /**
     * Get the properties of the task prior to any individual entity having been created on the catalog.
     * Typically, invoked when the {@link MigrationTask#listEntities()} call fails.
     * @return the properties associated with the task
     */
    protected abstract Map<String, String> properties();

    /**
     * Get the properties of task and the entity that had been or had been attempted to be created
     * on the target Polaris instance.
     * @param entity the entity to extract properties from
     * @return the properties associated with the task and/or entity
     */
    protected abstract Map<String, String> properties(T entity);

    private ImmutableEntityMigrationLog.Builder initLog() {
        return ImmutableEntityMigrationLog.builder()
                .entityType(entityType);
    }

    /**
     * Perform the migration associated with the task.
     */
    public void migrate() {
        List<T> entitiesOnSource;

        try {
            entitiesOnSource = listEntities();
        } catch (Exception e) {

            context.migrationLog().append(initLog()
                            .entityDescription("")
                            .status(TaskStatus.LIST_FAILED)
                            .putAllProperties(properties())
                            .reason(e.getMessage())
                            .build()
            );

            LOG.error("[{}] Failed to list {}s from source", TaskStatus.LIST_FAILED, entityType.name().toLowerCase());

            return;
        }

        LOG.info("Identified {} {}(s) from source", entitiesOnSource.size(), entityType.name().toLowerCase());

        // Will be modified by multiple completable future callbacks
        AtomicInteger completedMigrations = new AtomicInteger();

        List<CompletableFuture<Void>> futures = entitiesOnSource.stream()
                .map(entity -> CompletableFuture.runAsync(() -> {

                    try {
                        createEntity(entity);

                        context.migrationLog().append(initLog()
                                        .entityDescription(getDescription(entity))
                                        .status(TaskStatus.SUCCESS)
                                        .putAllProperties(properties(entity))
                                        .build()
                        );

                        LOG.info("[{}] Successfully migrated {} \"{}\" - {}/{}",
                                TaskStatus.SUCCESS,
                                entityType.name().toLowerCase(),
                                getDescription(entity),
                                completedMigrations.incrementAndGet(),
                                entitiesOnSource.size()
                        );

                    } catch (Exception e) {

                        TaskStatus status = TaskStatus.MIGRATION_FAILED;

                        if (e instanceof ApiException apiException && apiException.getCode() == HttpStatus.SC_CONFLICT) {
                            // Failures should be distinct from conflicts as they are far more common in the synchronization
                            // case and real failures should be more explicit on re-runs of the tool
                            status = TaskStatus.CONFLICT;
                        }

                        context.migrationLog().append(initLog()
                                        .entityDescription(getDescription(entity))
                                        .status(status)
                                        .putAllProperties(properties(entity))
                                        .reason(e.getMessage())
                                        .build()
                        );

                        LOG.error("[{}] Failed to migrate {} \"{}\" - {}/{}",
                                status,
                                entityType.name().toLowerCase(),
                                getDescription(entity),
                                completedMigrations.incrementAndGet(),
                                entitiesOnSource.size()
                        );

                    }

                }, context.executor())).toList();


        futures.forEach(CompletableFuture::join); // wait until all futures finish executing
    }

}
