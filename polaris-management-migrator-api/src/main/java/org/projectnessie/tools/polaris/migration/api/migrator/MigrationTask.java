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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MigrationTask<T> {

    protected Logger LOG = LoggerFactory.getLogger("console-log");

    protected ManagementEntityType entityType;

    protected MigrationContext context;

    protected MigrationTask(ManagementEntityType entityType, MigrationContext context) {
        this.entityType = entityType;
        this.context = context;
    }

    public abstract List<Class<? extends MigrationTask<?>>> dependsOn();

    public boolean dependsOn(Class<?> taskClass) {
        List<Class<? extends MigrationTask<?>>> dependencyClasses = this.dependsOn();

        for (Class<? extends MigrationTask<?>> dependencyClass : this.dependsOn()) {
            if (taskClass.isAssignableFrom(dependencyClass)) {
                return true;
            }
        }

        return false;
    }

    public boolean dependsOn(MigrationTask<?> task) {
        return this.dependsOn(task.getClass());
    }

    protected abstract List<T> getEntities();

    protected abstract void createEntity(T entity) throws Exception;

    protected ImmutableEntityMigrationResult.Builder prepareResultOnRetrievalFailure(Exception e) {
        return ImmutableEntityMigrationResult.builder();
    }

    protected abstract ImmutableEntityMigrationResult.Builder prepareResult(T entity, Exception e);

    public List<EntityMigrationResult> migrate() {
        LOG.info("Retrieving {}S from source.", entityType.name());

        List<T> entitiesOnSource;

        try {
            entitiesOnSource = getEntities();
        } catch (Exception e) {
            EntityMigrationResult result = prepareResultOnRetrievalFailure(e)
                    .entityType(entityType)
                    .entityName(this.getClass().getName())
                    .status(EntityMigrationResult.MigrationStatus.FAILED_RETRIEVAL)
                    .reason(e.toString())
                    .build();

            LOG.error("[{}] Retrieving {}S", result.status().name(), entityType.name());

            context.resultWriter().writeResult(result);

            return List.of(result);
        }

        LOG.info("Identified {} {}S from source.", entitiesOnSource.size(), entityType.name());

        if (!entitiesOnSource.isEmpty()) {
            LOG.info("Starting {} migration.", entityType.name());
        }

        AtomicInteger completedMigrations = new AtomicInteger();

        List<CompletableFuture<EntityMigrationResult>> futures = entitiesOnSource.stream()
                .map(entity -> CompletableFuture.supplyAsync(() -> {
                    EntityMigrationResult result;

                    try {
                        createEntity(entity);
                        result = prepareResult(entity, null)
                                .entityType(entityType)
                                .status(EntityMigrationResult.MigrationStatus.SUCCESS)
                                .reason("")
                                .build();
                        LOG.info("[{}] Migration of {} {} - {}/{}",
                                result.status().name(),
                                entityType.name(),
                                result.entityName(),
                                completedMigrations.incrementAndGet(),
                                entitiesOnSource.size()
                        );
                    } catch (Exception e) {
                        result = prepareResult(entity, e)
                                .entityType(entityType)
                                .status(EntityMigrationResult.MigrationStatus.FAILED_MIGRATION)
                                .reason(e.toString())
                                .build();
                        LOG.error("[{}] Migration of {} {} - {}/{}",
                                result.status().name(),
                                entityType.name(),
                                result.entityName(),
                                completedMigrations.incrementAndGet(),
                                entitiesOnSource.size()
                        );
                    }

                    context.resultWriter().writeResult(result);
                    return result;
                }, context.executor())).toList();


        return futures.stream()
                .map(CompletableFuture::join)
                .toList();
    }

}
