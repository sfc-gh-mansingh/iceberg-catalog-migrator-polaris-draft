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
import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.polaris.core.admin.model.Principal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.CatalogsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.migrator.tasks.PrincipalsMigrationTask;
import org.projectnessie.tools.polaris.migration.api.result.EntityMigrationLog;
import org.projectnessie.tools.polaris.migration.api.result.MigrationLog;
import org.projectnessie.tools.polaris.migration.api.result.TaskStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MigrationTasksTest {

    private PolarisManagementDefaultApi mockSource;

    private PolarisManagementDefaultApi mockTarget;

    private MigrationContext createContext(Consumer<EntityMigrationLog> logCaptor) {
        MigrationLog migrationLog = Mockito.mock(MigrationLog.class);
        doAnswer(i -> {
            logCaptor.accept(i.getArgument(0));
            return null;
        }).when(migrationLog).append(any());

        return new MigrationContext(mockSource, mockTarget, migrationLog, Executors.newSingleThreadExecutor());
    }

    @BeforeEach
    public void setUp() {
        mockSource = Mockito.mock(PolarisManagementDefaultApi.class);
        mockTarget = Mockito.mock(PolarisManagementDefaultApi.class);
    }

    @Test
    public void testListFailed() {
        List<EntityMigrationLog> logs = new ArrayList<>();
        MigrationContext ctx = createContext(logs::add);

        MigrationTask<?> task = Mockito.spy(
                new CatalogsMigrationTask(ctx, false, false, false));

        doThrow(new RuntimeException("Reason for exception.")).when(task).listEntities();

        task.migrate();

        Assertions.assertTrue(logs.stream().anyMatch(l ->
                        l.status() == TaskStatus.LIST_FAILED && l.entityType() == ManagementEntityType.CATALOG
                ));
    }

    @Test
    public void testConflictFailure() throws Exception {
        List<EntityMigrationLog> logs = new ArrayList<>();
        MigrationContext ctx = createContext(logs::add);

        MigrationTask<?> task = Mockito.spy(
                new PrincipalsMigrationTask(ctx, false));

        Principal principal = new Principal()
                .name("test-principal")
                .clientId("test-client-id");

        doReturn(List.of(principal)).when(task).listEntities();
        doThrow(new ApiException(409, "Conflict")).when(task).createEntity(any());

        task.migrate();

        Assertions.assertTrue(logs.stream().anyMatch(l ->
                l.status() == TaskStatus.CONFLICT && l.entityType() == ManagementEntityType.PRINCIPAL
        ));
    }

    @Test
    public void testMigrationFailure() throws Exception {
        List<EntityMigrationLog> logs = new ArrayList<>();
        MigrationContext ctx = createContext(logs::add);

        MigrationTask<?> task = Mockito.spy(
                new PrincipalsMigrationTask(ctx, false));

        Principal principal = new Principal()
                .name("test-principal")
                .clientId("test-client-id");

        doReturn(List.of(principal)).when(task).listEntities();
        doThrow(new ApiException("Reason")).when(task).createEntity(any());

        task.migrate();

        Assertions.assertTrue(logs.stream().anyMatch(l ->
                l.status() == TaskStatus.COPY_FAILED && l.entityType() == ManagementEntityType.PRINCIPAL
        ));
    }

    @Test
    public void testSuccess() throws Exception {
        List<EntityMigrationLog> logs = new ArrayList<>();
        MigrationContext ctx = createContext(logs::add);

        MigrationTask<?> task = Mockito.spy(
                new PrincipalsMigrationTask(ctx, false));

        Principal principal = new Principal()
                .name("test-principal")
                .clientId("test-client-id");

        doReturn(List.of(principal)).when(task).listEntities();
        doNothing().when(task).createEntity(any());

        task.migrate();

        Assertions.assertTrue(logs.stream().anyMatch(l ->
                l.status() == TaskStatus.SUCCESS && l.entityType() == ManagementEntityType.PRINCIPAL
        ));
    }

}
