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

package org.projectnessie.tools.polaris.migration.api.result;

import org.immutables.value.Value;
import org.projectnessie.tools.polaris.migration.api.ManagementEntityType;

import java.util.Map;

@Value.Immutable
public interface EntityMigrationResult {

    enum MigrationStatus {
        SUCCESS,
        FAILED_RETRIEVAL,
        FAILED_MIGRATION
    }

    String entityName();

    MigrationStatus status();

    ManagementEntityType entityType();

    String reason();

    Map<String, String> properties();

    @Value.Default
    default void setProperty(String name, String value) {
        properties().put(name, value);
    }

    @Value.Default
    default String getProperty(String name) {
        return properties().get(name);
    }

}
