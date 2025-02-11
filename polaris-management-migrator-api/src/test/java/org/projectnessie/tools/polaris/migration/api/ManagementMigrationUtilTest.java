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

package org.projectnessie.tools.polaris.migration.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ManagementMigrationUtilTest {

    @Test
    public void invalidIfNoUriProvided() {
        Map<String, String> properties = Map.of(ManagementMigrationUtil.ACCESS_TOKEN, "blahblahblah");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ManagementMigrationUtil.validatePolarisInstanceProperties(properties);
        });
    }

    @Test
    public void doesNotFailIfAccessTokenProvided() {
        Map<String, String> properties = Map.of(
                ManagementMigrationUtil.ACCESS_TOKEN, "blahblahblah",
                ManagementMigrationUtil.URI, "https://localhost:8181/polaris"
        );

        Assertions.assertDoesNotThrow(() -> {
            ManagementMigrationUtil.validatePolarisInstanceProperties(properties);
        });
    }

    @Test
    public void failsIfAllAuthDetailsNotProvided() {
        Map<String, String> properties = Map.of(
                ManagementMigrationUtil.OAUTH2_SERVER_URI, "https://localhost:8181/oauth2/token",
                ManagementMigrationUtil.CLIENT_ID, "MY_CLIENT_ID",
                ManagementMigrationUtil.URI, "https://localhost:8181/polaris"
        );

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ManagementMigrationUtil.validatePolarisInstanceProperties(properties);
        });
    }

}
