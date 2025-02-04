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

import com.snowflake.polaris.management.ApiClient;
import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;

import org.apache.http.HttpHeaders;

import java.io.IOException;
import java.util.Map;

public final class ManagementMigrationUtil {

    public static PolarisManagementDefaultApi buildPolarisManagementClient(
            Map<String, String> properties
    ) throws IOException {
        final String managementUri = properties.get("uri");
        final String clientId = properties.get("client-id");
        final String clientSecret = properties.get("client-secret");
        final String scope = properties.get("scope");
        final String oauth2ServerUri = properties.get("oauth2-server-uri");

        String bearerToken = properties.getOrDefault("bearer", null);

        ApiClient client = new ApiClient();
        client.updateBaseUri(managementUri);

        if (bearerToken == null) {
            bearerToken = OAuth2Util.fetchToken(oauth2ServerUri, clientId, clientSecret, scope);
        }

        final String accessToken = bearerToken;

        client.setRequestInterceptor(requestBuilder -> {
            requestBuilder.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
        });

        return new PolarisManagementDefaultApi(client);
    }

}
