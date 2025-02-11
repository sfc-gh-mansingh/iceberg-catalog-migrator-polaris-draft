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

    protected static final String URI = "uri";

    protected static final String CLIENT_ID = "client_id";

    protected static final String CLIENT_SECRET = "client_secret";

    protected static final String SCOPE = "scope";

    protected static final String OAUTH2_SERVER_URI = "oauth2-server-uri";

    protected static final String ACCESS_TOKEN = "access-token";

    protected static void assertPresent(String key, Map<String, String> props, String errorMessage) {
        if (!props.containsKey(key)) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    protected static void validatePolarisInstanceProperties(Map<String, String> props) {
        assertPresent("uri", props, "Property uri is required but was not provided");

        String oauthErrorMessage = String.format(
                "Either the %s property must be provided, or all of %s, %s, %s, %s",
                ACCESS_TOKEN, OAUTH2_SERVER_URI, CLIENT_ID, CLIENT_SECRET, SCOPE
        );

        if (props.containsKey(ACCESS_TOKEN)) {
            return;
        }

        assertPresent(OAUTH2_SERVER_URI, props, oauthErrorMessage);
        assertPresent(CLIENT_ID, props, oauthErrorMessage);
        assertPresent(CLIENT_SECRET, props, oauthErrorMessage);
        assertPresent(SCOPE, props, oauthErrorMessage);
    }

    public static PolarisManagementDefaultApi buildPolarisManagementClient(
            Map<String, String> properties
    ) throws IOException {
        validatePolarisInstanceProperties(properties);

        final String managementUri = properties.get(URI);
        final String clientId = properties.get(CLIENT_ID);
        final String clientSecret = properties.get(CLIENT_SECRET);
        final String scope = properties.get(SCOPE);
        final String oauth2ServerUri = properties.get(OAUTH2_SERVER_URI);

        String bearerToken = properties.getOrDefault(ACCESS_TOKEN, null);

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
