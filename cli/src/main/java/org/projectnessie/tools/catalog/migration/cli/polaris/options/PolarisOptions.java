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

package org.projectnessie.tools.catalog.migration.cli.polaris.options;

import com.snowflake.polaris.management.ApiClient;
import com.snowflake.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.http.HttpHeaders;
import org.projectnessie.tools.polaris.migration.api.OAuth2Util;
import org.projectnessie.tools.polaris.migration.api.PolarisService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class PolarisOptions {

    protected static final String BASE_URL = "base-url";

    protected static final String CLIENT_ID = "client-id";

    protected static final String CLIENT_SECRET = "client-secret";

    protected static final String SCOPE = "scope";

    protected static final String OAUTH2_SERVER_URI = "oauth2-server-uri";

    protected static final String ACCESS_TOKEN = "access-token";

    protected static final String CATALOG_PROPERTIES = "catalog-properties";

    protected String baseUrl;

    protected String oauth2ServerUri;

    protected String clientId;

    protected String clientSecret;

    protected String scope;

    protected String accessToken;

    protected Map<String, String> catalogProperties;

    public abstract String getServiceName();

    public abstract void setBaseUrl(String baseUrl);

    public abstract void setOauth2ServerUri(String oauth2ServerUri);

    public abstract void setClientId(String clientId);

    public abstract void setClientSecret(String clientSecret);

    public abstract void setScope(String scope);

    public abstract void setAccessToken(String accessToken);

    public abstract void setCatalogProperties(Map<String, String> properties);

    protected void assertPresent(Object var, String errorMessage) {
        if (var == null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private void validatePolarisInstanceProperties() {
        String serviceName = getServiceName();

        assertPresent(baseUrl, String.format(
                "Option --%s-%s is required but was not provided", serviceName, BASE_URL));

        String oauthErrorMessage = String.format(
                "Either the --%s-%s property must be provided, or all of --%s-%s, --%s-%s, --%s-%s, --%s-%s",
                serviceName, ACCESS_TOKEN, serviceName, OAUTH2_SERVER_URI, serviceName, CLIENT_ID, serviceName, CLIENT_SECRET, serviceName, SCOPE
        );

        if (accessToken != null) {
            return;
        }

        assertPresent(oauth2ServerUri, oauthErrorMessage);
        assertPresent(clientId, oauthErrorMessage);
        assertPresent(clientSecret, oauthErrorMessage);
        assertPresent(scope, oauthErrorMessage);
    }

    public PolarisService buildService() throws IOException {
        validatePolarisInstanceProperties();

        String bearerToken = accessToken;

        ApiClient client = new ApiClient();
        client.updateBaseUri(baseUrl + "/api/management/v1");

        if (bearerToken == null) {
            bearerToken = OAuth2Util.fetchToken(oauth2ServerUri, clientId, clientSecret, scope);
        }

        final String token = bearerToken;

        client.setRequestInterceptor(requestBuilder -> {
            requestBuilder.header(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        });

        if (catalogProperties == null)
            catalogProperties = new HashMap<>();

        catalogProperties.putIfAbsent("uri", baseUrl + "/api/catalog");

        PolarisManagementDefaultApi polarisClient = new PolarisManagementDefaultApi(client);
        return new PolarisService(polarisClient, catalogProperties);
    }

}
