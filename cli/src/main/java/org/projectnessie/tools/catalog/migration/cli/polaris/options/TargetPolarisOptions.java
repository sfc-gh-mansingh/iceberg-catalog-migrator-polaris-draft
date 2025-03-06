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

import picocli.CommandLine;

import java.util.Map;

public class TargetPolarisOptions extends PolarisOptions {

    @Override
    public String getServiceName() {
        return "target";
    }

    @CommandLine.Option(
            names = "--target-" + BASE_URL,
            required = true,
            description = "The base url of the Polaris instance. Example: http://localhost:8181/polaris."
    )
    @Override
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @CommandLine.Option(
            names = "--target-" + OAUTH2_SERVER_URI,
            description = {
                    "(Note: required if access-token not provided) the oauth2-server-uri to authenticate against to " +
                            "obtain an access token for the Polaris instance."
            }
    )
    @Override
    public void setOauth2ServerUri(String oauth2ServerUri) {
        this.oauth2ServerUri = oauth2ServerUri;
    }

    @CommandLine.Option(
            names = "--target-" + CLIENT_ID,
            description = {
                    "(Note: required if access-token not provided) The client id for the principal the tool will assume" +
                            " to carry out the copy. This principal must have SERVICE_MANAGE_ACCESS level privileges."
            }
    )
    @Override
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @CommandLine.Option(
            names = "--target-" + CLIENT_SECRET,
            description = {
                    "(Note: required if access-token not provided) The client secret for the principal the tool will assume" +
                            " to carry out the copy. This principal must have SERVICE_MANAGE_ACCESS level privileges."
            }
    )
    @Override
    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    @CommandLine.Option(
            names = "--target-" + SCOPE,
            description = {
                    "(Note: required if access-token not provided) The scope that the principal the tool will assume" +
                            " to carry out the copy. This principal must have SERVICE_MANAGE_ACCESS level privileges."
            }
    )
    @Override
    public void setScope(String scope) {
        this.scope = scope;
    }

    @CommandLine.Option(
            names = "--target-" + ACCESS_TOKEN,
            description = "The access token to authenticate to the Polaris instance"
    )
    @Override
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    @CommandLine.Option(
            names = "--target-" + CATALOG_PROPERTIES,
            description = "The properties to use to connect to catalogs within the Polaris instance."
    )
    @Override
    public void setCatalogProperties(Map<String, String> properties) {
        this.catalogProperties = properties;
    }

}
