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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;

public class OAuth2Util {

    private final static OkHttpClient httpClient = new OkHttpClient();

    private final static ObjectMapper objectMapper = new ObjectMapper();

    public static String fetchToken(
            String oauth2ServerUri,
            String clientId,
            String clientSecret,
            String scope
            ) throws IOException {
        RequestBody body = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("scope", scope)
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .build();

        Request tokenRequest = new Request.Builder()
                .url(oauth2ServerUri)
                .post(body)
                .build();

        try (Response response = httpClient.newCall(tokenRequest).execute()) {
            Map<String, Object> map = objectMapper.readValue(response.body().string(), Map.class);
            return map.get("access_token").toString();
        } catch (Exception e) {
            throw new IOException("Could not fetch access token.", e);
        }
    }

}
