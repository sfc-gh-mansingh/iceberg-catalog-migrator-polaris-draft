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

package org.projectnessie.tools.polaris.migration.api.workspace;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SignatureService {

    private final Workspace previousWorkspace;

    private final MessageDigest messageDigest;

    public SignatureService(Workspace previousWorkspace) {
        this.previousWorkspace = previousWorkspace;
        try {
            this.messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private String hash(String unhashed) {
        byte[] hashBytes = messageDigest.digest(unhashed.getBytes(StandardCharsets.UTF_8));
        BigInteger noHash = new BigInteger(1, hashBytes);
        return noHash.toString(16);
    }

    public boolean hasChanged(EntityPath entityPath, Object entity) {
        if (previousWorkspace == null) return true;

        WorkspaceRecord workspaceRecord = this.previousWorkspace.get(entityPath);

        if (workspaceRecord == null)
            return true;

        if (workspaceRecord.status() != Status.CREATED
                && workspaceRecord.status() != Status.OVERWRITTEN
                && workspaceRecord.status() != Status.NOT_MODIFIED)
            return true;

        String newHash = hash(entity.toString());
        return !workspaceRecord.signature().equals(newHash);
    }

    public String createSignature(Object entity) {
        return hash(entity.toString());
    }

}
