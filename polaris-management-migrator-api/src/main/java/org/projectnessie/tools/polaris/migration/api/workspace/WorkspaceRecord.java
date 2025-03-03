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

public record WorkspaceRecord(
        EntityPath path,
        Status status,
        String reason,
        String signature
) {

    public WorkspaceRecord(EntityPath path, Status status, Throwable throwable) {
        this(path, status, throwable.getMessage(), "");
    }

    public WorkspaceRecord(EntityPath path, Status status, String signature) {
        this(path, status, "", signature);
    }

    public WorkspaceRecord(EntityPath path, Status status) {
        this(path, status, "", "");
    }

}
