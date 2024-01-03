/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.migration;

import java.util.HashMap;
import java.util.Map;

import org.talend.sdk.component.api.component.MigrationHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOutputMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        log.debug("Starting JDBC sink component migration " + incomingVersion);

        if (incomingVersion == 1) {
            final String oldPropertyPathPrefix = "configuration.keys[";
            final String newPropertyPathPrefix = "configuration.keys.keys[";

            Map<String, String> correctConfig = new HashMap<>();
            incomingData.forEach((k, v) -> {
                if (k.startsWith(oldPropertyPathPrefix)) {
                    correctConfig.put(k.replace(oldPropertyPathPrefix, newPropertyPathPrefix), v);
                } else {
                    correctConfig.put(k, v);
                }
            });

            return correctConfig;
        }

        if (incomingVersion < 3) {
            incomingData.putIfAbsent("configuration.useOriginColumnName", "false");
        }

        final String useOriginColumnName = incomingData.remove("configuration.useOriginColumnName");
        if (useOriginColumnName != null && "false".equals(useOriginColumnName)) {
            incomingData.putIfAbsent("configuration.useSanitizedColumnName", "true");
        } else {
            incomingData.putIfAbsent("configuration.useSanitizedColumnName", "false");
        }
        return incomingData;
    }
}
