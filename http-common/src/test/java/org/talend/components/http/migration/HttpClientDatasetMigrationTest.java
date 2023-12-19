/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
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
package org.talend.components.http.migration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HttpClientDatasetMigrationTest {

    @Test
    public void testMigrateHeadersAddingQueryDestination() {
        Map<String, String> inputConfig = new HashMap<>();
        inputConfig.put("body.type", "TEXT");
        inputConfig.put("hasPathParams", "false");
        inputConfig.put("headers[0].key", "header1");
        inputConfig.put("headers[0].value", "value1");
        inputConfig.put("headers[1].key", "header2");
        inputConfig.put("headers[1].value", "valu2");
        inputConfig.put("headers[2].key", "header3");
        inputConfig.put("headers[2].value", "value3");
        inputConfig.put("maxRedirectOnSameURL", "3");
        inputConfig.put("returnedContent", "BODY_ONLY");
        inputConfig.put("resource", "get");
        inputConfig.put("hasQueryParams", "false");
        inputConfig.put("hasPagination", "false");
        inputConfig.put("datastore.receiveTimeout", "120000");
        inputConfig.put("datastore.authentication.type", "NoAuth");
        inputConfig.put("datastore.base", "https://httpbin.org");
        inputConfig.put("format", "RAW_TEXT");
        inputConfig.put("methodType", "GET");

        Map<String, String> migrated = new HashMap<>(inputConfig);

        HttpClientDatasetMigrationHandler migrationHandler = new HttpClientDatasetMigrationHandler();
        migrated = migrationHandler.migrate(1, migrated);

        Assertions.assertNotNull(migrated);
        for (Map.Entry<String, String> e : inputConfig.entrySet()) {
            Assertions.assertEquals(e.getValue(), migrated.get(e.getKey()));
            migrated.remove(e.getKey());
        }
        Assertions.assertEquals(3, migrated.size());

        for (int i = 0; i < 3; i++) {
            String key = String.format("headers[%s].query", i);
            Assertions.assertEquals("MAIN", migrated.get(key));
            migrated.remove(key);
        }
        Assertions.assertEquals(0, migrated.size());

    }

}
