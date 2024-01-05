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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JdbcMigrationHandlerTest {

    @Test
    void testOutputMigrate() {
        Map<String, String> incomingData = new HashMap<>();
        final JdbcOutputMigrationHandler jdbcOutputMigrationHandler = new JdbcOutputMigrationHandler();
        incomingData.put("configuration.keys[dummyData]", "dummyValue");
        final Map<String, String> migrate = jdbcOutputMigrationHandler.migrate(1, incomingData);
        Assertions.assertEquals("dummyValue", migrate.get("configuration.keys.keys[dummyData]"));
        incomingData = jdbcOutputMigrationHandler.migrate(2, incomingData);
        Assertions.assertEquals("true", incomingData.get("configuration.useSanitizedColumnName"));

    }

    @Test
    void testConnectionMigrate() {
        Map<String, String> incomingData = new HashMap<>();
        final JdbcConnectionMigrationHandler jdbcConnectionMigrationHandler = new JdbcConnectionMigrationHandler();
        incomingData = jdbcConnectionMigrationHandler.migrate(1, incomingData);
        Assertions.assertEquals("BASIC", incomingData.get("authenticationType"));
        Assertions.assertEquals("true", incomingData.get("setRawUrl"));

    }
}