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
package org.talend.components.azure.migration;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AzureStorageRuntimeDatasetMigrationTest {

    @Test
    void testMigrateEncoding() {
        AzureStorageRuntimeDatasetMigration sut = new AzureStorageRuntimeDatasetMigration();

        Map<String, String> configBefore = new HashMap<>();
        configBefore.put("configuration.dataset.csvOptions.encoding", "UFT8");
        configBefore.put("configuration.dataset.excelOptions.encoding", "UFT8");
        Map<String, String> result = sut.migrate(1, configBefore);

        Assertions.assertEquals(result.size(), configBefore.size());
        Assertions.assertEquals("UTF8", result.get("configuration.dataset.csvOptions.encoding"));
        Assertions.assertEquals("UTF8", result.get("configuration.dataset.excelOptions.encoding"));
    }
}