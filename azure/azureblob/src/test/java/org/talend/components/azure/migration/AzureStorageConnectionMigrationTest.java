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
import org.talend.components.common.connection.azureblob.AzureAuthType;

class AzureStorageConnectionMigrationTest {

    @Test
    void testAddingBasicAuthAfterMigration() {
        AzureStorageConnectionMigration sut = new AzureStorageConnectionMigration();
        Map<String, String> result = sut.migrate(1, new HashMap<>());

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(AzureAuthType.BASIC.toString(), result.get("accountConnection.authType"));

    }

}