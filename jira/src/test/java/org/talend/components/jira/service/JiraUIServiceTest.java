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
package org.talend.components.jira.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.jira.dataset.JiraDataset;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.jira")
class JiraUIServiceTest {

    @Service
    private JiraUIService jiraUIService;

    @Test
    void testDiscoverSchemaReturnsStaticOneStringColumn() {
        JiraDataset fakeJiraDataset = new JiraDataset();

        Schema schema = jiraUIService.discoverSchema(fakeJiraDataset);

        Assertions.assertEquals(1, schema.getEntries().size());
        Schema.Entry column = schema.getEntries().get(0);
        Assertions.assertEquals(Schema.Type.STRING, column.getType());
        Assertions.assertEquals("json", column.getName());
    }
}