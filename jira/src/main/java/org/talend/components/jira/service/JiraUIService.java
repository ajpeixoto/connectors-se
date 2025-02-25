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
package org.talend.components.jira.service;

import org.talend.components.jira.dataset.JiraDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

@Service
public class JiraUIService {

    public static final String JIRA_DATASET_NAME = "Jira_dataset";

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @DiscoverSchema(JIRA_DATASET_NAME)
    public Schema discoverSchema(@Option final JiraDataset dataset) {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);

        schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder()
                .withName("json")
                .withType(Schema.Type.STRING)
                .build());
        return schemaBuilder.build();
    }
}
