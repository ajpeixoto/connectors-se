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
package org.talend.components.jira.output;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.http.service.RecordBuilderService;
import org.talend.components.jira.base.JiraIntTestBase;
import org.talend.components.jira.base.JiraIntTestExtension;
import org.talend.components.jira.dataset.JiraDataset;
import org.talend.components.jira.dataset.ResourceType;
import org.talend.components.jira.datastore.JiraDatastore;
import org.talend.components.jira.source.JiraInputConfiguration;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith({ JiraIntTestExtension.class })
@WithComponents("org.talend.components.jira")
class JiraOutputIntTest extends JiraIntTestBase {

    @Injected
    protected BaseComponentsHandler componentsHandler;

    private JiraOutputConfiguration outputConfiguration;

    @BeforeEach
    public void setUp() {
        JiraDatastore datastore = new JiraDatastore();
        datastore.setJiraURL(HOST + ":" + mappedPort);
        datastore.setUser(JIRA_USER);
        datastore.setPass(JIRA_PASS);

        JiraDataset dataset = new JiraDataset();
        dataset.setDatastore(datastore);

        outputConfiguration = new JiraOutputConfiguration();
        outputConfiguration.setDataset(dataset);
    }

    @Test
    @Order(1)
    void testCreateProject() {
        outputConfiguration.getDataset().setResourceType(ResourceType.PROJECT);
        outputConfiguration.setOutputAction(OutputAction.CREATE);

        String createProjectJSONContent = JiraIntTestBase
                .loadResource("/projectCreateJson.json");
        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("json", createProjectJSONContent)
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setProjectId("INT");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runInputPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(1, records.size());
        Assertions.assertTrue(records.get(0).getString("json").contains("Integration test"));
    }

    @Test
    @Order(2)
    void testUpdateProject() {
        outputConfiguration.getDataset().setResourceType(ResourceType.PROJECT);
        outputConfiguration.setOutputAction(OutputAction.UPDATE);

        String createProjectJSONContent = JiraIntTestBase
                .loadResource("/projectUpdateJson.json");
        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "INT")
                .withString("json", createProjectJSONContent)
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setProjectId("INT");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runInputPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(1, records.size());
        Assertions.assertTrue(records.get(0).getString("json").contains("Integration test updated"));
    }

    @Test
    @Order(3)
    void testCreateIssue() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.CREATE);

        String createProjectJSONContent = JiraIntTestBase
                .loadResource("/issueCreateJson.json");
        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("json", createProjectJSONContent)
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setUseJQL(false);
        inputConfiguration.setIssueId("INT-1");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runInputPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(1, records.size());
        Assertions.assertTrue(records.get(0).getString("json").contains("Integration test task"));
    }

    @Test
    @Order(4)
    void testUpdateIssue() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.UPDATE);

        String createProjectJSONContent = JiraIntTestBase
                .loadResource("/issueUpdateJson.json");
        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "INT-1")
                .withString("json", createProjectJSONContent)
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setUseJQL(false);
        inputConfiguration.setIssueId("INT-1");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runInputPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(1, records.size());
        Assertions.assertTrue(records.get(0).getString("json").contains("Integration test task UPDATED"));
    }

    @Test
    @Order(5)
    void testDeleteIssueWithSubtaskWithoutCheckboxEnabledFailed() {
        // create subtask
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.CREATE);

        String createProjectJSONContent = JiraIntTestBase
                .loadResource("/subtaskCreateJson.json");
        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("json", createProjectJSONContent)
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.DELETE);
        outputConfiguration.setDeleteSubtasks(false);

        Record testRecordForDelete = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "INT-1")
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecordForDelete));

        String outputConfigForDelete = configurationByExample()
                .forInstance(outputConfiguration)
                .configured()
                .toQueryString();
        ComponentException exception =
                Assertions.assertThrows(ComponentException.class, () -> runOutputPipeline(outputConfigForDelete));

        Assertions.assertTrue(exception.getMessage()
                .contains("You must specify the 'deleteSubtasks' parameter to delete this issue and all its subtasks"),
                "Error message doesn't contain the deleteSubtasks parameter information");

    }

    @Test
    @Order(6)
    void testDeleteIssue() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.DELETE);
        outputConfiguration.setDeleteSubtasks(true);

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "INT-1")
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setIssueId("INT-1");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Assertions.assertThrows(ComponentException.class, () -> runInputPipeline(inputConfig));
    }

    @Test
    @Order(7)
    void testDeleteProject() {
        outputConfiguration.getDataset().setResourceType(ResourceType.PROJECT);
        outputConfiguration.setOutputAction(OutputAction.DELETE);

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "INT")
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        runOutputPipeline(outputConfig);

        JiraInputConfiguration inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(outputConfiguration.getDataset());
        inputConfiguration.setProjectId("INT");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Assertions.assertThrows(ComponentException.class, () -> runInputPipeline(inputConfig));
    }

    @Test
    @Order(8)
    void testRequestFailedIncorrectPassword() {
        outputConfiguration.getDataset().getDatastore().setPass("fakeIncorrectPass");

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("id", "whatever")
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.DELETE);

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        ComponentException thrownException =
                Assertions.assertThrows(ComponentException.class, () -> runOutputPipeline(outputConfig));

        Assertions.assertTrue(thrownException.getMessage().contains("401 Unauthorized"));
    }

    @Test
    @Order(9)
    void testInvalidSchemaForCreate() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.CREATE);

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("blahblah", "whatever") // no json column
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Assertions.assertThrows(ComponentException.class, () -> runOutputPipeline(outputConfig));
    }

    @Test
    @Order(10)
    void testInvalidSchemaForUpdate() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.UPDATE);

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("json", "whatever") // add json column but no id
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Assertions.assertThrows(ComponentException.class, () -> runOutputPipeline(outputConfig));
    }

    @Test
    @Order(11)
    void testInvalidSchemaForDelete() {
        outputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        outputConfiguration.setOutputAction(OutputAction.DELETE);

        Record testRecord = componentsHandler
                .findService(RecordBuilderService.class)
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("project", "whatever") // no id column
                .build();

        componentsHandler.setInputData(Collections.singletonList(testRecord));

        String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Assertions.assertThrows(ComponentException.class, () -> runOutputPipeline(outputConfig));
    }

    private static void runInputPipeline(final String inputConfig) {
        Job.components()
                .component("jiraInput", "JIRA://Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("jiraInput")
                .to("collector")
                .build()
                .run();
    }

    private static void runOutputPipeline(final String outputConfigForDelete) {
        Job.components()
                .component("inputFlow", "test://emitter")
                .component("outputComponent", "JIRA://Output?" + outputConfigForDelete)
                .connections()
                .from("inputFlow")
                .to("outputComponent")
                .build()
                .run();
    }
}