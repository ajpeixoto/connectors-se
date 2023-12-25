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
package org.talend.components.jira.source;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jira.base.JiraIntTestBase;
import org.talend.components.jira.base.JiraIntTestExtension;
import org.talend.components.jira.dataset.JiraDataset;
import org.talend.components.jira.dataset.ResourceType;
import org.talend.components.jira.datastore.JiraDatastore;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@WithComponents("org.talend.components.jira")
@ExtendWith({ JiraIntTestExtension.class })
class JiraInputIntTest extends JiraIntTestBase {

    @Injected
    protected BaseComponentsHandler componentsHandler;

    private JiraInputConfiguration inputConfiguration;

    @BeforeEach
    public void setUp() {
        JiraDatastore datastore = new JiraDatastore();
        datastore.setJiraURL(HOST + ":" + mappedPort);
        datastore.setUser(JIRA_USER);
        datastore.setPass(JIRA_PASS);

        JiraDataset dataset = new JiraDataset();
        dataset.setDatastore(datastore);

        inputConfiguration = new JiraInputConfiguration();
        inputConfiguration.setDataset(dataset);
    }

    @Test
    @Order(1)
    void testGetProjects() {
        int expectedAmountOfProjects = 3;

        inputConfiguration.getDataset().setResourceType(ResourceType.PROJECT);
        inputConfiguration.setProjectId(""); // get all available projects

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(expectedAmountOfProjects, records.size(), "Records amount is different");
        Assertions.assertTrue(records.stream().anyMatch(r -> r.getString("json").contains("Public Project 1")));
        Assertions.assertTrue(records.stream().anyMatch(r -> r.getString("json").contains("Public Project 2")));
        Assertions.assertTrue(records.stream().anyMatch(r -> r.getString("json").contains("Test Project")));
    }

    @Test
    @Order(2)
    void testGetProjectByKey() {
        int expectedAmountOfProjects = 1;

        inputConfiguration.getDataset().setResourceType(ResourceType.PROJECT);
        inputConfiguration.setProjectId("TP");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(expectedAmountOfProjects, records.size(), "Records amount is different");
        Assertions.assertTrue(records.stream().anyMatch(r -> r.getString("json").contains("Test Project")));
    }

    @Test
    @Order(3)
    void testGetOneIssueByKey() {
        int expectedAmountOfRecords = 1;
        final String issueKey = "TP-1";
        inputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        inputConfiguration.setUseJQL(false);
        inputConfiguration.setIssueId(issueKey);

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);
        Assertions.assertEquals(expectedAmountOfRecords, records.size(), "Records amount is different");
        Assertions.assertTrue(records.get(0).getString("json").contains(issueKey));
    }

    @Test
    @Order(4)
    void testIssueSearchWithEmptyJQL() {
        int expectedAmountOfRecords = 2;

        inputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        inputConfiguration.setUseJQL(true);
        inputConfiguration.setJql("");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);
        Assertions.assertEquals(expectedAmountOfRecords, records.size(), "Records amount is different");
        Assertions.assertTrue(records.get(0).getString("json").contains("TP-2"));
        Assertions.assertTrue(records.get(1).getString("json").contains("TP-1"));
    }

    @Test
    @Order(5)
    void testIssueSearchOneIssue() {
        String expectedIssueSummary = "Test issue 1";
        int expectedAmountOfRecords = 1;

        inputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        inputConfiguration.setUseJQL(true);
        inputConfiguration.setJql("summary ~ \"" + expectedIssueSummary + "\"");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        runPipeline(inputConfig);

        List<Record> records = componentsHandler.getCollectedData(Record.class);
        Assertions.assertEquals(expectedAmountOfRecords, records.size(), "Records amount is different");
        Assertions.assertTrue(records.get(0).getString("json").contains("TP-1"));
    }

    @Test
    @Order(6)
    void testFailureOnUnauthorized() {
        inputConfiguration.getDataset().getDatastore().setUser("notExistingUser");
        inputConfiguration.getDataset().getDatastore().setPass("somePass");

        inputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        inputConfiguration.setUseJQL(false);
        inputConfiguration.setIssueId("someFakeId");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        ComponentException componentException = Assertions.assertThrows(ComponentException.class,
                () -> runPipeline(inputConfig));

        componentException.printStackTrace();
        Assertions.assertTrue(componentException.getMessage().contains("401 Unauthorized"));
    }

    private static void runPipeline(final String inputConfig) {
        Job.components()
                .component("jiraInput", "JIRA://Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("jiraInput")
                .to("collector")
                .build()
                .run();
    }

    @Test
    @Order(7) // should be last otherwise user might be locked to enter captcha and other tests would fail
    void testFailureOnIncorrectPass403() {
        inputConfiguration.getDataset().getDatastore().setPass("fakeIncorrectPass");

        inputConfiguration.getDataset().setResourceType(ResourceType.ISSUE);
        inputConfiguration.setUseJQL(false);
        inputConfiguration.setIssueId("someFakeId");

        String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        ComponentException componentException = Assertions.assertThrows(ComponentException.class,
                () -> runPipeline(inputConfig));

        Assertions.assertTrue(componentException.getMessage().contains("Forbidden (403)"));
    }
}
