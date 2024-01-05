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
package org.talend.components.jira.base;

import java.time.Duration;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class JiraIntTestExtension extends JiraIntTestBase
        implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!started) {
            started = true;
            setUpContainer();
            context.getRoot().getStore(GLOBAL).put("jira", this);
        }
    }

    public void setUpContainer() {
        String dockerImageName = "artifactory.datapwn.com/"
                + "tlnd-docker-prod/talend/common/components/atlassian-jira:7.1.7v2";

        httpbin = new GenericContainer<>(dockerImageName).withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/rest/api/2/search").withStartupTimeout(Duration.ofSeconds(90)));
        httpbin.start();

        mappedPort = httpbin.getMappedPort(8080);
    }

    @Override
    public void close() throws Throwable {
        if (httpbin != null && httpbin.isRunning()) {
            httpbin.stop();
        }
    }

}
