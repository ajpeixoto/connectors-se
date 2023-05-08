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
package org.talend.components.dynamicscrm.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.dynamicscrm.DynamicsCrmTestBase;
import org.talend.components.dynamicscrm.datastore.DynamicsCrmConnection;
import org.talend.components.dynamicscrm.datastore.AppType;
import org.talend.components.dynamicscrm.datastore.OAuthFlow;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status;
import org.talend.sdk.component.junit5.WithComponents;

import javax.naming.AuthenticationException;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@WithComponents("org.talend.components.dynamicscrm")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UIActionServiceTestIT extends DynamicsCrmTestBase {

    @Service
    private UIActionService service;

    @Test
    public void testValidateConnection() {
        DynamicsCrmConnection connection = createDataset().getDatastore();
        connection.setFlow(OAuthFlow.CLIENT_CREDENTIALS);
        HealthCheckStatus status = service.validateConnection(connection);
        System.out.println(status.getComment());
        assertEquals(Status.OK, status.getStatus());
    }

    @Test
    public void testEntitySetsList() throws AuthenticationException {
        DynamicsCrmConnection connection = createDataset().getDatastore();
        connection.setFlow(OAuthFlow.CLIENT_CREDENTIALS);
        final SuggestionValues suggestionValues = service.entitySetsList(connection);
        final String collect = suggestionValues.getItems()
                .stream()
                .map(e -> e.getId() + " : " + e.getLabel())
                .collect(Collectors.joining(" "));
        System.out.println(collect);
    }

    @Test
    public void testValidateConnectionFailed() {
        DynamicsCrmConnection connection = new DynamicsCrmConnection();
        connection.setServiceRootUrl(rootUrl);
        connection.setAuthorizationEndpoint(authEndpoint);
        connection.setClientSecret("wrongSecret");
        connection.setClientId("wrongClientId");
        connection.setUsername("wrongUsername");
        connection.setPassword("wrongPassword");
        connection.setAppType(AppType.NATIVE);
        HealthCheckStatus status = service.validateConnection(connection);
        assertEquals(Status.KO, status.getStatus());
    }

}
