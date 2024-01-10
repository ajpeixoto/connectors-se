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
package org.talend.components.azure.service;

import java.net.URISyntaxException;
import java.util.Collections;

import javax.json.JsonBuilderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionSignature;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

class AzureBlobComponentServicesTest {

    @Mock
    private MessageService i18nServiceMock;

    @Mock
    private AzureComponentServices connectionServiceMock;

    @InjectMocks
    private AzureBlobComponentServices sut = new AzureBlobComponentServices();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @ParameterizedTest
    @EnumSource(value = HealthCheckStatus.Status.class, names = { "OK", "KO" })
    void testConnectionOK(HealthCheckStatus.Status status) {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        Mockito.when(connectionServiceMock.testConnection(Mockito.any()))
                .thenReturn(new HealthCheckStatus(status, ""));

        HealthCheckStatus result = sut.testConnection(testAccount);
        Assertions.assertEquals(status, result.getStatus());
    }

    @Test
    void testConnectionKOWhenExceptionAccountName() throws URISyntaxException {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        Mockito.when(connectionServiceMock.createStorageAccount(
                Mockito.any(),
                Mockito.anyString(), Mockito.anyString())).thenThrow(URISyntaxException.class);

        HealthCheckStatus result = sut.testConnection(testAccount);
        Assertions.assertEquals(HealthCheckStatus.Status.KO, result.getStatus());
        Mockito.verify(i18nServiceMock).illegalContainerName();
    }

    @Test
    void testConnectionKOWhenOtherException() throws URISyntaxException {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        Mockito.when(connectionServiceMock.createStorageAccount(
                Mockito.any(),
                Mockito.anyString(), Mockito.anyString())).thenThrow(RuntimeException.class);

        HealthCheckStatus result = sut.testConnection(testAccount);
        Assertions.assertEquals(HealthCheckStatus.Status.KO, result.getStatus());
    }

    @Test
    void testCreateStorageAccount() throws URISyntaxException {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        testAccount.setUseAzureSharedSignature(false);
        sut.createStorageAccount(testAccount);

        Mockito.verify(connectionServiceMock)
                .createStorageAccount(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    void testCreateStorageAccountSAS() throws URISyntaxException {
        String signatureValue = "test";
        AzureCloudConnection testAccount = new AzureCloudConnection();
        testAccount.setUseAzureSharedSignature(true);
        AzureStorageConnectionSignature signature = new AzureStorageConnectionSignature();
        signature.setAzureSharedAccessSignature(signatureValue);
        testAccount.setSignatureConnection(signature);
        sut.createStorageAccount(testAccount);

        Mockito.verify(connectionServiceMock).createStorageAccount(signature);
    }

    @Test
    void testGetContainerName() {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        CloudBlobClient clientMock = Mockito.mock();
        CloudBlobContainer containerMock = Mockito.mock();
        Mockito.when(containerMock.getName()).thenReturn("test1");
        Mockito.when(clientMock.listContainers(null, null, null,
                AzureComponentServices.getTalendOperationContext()))
                .thenReturn(Collections.singleton(containerMock));
        Mockito.when(connectionServiceMock
                .createCloudBlobClient(Mockito.any(), Mockito.eq(AzureComponentServices.DEFAULT_RETRY_POLICY)))
                .thenReturn(clientMock);
        SuggestionValues result = sut.getContainerNames(testAccount);

        Assertions.assertEquals(1, result.getItems().size());
        Assertions.assertEquals("test1", result.getItems().iterator().next().getId());
    }

    @Test
    void testGetContainersException() {
        AzureCloudConnection testAccount = new AzureCloudConnection();
        Mockito.when(connectionServiceMock
                .createCloudBlobClient(Mockito.any(), Mockito.eq(AzureComponentServices.DEFAULT_RETRY_POLICY)))
                .thenThrow(IllegalArgumentException.class);

        Assertions.assertThrows(ComponentException.class, () -> sut.getContainerNames(testAccount));
    }
}