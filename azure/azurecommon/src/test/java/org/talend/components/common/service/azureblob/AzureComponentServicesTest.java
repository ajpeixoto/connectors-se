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
package org.talend.components.common.service.azureblob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;

import static org.talend.components.common.service.azureblob.AzureComponentServices.getTalendOperationContext;

class AzureComponentServicesTest {

    @Mock
    private MessageService i18nService;

    @InjectMocks
    private AzureComponentServices sut = new AzureComponentServices();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConnectionFailingWhenConnectionNull() {
        CloudStorageAccount storageAccountMock = null;
        Assertions.assertEquals(HealthCheckStatus.Status.KO, sut.testConnection(storageAccountMock).getStatus());
    }

    @Test
    void testConnectionFailing() throws StorageException {
        CloudStorageAccount storageAccountMock = Mockito.mock();
        CloudBlobClient clientMock = Mockito.mock();
        Mockito.when(
                clientMock.listContainersSegmented(null, null, 1, null,
                        null, getTalendOperationContext()))
                .thenThrow(StorageException.class);
        Mockito.when(storageAccountMock.createCloudBlobClient()).thenReturn(clientMock);
        HealthCheckStatus result = sut.testConnection(storageAccountMock);

        Assertions.assertEquals(HealthCheckStatus.Status.KO, result.getStatus());
    }

    @Test
    void testConnectionOK() {
        CloudStorageAccount storageAccountMock = Mockito.mock();
        CloudBlobClient clientMock = Mockito.mock();
        Mockito.when(clientMock.getDefaultRequestOptions()).thenReturn(new BlobRequestOptions());

        Mockito.when(storageAccountMock.createCloudBlobClient()).thenReturn(clientMock);
        HealthCheckStatus result = sut.testConnection(storageAccountMock);

        Assertions.assertEquals(HealthCheckStatus.Status.OK, result.getStatus());
    }
}