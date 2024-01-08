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

import java.net.URISyntaxException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.runner.OrderWith;
import org.junit.runner.manipulation.Ordering;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.azure.runtime.token.AzureActiveDirectoryTokenGetter;
import org.talend.components.azure.runtime.token.AzureManagedIdentitiesTokenGetter;
import org.talend.components.common.connection.azureblob.AzureAuthType;
import org.talend.components.common.connection.azureblob.AzureConnectionActiveDir;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionAccount;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionSignature;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;

import static org.talend.components.common.service.azureblob.AzureComponentServices.getTalendOperationContext;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AzureComponentServicesTest {

    @Mock
    private MessageService i18nService;

    @InjectMocks
    private AzureComponentServices sut = new AzureComponentServices();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Order(0)
    @Test
    void testGetUserAgent() {
        String appVersion = "appVersion1";
        String compVersion = "compVersion1";
        AzureComponentServices.setApplicationVersion(appVersion);
        AzureComponentServices.setComponentVersion(compVersion);

        OperationContext context = AzureComponentServices.getTalendOperationContext();
        String userAgentHeader = context.getUserHeaders().get("User-Agent");
        Assertions.assertTrue(userAgentHeader.contains(appVersion));
        Assertions.assertTrue(userAgentHeader.contains(compVersion));
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

    @Test
    void testGetStorageAccountThrowsExceptionWhenAccountNameIsNull() {
        AzureStorageConnectionAccount fakeConnectionObj = new AzureStorageConnectionAccount();
        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.createStorageAccount(fakeConnectionObj));
    }

    @Test
    void testGetStorageAccountForSharedKeyAuthWhenKeyIsNull() {
        AzureStorageConnectionAccount fakeConnectionObj = createFakeDatastore();
        fakeConnectionObj.setAccountKey(null);
        fakeConnectionObj.setAuthType(AzureAuthType.BASIC);

        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.createStorageAccount(fakeConnectionObj));
    }

    @Test
    void testGetStorageAccountForSharedKeyAuth() throws URISyntaxException {
        AzureStorageConnectionAccount fakeConnectionObj = createFakeDatastore();
        fakeConnectionObj.setAuthType(AzureAuthType.BASIC);

        CloudStorageAccount result = sut.createStorageAccount(fakeConnectionObj);
        Assertions.assertInstanceOf(StorageCredentialsAccountAndKey.class, result.getCredentials());
    }

    @Test
    void testGetStorageActiveDirMissingCreds() {
        AzureStorageConnectionAccount fakeConnectionObj = createFakeDatastore();
        fakeConnectionObj.setActiveDirProperties(null);
        fakeConnectionObj.setAuthType(AzureAuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL);

        Assertions.assertThrows(IllegalArgumentException.class, () -> sut.createStorageAccount(fakeConnectionObj));
    }

    @Test
    void testGetStorageActiveDir() throws URISyntaxException {
        AzureStorageConnectionAccount fakeConnectionObj = createFakeDatastore();
        fakeConnectionObj.setAuthType(AzureAuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL);
        try (MockedConstruction<AzureActiveDirectoryTokenGetter> mockedConstr =
                Mockito.mockConstruction(AzureActiveDirectoryTokenGetter.class,
                        (mock, context) -> {
                            Mockito.when(mock.retrieveAccessToken()).thenReturn("token1");
                        })) {
            CloudStorageAccount result = sut.createStorageAccount(fakeConnectionObj);
            Assertions.assertInstanceOf(StorageCredentialsToken.class, result.getCredentials());
        }

    }

    @Test
    void testCreateAccountSignatureNotProvided() throws URISyntaxException {
        AzureStorageConnectionSignature fakeSignatureConnection = new AzureStorageConnectionSignature();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sut.createStorageAccount(fakeSignatureConnection));
    }

    @Test
    void testCreateAccountSignatureIncorrect() {
        AzureStorageConnectionSignature fakeSignatureConnection = new AzureStorageConnectionSignature();
        fakeSignatureConnection.setAzureSharedAccessSignature("someIncorrectValue");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sut.createStorageAccount(fakeSignatureConnection));
    }

    @Test
    void testCreateAccountSignature() throws URISyntaxException {
        String fakeSignature = "https://name1.blob.microsoft.com/someValue";
        AzureStorageConnectionSignature fakeSignatureConnection = new AzureStorageConnectionSignature();
        fakeSignatureConnection.setAzureSharedAccessSignature(fakeSignature);

        CloudStorageAccount result = sut.createStorageAccount(fakeSignatureConnection);
        Assertions.assertInstanceOf(StorageCredentialsSharedAccessSignature.class, result.getCredentials());
    }

    @Test
    void testCreateAccountManagedIdentities() throws URISyntaxException {
        AzureStorageConnectionAccount fakeAccount = createFakeDatastore();
        fakeAccount.setAuthType(AzureAuthType.MANAGED_IDENTITIES);
        try (MockedConstruction<AzureManagedIdentitiesTokenGetter> mockedConstr =
                Mockito.mockConstruction(AzureManagedIdentitiesTokenGetter.class,
                        (mock, context) -> {
                            Mockito.when(mock.retrieveSystemAssignMItoken()).thenReturn("token1");
                        })) {
            CloudStorageAccount result = sut.createStorageAccount(fakeAccount);
            Assertions.assertInstanceOf(StorageCredentialsToken.class, result.getCredentials());
        }
    }

    private static AzureStorageConnectionAccount createFakeDatastore() {
        AzureConnectionActiveDir activeDirProps = new AzureConnectionActiveDir();
        activeDirProps.setTenantId("TenantId");
        activeDirProps.setClientId("ClientId");
        activeDirProps.setClientSecret("ClientSecret");
        AzureStorageConnectionAccount fakeConnectionObj = new AzureStorageConnectionAccount();
        fakeConnectionObj.setAccountName("Name1");
        fakeConnectionObj.setAccountKey("Key1");
        fakeConnectionObj.setAuthType(AzureAuthType.BASIC);
        fakeConnectionObj.setActiveDirProperties(activeDirProps);
        return fakeConnectionObj;
    }
}