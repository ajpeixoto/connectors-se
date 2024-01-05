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
package org.talend.components.common.service.adls;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.common.connection.adls.AdlsGen2Connection;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.sdk.component.api.service.completion.SuggestionValues;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;
import com.azure.storage.file.datalake.models.PathItem;

class ADLSGen2SharedKeyCommonServiceTest {

    private ADLSGen2SharedKeyCommonService sut;

    private final String accountName = "someAccountName";

    private final String sharedKey = "someKey";

    private final String fakeFileSystemName = "fakeFS";

    private AdlsGen2Connection connection;

    @BeforeEach
    void setUp() {
        connection = new AdlsGen2Connection();
        connection.setAccountName(accountName);
        connection.setSharedKey(sharedKey);
        connection.setAuthMethod(AuthMethod.SharedKey);

        sut = new ADLSGen2SharedKeyCommonService();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void filesystemList(boolean credentials) {
        FileSystemItem item1 = new FileSystemItem();
        String item1Name = "item1";
        item1.setName(item1Name);
        FileSystemItem item2 = new FileSystemItem();
        String item2Name = "item2";
        item2.setName(item2Name);

        AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setAccountName(accountName);
        connection.setAuthMethod(AuthMethod.SharedKey);
        connection.setSharedKey(sharedKey);
        DataLakeServiceClient serviceClientMock = Mockito.mock();
        PagedIterable<FileSystemItem> pagedIterableMock = Mockito.mock();
        Stream<FileSystemItem> mockedServerResponse = Stream.of(item1, item2);
        Mockito.when(pagedIterableMock.stream()).thenReturn(mockedServerResponse);
        Mockito.when(serviceClientMock.listFileSystems()).thenReturn(pagedIterableMock);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (dataLakeServiceClientBuilder, context) -> {
                            Mockito.when(dataLakeServiceClientBuilder.buildClient()).thenReturn(serviceClientMock);
                            Mockito.when(dataLakeServiceClientBuilder.credential(
                                    Mockito.any(StorageSharedKeyCredential.class)))
                                    .thenReturn(dataLakeServiceClientBuilder);
                            Mockito.when(dataLakeServiceClientBuilder.endpoint(Mockito.anyString()))
                                    .thenReturn(dataLakeServiceClientBuilder);
                        })) {
            List<String> result = credentials ? sut.filesystemList(accountName, sharedKey)
                    .getItems()
                    .stream()
                    .map(SuggestionValues.Item::getLabel)
                    .collect(Collectors.toList()) : sut.filesystemList(connection);

            Assertions.assertEquals(2, result.size());
            Assertions.assertEquals(item1Name, result.get(0));
            Assertions.assertEquals(item2Name, result.get(1));
        }

    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testPathList(boolean useCredentials) throws URISyntaxException, InvalidKeyException {
        PathItem mockResult = Mockito.mock();
        String mockResultName = "name1";
        Mockito.when(mockResult.getName()).thenReturn(mockResultName);
        PagedIterable<PathItem> mockedResponse = Mockito.mock();
        Mockito.when(mockedResponse.stream()).thenReturn(Stream.of(mockResult));

        DataLakeServiceClient serviceClientMock = Mockito.mock();
        DataLakeFileSystemClient fsClientMock = Mockito.mock();
        DataLakeDirectoryClient directoryClientMock = Mockito.mock();
        Mockito.when(directoryClientMock.listPaths(Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.eq(null), Mockito.eq(null))).thenReturn(mockedResponse);
        Mockito.when(fsClientMock.getDirectoryClient(Mockito.anyString())).thenReturn(directoryClientMock);
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.anyString())).thenReturn(fsClientMock);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (dataLakeServiceClientBuilder, context) -> {
                            Mockito.when(dataLakeServiceClientBuilder.buildClient()).thenReturn(serviceClientMock);
                            Mockito.when(dataLakeServiceClientBuilder.credential(
                                    Mockito.any(StorageSharedKeyCredential.class)))
                                    .thenReturn(dataLakeServiceClientBuilder);
                            Mockito.when(dataLakeServiceClientBuilder.endpoint(Mockito.anyString()))
                                    .thenReturn(dataLakeServiceClientBuilder);
                        })) {

            List<String> result = useCredentials ? sut.pathList(accountName, accountName, fakeFileSystemName)
                    .getItems()
                    .stream()
                    .map(SuggestionValues.Item::getLabel)
                    .collect(Collectors.toList()) : sut.pathList(connection, fakeFileSystemName, false, "/", false);
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(mockResultName, result.get(0));
        }
    }

    @Test
    void testPathDeleteCalledOnlyWhenExist() {
        String fakePath = "/";
        sut.pathDelete(connection, fakeFileSystemName, fakePath);
        DataLakeServiceClient serviceClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.anyString())).thenReturn(Mockito.mock());
        DataLakeFileClient fileClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.anyString()).getFileClient(Mockito.anyString()))
                .thenReturn(fileClientMock);
        Mockito.when(fileClientMock.exists()).thenReturn(false, true);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (dataLakeServiceClientBuilder, context) -> {
                            Mockito.when(dataLakeServiceClientBuilder.buildClient()).thenReturn(serviceClientMock);
                            Mockito.when(dataLakeServiceClientBuilder.credential(
                                    Mockito.any(StorageSharedKeyCredential.class)))
                                    .thenReturn(dataLakeServiceClientBuilder);
                            Mockito.when(dataLakeServiceClientBuilder.endpoint(Mockito.anyString()))
                                    .thenReturn(dataLakeServiceClientBuilder);
                        })) {
            sut.pathDelete(connection, fakeFileSystemName, fakePath);
            Mockito.verify(fileClientMock).exists();
            Mockito.verifyNoMoreInteractions(fileClientMock);
            sut.pathDelete(connection, fakeFileSystemName, fakePath);
            Mockito.verify(fileClientMock).delete();
        }
    }

    @Test
    void testCreatePath() {
        String fakePath = "/file.txt";

        DataLakeServiceClient serviceClientMock = Mockito.mock();
        DataLakeFileSystemClient fileSystemClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient(fakeFileSystemName)).thenReturn(fileSystemClientMock);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (dataLakeServiceClientBuilder, context) -> {
                            Mockito.when(dataLakeServiceClientBuilder.buildClient()).thenReturn(serviceClientMock);
                            Mockito.when(dataLakeServiceClientBuilder.credential(
                                    Mockito.any(StorageSharedKeyCredential.class)))
                                    .thenReturn(dataLakeServiceClientBuilder);
                            Mockito.when(dataLakeServiceClientBuilder.endpoint(Mockito.anyString()))
                                    .thenReturn(dataLakeServiceClientBuilder);
                        })) {
            sut.createPath(connection, fakeFileSystemName, fakePath);
            Mockito.verify(fileSystemClientMock).createFile(fakePath);
        }
    }

    @Test
    void testLoadPathFromTempFile() {
        String fakePath = "/file.txt";
        Path tempFile = Mockito.mock();
        Mockito.when(tempFile.toString()).thenReturn("123");
        DataLakeServiceClient serviceClientMock = Mockito.mock();
        DataLakeFileSystemClient fileSystemClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient(fakeFileSystemName)).thenReturn(fileSystemClientMock);
        DataLakeFileClient fileClientMock = Mockito.mock();
        Mockito.when(fileSystemClientMock.getFileClient(fakePath)).thenReturn(fileClientMock);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (dataLakeServiceClientBuilder, context) -> {
                            Mockito.when(dataLakeServiceClientBuilder.buildClient()).thenReturn(serviceClientMock);
                            Mockito.when(dataLakeServiceClientBuilder.credential(
                                    Mockito.any(StorageSharedKeyCredential.class)))
                                    .thenReturn(dataLakeServiceClientBuilder);
                            Mockito.when(dataLakeServiceClientBuilder.endpoint(Mockito.anyString()))
                                    .thenReturn(dataLakeServiceClientBuilder);
                        })) {
            sut.loadPathFromTempFile(connection, fakeFileSystemName, fakePath, tempFile);
            Mockito.verify(fileClientMock).uploadFromFile("123", true);
        }
    }
}