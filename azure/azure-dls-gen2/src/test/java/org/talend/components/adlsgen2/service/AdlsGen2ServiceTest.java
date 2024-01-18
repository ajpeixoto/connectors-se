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
package org.talend.components.adlsgen2.service;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.common.connection.adls.AuthMethod;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdlsGen2ServiceTest {

    @Test
    void extractFileNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir/file*\"<a!b;c>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String fileName = service.extractFileName(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(linuxSupportedFileName.lastIndexOf("/")), fileName);
    }

    @Test
    void extractDirNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir\"*Dir;Name<>!/file*\"<abc>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String dirName = service.extractFolderPath(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(0, linuxSupportedFileName.lastIndexOf("/")), dirName);
    }

    @Test
    void testBlobExists() {
        Response<Boolean> responseMock = Mockito.mock();
        Mockito.when(responseMock.getValue()).thenReturn(true);
        DataLakeServiceClient serviceClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient("fakeFS")).thenReturn(Mockito.mock());
        Mockito.when(serviceClientMock.getFileSystemClient("fakeFS")
                .getDirectoryClient(Mockito.any())).thenReturn(Mockito.mock());
        Mockito.when(serviceClientMock.getFileSystemClient("fakeFS")
                .getDirectoryClient(Mockito.any())
                .getFileClient("fakePath"))
                .thenReturn(Mockito.mock());
        Mockito.when(serviceClientMock.getFileSystemClient("fakeFS")
                .getDirectoryClient(Mockito.any())
                .getFileClient("fakePath")
                .existsWithResponse(Mockito.any(), Mockito.any()))
                .thenReturn(responseMock);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (mock, context) -> {
                            Mockito.when(mock.credential(
                                    Mockito.any(StorageSharedKeyCredential.class))).thenReturn(mock);

                            Mockito.when(mock.endpoint(Mockito.any())).thenReturn(mock);
                            Mockito.when(mock.configuration(Mockito.any())).thenReturn(mock);
                            Mockito.when(mock.buildClient()).thenReturn(serviceClientMock);
                        })) {
            String fakeBlobName = "fakePath";
            AdlsGen2Service sut = new AdlsGen2Service();
            AdlsGen2DataSet fakeDataset = new AdlsGen2DataSet();
            fakeDataset.setConnection(new AdlsGen2Connection());
            fakeDataset.getConnection().setAuthMethod(AuthMethod.SharedKey);
            fakeDataset.getConnection().setAccountName("fakeName");
            fakeDataset.getConnection().setSharedKey("fakeKey");
            fakeDataset.setFilesystem("fakeFS");
            fakeDataset.setBlobPath(fakeBlobName);

            Assertions.assertTrue(sut.blobExists(fakeDataset, fakeBlobName));
        }
    }

    @Test
    void testGetBlobs() {
        DataLakeServiceClient serviceClientMock = Mockito.mock("ServiceMock");
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.any())).thenReturn(Mockito.mock());
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.any())
                .getDirectoryClient(Mockito.any())).thenReturn(Mockito.mock());
        PathItem blobMock = Mockito.mock();
        Mockito.when(blobMock.getName()).thenReturn("fakeName");
        Mockito.when(blobMock.getLastModified()).thenReturn(OffsetDateTime.now());
        PagedIterable<PathItem> pathItems = Mockito.mock();
        Mockito.when(pathItems.stream()).thenReturn(Stream.of(blobMock));
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.any())
                .getDirectoryClient(Mockito.any())
                .listPaths(Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.nullable(Integer.class),
                        Mockito.any()))
                .thenReturn(pathItems);
        try (MockedConstruction<DataLakeServiceClientBuilder> builderMock =
                Mockito.mockConstruction(DataLakeServiceClientBuilder.class,
                        (mock, context) -> {
                            Mockito.when(mock.credential(
                                    Mockito.any(StorageSharedKeyCredential.class))).thenReturn(mock);

                            Mockito.when(mock.endpoint(Mockito.any())).thenReturn(mock);
                            Mockito.when(mock.configuration(Mockito.any())).thenReturn(mock);
                            Mockito.when(mock.buildClient()).thenReturn(serviceClientMock);
                        })) {
            AdlsGen2Service sut = new AdlsGen2Service();
            AdlsGen2DataSet fakeDataset = new AdlsGen2DataSet();
            fakeDataset.setConnection(new AdlsGen2Connection());
            fakeDataset.getConnection().setAuthMethod(AuthMethod.SharedKey);
            fakeDataset.getConnection().setAccountName("fakeName");
            fakeDataset.getConnection().setSharedKey("fakeKey");
            fakeDataset.setFilesystem("fakeFS");
            fakeDataset.setBlobPath("fakeName");
            List<BlobInformations> result = sut.getBlobs(fakeDataset);

            Assertions.assertEquals(1, result.size());
            BlobInformations blobInformations = result.get(0);
            Assertions.assertEquals("fakeName", blobInformations.getName());
        }
    }
}
