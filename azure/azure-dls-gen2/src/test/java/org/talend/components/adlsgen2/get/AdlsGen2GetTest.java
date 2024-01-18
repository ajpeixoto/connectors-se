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
package org.talend.components.adlsgen2.get;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AdlsGen2GetTest {

    @InjectMocks
    private AdlsGen2Get sut;

    private AdlsGen2GetConfiguration configuration;

    private AdlsGen2Service serviceMock;

    @Mock
    private AdlsGen2Connection mockedConnection;

    private BlobInformations blobInfoMock;

    private MockedConstruction<File> fileMockedConstruction;

    private MockedConstruction<FileOutputStream> fileOutputStreamMockedConstruction;

    private MockedStatic<IOUtils> ioUtilsMockedStatic;

    @BeforeEach
    void setUp() throws IOException {
        serviceMock = Mockito.mock();

        fileOutputStreamMockedConstruction = Mockito.mockConstruction(FileOutputStream.class);

        ioUtilsMockedStatic = Mockito.mockStatic(IOUtils.class);

        configuration = new AdlsGen2GetConfiguration();
        configuration.setConnection(new AdlsGen2Connection());
        configuration.setFilesystem("someFileSystem");
        configuration.setBlobPath("someDir/someFile");
        configuration.setKeepRemoteDirStructure(false);
        configuration.setLocalFolder("localFolder");
        blobInfoMock = Mockito.mock();
        Mockito.when(blobInfoMock.getBlobPath()).thenReturn("someDir/someFile");
        Mockito.when(serviceMock.getBlobInputstream(Mockito.any(), Mockito.eq(configuration.getFilesystem()),
                Mockito.eq(blobInfoMock))).thenReturn(new ByteArrayInputStream(new byte[] { 1, 2, 3 }));

        List<BlobInformations> blobInformationsList = Collections.singletonList(blobInfoMock);
        Mockito.when(serviceMock.getBlobs(Mockito.any(), Mockito.eq(configuration.getFilesystem()),
                Mockito.eq(configuration.getBlobPath()), Mockito.nullable(FileFormat.class),
                Mockito.anyBoolean()))
                .thenReturn(blobInformationsList);

        sut = new AdlsGen2Get(configuration, serviceMock);

        fileMockedConstruction = Mockito.mockConstruction(File.class, (fileMock, context) -> {
            File parentFile = Mockito.mock();
            Mockito.when(fileMock.getParentFile()).thenReturn(parentFile);
            Mockito.when(parentFile.exists()).thenReturn(true);
        });
    }

    @Order(0)
    @Test
    void testSimpleGet() {
        sut.download();

        Mockito.verify(serviceMock)
                .getBlobs(configuration.getConnection(), configuration.getFilesystem(),
                        configuration.getBlobPath(), null, configuration.isIncludeSubDirectory());

    }

    @Order(1)
    @Test
    void testSimpleDeleteWithInjectedConnection() throws Exception {
        fileMockedConstruction.close();
        fileOutputStreamMockedConstruction.close();
        ioUtilsMockedStatic.close();
        try (AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
            fileMockedConstruction = Mockito.mockConstruction(File.class, (fileMock, context) -> {
                File parentFile = Mockito.mock();
                Mockito.when(fileMock.getParentFile()).thenReturn(parentFile);
                Mockito.when(parentFile.exists()).thenReturn(true);
            });
            fileOutputStreamMockedConstruction = Mockito.mockConstruction(FileOutputStream.class);

            ioUtilsMockedStatic = Mockito.mockStatic(IOUtils.class);

            sut.download();
            Mockito.verify(serviceMock)
                    .getBlobs(mockedConnection, configuration.getFilesystem(),
                            configuration.getBlobPath(), null, configuration.isIncludeSubDirectory());
        }
    }

    @Order(2)
    @Test
    void testDeleteThrowsAnException() throws IOException {
        configuration.setDieOnError(true);
        Mockito.doThrow(IOException.class)
                .when(serviceMock)
                .getBlobInputstream(configuration.getConnection(), configuration.getFilesystem(),
                        blobInfoMock);

        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.download());

        configuration.setDieOnError(false);
        Assertions.assertDoesNotThrow(() -> sut.download());
    }

    @AfterEach
    void closeMock() {
        fileMockedConstruction.close();
        fileOutputStreamMockedConstruction.close();
        ioUtilsMockedStatic.close();
    }
}
