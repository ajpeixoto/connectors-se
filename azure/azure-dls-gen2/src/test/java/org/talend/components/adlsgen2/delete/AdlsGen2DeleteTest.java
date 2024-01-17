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
package org.talend.components.adlsgen2.delete;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsGen2Service;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

class AdlsGen2DeleteTest {

    @InjectMocks
    private AdlsGen2Delete sut;

    private AdlsGen2DeleteConfiguration configuration;

    private AdlsGen2Service serviceMock;

    private DataLakeDirectoryClient directoryClientMock;

    @Mock
    private AdlsGen2Connection mockedConnection;

    @BeforeEach
    void setUp() {
        configuration = new AdlsGen2DeleteConfiguration();
        configuration.setConnection(new AdlsGen2Connection());
        serviceMock = Mockito.mock();
        DataLakeServiceClient serviceClientMock = Mockito.mock();
        Mockito.when(serviceMock.getDataLakeConnectionClient(Mockito.any()))
                .thenReturn(serviceClientMock);
        directoryClientMock = Mockito.mock();
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.any())).thenReturn(Mockito.mock());
        Mockito.when(serviceClientMock.getFileSystemClient(Mockito.any()).getDirectoryClient(Mockito.any()))
                .thenReturn(directoryClientMock);
        sut = new AdlsGen2Delete(configuration, serviceMock);
    }

    @Test
    void testSimpleDelete() {
        sut.delete();

        Mockito.verify(directoryClientMock)
                .deleteWithResponse(configuration.isRecursive(),
                        null, Duration.of(configuration.getConnection().getTimeout().longValue(), SECONDS),
                        Context.NONE);
    }

    @Test
    void testSimpleDeleteWithInjectedConnection() throws Exception {
        try (AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
            sut.delete();

            Mockito.verify(directoryClientMock)
                    .deleteWithResponse(configuration.isRecursive(),
                            null, Duration.of(configuration.getConnection().getTimeout().longValue(), SECONDS),
                            Context.NONE);
        }
        ;
    }

    @Test
    void testDeleteThrowsAnException() {
        configuration.setDieOnError(true);
        Mockito.doThrow(RuntimeException.class)
                .when(directoryClientMock)
                .deleteWithResponse(configuration.isRecursive(),
                        null, Duration.of(configuration.getConnection().getTimeout().longValue(), SECONDS),
                        Context.NONE);

        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.delete());

        configuration.setDieOnError(false);
        Assertions.assertDoesNotThrow(() -> sut.delete());
    }

}