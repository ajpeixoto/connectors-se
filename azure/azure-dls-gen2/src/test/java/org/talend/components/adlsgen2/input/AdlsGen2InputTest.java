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
package org.talend.components.adlsgen2.input;

import javax.json.JsonBuilderFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.runtime.input.BlobReader;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

class AdlsGen2InputTest {

    private AdlsGen2Input sut;

    private InputConfiguration inputConfiguration;

    private MockedStatic<BlobReader.BlobFileReaderFactory> mockedStatic;

    @BeforeEach
    void setUp() {
        mockedStatic = Mockito.mockStatic(BlobReader.BlobFileReaderFactory.class);
        inputConfiguration = new InputConfiguration();
        AdlsGen2Service serviceMock = Mockito.mock();
        RecordBuilderFactory recordBuilderFactoryMock = Mockito.mock();
        JsonBuilderFactory jsonBuilderFactoryMock = Mockito.mock();
        sut = new AdlsGen2Input(inputConfiguration, serviceMock, recordBuilderFactoryMock, jsonBuilderFactoryMock);
    }

    @Test
    void testNormalFlow() {
        Record recordMock = Mockito.mock();
        BlobReader readerMock = Mockito.mock();
        Mockito.when(readerMock.readRecord()).thenReturn(recordMock);
        mockedStatic.when(() -> BlobReader.BlobFileReaderFactory.getReader(Mockito.eq(inputConfiguration),
                Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(readerMock);

        sut.init();
        Record result = sut.next();

        Mockito.verify(readerMock).readRecord();
        Assertions.assertEquals(recordMock, result);
    }

    @Test
    void testGetReaderThrowsException() {
        mockedStatic.when(() -> BlobReader.BlobFileReaderFactory.getReader(Mockito.eq(inputConfiguration),
                Mockito.any(), Mockito.any(), Mockito.any())).thenThrow(RuntimeException.class);

        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.init());
    }

    @AfterEach
    void close() {
        mockedStatic.close();
    }
}
