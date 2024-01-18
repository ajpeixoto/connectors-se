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
package org.talend.components.adlsgen2.output;

import javax.json.JsonBuilderFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.runtime.output.BlobWriter;
import org.talend.components.adlsgen2.runtime.output.BlobWriterFactory;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;

class AdlsGen2OutputTest {

    private AdlsGen2Output sut;

    private OutputConfiguration outputConfiguration;

    private MockedStatic<BlobWriterFactory> mockedStatic;

    private RecordBuilderFactory recordBuilderFactoryMock;

    @BeforeEach
    void setUp() {
        mockedStatic = Mockito.mockStatic(BlobWriterFactory.class);
        outputConfiguration = new OutputConfiguration();
        outputConfiguration.setDataSet(new AdlsGen2DataSet());
        AdlsGen2Service serviceMock = Mockito.mock();
        recordBuilderFactoryMock = Mockito.mock();
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder()).thenReturn(new RecordImpl.BuilderImpl());
        JsonBuilderFactory jsonBuilderFactoryMock = Mockito.mock();
        sut = new AdlsGen2Output(outputConfiguration, serviceMock, recordBuilderFactoryMock, jsonBuilderFactoryMock);
    }

    @Test
    void testNormalFlow() throws Exception {
        Record testRecord = recordBuilderFactoryMock.newRecordBuilder().withString("key", "value").build();
        BlobWriter writerMock = Mockito.mock();
        mockedStatic.when(() -> BlobWriterFactory.getWriter(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(writerMock);

        sut.init();
        sut.beforeGroup();
        sut.onNext(testRecord);
        sut.afterGroup();
        sut.release();

        Mockito.verify(writerMock).writeRecord(testRecord);
        Mockito.verify(writerMock).complete();
    }

    @Test
    void testOnNextSkipEmptyRecords() {
        Record emptyRecord = recordBuilderFactoryMock.newRecordBuilder().build();
        Record testRecord = recordBuilderFactoryMock.newRecordBuilder().withString("key", "value2").build();
        BlobWriter writerMock = Mockito.mock();
        mockedStatic.when(() -> BlobWriterFactory.getWriter(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(writerMock);

        sut.init();
        sut.beforeGroup();
        sut.onNext(testRecord);
        sut.onNext(emptyRecord);
        sut.afterGroup();
        sut.release();

        Mockito.verify(writerMock, Mockito.times(1)).writeRecord(Mockito.any());
    }

    @Test
    void testGetReaderThrowsExceptionOnInit() {
        mockedStatic.when(() -> BlobWriterFactory.getWriter(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any())).thenThrow(RuntimeException.class);

        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.init());
    }

    @Test
    void testGetReaderThrowsExceptionOnFlush() throws Exception {
        Record testRecord = recordBuilderFactoryMock.newRecordBuilder().withString("key", "value").build();
        BlobWriter writerMock = Mockito.mock();
        mockedStatic.when(() -> BlobWriterFactory.getWriter(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(writerMock);

        Mockito.doThrow(RuntimeException.class).when(writerMock).flush();

        sut.init();
        sut.beforeGroup();
        sut.onNext(testRecord);
        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.afterGroup());
    }

    @Test
    void testGetReaderThrowsExceptionOnClose() throws Exception {
        Record testRecord = recordBuilderFactoryMock.newRecordBuilder().withString("key", "value").build();
        BlobWriter writerMock = Mockito.mock();
        mockedStatic.when(() -> BlobWriterFactory.getWriter(Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(writerMock);

        Mockito.doThrow(RuntimeException.class).when(writerMock).complete();

        sut.init();
        sut.beforeGroup();
        sut.onNext(testRecord);
        sut.afterGroup();
        Assertions.assertThrows(AdlsGen2RuntimeException.class, () -> sut.release());
    }

    @AfterEach
    void close() {
        mockedStatic.close();
    }

}
