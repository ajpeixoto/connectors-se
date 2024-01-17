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
package org.talend.components.azure.runtime.input;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

class AvroBlobFileReaderTest extends BaseFileReaderTest {

    private AvroBlobFileReader sut;

    private MockedConstruction<DataFileStream> avroItemIteratorConstructorMock;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        initRecordBuilderFactoryMocks();
        initConfig();

        initComponentServicesMock();

        Schema testAvroSchema = SchemaBuilder
                .record("X")
                .fields()
                .nullableString("test", "")
                .endRecord();
        GenericRecord testResultRecord = new GenericData.Record(testAvroSchema);
        testResultRecord.put("test", "value");
        avroItemIteratorConstructorMock = Mockito.mockConstruction(DataFileStream.class,
                (mock, context) -> {
                    Mockito.when(mock.hasNext()).thenReturn(true, false);
                    Mockito.when(mock.next()).thenReturn(testResultRecord);
                });

        sut = new AvroBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
    }

    @Test
    void testAvroNoData() throws URISyntaxException, StorageException {
        Mockito.when(clientMock.getContainerReference(Mockito.any())
                .listBlobs(Mockito.any(), Mockito.anyBoolean(),
                        Mockito.nullable(EnumSet.class), Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
        Assertions.assertThrows(RuntimeException.class, () -> new AvroBlobFileReader(config, recordBuilderFactoryMock,
                componentServicesMock, messageServiceMock));
    }

    @Test
    void testAvroRead() {
        Record result = sut.readRecord();
        Assertions.assertEquals(1, result.getSchema().getEntries().size());

        Assertions.assertEquals("value", result.getString("test"));
    }

    @AfterEach
    void closeMocks() {
        avroItemIteratorConstructorMock.close();
    }
}