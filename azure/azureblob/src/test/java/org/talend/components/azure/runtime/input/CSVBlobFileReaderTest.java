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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.formats.Encoding;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

class CSVBlobFileReaderTest extends BaseFileReaderTest {

    private CSVBlobFileReader sut;

    private Iterator<CSVRecord> fileRecordIteratorMock;

    private MockedConstruction<CSVParser> mockedConstruction;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException, IOException {
        messageServiceMock = Mockito.mock();
        initRecordBuilderFactoryMocks();
        initConfig();
        CSVFormatOptions csvFormatOptions = new CSVFormatOptions();
        csvFormatOptions.setEncoding(Encoding.UTF8);
        config.setCsvOptions(csvFormatOptions);
        initComponentServicesMock();

        fileRecordIteratorMock = Mockito.mock();
        // hasNext called one more time when file header parsed, so return true,true,false
        Mockito.when(fileRecordIteratorMock.hasNext()).thenReturn(true, true, false);
        CSVRecord csvRecord = CSVParser.parse("key,value", CSVFormat.DEFAULT).getRecords().get(0);
        Mockito.when(fileRecordIteratorMock.next()).thenReturn(csvRecord);
        initCSVParserMockConstruction();
        sut = new CSVBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
    }

    private void initCSVParserMockConstruction() {
        mockedConstruction = Mockito.mockConstruction(CSVParser.class,
                (mock, context) -> {
                    Mockito.when(mock.iterator()).thenReturn(fileRecordIteratorMock);
                });
    }

    @Test
    void testCSVNoData() throws URISyntaxException, StorageException {
        Mockito.when(clientMock.getContainerReference(Mockito.any())
                .listBlobs(Mockito.any(), Mockito.anyBoolean(),
                        Mockito.nullable(EnumSet.class),
                        Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
        Assertions.assertThrows(RuntimeException.class,
                () -> new CSVBlobFileReader(config,
                        recordBuilderFactoryMock, componentServicesMock, messageServiceMock));
    }

    @Test
    void testCSVReadSimpleRecord() {
        Record record = sut.readRecord();
        Assertions.assertEquals(2, record.getSchema().getEntries().size());
        Assertions.assertEquals("key", record.getString("field0"));
        Assertions.assertEquals("value", record.getString("field1"));
    }

    @Test
    void testCSVWithBigHeader() throws IOException, URISyntaxException, StorageException {
        config.getCsvOptions().setUseHeader(true);
        config.getCsvOptions().setHeader(2);

        mockedConstruction.close();
        Mockito.when(fileRecordIteratorMock.hasNext()).thenReturn(true, true, true, true, false);
        List<CSVRecord> csvRecords = CSVParser.parse("key1,key2\r\nvalue1,value2", CSVFormat.DEFAULT)
                .getRecords();
        Mockito.when(fileRecordIteratorMock.next()).thenReturn(null, csvRecords.get(0), csvRecords.get(1));
        initCSVParserMockConstruction();

        sut = new CSVBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
        Record record = sut.readRecord();
        Assertions.assertEquals(2, record.getSchema().getEntries().size());
        Assertions.assertEquals("value1", record.getString("key1"));
        Assertions.assertEquals("value2", record.getString("key2"));
    }

    @AfterEach
    void closeMocks() {
        mockedConstruction.close();
    }
}