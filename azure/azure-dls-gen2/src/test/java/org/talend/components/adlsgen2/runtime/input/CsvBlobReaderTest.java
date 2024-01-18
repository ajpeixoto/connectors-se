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
package org.talend.components.adlsgen2.runtime.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.common.formats.Encoding;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.sdk.component.api.record.Record;

class CsvBlobReaderTest extends BaseBlobReaderTest {

    private CsvBlobReader sut;

    private Iterator<CSVRecord> fileRecordIteratorMock;

    private MockedConstruction<CSVParser> mockedConstruction;

    @BeforeEach
    void setUp() throws IOException {
        initRecordBuilderFactoryMocks();
        initConfig();
        CSVFormatOptions csvFormatOptions = new CSVFormatOptions();
        csvFormatOptions.setEncoding(Encoding.UTF8);
        CSVFormatOptionsWithSchema csvConfig = new CSVFormatOptionsWithSchema();
        csvConfig.setCsvSchema("field0;field1");
        csvConfig.setCsvFormatOptions(csvFormatOptions);
        config.getDataSet().setCsvConfiguration(csvConfig);
        initComponentServicesMock();

        fileRecordIteratorMock = Mockito.mock();
        // hasNext called one more time when file header parsed, so return true,true,false
        Mockito.when(fileRecordIteratorMock.hasNext()).thenReturn(true, true, false);
        CSVRecord csvRecord = CSVParser.parse("key,value", CSVFormat.DEFAULT).getRecords().get(0);
        Mockito.when(fileRecordIteratorMock.next()).thenReturn(csvRecord);
        initCSVParserMockConstruction();
        sut = new CsvBlobReader(config, recordBuilderFactoryMock, servicesMock);
    }

    private void initCSVParserMockConstruction() {
        mockedConstruction = Mockito.mockConstruction(CSVParser.class,
                (mock, context) -> {
                    Mockito.when(mock.iterator()).thenReturn(fileRecordIteratorMock);
                });
    }

    @Test
    void testCSVReadSimpleRecord() {
        Record record = sut.readRecord();
        Assertions.assertEquals(2, record.getSchema().getEntries().size());
        Assertions.assertEquals("key", record.getString("field0"));
        Assertions.assertEquals("value", record.getString("field1"));
    }

    @Test
    void testCSVWithBigHeader() throws IOException {
        config.getDataSet().getCsvConfiguration().getCsvFormatOptions().setUseHeader(true);
        config.getDataSet().getCsvConfiguration().getCsvFormatOptions().setHeader(2);

        mockedConstruction.close();
        Mockito.when(fileRecordIteratorMock.hasNext()).thenReturn(true, true, true, true, false);
        List<CSVRecord> csvRecords = CSVParser.parse("key1,key2\r\nvalue1,value2", CSVFormat.DEFAULT)
                .getRecords();
        Mockito.when(fileRecordIteratorMock.next()).thenReturn(null, csvRecords.get(0), csvRecords.get(1));
        initCSVParserMockConstruction();

        sut = new CsvBlobReader(config, recordBuilderFactoryMock, servicesMock);
        Record record = sut.readRecord();
        Assertions.assertEquals(2, record.getSchema().getEntries().size());
        Assertions.assertEquals("value1", record.getString("field0"));
        Assertions.assertEquals("value2", record.getString("field1"));
    }

    @AfterEach
    void closeMocks() {
        mockedConstruction.close();
    }

}
