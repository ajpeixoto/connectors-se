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
package org.talend.components.common.converters;

import java.util.Collections;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.common.stream.input.excel.ExcelToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;

class ExcelConverterTest {

    private ExcelConverter sut;

    private RecordBuilderFactory recordBuilderFactoryMock;

    private Record testRecord;

    private ExcelToRecord nestedConverter;

    private MockedConstruction<ExcelToRecord> mockedConstruction;

    @BeforeEach
    void setUp() {
        recordBuilderFactoryMock = Mockito.mock();
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder()).thenReturn(new RecordImpl.BuilderImpl());
        mockedConstruction =
                Mockito.mockConstruction(ExcelToRecord.class);
        sut = ExcelConverter.of(recordBuilderFactoryMock);
        nestedConverter = mockedConstruction.constructed().get(0);
        testRecord = recordBuilderFactoryMock.newRecordBuilder()
                .withString("a", "1")
                .withInt("b", 2)
                .build();
    }

    @Test
    void testInferRecordColumns() {
        List<String> result = sut.inferRecordColumns(testRecord);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("a", result.get(0));
        Assertions.assertEquals("b", result.get(1));
    }

    @Test
    void testToRecord() {
        Row mock = Mockito.mock();
        sut.toRecord(mock);
        Mockito.verify(nestedConverter).toRecord(mock);
    }

    @Test
    void testInferSchema() {
        Row mock = Mockito.mock();
        sut.inferSchema(mock);
        Mockito.verify(nestedConverter).inferSchema(mock, false);
    }

    @Test
    void testInferSchemaNames() {
        Row mock = Mockito.mock();
        sut.inferSchemaNames(mock, false);
        Mockito.verify(nestedConverter).inferSchema(mock, false);
    }

    @Test
    void testFromRecordReturnsNull() {
        Assertions.assertNull(sut.fromRecord(null));
    }

    @Test
    void testAppendBatchThrowsAnException() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> sut.appendBatchToTheSheet(null, 0));
    }

    @Test
    void testAppendBatchToTheSheet() {
        Sheet mockedSheet = Mockito.mock();
        Row rowMock = Mockito.mock();
        Cell cellMock = Mockito.mock();
        Mockito.when(mockedSheet.createRow(Mockito.anyInt())).thenReturn(rowMock);
        Mockito.when(rowMock.createCell(Mockito.anyInt())).thenReturn(cellMock);

        ExcelConverter localSut = ExcelConverter.ofOutput(mockedSheet);

        Record r = recordBuilderFactoryMock.newRecordBuilder()
                .withString("str", "1")
                .withInt("inte", 2)
                .withLong("lon", 3L)
                .withDouble("doubl", 4.0)
                .withFloat("float", 5.0f)
                .withBytes("bytes", new byte[] { 6, 7, 8 })
                .withBoolean("boole", true)
                .build();

        localSut.appendBatchToTheSheet(Collections.singletonList(r), 0);

        Mockito.verify(mockedSheet, Mockito.times(1)).createRow(Mockito.anyInt());
        Mockito.verify(rowMock, Mockito.times(r.getSchema().getEntries().size())).createCell(Mockito.anyInt());
    }

    @AfterEach
    void release() {
        mockedConstruction.close();
    }
}