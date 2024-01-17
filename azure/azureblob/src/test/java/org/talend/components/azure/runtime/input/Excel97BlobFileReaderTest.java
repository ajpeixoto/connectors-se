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
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.sdk.component.api.record.Record;

import java.net.URISyntaxException;
import java.util.Spliterators;

class Excel97BlobFileReaderTest extends BaseFileReaderTest {

    private ExcelBlobFileReader sut;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        config = new AzureBlobDataset();
        config.setExcelOptions(new ExcelFormatOptions());
        config.getExcelOptions().setExcelFormat(ExcelFormat.EXCEL97);

        initComponentServicesMock();
        initRecordBuilderFactoryMocks();
    }

    @Test
    void testReadSimpleExcel() throws URISyntaxException, StorageException {
        HSSFSheet mockedSheet = Mockito.mock();
        HSSFRow mockedRow = Mockito.mock();
        HSSFCell mockedCell = Mockito.mock();
        Mockito.when(mockedCell.getCellType()).thenReturn(CellType.STRING);
        Mockito.when(mockedCell.getStringCellValue()).thenReturn("value");
        Mockito.when(mockedRow.getCell(0)).thenReturn(mockedCell);
        Mockito.when(mockedRow.spliterator()).thenReturn(Spliterators.spliterator(new Object[] { mockedCell }, 0));
        Mockito.when(mockedSheet.getPhysicalNumberOfRows()).thenReturn(1);
        Mockito.when(mockedSheet.getRow(0)).thenReturn(mockedRow);
        try (MockedConstruction<HSSFWorkbook> workbookMockedConstruction =
                Mockito.mockConstruction(HSSFWorkbook.class, (mock, context) -> {
                    Mockito.when(mock.getSheet(Mockito.any())).thenReturn(mockedSheet);
                })) {
            sut = new ExcelBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
            Record record = sut.readRecord();
            Assertions.assertEquals(1, record.getSchema().getEntries().size());
            Assertions.assertEquals("value", record.getString("field0"));
        }
    }

    @Test
    void testReadExcelWithHeader() throws URISyntaxException, StorageException {
        config.getExcelOptions().setUseHeader(true);
        config.getExcelOptions().setHeader(1);

        HSSFSheet mockedSheet = Mockito.mock();
        HSSFRow mockedRow = Mockito.mock();
        HSSFCell mockedCell = Mockito.mock();
        Mockito.when(mockedCell.getCellType()).thenReturn(CellType.STRING);
        Mockito.when(mockedCell.getStringCellValue()).thenReturn("value");
        Mockito.when(mockedRow.getCell(0)).thenReturn(mockedCell);
        Mockito.when(mockedRow.spliterator()).thenReturn(Spliterators.spliterator(new Object[] { mockedCell }, 0));
        Mockito.when(mockedSheet.getPhysicalNumberOfRows()).thenReturn(2);
        Mockito.when(mockedSheet.getRow(0)).thenReturn(mockedRow);
        Mockito.when(mockedSheet.getRow(1)).thenReturn(mockedRow);
        try (MockedConstruction<HSSFWorkbook> workbookMockedConstruction =
                Mockito.mockConstruction(HSSFWorkbook.class, (mock, context) -> {
                    Mockito.when(mock.getSheet(Mockito.any())).thenReturn(mockedSheet);
                })) {
            sut = new ExcelBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
            Record record = sut.readRecord();
            Assertions.assertEquals(1, record.getSchema().getEntries().size());
            Assertions.assertEquals("value", record.getString("value"));
        }
    }

}