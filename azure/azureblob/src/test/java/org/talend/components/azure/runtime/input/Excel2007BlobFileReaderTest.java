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
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.impl.StreamingSheet;
import com.monitorjbl.xlsx.impl.StreamingWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterators;

class Excel2007BlobFileReaderTest extends BaseBlobFileReaderTest {

    private ExcelBlobFileReader sut;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        config = new AzureBlobDataset();
        config.setExcelOptions(new ExcelFormatOptions());
        config.getExcelOptions().setExcelFormat(ExcelFormat.EXCEL2007);

        initComponentServicesMock();
        initRecordBuilderFactoryMocks();
    }

    @Test
    void testReadSimpleExcel() throws URISyntaxException, StorageException, IOException {
        StreamingWorkbook mockedWorkbook = Mockito.mock();
        StreamingSheet mockedSheet = Mockito.mock();
        Row mockedRow = Mockito.mock();
        Cell mockedCell = Mockito.mock();
        Mockito.when(mockedCell.getCellType()).thenReturn(CellType.STRING);
        Mockito.when(mockedCell.getStringCellValue()).thenReturn("value");
        Mockito.when(mockedRow.getCell(0)).thenReturn(mockedCell);
        Mockito.when(mockedRow.spliterator()).thenReturn(Spliterators.spliterator(new Object[] { mockedCell }, 0));
        Mockito.when(mockedSheet.getPhysicalNumberOfRows()).thenReturn(1);
        Iterator<Row> iteratorRows = Collections.singleton(mockedRow).iterator();
        Mockito.when(mockedSheet.rowIterator()).thenReturn(iteratorRows);
        Mockito.when(mockedWorkbook.getSheet(Mockito.any())).thenReturn(mockedSheet);
        try (MockedStatic<StreamingReader> streamingReaderMockedStatic = Mockito.mockStatic(StreamingReader.class)) {
            StreamingReader.Builder builderMock = Mockito.mock();
            Mockito.when(builderMock.rowCacheSize(Mockito.anyInt())).thenReturn(builderMock);
            Mockito.when(builderMock.open(Mockito.any(BlobInputStream.class))).thenReturn(mockedWorkbook);
            streamingReaderMockedStatic.when(StreamingReader::builder).thenReturn(builderMock);
            sut = new ExcelBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
            Record record = sut.readRecord();
            Assertions.assertEquals(1, record.getSchema().getEntries().size());
            Assertions.assertEquals("value", record.getString("field0"));

            Assertions.assertNull(sut.readRecord());
            Mockito.verify(mockedWorkbook).close();
        }
    }

}
