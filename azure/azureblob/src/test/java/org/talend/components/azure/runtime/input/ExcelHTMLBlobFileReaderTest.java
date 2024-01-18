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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;

import java.io.InputStream;
import java.net.URISyntaxException;

class ExcelHTMLBlobFileReaderTest extends BaseBlobFileReaderTest {

    private ExcelHTMLBlobFileReader sut;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        config = new AzureBlobDataset();
        config.setExcelOptions(new ExcelFormatOptions());
        config.getExcelOptions().setExcelFormat(ExcelFormat.HTML);

        initComponentServicesMock();
        initRecordBuilderFactoryMocks();
    }

    @Test
    void testReadNotValidHTML() {
        Document documentMock = Mockito.mock();
        Element bodyMock = Mockito.mock();
        Elements rowsMock = Mockito.mock();
        Mockito.when(rowsMock.isEmpty()).thenReturn(true);
        Mockito.when(bodyMock.getElementsByTag("tr")).thenReturn(rowsMock);
        Mockito.when(documentMock.body()).thenReturn(bodyMock);
        try (MockedStatic<Jsoup> mockedStatic = Mockito.mockStatic(Jsoup.class)) {
            mockedStatic.when(() -> Jsoup.parse(Mockito.any(InputStream.class),
                    Mockito.anyString(), Mockito.eq(""))).thenReturn(documentMock);

            Assertions.assertThrows(ComponentException.class,
                    () -> new ExcelHTMLBlobFileReader(config, recordBuilderFactoryMock,
                            componentServicesMock, messageServiceMock));
            Mockito.verify(messageServiceMock).fileIsNotValidExcelHTML();
        }
    }

    @Test
    void testReadHTMLOneRow() throws URISyntaxException, StorageException {
        Document documentMock = Mockito.mock();
        Element bodyMock = Mockito.mock();
        Element recordElement = new Element("tr");
        Element first = new Element("td").text("key");
        Element second = new Element("td").text("value");

        recordElement.appendChild(first);
        recordElement.appendChild(second);
        Elements rows = new Elements(recordElement);
        Mockito.when(bodyMock.getElementsByTag("tr")).thenReturn(rows);
        Mockito.when(documentMock.body()).thenReturn(bodyMock);
        try (MockedStatic<Jsoup> mockedStatic = Mockito.mockStatic(Jsoup.class)) {
            mockedStatic.when(() -> Jsoup.parse(Mockito.any(InputStream.class),
                    Mockito.anyString(), Mockito.eq(""))).thenReturn(documentMock);

            sut = new ExcelHTMLBlobFileReader(config, recordBuilderFactoryMock,
                    componentServicesMock, messageServiceMock);
            Record record = sut.readRecord();
            Assertions.assertEquals(2, record.getSchema().getEntries().size());
            Assertions.assertEquals("key", record.getString("field0"));
            Assertions.assertEquals("value", record.getString("field1"));
        }
    }
}
