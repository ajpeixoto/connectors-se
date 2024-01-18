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
package org.talend.components.azure.runtime.output;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.runtime.output.excel.ExcelBlobFileWriter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.components.common.service.azureblob.AzureComponentServices;

import com.microsoft.azure.storage.blob.CloudBlobClient;

class BlobFileWriterFactoryTest {

    private AzureBlobComponentServices servicesMock;

    private BlobOutputConfiguration outputConfig;

    @BeforeEach
    void setUp() {
        servicesMock = Mockito.mock();
        AzureComponentServices componentServicesMock = Mockito.mock();
        CloudBlobClient clientMock = Mockito.mock();
        Mockito.when(componentServicesMock.createCloudBlobClient(Mockito.any(), Mockito.any())).thenReturn(clientMock);
        Mockito.when(servicesMock.getConnectionService()).thenReturn(componentServicesMock);
        outputConfig = new BlobOutputConfiguration();
        outputConfig.setDataset(new AzureBlobDataset());
    }

    @Test
    void testGetCsvWriter() throws Exception {
        outputConfig.getDataset().setFileFormat(FileFormat.CSV);

        BlobFileWriter resultCsv = BlobFileWriterFactory.getWriter(outputConfig, servicesMock);
        Assertions.assertInstanceOf(CSVBlobFileWriter.class, resultCsv);

    }

    @Test
    void testGetExcelWriter() throws Exception {
        outputConfig.getDataset().setFileFormat(FileFormat.EXCEL);
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setExcelFormat(ExcelFormat.EXCEL97);
        outputConfig.getDataset().setExcelOptions(excelFormatOptions);
        BlobFileWriter resultExcel = BlobFileWriterFactory.getWriter(outputConfig, servicesMock);
        Assertions.assertInstanceOf(ExcelBlobFileWriter.class, resultExcel);
    }

    @Test
    void testGetJsonWriter() throws Exception {
        outputConfig.getDataset().setFileFormat(FileFormat.JSON);
        BlobFileWriter resultJson = BlobFileWriterFactory.getWriter(outputConfig, servicesMock);
        Assertions.assertInstanceOf(JsonBlobFileWriter.class, resultJson);
    }

    @Test
    void testGetAvroWriter() throws Exception {
        outputConfig.getDataset().setFileFormat(FileFormat.AVRO);
        BlobFileWriter resultAvro = BlobFileWriterFactory.getWriter(outputConfig, servicesMock);
        Assertions.assertInstanceOf(AvroBlobFileWriter.class, resultAvro);
    }

    @Test
    void testGetParquetWriter() throws Exception {
        outputConfig.getDataset().setFileFormat(FileFormat.PARQUET);
        BlobFileWriter resultParquet = BlobFileWriterFactory.getWriter(outputConfig, servicesMock);
        Assertions.assertInstanceOf(ParquetBlobFileWriter.class, resultParquet);
    }

}
