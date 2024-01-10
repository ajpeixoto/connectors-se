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
package org.talend.components.azure.output;

import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.output.BlobFileWriter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;

import static org.mockito.ArgumentMatchers.any;

class BlobOutputTest {

    AzureBlobComponentServices blobComponentServicesMock;

    @Mock
    BlobFileWriter writerMock;

    @InjectMocks
    BlobOutput sut;

    private MessageService messageServiceMock;

    @BeforeEach
    public void initMocks() throws URISyntaxException {
        messageServiceMock = Mockito.mock();
        sut = new BlobOutput(null, null, messageServiceMock);
        MockitoAnnotations.openMocks(this);

        CloudBlobClient blobClientMock = Mockito.mock(CloudBlobClient.class);
        CloudStorageAccount cloudStorageAccountMock = Mockito.mock(CloudStorageAccount.class);
        AzureComponentServices componentServicesMock = Mockito.mock(AzureComponentServices.class);
        Mockito.when(componentServicesMock.createCloudBlobClient(any(), any())).thenReturn(blobClientMock);
        blobComponentServicesMock = Mockito.mock(AzureBlobComponentServices.class);
        Mockito.when(blobComponentServicesMock.getConnectionService()).thenReturn(componentServicesMock);
        Mockito.when(blobComponentServicesMock.createStorageAccount(any())).thenReturn(cloudStorageAccountMock);
    }

    @Test
    void testHTMLOutputNotSupported() {
        final String expectedExceptionMessage = "HTML excel output is not supported";
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setExcelFormat(ExcelFormat.HTML);

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setDirectory("testDir");
        dataset.setFileFormat(FileFormat.EXCEL);
        dataset.setExcelOptions(excelFormatOptions);
        BlobOutputConfiguration outputConfiguration = new BlobOutputConfiguration();
        outputConfiguration.setDataset(dataset);

        BlobOutput output =
                new BlobOutput(outputConfiguration, blobComponentServicesMock, Mockito.mock(MessageService.class));
        ComponentException thrownException = Assertions.assertThrows(ComponentException.class, output::init);

        Assertions
                .assertEquals(expectedExceptionMessage, thrownException.getCause().getMessage(),
                        "Exception message is different");
    }

    @Test
    void testCSVOutputWithEmptyDirNotFailing() {
        final String expectedExceptionMessage = "Directory for output action should be specified";
        CSVFormatOptions csvFormatOptions = new CSVFormatOptions();
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setDirectory(null);
        dataset.setFileFormat(FileFormat.CSV);
        dataset.setCsvOptions(csvFormatOptions);
        BlobOutputConfiguration outputConfiguration = new BlobOutputConfiguration();
        outputConfiguration.setDataset(dataset);

        BlobOutput output =
                new BlobOutput(outputConfiguration, blobComponentServicesMock, Mockito.mock(MessageService.class));
        Assertions.assertDoesNotThrow(output::init);
    }

    @Test
    void testBeforeGroup() {
        sut.beforeGroup();
        Mockito.verify(writerMock).newBatch();
    }

    @Test
    void testAfterGroup() throws IOException, StorageException {
        sut.afterGroup();
        Mockito.verify(writerMock).flush();
    }

    @Test
    void testAfterGroupException() throws IOException, StorageException {
        Mockito.doThrow(IOException.class).when(writerMock).flush();
        Assertions.assertThrows(ComponentException.class, () -> sut.afterGroup());
        Mockito.verify(messageServiceMock).errorSubmitRows();
    }

    @Test
    void testNextRecord() {
        Record someRecord = Mockito.mock();
        sut.onNext(someRecord);

        Mockito.verify(writerMock).writeRecord(someRecord);
    }

    @Test
    void testRelease() throws Exception {
        sut.release();

        Mockito.verify(writerMock).complete();
    }

    @Test
    void testReleaseException() throws Exception {
        Mockito.doThrow(IOException.class).when(writerMock).complete();
        Assertions.assertThrows(ComponentException.class, () -> sut.release());
        Mockito.verify(messageServiceMock).errorSubmitRows();
    }
}