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
import com.microsoft.azure.storage.blob.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.common.connection.azureblob.AzureAuthType;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionAccount;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.record.Record;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

class BlobFileReaderTest extends BaseBlobFileReaderTest {

    @Test
    void testGetParquetFileReaderViaFactory() throws Exception {
        initConfig();
        initRecordBuilderFactoryMocks();
        componentServicesMock = Mockito.mock();
        AzureComponentServices connectionServicesMock = Mockito.mock();
        CloudBlobContainer containerMock = initMockContainerReturnTwoFiles();

        clientMock = Mockito.mock();
        Mockito.when(clientMock.getContainerReference(Mockito.any())).thenReturn(containerMock);
        Mockito.when(connectionServicesMock.createCloudBlobClient(Mockito.any(), Mockito.any())).thenReturn(clientMock);
        Mockito.when(componentServicesMock.getConnectionService()).thenReturn(connectionServicesMock);

        config.setFileFormat(FileFormat.PARQUET);
        config.getConnection().setAccountConnection(new AzureStorageConnectionAccount());
        config.getConnection().getAccountConnection().setAuthType(AzureAuthType.BASIC);
        config.getConnection().getAccountConnection().setAccountName("testName");
        config.getConnection().getAccountConnection().setAccountKey("testKey");
        config.setContainerName("testContainerName");

        try (MockedStatic<HadoopInputFile> hadoopInputFileMockedStatic = Mockito.mockStatic(HadoopInputFile.class);
                MockedStatic<AvroParquetReader> avroReaderMockedStatic = Mockito.mockStatic(AvroParquetReader.class)) {
            HadoopInputFile inputFileMock = Mockito.mock();
            ParquetReader<GenericRecord> readerMock = Mockito.mock();
            AvroParquetReader.Builder<GenericRecord> builderMock = Mockito.mock();
            avroReaderMockedStatic.when(() -> AvroParquetReader.builder(inputFileMock)).thenReturn(builderMock);
            Mockito.when(builderMock.build()).thenReturn(readerMock);
            hadoopInputFileMockedStatic.when(() -> HadoopInputFile.fromPath(Mockito.any(), Mockito.any()))
                    .thenReturn(inputFileMock);

            Schema testAvroSchema = SchemaBuilder
                    .record("X")
                    .fields()
                    .nullableString("test", "")
                    .endRecord();
            GenericRecord testResultRecord = new GenericData.Record(testAvroSchema);
            testResultRecord.put("test", "value");
            Mockito.when(readerMock.read()).thenReturn(testResultRecord, testResultRecord, null);
            BlobFileReader sut = BlobFileReader.BlobFileReaderFactory.getReader(config, recordBuilderFactoryMock,
                    componentServicesMock, messageServiceMock);

            Record first = sut.readRecord();
            Record second = sut.readRecord();
            Assertions.assertEquals(first, second);
            Assertions.assertEquals(1, first.getSchema().getEntries().size());
            Assertions.assertNull(sut.readRecord());
        }
    }

    private CloudBlobContainer initMockContainerReturnTwoFiles() throws StorageException {
        List<ListBlobItem> listItems = new ArrayList<>();
        CloudBlob blobItemMock = Mockito.mock();
        BlobInputStream blobInputStreamMock = Mockito.mock();
        Mockito.when(blobItemMock.openInputStream()).thenReturn(blobInputStreamMock);
        listItems.add(blobItemMock);
        listItems.add(blobItemMock);
        CloudBlobContainer mock = Mockito.mock();
        Mockito.when(mock.exists()).thenReturn(true);
        Mockito.when(mock.listBlobs(Mockito.any(), Mockito.anyBoolean(), Mockito.nullable(EnumSet.class),
                Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(listItems);

        return mock;
    }

}
