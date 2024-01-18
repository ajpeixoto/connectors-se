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
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.common.connection.adls.AuthMethod;
import org.talend.sdk.component.api.record.Record;

class BlobReaderTest extends BaseBlobReaderTest {

    @Test
    void testGetParquetFileReaderViaFactory() throws Exception {
        initConfig();
        initRecordBuilderFactoryMocks();
        initComponentServicesMock();

        config.getDataSet().setFormat(FileFormat.PARQUET);
        config.getDataSet().getConnection().setAuthMethod(AuthMethod.SharedKey);
        config.getDataSet().getConnection().setAccountName("testName");
        config.getDataSet().getConnection().setSharedKey("testKey");
        config.getDataSet().setFilesystem("testFSName");

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
            BlobReader sut = BlobReader.BlobFileReaderFactory.getReader(config, recordBuilderFactoryMock,
                    Mockito.mock(), servicesMock);

            Record first = sut.readRecord();
            Assertions.assertEquals(1, first.getSchema().getEntries().size());
            Assertions.assertEquals("value", first.getString("test"));
        }
    }
}
