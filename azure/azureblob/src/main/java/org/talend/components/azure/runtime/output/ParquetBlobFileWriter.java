/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;

import com.microsoft.azure.storage.StorageException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.common.converters.ParquetConverter;
import org.talend.sdk.component.api.record.Record;

public class ParquetBlobFileWriter extends BlobFileWriter {

    private BlobOutputConfiguration config;

    private ParquetConverter converter;

    public ParquetBlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices)
            throws Exception {
        super(config, connectionServices);
        this.config = config;
        this.converter = ParquetConverter.of(null);
    }

    @Override
    public void newBatch() {
        super.newBatch();

        try {
            generateFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void generateFile(String directoryName) throws URISyntaxException, StorageException {
        final CloudBlobWriter writer = getWriterBuilder()
                .build(() -> directoryName + config.getBlobNameTemplate() + System.currentTimeMillis() + ".parquet");
        setCurrentItem(writer);
    }

    @Override
    public void flush() {
        if (getBatch().isEmpty()) {
            return;
        }

        File tempFilePath = null;
        try {
            tempFilePath = File.createTempFile("tempFile", ".parquet");
            Path tempFile = new org.apache.hadoop.fs.Path(tempFilePath.getPath());
            ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord> builder(tempFile)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withSchema(converter.inferAvroSchema(getSchema()))
                    .build();
            for (Record r : getBatch()) {
                writer.write(converter.fromRecord(r));
            }

            writer.close();
            final java.nio.file.Path tempPath = tempFilePath.toPath();
            this.getCurrentItem()
                    .onOutput(
                            (OutputStream out) -> Files.copy(tempPath, out));
        } catch (IOException | StorageException e) {
            throw new RuntimeException(e);
        } finally {
            getBatch().clear();
            if (tempFilePath != null) {
                tempFilePath.delete();
            }
        }
    }
}
