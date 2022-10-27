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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import com.microsoft.azure.storage.StorageException;

import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BlobFileWriter {

    private List<Record> batch;

    private Schema schema;

    private CloudBlobWriter currentItem = null;

    private final CloudBlobWriterBuilder writerBuilder;

    private String directoryName;

    public BlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices)
            throws Exception {

        this.writerBuilder = connectionServices.buildWriter(config);

        directoryName = config.getDataset().getDirectory();

        if (directoryName == null) {
            directoryName = "";
        } else if (!directoryName.endsWith("/")) {
            directoryName += "/";
        }
    }

    public void newBatch() {
        batch = new LinkedList<>();
        log.debug("New batch created");
    }

    @Deprecated
    protected void generateFile() throws URISyntaxException, StorageException {
        generateFile(this.directoryName);
    }

    protected abstract void generateFile(String directoryName) throws URISyntaxException, StorageException;

    public void writeRecord(Record record) {
        if (schema == null) {
            schema = record.getSchema();
        }

        batch.add(record);
    }

    protected List<Record> getBatch() {
        return batch;
    }

    protected CloudBlobWriter getCurrentItem() {
        return currentItem;
    }

    protected void setCurrentItem(CloudBlobWriter currentItem) {
        this.currentItem = currentItem;
    }

    protected CloudBlobWriterBuilder getWriterBuilder() {
        return this.writerBuilder;
    }

    protected Schema getSchema() {
        return schema;
    }

    /**
     * Upload prepared batch
     *
     * @throws IOException
     * @throws StorageException
     */
    public abstract void flush() throws IOException, StorageException;

    /**
     * Finish everything
     *
     * @throws Exception
     */
    public void complete() throws Exception {
        if (!getBatch().isEmpty()) {
            log.info("Executing last batch with " + getBatch().size() + " records");
            flush();
        }

    }

}
