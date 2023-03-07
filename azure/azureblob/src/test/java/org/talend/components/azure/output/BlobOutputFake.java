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
package org.talend.components.azure.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;

import org.talend.components.azure.migration.AzureStorageRuntimeDatasetMigration;
import org.talend.components.azure.runtime.output.CSVBlobFileWriter;
import org.talend.components.azure.runtime.output.CloudBlobWriter;
import org.talend.components.azure.runtime.output.CloudBlobWriterBuilder;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.Processor;

@Version(value = 2, migrationHandler = AzureStorageRuntimeDatasetMigration.class)
@Icon(value = Icon.IconType.CUSTOM, custom = "azure-blob-output")
@Processor(name = "OutputTest")
@Documentation("Azure Blob Storage Writer")
public class BlobOutputFake extends BlobOutput {

    static final MessageService msg = new MessageService() {

        @Override
        public String illegalContainerName() {
            return "illegalContainerName";
        }

        @Override
        public String errorRetrieveContainers() {
            return "errorRetrieveContainers";
        }

        @Override
        public String errorCreateBlobItem() {
            return "errorCreateBlobItem";
        }

        @Override
        public String errorSubmitRows() {
            return "errorSubmitRows";
        }

        @Override
        public String cantStartReadBlobItems(String message) {
            return "cantStartReadBlobItems";
        }

        @Override
        public String containerNotExist(String containerName) {
            return "containerNotExist";
        }

        @Override
        public String authTypeNotSupportedForParquet() {
            return "authTypeNotSupportedForParquet";
        }

        @Override
        public String encodingNotSupported(String incorrectEncoding) {
            return "encodingNotSupported";
        }

        @Override
        public String fileIsNotValidExcelHTML() {
            return "fileIsNotValidExcelHTML";
        }

    };

    public BlobOutputFake(@Option("configuration") BlobOutputConfiguration configuration,
            AzureBlobComponentServices service) {
        super(configuration, service, BlobOutputFake.msg);
    }

    @PostConstruct
    public void init() {
        try {
            BlobOutputConfiguration cfg = this.getConfiguration();
            AzureBlobComponentServices srv = this.getService();

            final CSVBlobFileWriter writer = new CSVBlobFileWriterFake(cfg, srv);
            this.setFileWriter(writer);
        } catch (Exception ex) {
            throw new RuntimeException("Error on init", ex);
        }

    }

    static class CSVBlobFileWriterFake extends CSVBlobFileWriter {

        private File outputFile;

        public CSVBlobFileWriterFake(BlobOutputConfiguration config,
                AzureBlobComponentServices connectionServices)
                throws Exception {
            super(config, connectionServices);
        }

        @Override
        public void generateFile(String directoryName) throws URISyntaxException, StorageException {
            this.outputFile = new File(directoryName, UUID.randomUUID() + ".csv");
            this.setCurrentItem(new CloudBlobWriterFake(this.outputFile));
        }

    }

    static class CloudBlobWriterFake implements CloudBlobWriter {

        private final File output;

        public CloudBlobWriterFake(File output) {
            this.output = output;
        }

        @Override
        public void upload(byte[] buffer) throws IOException {
            Files.write(this.output.toPath(), buffer, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        }

        @Override
        public void append(byte[] buffer, OperationContext opContext) throws IOException {
            Files.write(this.output.toPath(), buffer, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        }

        @Override
        public void upload(InputStream sourceStream) throws IOException {
            Files.copy(sourceStream, this.output.toPath());
        }

        @Override
        public void appendText(String content) throws IOException {
            Files.write(this.output.toPath(), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);
        }

        @Override
        public void onOutput(OutputFunction action) throws IOException {
            try (OutputStream out = new FileOutputStream(this.output)) {
                action.onOutputStream(out);
            }
        }
    }

    static class CloudBlobWriterBuilderFake implements CloudBlobWriterBuilder {

        @Override
        public CloudBlobWriter build(Supplier<String> nameBuilder) throws URISyntaxException, StorageException {
            final String name = nameBuilder.get();
            return new CloudBlobWriterFake(new File(name));
        }
    }

    public static class AzureBlobComponentServicesFake extends AzureBlobComponentServices {

        @Override
        public CloudBlobWriterBuilder buildWriter(BlobOutputConfiguration config)
                throws URISyntaxException, StorageException {
            return new CloudBlobWriterBuilderFake();
        }
    }
}
