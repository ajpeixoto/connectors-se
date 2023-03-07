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

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.microsoft.azure.storage.StorageException;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.connection.azureblob.AzureStorageConnectionAccount;
import org.talend.components.common.converters.CSVConverter;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.azure")
class CSVOutputTest {

    private BlobOutputConfiguration blobOutputProperties;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private MessageService i18n;

    @Service
    private AzureBlobComponentServices services;

    public static <T> T inject(final ComponentManager manager,
            final Class<T> clazz,
            final T service) {
        final Container container = manager.findPlugin("test-classes").orElse(null);
        final ComponentManager.AllServices allServices = container.get(ComponentManager.AllServices.class);

        // put fake as real
        allServices.getServices().put(clazz, service);

        // inject fake on all others services that use it
        final T real = clazz.cast(allServices.getServices().get(clazz));
        final Injector injector = Injector.class.cast(allServices.getServices().get(Injector.class));
        injector.inject(service);

        final Container containerClass = manager.findPlugin("classes").orElse(null);
        if (containerClass != null) {
            final ComponentManager.AllServices allServicesClass =
                    containerClass.get(ComponentManager.AllServices.class);
            final Injector injectorClass = Injector.class.cast(allServices.getServices().get(Injector.class));
            injectorClass.inject(service);
        }
        return real;
    }

    @Injected
    protected BaseComponentsHandler componentsHandler;

    @BeforeEach
    void initDataset() {
        BlobOutputFake.AzureBlobComponentServicesFake servicesFake =
                new BlobOutputFake.AzureBlobComponentServicesFake();
        componentsHandler.injectServices(this.i18n);
        this.services =
                CSVOutputTest.inject(componentsHandler.asManager(), AzureBlobComponentServices.class, servicesFake);

        AzureBlobDataset dataset = new AzureBlobDataset();
        AzureCloudConnection datastore = this.createDatastore();
        dataset.setConnection(datastore);
        dataset.setFileFormat(FileFormat.CSV);

        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(CSVRecordDelimiter.LF);
        dataset.setCsvOptions(formatOptions);
        dataset.setContainerName("name");
        blobOutputProperties = new BlobOutputConfiguration();
        blobOutputProperties.setDataset(dataset);
    }

    @Test
    void outputTestWithSixSameRecordsAndStandardConfig() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 6;
        final boolean testBooleanValue = true;
        final long testLongValue = 0L;
        final int testIntValue = 1;
        final double testDoubleValue = 2.0;
        final ZonedDateTime testDateValue = ZonedDateTime.now();
        final byte[] bytes = new byte[] { 1, 2, 3 };

        URL testDir = Thread.currentThread().getContextClassLoader().getResource("./testDir");
        cleanFolder(testDir.getPath());

        final AzureBlobDataset dataset = blobOutputProperties.getDataset();
        dataset.setDirectory(testDir.getPath());
        blobOutputProperties.setBlobNameTemplate("testFile");
        dataset.getCsvOptions().setTextEnclosureCharacter("\"");

        Record testRecord = componentsHandler
                .findService(RecordBuilderFactory.class)
                .newRecordBuilder()
                .withBoolean("booleanValue", testBooleanValue)
                .withLong("longValue", testLongValue)
                .withInt("intValue", testIntValue)
                .withDouble("doubleValue", testDoubleValue)
                .withDateTime("dateValue", testDateValue)
                .withBytes("byteArray", bytes)
                .build();

        List<Record> testRecords = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            testRecords.add(testRecord);
        }
        componentsHandler.setInputData(testRecords);

        String outputConfig = configurationByExample().forInstance(blobOutputProperties).configured().toQueryString();
        Job
                .components()
                .component("inputFlow", "test://emitter")
                .component("outputComponent", "Azure://OutputTest?" + outputConfig)
                .connections()
                .from("inputFlow")
                .to("outputComponent")
                .build()
                .run();
        BlobTestUtils.recordBuilderFactory = componentsHandler.findService(RecordBuilderFactory.class);

        CSVFormat csvFormat = CSVConverter.of(recordBuilderFactory, dataset.getCsvOptions()).getCsvFormat();
        List<Record> retrievedRecords =
                BlobTestUtils.readRecordFromCSVDirectory(dataset.getDirectory(), dataset, csvFormat);

        Assertions.assertEquals(recordSize, retrievedRecords.size());
        Assertions
                .assertEquals(testRecord.getSchema().getEntries().size(),
                        retrievedRecords.get(0).getSchema().getEntries().size());
        Assertions
                .assertEquals(String.valueOf(testRecord.getBoolean("booleanValue")),
                        retrievedRecords.get(0).getString("field0"));
        Assertions
                .assertEquals(String.valueOf(testRecord.getLong("longValue")),
                        retrievedRecords.get(0).getString("field1"));
        Assertions
                .assertEquals(String.valueOf(testRecord.getInt("intValue")),
                        retrievedRecords.get(0).getString("field2"));
        Assertions
                .assertEquals(String.valueOf(testRecord.getDouble("doubleValue")),
                        retrievedRecords.get(0).getString("field3"));
        Assertions
                .assertEquals(String.valueOf(testRecord.getDateTime("dateValue")),
                        retrievedRecords.get(0).getString("field4"));
        Assertions
                .assertEquals(Arrays.toString(testRecord.getBytes("byteArray")),
                        retrievedRecords.get(0).getString("field5"));

    }

    private AzureCloudConnection createDatastore() {
        AzureCloudConnection dataStore = new AzureCloudConnection();
        dataStore.setUseAzureSharedSignature(false);
        AzureStorageConnectionAccount accountConnection = new AzureStorageConnectionAccount();
        accountConnection.setAccountName("user");
        accountConnection
                .setAccountKey(Base64.getEncoder().encodeToString("azure_test_key".getBytes(StandardCharsets.UTF_8)));

        dataStore.setAccountConnection(accountConnection);
        return dataStore;
    }

    private void cleanFolder(String folderName) throws IOException {
        Files.walk(new File(folderName).toPath())
                .filter((Path f) -> f.toFile().isFile() && f.toFile().getName().endsWith(".csv"))
                .forEach((Path p) -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

}