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

import java.io.Reader;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

class JsonBlobFileReaderTest {

    private JsonBlobFileReader sut;

    private RecordBuilderFactory recordBuilderFactoryMock;

    private MessageService messageServiceMock;

    private CloudBlobClient clientMock;

    private AzureBlobComponentServices componentServicesMock;

    private List<ListBlobItem> listItems;

    private MockedStatic<Json> mockedStaticJson;

    private AzureBlobDataset config;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        initRecordBuilderFactoryMocks();

        componentServicesMock = Mockito.mock();
        config = new AzureBlobDataset();
        config.setConnection(new AzureCloudConnection());

        AzureComponentServices connectionServicesMock = Mockito.mock();
        clientMock = Mockito.mock();

        CloudBlob blobItemMock = Mockito.mock();
        BlobInputStream blobInputStreamMock = Mockito.mock();
        Mockito.when(blobItemMock.openInputStream()).thenReturn(blobInputStreamMock);

        initMockJson();
        listItems = Collections.singletonList(blobItemMock);
        CloudBlobContainer containerMock = initMockContainer();

        Mockito.when(clientMock.getContainerReference(Mockito.any())).thenReturn(containerMock);
        Mockito.when(connectionServicesMock.createCloudBlobClient(Mockito.any(), Mockito.any())).thenReturn(clientMock);
        Mockito.when(componentServicesMock.getConnectionService()).thenReturn(connectionServicesMock);
        sut = new JsonBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
    }

    private CloudBlobContainer initMockContainer() throws StorageException {
        CloudBlobContainer mock = Mockito.mock();
        Mockito.when(mock.exists()).thenReturn(true);
        Mockito.when(mock.listBlobs(Mockito.any(), Mockito.anyBoolean(), Mockito.nullable(EnumSet.class),
                        Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(listItems);

        return mock;
    }

    private void initRecordBuilderFactoryMocks() {
        recordBuilderFactoryMock = Mockito.mock();
        Mockito.when(recordBuilderFactoryMock.newSchemaBuilder(Schema.Type.RECORD))
                .thenReturn(new SchemaImpl.BuilderImpl().withType(Schema.Type.RECORD));
        Mockito.when(recordBuilderFactoryMock.newEntryBuilder())
                .thenReturn(new SchemaImpl.EntryImpl.BuilderImpl());
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder(Mockito.any())).thenReturn(new RecordImpl.BuilderImpl());
    }

    private void initMockJson() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        mockedStaticJson = Mockito.mockStatic(Json.class);
        JsonReader jsonReaderMock = Mockito.mock();
        mockedStaticJson.when(() -> Json.createReader(Mockito.any(Reader.class))).thenReturn(jsonReaderMock);
        Mockito.when(jsonReaderMock.read())
                .thenReturn(builder.add("testKey", "value").build(), null);
        ;
    }

    @Test
    void testReadJsonNoData() throws URISyntaxException, StorageException {
        listItems = Collections.emptyList();
        sut = new JsonBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
        Record record = sut.readRecord();

        Assertions.assertNull(record);
    }

    @Test
    void testReadJson() {
        Record record = sut.readRecord();

        Assertions.assertEquals(1, record.getSchema().getEntries().size());
        Assertions.assertEquals("value", record.getString("testKey"));
    }

    @AfterEach
    void release() {
        mockedStaticJson.close();
    }
}