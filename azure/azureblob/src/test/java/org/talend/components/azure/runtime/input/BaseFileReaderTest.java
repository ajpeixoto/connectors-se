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

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.service.azureblob.AzureComponentServices;
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

public class BaseFileReaderTest {

    protected RecordBuilderFactory recordBuilderFactoryMock;

    protected CloudBlobClient clientMock;

    protected AzureBlobDataset config;

    protected AzureBlobComponentServices componentServicesMock;

    protected List<ListBlobItem> listItems;

    protected MessageService messageServiceMock;

    protected BaseFileReaderTest() {
        messageServiceMock = Mockito.mock();
    }

    protected void initConfig() {
        config = new AzureBlobDataset();
        config.setConnection(new AzureCloudConnection());
    }

    protected void initRecordBuilderFactoryMocks() {
        recordBuilderFactoryMock = Mockito.mock();
        Mockito.when(recordBuilderFactoryMock.newSchemaBuilder(Schema.Type.RECORD))
                .thenReturn(new SchemaImpl.BuilderImpl().withType(Schema.Type.RECORD));
        Mockito.when(recordBuilderFactoryMock.newEntryBuilder())
                .thenReturn(new SchemaImpl.EntryImpl.BuilderImpl());
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder(Mockito.any())).thenReturn(new RecordImpl.BuilderImpl());
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder()).thenReturn(new RecordImpl.BuilderImpl());
    }

    protected void initComponentServicesMock() throws StorageException, URISyntaxException {
        componentServicesMock = Mockito.mock();
        AzureComponentServices connectionServicesMock = Mockito.mock();
        CloudBlobContainer containerMock = initMockContainer();

        clientMock = Mockito.mock();
        Mockito.when(clientMock.getContainerReference(Mockito.any())).thenReturn(containerMock);
        Mockito.when(connectionServicesMock.createCloudBlobClient(Mockito.any(), Mockito.any())).thenReturn(clientMock);
        Mockito.when(componentServicesMock.getConnectionService()).thenReturn(connectionServicesMock);
    }

    private void initListItems() throws StorageException {
        CloudBlob blobItemMock = Mockito.mock();
        BlobInputStream blobInputStreamMock = Mockito.mock();
        Mockito.when(blobItemMock.openInputStream()).thenReturn(blobInputStreamMock);

        listItems = Collections.singletonList(blobItemMock);
    }

    private CloudBlobContainer initMockContainer() throws StorageException {
        initListItems();
        CloudBlobContainer mock = Mockito.mock();
        Mockito.when(mock.exists()).thenReturn(true);
        Mockito.when(mock.listBlobs(Mockito.any(), Mockito.anyBoolean(), Mockito.nullable(EnumSet.class),
                Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(listItems);

        return mock;
    }

}
