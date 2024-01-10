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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

@Disabled
class JsonBlobFileReaderTest {

    private JsonBlobFileReader sut;

    private RecordBuilderFactory recordBuilderFactoryMock;

    private MessageService messageServiceMock;

    private CloudBlobClient clientMock;

    private List<ListBlobItem> listItems;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        AzureBlobComponentServices componentServicesMock = Mockito.mock();
        recordBuilderFactoryMock = Mockito.mock();
        AzureBlobDataset config = new AzureBlobDataset();
        config.setConnection(new AzureCloudConnection());
        AzureComponentServices connectionServicesMock = Mockito.mock();
        clientMock = Mockito.mock();
        CloudBlobContainer containerMock = Mockito.mock();
        CloudBlob blobItemMock = Mockito.mock();
        listItems = Collections.singletonList(blobItemMock);
        Mockito.when(containerMock.exists()).thenReturn(true);
        Mockito.when(containerMock.listBlobs(Mockito.any(), Mockito.anyBoolean(), Mockito.nullable(EnumSet.class),
                Mockito.nullable(BlobRequestOptions.class), Mockito.any()))
                .thenReturn(listItems);
        Mockito.when(clientMock.getContainerReference(Mockito.any())).thenReturn(containerMock);
        Mockito.when(connectionServicesMock.createCloudBlobClient(Mockito.any(), Mockito.any())).thenReturn(clientMock);
        Mockito.when(componentServicesMock.getConnectionService()).thenReturn(connectionServicesMock);
        sut = new JsonBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
    }

    @Test
    void testReadJsonNoData() {
        sut.initItemRecordIterator(listItems);
        Record record = sut.readRecord();

        Assertions.assertNull(record);
    }

    @Test
    void testReadJson() {
        sut.initItemRecordIterator(listItems);
    }
}