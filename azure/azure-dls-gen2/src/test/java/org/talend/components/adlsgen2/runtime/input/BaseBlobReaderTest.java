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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.mockito.Mockito;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;
import org.talend.sdk.component.runtime.record.SchemaImpl;

public abstract class BaseBlobReaderTest {

    protected RecordBuilderFactory recordBuilderFactoryMock;

    protected InputConfiguration config;

    protected AdlsGen2Service servicesMock;

    protected List<BlobInformations> listItems;

    protected void initConfig() {
        config = new InputConfiguration();
        config.setDataSet(new AdlsGen2DataSet());
        config.getDataSet().setConnection(new AdlsGen2Connection());
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

    protected void initComponentServicesMock() throws IOException {
        servicesMock = Mockito.mock();
        initMockContainer();
    }

    private void initListItems() throws IOException {
        BlobInformations blobItemMock = Mockito.mock();
        InputStream blobInputStreamMock = Mockito.mock();
        Mockito.when(servicesMock.getBlobInputstream(Mockito.any(), Mockito.eq(blobItemMock)))
                .thenReturn(blobInputStreamMock);

        listItems = Collections.singletonList(blobItemMock);
    }

    private void initMockContainer() throws IOException {
        initListItems();
        Mockito.when(servicesMock.getBlobs(Mockito.any())).thenReturn(listItems);
    }

}
