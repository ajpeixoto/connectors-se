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

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;

class JsonBlobFileReaderTest extends BaseFileReaderTest {

    private JsonBlobFileReader sut;

    private MessageService messageServiceMock;

    private MockedStatic<Json> mockedStaticJson;

    @BeforeEach
    void setUp() throws URISyntaxException, StorageException {
        messageServiceMock = Mockito.mock();
        initRecordBuilderFactoryMocks();
        initConfig();
        initMockJson();
        initComponentServicesMock();

        sut = new JsonBlobFileReader(config, recordBuilderFactoryMock, componentServicesMock, messageServiceMock);
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