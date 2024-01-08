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
package org.talend.components.common.converters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

class DeltaConverterTest {

    private DeltaConverter sut;

    private RecordBuilderFactory builderFactory;

    @BeforeEach
    void setUp() {
        builderFactory = Mockito.mock();
        Mockito.when(builderFactory.newRecordBuilder()).thenReturn(new RecordImpl.BuilderImpl());
        Schema.Builder schemaBuilder = new SchemaImpl.BuilderImpl();
        schemaBuilder.withType(Schema.Type.RECORD);
        Mockito.when(builderFactory.newSchemaBuilder(Schema.Type.RECORD)).thenReturn(schemaBuilder);
        Mockito.when(builderFactory.newEntryBuilder()).thenReturn(new SchemaImpl.EntryImpl.BuilderImpl());
        sut = DeltaConverter.of(builderFactory);
    }

    @Test
    void testInferSchema() {
        RowRecord someRecordMock = Mockito.mock();
        StructType structMock = Mockito.mock();
        StructField field = new StructField("a", new StringType());
        Mockito.when(structMock.getFields()).thenReturn(new StructField[] { field });
        Mockito.when(someRecordMock.getSchema()).thenReturn(structMock);
        Schema result = sut.inferSchema(someRecordMock);

        Assertions.assertEquals(1, result.getEntries().size());
        Assertions.assertEquals(Schema.Type.STRING, result.getEntries().get(0).getType());
        Assertions.assertEquals("a", result.getEntries().get(0).getName());
    }

}