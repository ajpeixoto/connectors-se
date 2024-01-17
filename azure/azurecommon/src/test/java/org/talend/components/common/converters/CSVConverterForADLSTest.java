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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVFormatOptionsWithSchema;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordImpl;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CSVConverterForADLSTest {

    private CSVConverterForADLS sut;

    private RecordBuilderFactory recordBuilderFactoryMock;

    @BeforeEach
    void setUp() {
        recordBuilderFactoryMock = Mockito.mock();
        Mockito.when(recordBuilderFactoryMock.newSchemaBuilder(Schema.Type.RECORD))
                .thenReturn(new SchemaImpl.BuilderImpl().withType(Schema.Type.RECORD));
        Mockito.when(recordBuilderFactoryMock.newEntryBuilder())
                .thenReturn(new SchemaImpl.EntryImpl.BuilderImpl());
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder(Mockito.any())).thenReturn(new RecordImpl.BuilderImpl());
        Mockito.when(recordBuilderFactoryMock.newRecordBuilder()).thenReturn(new RecordImpl.BuilderImpl());

        CSVFormatOptionsWithSchema formatOptionsWithSchema = new CSVFormatOptionsWithSchema();
        formatOptionsWithSchema.setCsvFormatOptions(new CSVFormatOptions());
        sut = CSVConverterForADLS.of(recordBuilderFactoryMock, formatOptionsWithSchema);
    }

    @Test
    void testInferSchema() throws IOException {
        CSVRecord record = CSVParser.parse("value1,value2", CSVFormat.DEFAULT)
                .getRecords()
                .get(0);
        Schema schema = sut.inferSchema(record);

        Assertions.assertEquals(2, schema.getEntries().size());
        Assertions.assertEquals("field0", schema.getEntries().get(0).getName());
        Assertions.assertEquals("field1", schema.getEntries().get(1).getName());
    }

}