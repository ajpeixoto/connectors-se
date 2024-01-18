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
package org.talend.components.adlsgen2.runtime.formatter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;
import org.talend.sdk.component.runtime.record.RecordImpl;

import static org.junit.jupiter.api.Assertions.*;

class AvroContentFormatterTest {

    @Test
    void feedContent() throws IOException {
        Record testRecord = new RecordImpl.BuilderImpl().withString("key", "value").build();
        OutputConfiguration configuration = new OutputConfiguration();
        RecordBuilderFactory mock = Mockito.mock();
        AvroContentFormatter sut = new AvroContentFormatter(configuration, mock);

        byte[] result = sut.feedContent(Collections.singletonList(testRecord));

        Assertions.assertNotEquals(0, result.length);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> readerStream = new DataFileStream<>(new ByteArrayInputStream(result), reader);

        Assertions.assertTrue(readerStream.hasNext());
        Assertions.assertEquals("value", String.valueOf(readerStream.next().get("key")));

    }
}
