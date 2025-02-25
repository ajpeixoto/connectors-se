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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2IntegrationTestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Disabled("https://jira.talendforge.org/browse/TDI-50690")
@WithComponents("org.talend.components.adlsgen2")
class AvroInputIT extends AdlsGen2IntegrationTestBase {

    InputConfiguration adlsInputProperties;

    @BeforeEach
    void initDataset() {
        dataSet.setFormat(FileFormat.AVRO);

        dataSet.setBlobPath("avro/");
        adlsInputProperties = new InputConfiguration();
        adlsInputProperties.setDataSet(dataSet);
    }

    @Test
    void testInput1File1000Record() throws Exception {
        final int recordSize = 1000;
        final int columnSize = 7;
        final int expectedId = 0;
        final String expectedName = "Betty's Cafe";

        uploadTestFile("common/format/avro/business.avro", "business.avro");

        String inputConfig = configurationByExample().forInstance(adlsInputProperties).configured().toQueryString();
        Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build()
                .run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size());
        Assertions.assertEquals(expectedName, firstRecord.getString("name"));
        Assertions.assertEquals(expectedId, firstRecord.getInt("business_id"));
    }

    @Test
    void testInputMultipleFiles() throws Exception {
        final int recordSize = 1000 * 2;
        uploadTestFile("common/format/avro/business.avro", "business1.avro");
        uploadTestFile("common/format/avro/business.avro", "business2.avro");

        String inputConfig = configurationByExample().forInstance(adlsInputProperties).configured().toQueryString();
        Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build()
                .run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testInputFileWithNullValues() throws Exception {
        final int recordSize = 1;
        final int columnSize = 9;
        uploadTestFile("common/format/avro/testAvro1RecordNull.avro", "testAvro1RecordNull.avro");

        String inputConfig = configurationByExample().forInstance(adlsInputProperties).configured().toQueryString();
        Job
                .components()
                .component("azureInput", "Azure://AdlsGen2Input?" + inputConfig)
                .component("collector", "test://collector")
                .connections()
                .from("azureInput")
                .to("collector")
                .build()
                .run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size());
        Assertions.assertNull(firstRecord.getString("nullStringColumn"));
        Assertions.assertNull(firstRecord.getString("nullStringColumn2"));
        Assertions.assertNull(firstRecord.get(Integer.class, "nullIntColumn"));
        Assertions.assertNull(firstRecord.get(Long.class, "nullLongColumn"));
        Assertions.assertNull(firstRecord.get(Float.class, "nullFloatColumn"));
        Assertions.assertNull(firstRecord.get(Double.class, "nullDoubleColumn"));
        Assertions.assertNull(firstRecord.get(Boolean.class, "nullBooleanColumn"));
        Assertions.assertNull(firstRecord.get(byte[].class, "nullByteArrayColumn"));
        Assertions.assertNull(firstRecord.getDateTime("nullDateColumn"));
    }
}
