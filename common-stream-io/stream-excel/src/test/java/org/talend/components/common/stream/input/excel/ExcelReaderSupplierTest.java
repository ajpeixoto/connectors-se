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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.FooterLine;
import org.talend.components.common.stream.format.HeaderLine;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.components.common.stream.format.excel.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ExcelReaderSupplierTest {

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    private final ExcelConfiguration config = new ExcelConfiguration();

    @BeforeEach
    void initDataSet() {
        config.setSheetName("Sheet1");
        config.setExcelFormat(ExcelFormat.EXCEL2007);
        // config.setEncoding(new Encoding());
        config.setHeader(new HeaderLine());
        config.getHeader().setActive(false);

        config.setFooter(new FooterLine());
        config.getFooter().setActive(false);
    }

    @Test
    void test1File1RecordsWithoutHeader() throws IOException {
        config.getHeader().setActive(false);

        this.testOneValueFile("excel2007/excel_2007_1_record_no_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_no_header.xls");
    }

    @Test
    void testSimple() throws IOException {
        final String path = "excel2007/excel2007_File.xlsx";
        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                final RecordReader reader = new ExcelReaderSupplier().getReader(factory, config)) {

            config.getHeader().setActive(true);
            config.getHeader().setSize(1);
            config.setSheetName("Another Sheet");
            final Iterator<Record> records = reader.read(stream);
            Assertions.assertNotNull(records);

            final List<Record> listOfRecords = StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(records, Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
            Assertions.assertEquals(100, listOfRecords.size());
            for (final Record rec : listOfRecords) {
                Assertions.assertNotNull(rec.getString("newColumn"));
                Assertions.assertNotNull(rec.getString("newColumn1"));
                Assertions.assertNotNull(rec.getString("newColumn2"));
                Assertions.assertTrue(rec.getString("newColumn2").isEmpty());
                Assertions.assertNotNull(rec.getString("newColumn3"));
            }
        }
    }

    private void testOneValueFile(String path) throws IOException {

        double idValue = 1.0;
        final String nameValue = "a";
        final double longValue = 10000000000000.0;
        final double doubleValue = 2.5;
        final long dateValue = 1549317600; // epoch seconds, original excel value: 43501.0
        final boolean booleanValue = true;

        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                final RecordReader reader = new ExcelReaderSupplier().getReader(factory, config)) {

            final Iterator<Record> records = reader.read(stream);
            for (int i = 0; i < 4; i++) {
                Assertions.assertTrue(records.hasNext(), "no record or not re-entrant"); // re-entrant.
            }
            final Record firstRecord = records.next();

            if (config.getHeader().isActive()) {

                Assertions.assertEquals(idValue, firstRecord.getDouble("id"), 0.01);
                Assertions.assertEquals(nameValue, firstRecord.getString("name"));
                Assertions.assertEquals(longValue, firstRecord.getDouble("longValue"), 0.01);
                Assertions.assertEquals(doubleValue, firstRecord.getDouble("doubleValue"), 0.01);
                // delta == 1 day, date is read in local time zone
                Assertions.assertEquals(dateValue, firstRecord.getDateTime("dateValue").toEpochSecond(), 86400);
                Assertions.assertEquals(booleanValue, firstRecord.getBoolean("booleanValue"));
            } else {
                Assertions.assertEquals(idValue, firstRecord.getDouble("field0"), 0.01);
                Assertions.assertEquals(nameValue, firstRecord.getString("field1"));
                Assertions.assertEquals(longValue, firstRecord.getDouble("field2"), 0.01);
                Assertions.assertEquals(doubleValue, firstRecord.getDouble("field3"), 0.01);
                // delta == 1 day
                Assertions.assertEquals(dateValue, firstRecord.getDateTime("field4").toEpochSecond(), 86400);
                Assertions.assertEquals(booleanValue, firstRecord.getBoolean("field5"));
            }
            Assertions.assertFalse(records.hasNext(), "more than one record");
        }
    }

    private void testRecordsSize(String path, int nbeRecord) throws IOException {
        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                final RecordReader reader = new ExcelReaderSupplier().getReader(factory, config)) {

            final Iterator<Record> records = reader.read(stream);
            for (int i = 0; i < 4; i++) {
                Assertions.assertTrue(records.hasNext(), "no record or not re-entrant"); // re-entrant.
            }
            for (int i = 0; i < nbeRecord; i++) {
                this.ensureNext(records, i);
            }
            Assertions.assertFalse(records.hasNext(), "more than " + nbeRecord + " records");
        }
    }

    @Test
    void test1File5RecordsWithoutHeader() throws IOException {
        config.getHeader().setActive(false);

        this.testRecordsSize("excel2007/excel_2007_5_records_no_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_no_header.xls", 5);
    }

    private void ensureNext(Iterator<Record> records, int number) {
        Assertions.assertTrue(records.hasNext(), "no more record (" + (number + 1) + ")");
        final Record record = records.next();
        Assertions.assertNotNull(record);
    }

    @Test
    void testInput1FileWithHeader1Row() throws IOException {
        config.getHeader().setActive(true);
        config.getHeader().setSize(1);
        this.testOneValueFile("excel2007/excel_2007_1_record_with_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_with_header.xls");
    }

    @Test
    void testInput1FileMultipleRows() throws IOException {
        config.getHeader().setActive(true);
        config.getHeader().setSize(1);

        this.testRecordsSize("excel2007/excel_2007_5_records_with_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_with_header.xls", 5);
    }

    @Test
    void test1File1RecordWithBigHeader() throws IOException {
        config.getHeader().setActive(true);
        config.getHeader().setSize(2);

        this.testOneValueFile("excel2007/excel_2007_1_record_with_big_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_with_big_header.xls");
    }

    @Test
    void test1File5RecordsWithBigHeader() throws IOException {
        config.getHeader().setActive(true);
        config.getHeader().setSize(2);

        this.testRecordsSize("excel2007/excel_2007_5_records_with_big_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_with_big_header.xls", 5);

    }

    @Test
    void test1FileWithFooter() throws IOException {

        config.getFooter().setActive(true);
        config.getFooter().setSize(1);

        this.testOneValueFile("excel2007/excel_2007_1_record_footer.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_footer.xls");
    }

    /*
     * @Test
     * void testHTMLFile() throws IOException {
     * config.getHeader().setActive(true);
     * config.getHeader().setSize(1);
     * config.getFooter().setActive(true);
     * config.getFooter().setSize(5);
     * config.setExcelFormat(ExcelFormat.HTML);
     * 
     * this.testRecordsSize("html/HTML-utf8.xls", 15);
     * 
     * }
     */

}