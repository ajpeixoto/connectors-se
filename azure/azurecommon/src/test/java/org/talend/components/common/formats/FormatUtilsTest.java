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
package org.talend.components.common.formats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.components.common.formats.excel.ExcelFormatOptions;

class FormatUtilsTest {

    @Test
    void testGetRecordDelimiterForCustom() {
        String customDelimiter = "!";
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(CSVRecordDelimiter.OTHER);
        formatOptions.setCustomRecordDelimiter(customDelimiter);
        String result = FormatUtils.getRecordDelimiterValue(formatOptions);

        Assertions.assertEquals(customDelimiter, result);
    }

    @Test
    void testGetRecordDelimiter() {
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(CSVRecordDelimiter.CRLF);
        String result = FormatUtils.getRecordDelimiterValue(formatOptions);

        Assertions.assertEquals(CSVRecordDelimiter.CRLF.getDelimiterValue(), result);
    }

    @Test
    void testGetFieldDelimiterValue() {
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setFieldDelimiter(CSVFieldDelimiter.COMMA);
        char result = FormatUtils.getFieldDelimiterValue(formatOptions);

        Assertions.assertEquals(CSVFieldDelimiter.COMMA.getDelimiterValue(), result);
    }

    @Test
    void testGetCustomFieldDelimiterValue() {
        char customDelimiter = '!';
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setFieldDelimiter(CSVFieldDelimiter.OTHER);
        formatOptions.setCustomFieldDelimiter(String.valueOf(customDelimiter));
        char result = FormatUtils.getFieldDelimiterValue(formatOptions);

        Assertions.assertEquals(result, customDelimiter);
    }

    @Test
    void testUsedEncodingCSV() {
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setEncoding(Encoding.UTF8);
        String result = FormatUtils.getUsedEncodingValue(formatOptions);

        Assertions.assertEquals(Encoding.UTF8.getEncodingCharsetValue(), result);
    }

    @Test
    void testUsedEncodingCSVCustom() {
        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setEncoding(Encoding.OTHER);
        formatOptions.setCustomEncoding("UTF-16");
        String result = FormatUtils.getUsedEncodingValue(formatOptions);

        Assertions.assertEquals("UTF-16", result);
    }

    @Test
    void testUsedEncodingExcel() {
        ExcelFormatOptions formatOptions = new ExcelFormatOptions();
        formatOptions.setEncoding(Encoding.UTF8);
        String result = FormatUtils.getUsedEncodingValue(formatOptions);

        Assertions.assertEquals(Encoding.UTF8.getEncodingCharsetValue(), result);
    }

    @Test
    void testUsedEncodingExcelCustom() {
        ExcelFormatOptions formatOptions = new ExcelFormatOptions();
        formatOptions.setEncoding(Encoding.OTHER);
        formatOptions.setCustomEncoding("UTF-16");
        String result = FormatUtils.getUsedEncodingValue(formatOptions);

        Assertions.assertEquals("UTF-16", result);
    }
}