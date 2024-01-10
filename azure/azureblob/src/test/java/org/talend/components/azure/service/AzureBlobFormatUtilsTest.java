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
package org.talend.components.azure.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.formats.Encoding;
import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.excel.ExcelFormatOptions;

class AzureBlobFormatUtilsTest {

    private AzureBlobDataset dataset;

    @BeforeEach
    void setUp() {
        dataset = new AzureBlobDataset();
    }

    @Test
    void getUsedEncodingValueCsv() {
        dataset.setFileFormat(FileFormat.CSV);
        CSVFormatOptions csvConfig = new CSVFormatOptions();
        csvConfig.setEncoding(Encoding.UTF8);
        dataset.setCsvOptions(csvConfig);
        String resultUTF8 = AzureBlobFormatUtils.getUsedEncodingValue(dataset);

        Assertions.assertEquals(Encoding.UTF8.getEncodingCharsetValue(), resultUTF8);

        csvConfig.setEncoding(Encoding.OTHER);
        csvConfig.setCustomEncoding("UTF-16");

        String resultUTF16 = AzureBlobFormatUtils.getUsedEncodingValue(dataset);

        Assertions.assertEquals("UTF-16", resultUTF16);
    }

    @Test
    void getUsedEncodingValueExcel() {
        dataset.setFileFormat(FileFormat.EXCEL);
        ExcelFormatOptions excelConfig = new ExcelFormatOptions();
        excelConfig.setEncoding(Encoding.UTF8);
        dataset.setExcelOptions(excelConfig);
        String resultUTF8 = AzureBlobFormatUtils.getUsedEncodingValue(dataset);

        Assertions.assertEquals(Encoding.UTF8.getEncodingCharsetValue(), resultUTF8);

        excelConfig.setEncoding(Encoding.OTHER);
        excelConfig.setCustomEncoding("UTF-16");

        String resultUTF16 = AzureBlobFormatUtils.getUsedEncodingValue(dataset);

        Assertions.assertEquals("UTF-16", resultUTF16);
    }

    @Test
    void getUsedEncodingValueAvro() {
        dataset.setFileFormat(FileFormat.AVRO);

        Assertions.assertThrows(IllegalStateException.class, () -> AzureBlobFormatUtils.getUsedEncodingValue(dataset));

    }
}