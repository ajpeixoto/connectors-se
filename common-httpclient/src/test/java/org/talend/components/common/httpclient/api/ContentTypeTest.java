/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
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
package org.talend.components.common.httpclient.api;

import java.util.HashMap;
import java.util.Map;

import org.apache.cxf.helpers.HttpHeaderHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ContentTypeTest {

    @ParameterizedTest
    @CsvSource(value = { "application/json; ,",
            "application/json; another value; ,",
            "application/json; another value;,",
            "application/json; another valueA; ,; another valueB,",
            "application/json; another valueA;,;another valueB,",
            "application/json; another=valueA;,;another=valueB,",
            "application/json; another=valueA;  ,   ;another=valueB,",
            "application/json; another valueA; ,; another valueB; another valueB",
            "application/json; another valueA; ,; another valueB; another valueB;",
            ",; another valueB; another valueB",
            "," }, ignoreLeadingAndTrailingWhitespace = false)
    public void getCharsetName(String prefix, String suffix) {
        prefix = prefix == null ? "" : prefix;
        suffix = suffix == null ? "" : suffix;
        Map<String, String> headers = new HashMap<>();
        headers.put("headerA", "valueA1; ValueA2");
        String iso8859 = "iso-8859-1";
        String contentType = String.format("%scharset=%s%s", prefix, iso8859, suffix);
        headers.put(HttpHeaderHelper.CONTENT_TYPE, contentType);
        headers.put("headerB", "valueB1; ValueB2");

        String charsetName = ContentType.getCharsetName(headers);
        Assertions.assertEquals(iso8859, charsetName);
    }

}