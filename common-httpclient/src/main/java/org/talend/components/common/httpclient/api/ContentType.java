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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ContentType {

    final static String DEFAULT_ENCODING = System
            .getProperty("org.talend.components.rest.default_encoding",
                    StandardCharsets.UTF_8.name());

    public final static String HEADER_KEY = "Content-Type";

    public final static String CHARSET_KEY = "charset=";

    private ContentType() {
        /* Don't instantiate */
    }

    public static String getCharsetName(final Map<String, String> headers) {
        return getCharsetName(headers, DEFAULT_ENCODING);
    }

    public static String getCharsetName(final Map<String, String> headers, final String defaultCharsetName) {
        String contentType = Optional.ofNullable(headers.get(ContentType.HEADER_KEY)).orElse(defaultCharsetName);

        if (contentType == null) {
            // can happen if defaultCharsetName == null && ContentType.HEADER_KEY is not present in headers
            return null;
        }

        List<String> values = new ArrayList<>();
        int split = contentType.indexOf(';');
        int previous = 0;
        while (split > 0) {
            values.add(contentType.substring(previous, split).trim());
            previous = split + 1;
            split = contentType.indexOf(';', previous);
        }

        if (previous == 0) {
            values.add(contentType);
        } else {
            String substring = contentType.substring(previous);
            values.add(substring.trim());
        }

        String encoding = values
                .stream()
                .filter(h -> h.startsWith(ContentType.CHARSET_KEY))
                .map(h -> h.substring(ContentType.CHARSET_KEY.length()))
                .findFirst()
                .orElse(defaultCharsetName);

        return encoding;
    }

}
