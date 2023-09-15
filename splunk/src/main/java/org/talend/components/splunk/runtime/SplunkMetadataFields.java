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
package org.talend.components.splunk.runtime;

import java.time.ZonedDateTime;
import java.util.Arrays;

public enum SplunkMetadataFields {

    TIME("time", ZonedDateTime.class),
    SOURCE("source", String.class),
    SOURCE_TYPE("sourcetype", String.class),
    HOST("host", String.class),
    INDEX("index", String.class);

    private final String name;

    private final Class<?> dataType;

    public String getName() {
        return name;
    }

    public Class<?> getDataType() {
        return dataType;
    }

    SplunkMetadataFields(String name, Class<?> dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public static boolean isMetadataField(String fieldName) {
        return Arrays.stream(values()).anyMatch(value -> value.getName().equals(fieldName));
    }
}
