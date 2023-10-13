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
package org.talend.components.common.stream.input.json;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.format.json.JsonPointerParser;
import org.talend.components.jsondecorator.api.JsonDecoratorBuilder;
import org.talend.components.jsondecorator.api.JsonDecoratorFactory;
import org.talend.components.jsondecorator.impl.JsonDecoratorFactoryImpl;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonValue;

public class JsonReaderSupplier implements RecordReaderSupplier {

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config, Object extraParameter) {
        if (!(config instanceof JsonConfiguration)) {
            throw new IllegalArgumentException("try to get json-reader with other than json-config");
        }

        final JsonConfiguration jsonConfig = (JsonConfiguration) config;
        final JsonPointerParser parser = JsonPointerParser.of(jsonConfig.getJsonPointer());
        final JsonToRecord toRecord = new JsonToRecord(factory, jsonConfig.isForceDouble());

        JsonDecoratorBuilder jsonDecoratorBuilder = null;
        if (jsonConfig.getFilterCastList() != null && !jsonConfig.getFilterCastList().isEmpty()) {
            final JsonDecoratorBuilder jsonDecoratorBuilderLocal =
                    JsonDecoratorFactoryImpl.getInstance().createBuilder();
            jsonConfig.getFilterCastList().stream().forEach(e -> {
                if (e.getAction() == JsonConfiguration.FilterCastAction.CAST) {
                    jsonDecoratorBuilderLocal.cast(e.getPath(), getType(e.getType()));
                } else if (e.getAction() == JsonConfiguration.FilterCastAction.FILTER) {
                    jsonDecoratorBuilderLocal.filterByType(e.getPath(), getType(e.getType()));
                }
            });

            jsonDecoratorBuilder = jsonDecoratorBuilderLocal;
        }

        return new JsonRecordReader(parser, toRecord, jsonDecoratorBuilder);
    }

    private static JsonDecoratorBuilder.ValueTypeExtended getType(JsonConfiguration.FilterCastType type) {
        switch (type) {
        case ARRAY:
            return JsonDecoratorBuilder.ValueTypeExtended.ARRAY;
        case BOOLEAN:
            return JsonDecoratorBuilder.ValueTypeExtended.BOOLEAN;
        case FLOAT:
            return JsonDecoratorBuilder.ValueTypeExtended.FLOAT;
        case INT:
            return JsonDecoratorBuilder.ValueTypeExtended.INT;
        case OBJECT:
            return JsonDecoratorBuilder.ValueTypeExtended.OBJECT;
        case STRING:
            return JsonDecoratorBuilder.ValueTypeExtended.STRING;
        }
        throw new IllegalArgumentException(String.format("Not supported type '%s'", type));
    }
}
