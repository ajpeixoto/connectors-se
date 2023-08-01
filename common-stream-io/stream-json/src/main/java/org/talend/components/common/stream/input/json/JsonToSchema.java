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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonToSchema {

    private final RecordBuilderFactory factory;

    private final Function<JsonNumber, Schema.Type> numberOption;

    private final boolean emptyJsonAsString;

    private final Map<String, JsonConfiguration.ForcedType> pathTypeList;

    public JsonToSchema(final RecordBuilderFactory factory,
                        final Function<JsonNumber, Schema.Type> numberOption,
                        boolean emptyJsonAsString,
                        final List<JsonConfiguration.PathType> pathTypeList) {
        this.factory = factory;
        this.numberOption = numberOption;
        this.emptyJsonAsString = emptyJsonAsString;
        this.pathTypeList = pathTypeList.stream()
                .collect(Collectors.toMap(JsonConfiguration.PathType::getPath, JsonConfiguration.PathType::getType));

    }

    /**
     * Guess schema from json object.
     *
     * @param json : json object.
     * @return guess record schema.
     */
    public Schema inferSchema(final JsonObject json) {
        final Schema.Builder builder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
        this.populateJsonObjectEntries(builder, json, "");
        return builder.build();
    }

    private void populateJsonObjectEntries(Schema.Builder builder, JsonObject value, String path) {
        log.info("populateJsonObjectEntries : " + path);
        value.entrySet()
                .stream() //
                .filter(e -> this.emptyJsonAsString || e.getValue() != JsonValue.NULL) //
                .map(s -> createEntry(s.getKey(), s.getValue(), path + "." + s.getKey())) //
                .forEach(builder::withEntry);
    }

    private Schema.Entry createEntry(final String name, final JsonValue jsonValue, String path) {
        ValueType valueType = jsonValue.getValueType();
        log.info("[createEntry#{}] ({}) {} ", path, valueType, jsonValue);

        final Schema.Entry.Builder builder = this.factory.newEntryBuilder();

        // use comment to store the real element name, for example, "$oid"
        builder.withName(name).withComment(name).withNullable(true);

        Optional<JsonConfiguration.ForcedType> optionalForcedType = Optional.ofNullable(this.pathTypeList.get(path));
        if (optionalForcedType.isPresent()) {
            log.info("[createEntry{}] force type {} instead of {}.", path, optionalForcedType.get(), valueType);
        }

        switch (valueType) {
            case ARRAY:
                final Schema subSchema = this.inferSchema(jsonValue.asJsonArray(), path, optionalForcedType);
                if (subSchema != null) {
                    builder.withElementSchema(subSchema).withType(Schema.Type.ARRAY);
                }
                break;
            case OBJECT:
                if (jsonValue.asJsonObject().entrySet().isEmpty() && this.emptyJsonAsString) {
                    builder.withType(Schema.Type.STRING);
                } else {
                    builder.withType(Schema.Type.RECORD);
                    Schema.Builder nestedSchemaBuilder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
                    populateJsonObjectEntries(nestedSchemaBuilder, jsonValue.asJsonObject(), path);
                    builder.withElementSchema(nestedSchemaBuilder.build());
                }
                break;
            case STRING:
            case NUMBER:
            case TRUE:
            case FALSE:
            case NULL:
                builder.withType(translateType(jsonValue, optionalForcedType));
                break;
            default:
                log.warn("Unexpected json type " + valueType);
        }
        Schema.Entry entry = builder.build();
        log.debug("[createEntry#{}] generated ({})", name, entry);
        return entry;
    }

    private Schema inferSchema(final JsonArray array, String path) {
        return inferSchema(array, path, Optional.empty());
    }
    private Schema inferSchema(final JsonArray array, String path, Optional<JsonConfiguration.ForcedType> optionalForcedType) {
        log.info("inferSchema : " + path);

        if (optionalForcedType.isPresent()) {
            return this.factory.newSchemaBuilder(getTCKForcedType(optionalForcedType)).build();
        }

        if (array == null || array.isEmpty()) {
            return this.factory.newSchemaBuilder(Schema.Type.LONG).build();
        }
        final JsonValue value = array.get(0);
        if (value == null) {
            return this.factory.newSchemaBuilder(Schema.Type.LONG).build();
        }
        final Schema.Builder builder;
        if (value.getValueType() == JsonValue.ValueType.OBJECT) {
            // if first element is object, supposing all elements are object (otherwise not compatible with record),
            // merge all object schemas.
            final JsonObject object = mergeAll(array);
            builder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
            this.populateJsonObjectEntries(builder, object, path);
        } else if (value.getValueType() == JsonValue.ValueType.ARRAY) {
            builder = this.factory.newSchemaBuilder(Schema.Type.ARRAY);
            final Schema subSchema = inferSchema(value.asJsonArray(), path);
            builder.withElementSchema(subSchema);
        } else {
            final Schema.Type type = this.translateType(value);
            final Schema.Type mixed = array.stream() //
                    .skip(1L) //
                    .map(this::translateType) //
                    .reduce(type, this::mixType);
            builder = this.factory.newSchemaBuilder(mixed);
        }
        return builder.build();
    }

    /**
     * create a merged object for records.
     * allow array of differents jsonObject.
     * [ { "f1": "v1"}, {"f1":"v11", "f2": "V2"} ]
     */
    private JsonObject mergeAll(JsonArray array) {
        final JsonObject emptyObject = Json.createObjectBuilder().build();
        final JsonObject mergedObject = array.stream()
                .filter(JsonObject.class::isInstance)
                .map(JsonValue::asJsonObject)
                .reduce(emptyObject, this::merge);
        return mergedObject;
    }

    private JsonObject merge(final JsonObject o1, final JsonObject o2) {
        final Map<String, JsonValue> fields = new LinkedHashMap<>();
        this.put(fields, o1);
        this.put(fields, o2);

        final JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
        fields.forEach(objectBuilder::add);
        return objectBuilder.build();
    }

    private void put(final Map<String, JsonValue> fields, final JsonObject newObject) {
        newObject.entrySet().forEach((Map.Entry<String, JsonValue> entry) -> this.putEntry(fields, entry));
    }

    private void putEntry(final Map<String, JsonValue> fields, final Map.Entry<String, JsonValue> entry) {
        if (!fields.containsKey(entry.getKey())) {
            fields.put(entry.getKey(), entry.getValue());
        } else {
            final JsonValue content = fields.get(entry.getKey());
            final JsonValue newEntry = entry.getValue();
            final JsonValue mergedValue = this.getMergedValue(content, newEntry);
            if (mergedValue != null) {
                fields.put(entry.getKey(), mergedValue);
            }
        }
    }

    private JsonValue getMergedValue(final JsonValue value1, final JsonValue value2) {
        JsonValue value = null;
        final ValueType valueType1 = value1.getValueType();
        final ValueType valueType2 = value2.getValueType();
        if (valueType1 == JsonValue.ValueType.OBJECT && valueType2 == JsonValue.ValueType.OBJECT) {
            value = this.merge(value1.asJsonObject(), value2.asJsonObject());
        } else if (valueType1 == JsonValue.ValueType.ARRAY && valueType2 == JsonValue.ValueType.ARRAY) {
            final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            value1.asJsonArray().forEach(arrayBuilder::add);
            value2.asJsonArray().forEach(arrayBuilder::add);
            value = arrayBuilder.build();
        } else if (valueType1 != valueType2) {
            if ((valueType1 == JsonValue.ValueType.TRUE && valueType2 == JsonValue.ValueType.FALSE)
                    || (valueType1 == JsonValue.ValueType.FALSE && valueType2 == JsonValue.ValueType.TRUE)) {
                value = JsonValue.TRUE;
            } else if (valueType1 == JsonValue.ValueType.STRING || valueType2 == JsonValue.ValueType.STRING) {
                value = Json.createValue("a_String");
            }
        }
        return value;
    }

    private Schema.Type translateType(JsonValue value) {
        return translateType(value, Optional.empty());
    }
    private Schema.Type translateType(JsonValue value, Optional<JsonConfiguration.ForcedType> optionalForcedType) {
        ValueType valueType = value.getValueType();

        if (optionalForcedType.isPresent()) {
            return getTCKForcedType(optionalForcedType);
        }

        switch (valueType) {
            case STRING:
                return Schema.Type.STRING;
            case NUMBER:
                return this.numberOption.apply((JsonNumber) value);
            case TRUE:
            case FALSE:
                return Schema.Type.BOOLEAN;
            case ARRAY:
                return Schema.Type.ARRAY;
            case OBJECT:
                return Schema.Type.RECORD;
            case NULL:
                return Schema.Type.STRING;
        }
        throw new RuntimeException("The data type " + valueType + " is not handled.");
    }

    private Schema.Type getTCKForcedType(Optional<JsonConfiguration.ForcedType> optionalForcedType) {
        JsonConfiguration.ForcedType forcedType = optionalForcedType.get();
        switch (forcedType) {
            case BOOLEAN:
                return Schema.Type.BOOLEAN;
            case INT:
                return Schema.Type.LONG;
            case FLOAT:
                return Schema.Type.DOUBLE;
            case ARRAY:
                return Schema.Type.ARRAY;
            case RECORD:
                return Schema.Type.RECORD;
            case STRING:
            default:
                return Schema.Type.STRING;
        }
    }

    private Schema.Type mixType(final Schema.Type t1, final Schema.Type t2) {
        if (t1 == t2) {
            return t1;
        }
        if ((t1 == Schema.Type.LONG && t2 == Schema.Type.DOUBLE)
                || (t1 == Schema.Type.DOUBLE && t2 == Schema.Type.LONG)) {
            return Schema.Type.DOUBLE;
        }
        return Schema.Type.STRING;
    }

}
