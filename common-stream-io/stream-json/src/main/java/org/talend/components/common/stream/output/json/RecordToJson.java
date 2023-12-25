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
package org.talend.components.common.stream.output.json;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.util.Collection;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.extern.slf4j.Slf4j;

/**
 * Transform record to json object.
 * here, currently no need of json schema.
 * if needed, could use https://github.com/leadpony/justify
 * (java lib for json schema, can be used with johnzon lib.
 */
@Slf4j
public class RecordToJson implements RecordConverter<JsonObject, Void> {

    private boolean useOriginColumnName = false;

    @Override
    public JsonObject fromRecord(Record rec) {

        if (rec == null) {
            return null;
        }
        return convertRecordToJsonObject(rec);
    }

    public void setUseOriginColumnName(boolean useOriginColumnName) {
        this.useOriginColumnName = useOriginColumnName;
    }

    public boolean isUseOriginColumnName() {
        return useOriginColumnName;
    }

    @Override
    public Void fromRecordSchema(Schema rec) {
        return null;
    }

    private JsonObject convertRecordToJsonObject(Record rec) {
        final JsonObjectBuilder json = Json.createObjectBuilder();

        for (Entry entry : rec.getSchema().getEntries()) {
            final String fieldName = entry.getName();
            Object val = rec.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                json.addNull(useOriginColumnName ? entry.getOriginalFieldName() : fieldName);
            } else {
                this.addField(json, rec, entry);
            }
        }
        return json.build();
    }

    private JsonArray toJsonArray(Collection<Object> objects) {
        JsonArrayBuilder array = Json.createArrayBuilder();
        for (Object obj : objects) {
            if (obj instanceof Collection) {
                JsonArray subArray = toJsonArray((Collection) obj);
                array.add(subArray);
            } else if (obj instanceof String) {
                array.add((String) obj);
            } else if (obj instanceof Record) {
                JsonObject subObject = convertRecordToJsonObject((Record) obj);
                array.add(subObject);
            } else if (obj instanceof Integer) {
                array.add((Integer) obj);
            } else if (obj instanceof Long) {
                array.add((Long) obj);
            } else if (obj instanceof Double) {
                array.add((Double) obj);
            } else if (obj instanceof Boolean) {
                array.add((Boolean) obj);
            }
        }
        return array.build();
    }

    private void addField(final JsonObjectBuilder json, final Record rec, final Entry entry) {
        final String fieldName = entry.getName();
        final String outputFieldName = useOriginColumnName ? entry.getOriginalFieldName() : fieldName;

        switch (entry.getType()) {
        case RECORD:
            final Record subRecord = rec.getRecord(fieldName);
            json.add(outputFieldName, convertRecordToJsonObject(subRecord));
            break;
        case ARRAY:
            final Collection<Object> array = rec.getArray(Object.class, fieldName);
            final JsonArray jarray = toJsonArray(array);
            json.add(outputFieldName, jarray);
            break;
        case STRING:
            json.add(outputFieldName, rec.getString(fieldName));
            break;
        case BYTES:
            json.add(outputFieldName, new String(rec.getBytes(fieldName), Charset.defaultCharset()));
            break;
        case INT:
            json.add(outputFieldName, rec.getInt(fieldName));
            break;
        case LONG:
            json.add(outputFieldName, rec.getLong(fieldName));
            break;
        case DECIMAL:
            // worry json ser/desr lose precision for decimal, also here keep like before for safe
            BigDecimal decimal = rec.getDecimal(fieldName);
            if (decimal != null) {
                json.add(outputFieldName, decimal.toString());
            } else {
                json.addNull(outputFieldName);
            }
            break;
        case FLOAT:
            json.add(outputFieldName, rec.getFloat(fieldName));
            break;
        case DOUBLE:
            json.add(outputFieldName, rec.getDouble(fieldName));
            break;
        case BOOLEAN:
            json.add(outputFieldName, rec.getBoolean(fieldName));
            break;
        case DATETIME:
            final ZonedDateTime dateTime = rec.getDateTime(fieldName);
            if (dateTime != null) {
                json.add(outputFieldName, dateTime.toString());
            } else {
                json.addNull(outputFieldName);
            }
            break;
        default:
            log.warn("Unexpected TCK type for entry " + entry.getType());
        }
    }

}
