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
package org.talend.components.mongo.output;

import org.bson.Document;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.components.mongo.KeyMapping;
import org.talend.components.mongo.Mode;
import org.talend.components.mongo.service.RecordToDocument;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import javax.json.JsonObject;
import java.io.Serializable;
import java.util.List;

public class MongoCommonOutput implements Serializable {

    protected MongoCommonOutputConfiguration configuration;

    protected transient RecordToJson recordToJson;

    protected transient RecordToDocument recordToDocument;

    protected Document getKeysQueryDocumentAndRemoveKeysFromSourceDocument(List<KeyMapping> keyMappings, Record rec,
            Document document) {
        Document keysQueryDocument = new Document();
        if (keyMappings == null || keyMappings.isEmpty()) {
            throw new RuntimeException("need at least one key for set update/upsert action.");
        }
        for (KeyMapping keyMapping : configuration.getKeyMappings()) {
            String column = keyMapping.getColumn();
            String keyPath = keyMapping.getOriginElementPath();

            // TODO format it for right value format for lookup, now only follow the logic in studio,
            // so may not work for ObjectId, ISODate, NumberDecimal and so on, but they are not common as key
            // and "_id" can set in mongodb, not necessary as ObjectId type
            // in fact, record can be a tree like a json, not flat, but here we only suppose it's flat, not go deep
            if (configuration.getDataset().getMode() == Mode.TEXT) {
                // when TEXT mode, record is expected only have one column which contains the whole json content as text
                // so need to get it from document, not record
                Object value = getKeyValueFromDocumentAndRemoveKeys(document, column);
                keysQueryDocument.put(isEmpty(keyPath) ? column : keyPath, value);
            } else {
                Object value = getKeyValueFromRecord(rec, column);
                getKeyValueFromDocumentAndRemoveKeys(document, column);
                keysQueryDocument.put(isEmpty(keyPath) ? column : keyPath, value);
            }
        }
        return keysQueryDocument;
    }

    // only support path like a.b.c, not support array
    protected Object getKeyValueFromDocumentAndRemoveKeys(Document document, String keyColumnPath) {
        if (isEmpty(keyColumnPath)) {
            throw new RuntimeException("Please set the key column for update or upsert.");
        }

        String[] paths = keyColumnPath.split("\\.");
        Object result = null;
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];

            if (isEmpty(path)) {
                throw new RuntimeException("Please set the right key column for update or upsert.");
            }

            if (document != null) {
                Object v = document.get(path);
                if (v instanceof Document) {
                    document = (Document) v;
                } else if (i == (paths.length - 1)) {
                    result = v;
                    // need to remove origin key-value for update and upsert
                    // https://jira.talendforge.org/browse/TDI-44003
                    document.remove(path);
                }
            } else {
                break;
            }
        }

        return result;
    }

    // only support path like a.b.c, not support array
    protected Object getKeyValueFromRecord(Record rec, String keyColumnPath) {
        if (isEmpty(keyColumnPath)) {
            throw new RuntimeException("Please set the key column for update or upsert.");
        }

        String[] paths = keyColumnPath.split("\\.");
        Object result = null;
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];

            if (isEmpty(path)) {
                throw new RuntimeException("Please set the right key column for update or upsert.");
            }

            if (rec != null) {
                Object v = rec.get(Object.class, path);
                if (v instanceof Record) {
                    rec = (Record) v;
                } else if (i == (paths.length - 1)) {
                    result = v;
                }
            } else {
                break;
            }
        }

        return result;
    }

    protected boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    protected Document convertRecord2Document(Record rec) {
        JsonObject jsonObject = this.recordToJson.fromRecord(rec);
        String jsonContent = jsonObject.toString();
        return Document.parse(jsonContent);
    }

    protected Document convertRecord2DocumentDirectly(Record rec) {
        return recordToDocument.fromRecord(rec);
    }

    // copy from couchbase, not use now, will use it maybe
    protected Object jsonValueFromRecordValue(Schema.Entry entry, Record rec) {
        String entryName = entry.getName();
        Object value = rec.get(Object.class, entryName);
        if (null == value) {
            // TODO check use what explain null
            return "";
        }
        switch (entry.getType()) {
        case INT:
            return rec.getInt(entryName);
        case LONG:
            return rec.getLong(entryName);
        case BYTES:
            return java.util.Base64.getEncoder().encodeToString(rec.getBytes(entryName));
        case FLOAT:
            return Double.parseDouble(String.valueOf(rec.getFloat(entryName)));
        case DOUBLE:
            return rec.getDouble(entryName);
        case STRING:
        case DECIMAL:
            return createJsonFromString(rec.getString(entryName));
        case BOOLEAN:
            return rec.getBoolean(entryName);
        case ARRAY:
            return rec.getArray(List.class, entryName);
        case DATETIME:
            return rec.getDateTime(entryName).toString();
        case RECORD:
            return rec.getRecord(entryName);
        default:
            throw new IllegalArgumentException("Unknown Type " + entry.getType());
        }
    }

    protected Object createJsonFromString(String str) {
        Object value = null;
        try {
            value = Document.parse(str);
        } catch (Exception e) {
            // can't create JSON object from String ignore exception
            // and try to create JSON array
        } finally {
            if (value != null)
                return value;
        }
        // TODO consider array case
        /*
         * try {
         * value = Document.fromArray(str);
         * } catch (Exception e) {
         * value = str;
         * }
         */
        return value;
    }
}
