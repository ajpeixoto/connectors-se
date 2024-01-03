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
package org.talend.components.mongo.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Transform record to document object.
 */
@Slf4j
public class RecordToDocument {

    public Document fromRecord(Record record) {
        return convertRecordToDocument(record);
    }

    private Document convertRecordToDocument(Record record) {
        final Document document = new Document();

        for (Entry entry : record.getSchema().getEntries()) {
            final String fieldName = entry.getName();
            Object val = record.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                document.put(fieldName, null);
            } else {
                this.addField(document, record, entry);
            }
        }
        return document;
    }

    private List toArray(Collection<Object> objects, DocumentToRecord.DataType originDatatype) {
        List array = new ArrayList();
        for (Object obj : objects) {
            if (obj instanceof Collection) {
                List subArray = toArray((Collection) obj, originDatatype);
                array.add(subArray);
            } else if (obj instanceof String) {
                array.add(convertToMongoDataTypeIfNecessary((String) obj, originDatatype));
            } else if (obj instanceof Record) {
                Document subObject = convertRecordToDocument((Record) obj);
                array.add(subObject);
            } else if (obj instanceof Integer) {
                array.add((Integer) obj);
            } else if (obj instanceof Long) {
                array.add((Long) obj);
            } else if (obj instanceof Double) {
                array.add((Double) obj);
            } else if (obj instanceof Boolean) {
                array.add((Boolean) obj);
            } else if (obj instanceof ZonedDateTime) {
                array.add(Date.from(ZonedDateTime.class.cast(obj).toInstant()));
            } else {
                array.add(obj);
            }
        }
        return array;
    }

    private void addField(Document document, Record rec, Entry entry) {
        final String fieldName = entry.getName();
        switch (entry.getType()) {
        case RECORD:
            final Record subRecord = rec.getRecord(fieldName);
            document.put(fieldName, convertRecordToDocument(subRecord));
            break;
        case ARRAY:
            final Collection<Object> list = rec.getArray(Object.class, fieldName);
            final List array = toArray(list, getOriginDataType(entry));
            document.put(fieldName, array);
            break;
        case STRING:
        case DECIMAL:
            DocumentToRecord.DataType originDataType = getOriginDataType(entry);
            document.put(fieldName, convertToMongoDataTypeIfNecessary(rec.getString(fieldName), originDataType));
            break;
        case BYTES:
            // TODO check it
            document.put(fieldName, new String(rec.getBytes(fieldName)));
            break;
        case INT:
            document.put(fieldName, rec.getInt(fieldName));
            break;
        case LONG:
            document.put(fieldName, rec.getLong(fieldName));
            break;
        case FLOAT:
            document.put(fieldName, rec.getFloat(fieldName));
            break;
        case DOUBLE:
            document.put(fieldName, rec.getDouble(fieldName));
            break;
        case BOOLEAN:
            document.put(fieldName, rec.getBoolean(fieldName));
            break;
        case DATETIME:
            document.put(fieldName, rec.get(Date.class, fieldName));
            break;
        }
    }

    private DocumentToRecord.DataType getOriginDataType(Entry entry) {
        // now use comment to store origin name and origin type information, not good, TODO move to framework
        final String comment = entry.getComment();
        if (comment != null && comment.contains(DocumentToRecord.TYPE_SPLIT_CHARS)) {
            String originDataType = comment.substring(comment.lastIndexOf(DocumentToRecord.TYPE_SPLIT_CHARS) + 2);
            if (DocumentToRecord.DataType.OBJECTID.origin_type.equals(originDataType)) {
                return DocumentToRecord.DataType.OBJECTID;
            } else if (DocumentToRecord.DataType.CODE.origin_type.equals(originDataType)) {
                return DocumentToRecord.DataType.CODE;
            } else if (DocumentToRecord.DataType.DECIMAL128.origin_type.equals(originDataType)) {
                return DocumentToRecord.DataType.DECIMAL128;
            }
        }

        return null;
    }

    private Object convertToMongoDataTypeIfNecessary(String content, DocumentToRecord.DataType originDatatype) {
        if (content == null || originDatatype == null) {
            return content;
        }

        if (DocumentToRecord.DataType.OBJECTID == originDatatype) {
            return new ObjectId(content);
        }

        if (DocumentToRecord.DataType.CODE == originDatatype) {
            return new Code(content);
        }

        if (DocumentToRecord.DataType.DECIMAL128 == originDatatype) {
            return Decimal128.parse(content);
        }

        return content;
    }

}