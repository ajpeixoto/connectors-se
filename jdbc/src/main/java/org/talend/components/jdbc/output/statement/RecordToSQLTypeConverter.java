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
package org.talend.components.jdbc.output.statement;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Optional;

public enum RecordToSQLTypeConverter {

    RECORD {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            statement.setObject(index, rec.get(Record.class, entry.getName()).toString());
        }

    },
    ARRAY {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            statement
                    .setArray(index, statement
                            .getConnection()
                            .createArrayOf(entry.getName(),
                                    rec
                                            .getOptionalArray(Object.class, entry.getName())
                                            .orElseGet(ArrayList::new)
                                            .toArray()));
        }

    },
    STRING {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            Optional<String> value = rec.getOptionalString(entry.getName());
            if (value.isPresent()) {
                statement.setString(index, value.get());
            } else {
                statement.setNull(index, Types.VARCHAR);
            }
        }

    },
    BYTES {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            statement.setBytes(index, rec.getOptionalBytes(entry.getName()).orElse(null));
        }

    },
    INT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            if (rec.getOptionalInt(entry.getName()).isPresent()) {
                statement.setInt(index, rec.getInt(entry.getName()));
            } else {
                statement.setNull(index, Types.INTEGER);
            }
        }

    },
    LONG {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            if (rec.getOptionalLong(entry.getName()).isPresent()) {
                statement.setLong(index, rec.getLong(entry.getName()));
            } else {
                statement.setNull(index, Types.BIGINT);
            }
        }

    },
    FLOAT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            if (rec.getOptionalFloat(entry.getName()).isPresent()) {
                statement.setFloat(index, rec.getFloat(entry.getName()));
            } else {
                statement.setNull(index, Types.FLOAT);
            }
        }

    },
    DOUBLE {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            if (rec.getOptionalDouble(entry.getName()).isPresent()) {
                statement.setDouble(index, rec.getDouble(entry.getName()));
            } else {
                statement.setNull(index, Types.DOUBLE);
            }
        }

    },
    BOOLEAN {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            if (rec.getOptionalBoolean(entry.getName()).isPresent()) {
                statement.setBoolean(index, rec.getBoolean(entry.getName()));
            } else {
                statement.setNull(index, Types.BOOLEAN);
            }
        }

    },
    DATETIME {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                final Record rec)
                throws SQLException {
            statement
                    .setTimestamp(index, rec
                            .getOptionalDateTime(entry.getName())
                            .map(d -> new Timestamp(d.toInstant().toEpochMilli()))
                            .orElse(null));
        }

    };

    public abstract void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
            final Record rec) throws SQLException;

}
