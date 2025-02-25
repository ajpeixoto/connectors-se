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
package org.talend.components.jdbc.output.statement.operations;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.QueryManager;
import org.talend.components.jdbc.output.statement.RecordToSQLTypeConverter;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Data
@Slf4j
public abstract class QueryManagerImpl implements QueryManager {

    private final Platform platform;

    private final OutputConfig configuration;

    private final I18nMessage i18n;

    private final Integer maxRetry = 10;

    private Integer retryCount = 0;

    protected abstract String buildQuery(List<Record> records);

    protected abstract Map<Integer, Schema.Entry> getQueryParams();

    protected abstract boolean validateQueryParam(Record rec);

    @Override
    public List<Reject> execute(final List<Record> records, final JdbcService.JdbcDatasource dataSource)
            throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        try (final Connection connection = dataSource.getConnection()) {
            return processRecords(records, connection, buildQuery(records));
        }
    }

    private List<Reject> processRecords(final List<Record> records, final Connection connection, final String query)
            throws SQLException {
        List<Reject> rejects;
        do {
            rejects = new ArrayList<>();
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                final Map<Integer, Integer> batchOrder = new HashMap<>();
                int recordIndex = -1;
                int batchNumber = -1;
                for (final Record record : records) {
                    recordIndex++;
                    statement.clearParameters();
                    if (!validateQueryParam(record)) {
                        rejects.add(new Reject("missing required query param in this record", record));
                        continue;
                    }
                    for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                        RecordToSQLTypeConverter
                                .valueOf(entry.getValue().getType().name())
                                .setValue(statement, entry.getKey(),
                                        entry.getValue(), record);
                    }
                    statement.addBatch();
                    batchNumber++;
                    batchOrder.put(batchNumber, recordIndex);
                }

                try {
                    statement.executeBatch();
                    if (!connection.getAutoCommit()) {
                        connection.commit();
                    }
                    break;
                } catch (final SQLException e) {
                    if (!connection.getAutoCommit()) {
                        connection.rollback();
                    }
                    if (!retry(e) || retryCount > maxRetry) {
                        rejects.addAll(handleRejects(records, batchOrder, e));
                        break;
                    }
                    retryCount++;
                    log.warn("Deadlock detected. retrying for the " + retryCount + " time", e);
                    try {
                        Thread.sleep((long) Math.exp(retryCount) * 2000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } while (true);

        return rejects;
    }

    /**
     * A default retry strategy. We try to detect deadl lock by testing the sql state code.
     * 40001 is the state code used by almost all database to rise a dead lock issue
     */
    private boolean retry(final SQLException e) {
        return "40001".equals(ofNullable(e.getNextException()).orElse(e).getSQLState());
    }

    private List<Reject> handleRejects(final List<Record> records, Map<Integer, Integer> batchOrder,
            final SQLException e)
            throws SQLException {
        if (!(e instanceof BatchUpdateException)) {
            throw e;
        }
        final List<Reject> discards = new ArrayList<>();
        final int[] result = ((BatchUpdateException) e).getUpdateCounts();
        SQLException error = e;
        if (result.length == records.size()) {
            for (int i = 0; i < result.length; i++) {
                if (result[i] == Statement.EXECUTE_FAILED) {
                    error = ofNullable(error.getNextException()).orElse(error);
                    discards
                            .add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(),
                                    records.get(batchOrder.get(i))));
                }
            }
        } else {
            int failurePoint = result.length;
            error = ofNullable(error.getNextException()).orElse(error);
            discards
                    .add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(),
                            records.get(batchOrder.get(failurePoint))));
            // todo we may retry for this sub list
            discards
                    .addAll(records
                            .subList(batchOrder.get(failurePoint) + 1, records.size())
                            .stream()
                            .map(r -> new Reject("rejected due to error in previous elements error in this transaction",
                                    r))
                            .collect(toList()));
        }

        return discards;
    }

    public String namespace(final Connection connection) throws SQLException {
        String schenma = JdbcService.getSchema(connection);
        return (connection.getCatalog() != null && !connection.getCatalog().isEmpty()
                ? getPlatform().identifier(connection.getCatalog()) + "."
                : "") + (schenma != null && !schenma.isEmpty() ? getPlatform().identifier(connection.getSchema()) : "");
    }

    public static Optional<Object> valueOf(final Record rec, final Schema.Entry entry) {
        switch (entry.getType()) {
        case INT:
            return rec.getOptionalInt(entry.getName()).isPresent()
                    ? of(rec.getOptionalInt(entry.getName()).getAsInt())
                    : empty();
        case LONG:
            return rec.getOptionalLong(entry.getName()).isPresent()
                    ? of(rec.getOptionalLong(entry.getName()).getAsLong())
                    : empty();
        case FLOAT:
            return rec.getOptionalFloat(entry.getName()).isPresent()
                    ? of(rec.getOptionalFloat(entry.getName()).getAsDouble())
                    : empty();
        case DOUBLE:
            return rec.getOptionalDouble(entry.getName()).isPresent()
                    ? of(rec.getOptionalDouble(entry.getName()).getAsDouble())
                    : empty();
        case BOOLEAN:
            return rec.getOptionalBoolean(entry.getName()).isPresent()
                    ? of(rec.getOptionalBoolean(entry.getName()).get())
                    : empty();
        case BYTES:
            return rec.getOptionalBytes(entry.getName()).isPresent()
                    ? of(rec.getOptionalBytes(entry.getName()).get())
                    : empty();
        case DATETIME:
            return rec.getOptionalDateTime(entry.getName()).isPresent()
                    ? of(rec.getOptionalDateTime(entry.getName()).get())
                    : empty();
        case STRING:
            return rec.getOptionalString(entry.getName()).isPresent()
                    ? of(rec.getOptionalString(entry.getName()).get())
                    : empty();
        case RECORD:
            return rec.getOptionalRecord(entry.getName()).isPresent()
                    ? of(rec.getOptionalRecord(entry.getName()).get())
                    : empty();
        case ARRAY:
        default:
            return empty();
        }
    }

}
